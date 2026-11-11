use crate::domain::message_formatting::{
    parse_code_span_end, push_plain_text_fragment, push_text_fragment, strip_quote_prefix,
};
mod attachment_hydration;
mod channel_membership;
mod outbox;
mod user_search;
use super::history_progress::{EnvelopeDisposition, PageProgress};
use crate::{
    domain::{
        affinity::Affinity,
        attachment::{AttachmentKind, AttachmentPreview, AttachmentSource, AttachmentSummary},
        backend::{
            AccountId, BackendCapabilities, BackendId, ProviderConversationRef, ProviderMessageRef,
            ProviderWorkspaceRef,
        },
        channel_details::{ConversationNotificationSettings, NotificationLevel},
        conversation::{
            ConversationBadgeCounts, ConversationGroup, ConversationKind, ConversationSummary,
            ConversationTeamType,
        },
        desktop_notification::DesktopNotification,
        ids::{ChannelId, ConversationId, DmId, MessageId, UserId, WorkspaceId},
        message::{
            BroadcastKind, ChatEvent, EditMeta, EmojiSourceRef, LinkPreview, MessageFragment,
            MessageReaction, MessageRecord, MessageSendState,
        },
        message_preview::{MessagePreview, PreviewBody},
        pins::{PinnedItem, PinnedPreview, PinnedState, PinnedTarget},
        presence::{Availability, Presence},
        profile::{
            IdentityProof, ProfileSection, ProofState, SocialGraph, SocialGraphEntry,
            SocialGraphListType, TeamShowcaseEntry, UserProfile,
        },
        route::Route,
        search::SearchResult,
        user::UserSummary,
    },
    services::{
        backends::traits::{BackendError, ChatBackend, RoutedBackendCommand},
        local_store::{
            CachedBootstrapSeed, CachedConversationEmoji, CachedMessageReaction, CachedTeamRoleMap,
            LocalStore,
        },
        search_index::SearchIndex,
        sync_service::SyncService,
    },
    state::{
        bindings::{ConversationBinding, MessageBinding, WorkspaceBinding},
        effect::BackendCommand,
        event::{
            BackendEvent, BootstrapPayload, ConversationEmojiEntry, MessageReactionEntry,
            MessageReactionsForMessage, PresencePatch, TeamRoleEntry, TeamRoleKind,
        },
        ids::{ClientMessageId, OpId},
    },
    util::interactive_qos::crawl_throttle_delay,
};
use attachment_hydration::{
    hydrate_attachment_paths_for_messages, message_needs_file_attachment_hydration,
    message_needs_image_attachment_hydration, message_needs_video_attachment_hydration,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use outbox::PendingOutboxSends;
use reqwest::{Url, header::CONTENT_TYPE};
use rmpv::Value;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, Sender},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Builder;
use tracing::{info, warn};

use super::{
    notify_inventory::KeybaseNotifyEvent,
    paths::{ensure_service_running, socket_path},
    rpc::{
        client::{KeybaseRpcClient, NotificationChannels, RpcNotification},
        transport::FramedMsgpackTransport,
    },
    task_runtime::{self, TaskPriority},
};

use crate::services::local_store::paths as store_paths;
use crate::util::video_decoder::extract_video_thumbnail_to_file;

fn benchmark_read_only() -> bool {
    std::env::var("ZBASE_BENCH_READ_ONLY").is_ok_and(|value| value == "1")
        && (std::env::var("ZBASE_BENCH_AUTOSTART").is_ok()
            || std::env::var("ZBASE_BENCH_SCRIPT").is_ok())
}

pub struct KeybaseBackend {
    backend_id: BackendId,
    local_store: Arc<LocalStore>,
    search_index: Arc<SearchIndex>,
    pending_outbox_sends: Arc<Mutex<PendingOutboxSends>>,
    inbound_events: Option<Receiver<BackendEvent>>,
    inbound_sender: Option<Sender<BackendEvent>>,
    listener_handle: Option<JoinHandle<()>>,
    listener_shutdown: Arc<AtomicBool>,
    user_search_generation: Arc<AtomicU64>,
    account_id: AccountId,
    sync: Arc<SyncService>,
}

#[derive(Clone, Debug)]
struct PendingSendMeta {
    op_id: OpId,
    client_message_id: ClientMessageId,
    local_attachment_path: Option<String>,
}

struct BackendTaskOutcome {
    sender: Sender<BackendEvent>,
    command: Option<BackendCommand>,
}

impl BackendTaskOutcome {
    fn new(sender: &Sender<BackendEvent>, command: BackendCommand) -> Self {
        Self {
            sender: sender.clone(),
            command: Some(command),
        }
    }
    fn complete(&mut self) {
        self.command = None;
    }
}

impl Drop for BackendTaskOutcome {
    fn drop(&mut self) {
        if let Some(command) = self.command.take() {
            let _ = self.sender.send(BackendEvent::CommandFailed {
                command: Box::new(command),
                error: "Could not complete the request. Please try again.".into(),
            });
        }
    }
}

impl KeybaseBackend {
    pub fn new(local_store: Arc<LocalStore>) -> Self {
        let search_index =
            Arc::new(SearchIndex::open().unwrap_or_else(|error| {
                panic!("failed to initialize local Tantivy index: {error}")
            }));
        search_index.sync_from_store(Arc::clone(&local_store), WORKSPACE_ID.into());
        Self {
            backend_id: BackendId::new(KEYBASE_BACKEND_ID),
            local_store,
            search_index,
            pending_outbox_sends: Arc::new(Mutex::new(PendingOutboxSends::default())),
            inbound_events: None,
            inbound_sender: None,
            listener_handle: None,
            listener_shutdown: Arc::new(AtomicBool::new(false)),
            user_search_generation: Arc::new(AtomicU64::new(0)),
            account_id: AccountId::new(DEFAULT_ACCOUNT_ID),
            sync: Arc::new(SyncService::default()),
        }
    }

    fn ensure_listener_started(&mut self) {
        if self
            .listener_handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            return;
        }

        let (sender, receiver) = mpsc::channel();
        self.inbound_events = Some(receiver);
        self.inbound_sender = Some(sender.clone());
        if benchmark_read_only() {
            let pulse_sender = sender.clone();
            let cancelled = Arc::clone(&self.listener_shutdown);
            thread::spawn(move || {
                while !cancelled.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(73));
                    if pulse_sender
                        .send(BackendEvent::BenchmarkPulse(Instant::now()))
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
        let local_store = Arc::clone(&self.local_store);
        let pending_outbox_sends = Arc::clone(&self.pending_outbox_sends);
        let shutdown = Arc::clone(&self.listener_shutdown);
        let account_id = self.account_id.clone();
        self.listener_handle = Some(thread::spawn(move || {
            run_listener(
                sender,
                local_store,
                pending_outbox_sends,
                account_id,
                shutdown,
            )
        }));
    }

    fn start_message_mutation(
        &self,
        op_id: OpId,
        conversation: ProviderConversationRef,
        message: ProviderMessageRef,
        text: Option<String>,
    ) -> Result<Vec<BackendEvent>, BackendError> {
        let sender = self
            .inbound_sender
            .clone()
            .ok_or_else(|| BackendError::OperationFailed("Keybase listener unavailable".into()))?;
        let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
        let message_id = provider_message_ref_to_message_id(&message)
            .ok_or_else(|| BackendError::OperationFailed("Invalid message ID".into()))?;
        let command = match text.as_ref() {
            Some(text) => BackendCommand::EditMessage {
                op_id: op_id.clone(),
                conversation_id: conversation_id.clone(),
                message_id: message_id.clone(),
                text: text.clone(),
            },
            None => BackendCommand::DeleteMessage {
                op_id: op_id.clone(),
                conversation_id: conversation_id.clone(),
                message_id: message_id.clone(),
            },
        };
        let store = Arc::clone(&self.local_store);
        let scheduled = task_runtime::spawn_task(
            TaskPriority::High,
            Some(format!("mutation:{}", op_id.0)),
            move || {
                let result = (|| -> io::Result<()> {
                    let path = socket_path()
                        .ok_or_else(|| io::Error::other("Keybase socket unavailable"))?;
                    let raw = provider_ref_to_conversation_id_bytes(&conversation)
                        .ok_or_else(|| io::Error::other("Invalid conversation ID"))?;
                    let target = message_id.0.parse::<i64>().map_err(io::Error::other)?;
                    let previous = store
                        .load_recent_messages_for_conversation(&conversation_id, 1)?
                        .last()
                        .and_then(|message| message.id.0.parse::<i64>().ok())
                        .unwrap_or(0);
                    let runtime = Builder::new_current_thread().enable_all().build()?;
                    runtime.block_on(async {
                        tokio::time::timeout(Duration::from_secs(30), async {
                            let transport = FramedMsgpackTransport::connect(&path).await?;
                            let mut client = KeybaseRpcClient::new(transport);
                            let inbox = fetch_inbox_for_conversation_id(&mut client, &raw).await?;
                            let (tlf_name, _, public) =
                                extract_team_lookup_params_from_inbox_response(&inbox).ok_or_else(
                                    || io::Error::other("Missing conversation metadata"),
                                )?;
                            if let Some(text) = text.as_ref() {
                                call_post_edit_nonblock(
                                    &mut client,
                                    &raw,
                                    &tlf_name,
                                    public,
                                    target,
                                    text,
                                    previous,
                                )
                                .await?;
                            } else {
                                call_post_delete_nonblock(
                                    &mut client,
                                    &raw,
                                    &tlf_name,
                                    public,
                                    target,
                                    previous,
                                )
                                .await?;
                            }
                            Ok::<_, io::Error>(())
                        })
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "Message operation timed out")
                        })?
                    })?;
                    if text.is_none() {
                        store.delete_message(&conversation_id, &message_id)?;
                        let _ = sender.send(BackendEvent::MessageDeleted {
                            conversation_id,
                            message_id,
                        });
                    }
                    Ok(())
                })();
                if let Err(error) = result {
                    let _ = sender.send(BackendEvent::CommandFailed {
                        command: Box::new(command),
                        error: error.to_string(),
                    });
                }
            },
        );
        if !scheduled {
            return Err(BackendError::OperationFailed(
                "Message operation could not be scheduled".into(),
            ));
        }
        Ok(Vec::new())
    }

    fn map_notify_event(event: KeybaseNotifyEvent) -> Option<BackendEvent> {
        let method = match event {
            KeybaseNotifyEvent::Known { kind, .. } => kind.method_name().to_string(),
            KeybaseNotifyEvent::Unknown { method, .. } => method,
        };
        if matches!(method.as_str(), "chat.1.NotifyChat.ChatTypingUpdate") {
            return None;
        }
        Some(BackendEvent::KeybaseNotifyStub {
            method,
            payload_preview: None,
        })
    }
}

impl ChatBackend for KeybaseBackend {
    fn backend_id(&self) -> BackendId {
        self.backend_id.clone()
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::keybase_defaults()
    }

    fn connect_account(
        &mut self,
        account_id: &AccountId,
    ) -> Result<Vec<BackendEvent>, BackendError> {
        self.account_id = account_id.clone();
        self.ensure_listener_started();
        Ok(Vec::new())
    }

    fn execute(&mut self, cmd: RoutedBackendCommand) -> Result<Vec<BackendEvent>, BackendError> {
        if benchmark_read_only() {
            match &cmd {
                RoutedBackendCommand::MarkRead { .. } => return Ok(Vec::new()),
                RoutedBackendCommand::SendMessage { .. }
                | RoutedBackendCommand::SendAttachment { .. }
                | RoutedBackendCommand::EditMessage { .. }
                | RoutedBackendCommand::DeleteMessage { .. }
                | RoutedBackendCommand::ReactToMessage { .. }
                | RoutedBackendCommand::PinMessage { .. }
                | RoutedBackendCommand::UnpinMessage { .. }
                | RoutedBackendCommand::CreateConversation { .. }
                | RoutedBackendCommand::LeaveChannel { .. }
                | RoutedBackendCommand::JoinChannel { .. }
                | RoutedBackendCommand::SetConversationNotificationSettings { .. }
                | RoutedBackendCommand::SetNotifySound { .. }
                | RoutedBackendCommand::FollowUser { .. }
                | RoutedBackendCommand::UnfollowUser { .. } => {
                    return Err(BackendError::OperationFailed(
                        "Server mutation blocked in read-only benchmark mode".into(),
                    ));
                }
                _ => {}
            }
        }
        match cmd {
            RoutedBackendCommand::LoadBootstrap { account_id } => {
                self.ensure_listener_started();
                let Some(sender) = self.inbound_sender.clone() else {
                    return Ok(Vec::new());
                };
                let backend_id = self.backend_id.clone();
                let local_store = Arc::clone(&self.local_store);
                let sync = Arc::clone(&self.sync);
                let sender_for_task = sender.clone();
                let scheduled = task_runtime::spawn_task(
                    TaskPriority::High,
                    Some("bootstrap".to_string()),
                    move || {
                        run_bootstrap(sender_for_task, backend_id, account_id, local_store, sync)
                    },
                );
                if !scheduled {
                    send_internal(
                        &sender,
                        "zbase.internal.task_runtime.deduped",
                        Value::from("bootstrap"),
                    );
                }
                emit_task_runtime_stats(&sender, "bootstrap.schedule");
                Ok(Vec::new())
            }
            RoutedBackendCommand::LoadConversation {
                account_id: _,
                conversation,
            } => {
                self.ensure_listener_started();
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let cached_messages = self
                    .local_store
                    .load_recent_messages_for_conversation(
                        &conversation_id,
                        CONVERSATION_OPEN_CACHE_LOAD_LIMIT,
                    )
                    .unwrap_or_default();
                let (mut cached_messages, filtered_placeholder_count) =
                    if self.inbound_sender.is_some() {
                        strip_placeholder_messages(cached_messages)
                    } else {
                        (cached_messages, 0)
                    };
                strip_reaction_delete_tombstones(
                    &self.local_store,
                    &conversation_id,
                    &mut cached_messages,
                );
                apply_regular_delete_tombstones(
                    &self.local_store,
                    &conversation_id,
                    self.inbound_sender.as_ref(),
                    &mut cached_messages,
                );
                if filtered_placeholder_count > 0
                    && let Some(sender) = self.inbound_sender.clone()
                {
                    send_internal(
                        &sender,
                        "zbase.internal.cached_placeholder_filtered",
                        Value::Map(vec![
                            (
                                Value::from("conversation_id"),
                                Value::from(conversation_id.0.clone()),
                            ),
                            (
                                Value::from("filtered_messages"),
                                Value::from(filtered_placeholder_count as i64),
                            ),
                        ]),
                    );
                }
                let older_cursor = cached_messages.first().map(|message| message.id.0.clone());
                let newer_cursor = cached_messages.last().map(|message| message.id.0.clone());
                let mut events = vec![BackendEvent::TimelineReplaced {
                    conversation_id: conversation_id.clone(),
                    messages: cached_messages,
                    older_cursor,
                    newer_cursor,
                }];
                if let Ok(cached_emojis) =
                    self.local_store.load_conversation_emojis(&conversation_id)
                    && !cached_emojis.is_empty()
                {
                    events.push(BackendEvent::ConversationEmojisSynced {
                        conversation_id: conversation_id.clone(),
                        emojis: cached_emoji_entries(&cached_emojis),
                    });
                }
                if let Some(team_roles_event) =
                    team_roles_event_from_cache(&self.local_store, &conversation_id)
                {
                    events.push(team_roles_event);
                }

                if let Some(sender) = self.inbound_sender.clone() {
                    let local_store = Arc::clone(&self.local_store);
                    let dedupe_key = format!("load_conversation:{}", conversation_id.0);
                    let sender_for_stats = sender.clone();
                    let queued_at = Instant::now();
                    let _ = task_runtime::spawn_task(
                        TaskPriority::Interactive,
                        Some(dedupe_key),
                        move || run_load_conversation(sender, conversation, local_store, queued_at),
                    );
                    emit_task_runtime_stats(&sender_for_stats, "load_conversation.schedule");
                }

                Ok(events)
            }
            RoutedBackendCommand::LoadThread {
                account_id: _,
                conversation,
                root_message,
            } => {
                let load_thread_started_at = Instant::now();
                let mut reindex_us = 0u128;
                let mut warm_recent_us = 0u128;
                let mut root_fallback_us = 0u128;

                self.ensure_listener_started();
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let requested_root = provider_message_ref_to_message_id(&root_message)
                    .unwrap_or_else(|| MessageId::new(root_message.0.clone()));
                let sender_for_debug = self.inbound_sender.clone();
                let edge_repair_started_at = Instant::now();
                let conversation_edge_migrated = self
                    .local_store
                    .is_thread_edge_conversation_migrated(&conversation_id)
                    .unwrap_or(false);
                if !conversation_edge_migrated {
                    let repaired_edges = self
                        .local_store
                        .repair_thread_edges_for_conversation(
                            &conversation_id,
                            THREAD_EDGE_REPAIR_SCAN_LIMIT,
                        )
                        .unwrap_or(0);
                    let _ = self
                        .local_store
                        .mark_thread_edge_conversation_migrated(&conversation_id);
                    if let Some(sender) = sender_for_debug.as_ref() {
                        send_internal(
                            sender,
                            "zbase.internal.thread_edge_repair.on_access",
                            Value::Map(vec![
                                (
                                    Value::from("conversation_id"),
                                    Value::from(conversation_id.0.clone()),
                                ),
                                (
                                    Value::from("rewritten_edges"),
                                    Value::from(repaired_edges as i64),
                                ),
                            ]),
                        );
                    }
                }
                let edge_repair_us = edge_repair_started_at.elapsed().as_micros();
                let root_resolution_started_at = Instant::now();
                let canonical_root = self
                    .local_store
                    .get_message(&conversation_id, &requested_root)
                    .ok()
                    .flatten()
                    .map(|message| {
                        resolve_thread_root_for_message(
                            &self.local_store,
                            &conversation_id,
                            &requested_root,
                            message.reply_to.as_ref(),
                        )
                    })
                    .unwrap_or_else(|| requested_root.clone());
                let root_resolution_us = root_resolution_started_at.elapsed().as_micros();
                let mut effective_root = canonical_root.clone();
                let initial_load_started_at = Instant::now();
                let mut messages = self
                    .local_store
                    .load_thread_messages(&conversation_id, &effective_root)
                    .unwrap_or_default();
                let initial_local_load_us = initial_load_started_at.elapsed().as_micros();
                if let Some(sender) = sender_for_debug.as_ref() {
                    maybe_emit_thread_debug_snapshot(
                        sender,
                        &self.local_store,
                        &conversation_id,
                        &requested_root,
                        &effective_root,
                        "load_thread.initial_local",
                    );
                }
                if messages.len() <= 1 {
                    let reindex_started_at = Instant::now();
                    rebuild_thread_index_from_cached_messages(
                        &self.local_store,
                        &conversation_id,
                        THREAD_REINDEX_SCAN_LIMIT,
                    );
                    messages = self
                        .local_store
                        .load_thread_messages(&conversation_id, &effective_root)
                        .unwrap_or_default();
                    reindex_us = reindex_started_at.elapsed().as_micros();
                    if let Some(sender) = sender_for_debug.as_ref() {
                        maybe_emit_thread_debug_snapshot(
                            sender,
                            &self.local_store,
                            &conversation_id,
                            &requested_root,
                            &effective_root,
                            "load_thread.after_reindex",
                        );
                    }
                    if messages.len() <= 1
                        && let Some(raw_conversation_id) =
                            provider_ref_to_conversation_id_bytes(&conversation)
                        && let Some(path) = socket_path()
                    {
                        let warm_recent_started_at = Instant::now();
                        let _ = warm_recent_conversation_messages_for_thread(
                            &self.local_store,
                            &conversation_id,
                            &raw_conversation_id,
                            &path,
                        );
                        rebuild_thread_index_from_cached_messages(
                            &self.local_store,
                            &conversation_id,
                            THREAD_REINDEX_SCAN_LIMIT,
                        );
                        messages = self
                            .local_store
                            .load_thread_messages(&conversation_id, &effective_root)
                            .unwrap_or_default();
                        warm_recent_us = warm_recent_started_at.elapsed().as_micros();
                        if let Some(sender) = sender_for_debug.as_ref() {
                            maybe_emit_thread_debug_snapshot(
                                sender,
                                &self.local_store,
                                &conversation_id,
                                &requested_root,
                                &effective_root,
                                "load_thread.after_warm_recent",
                            );
                        }
                    }
                    if messages.len() <= 1 && effective_root != requested_root {
                        let root_fallback_started_at = Instant::now();
                        effective_root = requested_root.clone();
                        messages = self
                            .local_store
                            .load_thread_messages(&conversation_id, &effective_root)
                            .unwrap_or_default();
                        root_fallback_us = root_fallback_started_at.elapsed().as_micros();
                        if let Some(sender) = sender_for_debug.as_ref() {
                            maybe_emit_thread_debug_snapshot(
                                sender,
                                &self.local_store,
                                &conversation_id,
                                &requested_root,
                                &effective_root,
                                "load_thread.after_root_fallback",
                            );
                        }
                    }
                }
                if let Some(sender) = sender_for_debug.as_ref() {
                    maybe_emit_thread_debug_snapshot(
                        sender,
                        &self.local_store,
                        &conversation_id,
                        &requested_root,
                        &effective_root,
                        "load_thread.before_emit",
                    );
                }

                if thread_open_profile_enabled() {
                    let total_us = load_thread_started_at.elapsed().as_micros();
                    warn!(
                        target: "zbase.thread.open.perf",
                        phase = "backend_load_thread",
                        conversation_id = %conversation_id.0,
                        requested_root = %requested_root.0,
                        effective_root = %effective_root.0,
                        total_us = total_us as i64,
                        edge_repair_us = edge_repair_us as i64,
                        root_resolution_us = root_resolution_us as i64,
                        initial_local_load_us = initial_local_load_us as i64,
                        reindex_us = reindex_us as i64,
                        warm_recent_us = warm_recent_us as i64,
                        root_fallback_us = root_fallback_us as i64,
                        emitted_message_count = messages.len() as i64,
                        "thread_open_measurement"
                    );
                }

                if let Some(raw_conversation_id) =
                    provider_ref_to_conversation_id_bytes(&conversation)
                    && let Some(path) = socket_path()
                    && let Some(sender) = self.inbound_sender.clone()
                {
                    let local_store = Arc::clone(&self.local_store);
                    let conversation_for_backfill = conversation_id.clone();
                    let root_for_backfill = requested_root.clone();
                    let sender_for_stats = sender.clone();
                    let dedupe_key = format!(
                        "thread_backfill:{}:{}",
                        conversation_for_backfill.0, root_for_backfill.0
                    );
                    let _ =
                        task_runtime::spawn_task(TaskPriority::High, Some(dedupe_key), move || {
                            run_backfill_thread_history(
                                sender,
                                local_store,
                                conversation_for_backfill,
                                root_for_backfill,
                                raw_conversation_id,
                                path,
                            )
                        });
                    emit_task_runtime_stats(&sender_for_stats, "thread_backfill.schedule");
                }

                Ok(vec![BackendEvent::MessagesPrepended {
                    key: crate::state::state::TimelineKey::Thread {
                        conversation_id,
                        root_id: effective_root,
                        requested_root_id: requested_root,
                    },
                    messages,
                    cursor: None,
                }])
            }
            RoutedBackendCommand::LoadOlderMessages {
                account_id: _,
                conversation,
                cursor,
            } => {
                self.ensure_listener_started();
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let before_id = MessageId::new(cursor);
                let mut messages = self
                    .local_store
                    .load_messages_before(&conversation_id, Some(&before_id), LOAD_OLDER_PAGE_SIZE)
                    .unwrap_or_default();
                retain_strictly_older_messages(&mut messages, &before_id);
                let mut reaction_delete_events = Vec::new();
                let mut fetched_page_has_more = false;
                let needs_remote_fetch =
                    messages.is_empty() || messages_have_non_text_placeholders(&messages);
                if needs_remote_fetch
                    && let Some(raw_conversation_id) =
                        provider_ref_to_conversation_id_bytes(&conversation)
                    && let Some(path) = socket_path()
                    && let Some(sender) = self.inbound_sender.clone()
                {
                    let local_store = Arc::clone(&self.local_store);
                    let conversation_id_for_fetch = conversation_id.clone();
                    let before_id_for_fetch = before_id.clone();
                    let sender_for_stats = sender.clone();
                    let dedupe_key = format!(
                        "load_older:{}:{}",
                        conversation_id_for_fetch.0, before_id_for_fetch.0
                    );
                    let started =
                        task_runtime::spawn_task(TaskPriority::High, Some(dedupe_key), move || {
                            run_load_older_messages(
                                sender,
                                local_store,
                                conversation_id_for_fetch,
                                before_id_for_fetch,
                                raw_conversation_id,
                                path,
                            );
                        });
                    emit_task_runtime_stats(&sender_for_stats, "load_older.schedule");
                    if !started {
                        return Ok(vec![BackendEvent::MessagesPrepended {
                            key: crate::state::state::TimelineKey::Conversation(conversation_id),
                            messages: Vec::new(),
                            cursor: Some(before_id.0),
                        }]);
                    }
                    return Ok(Vec::new());
                }
                if needs_remote_fetch
                    && let Some(raw_conversation_id) =
                        provider_ref_to_conversation_id_bytes(&conversation)
                    && let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let conversation_id_for_fetch = conversation_id.clone();
                    let before_id_for_fetch = before_id.clone();
                    if let Ok(mut fetched) = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        fetch_thread_messages_before_anchor(
                            &mut client,
                            &conversation_id_for_fetch,
                            &raw_conversation_id,
                            &before_id_for_fetch,
                            LOAD_OLDER_PAGE_SIZE,
                        )
                        .await
                    }) {
                        fetched_page_has_more =
                            page_may_have_more_older_messages(&fetched, LOAD_OLDER_PAGE_SIZE);
                        persist_reaction_deltas(
                            self.inbound_sender.as_ref(),
                            &self.local_store,
                            &fetched.reaction_deltas,
                        );
                        reaction_delete_events =
                            reaction_op_delete_events(&fetched.reaction_deltas);
                        strip_reaction_delete_tombstones(
                            &self.local_store,
                            &conversation_id,
                            &mut fetched.messages,
                        );
                        apply_regular_delete_tombstones(
                            &self.local_store,
                            &conversation_id,
                            self.inbound_sender.as_ref(),
                            &mut fetched.messages,
                        );
                        let message_ids = fetched
                            .messages
                            .iter()
                            .map(|message| message.id.clone())
                            .collect::<Vec<_>>();
                        let loaded_reactions = self
                            .local_store
                            .load_message_reactions_for_messages(&conversation_id, &message_ids)
                            .unwrap_or_default();
                        let mut hydrated = Vec::with_capacity(fetched.messages.len());
                        for mut message in fetched.messages {
                            message.reactions =
                                domain_message_reactions(loaded_reactions.get(&message.id));
                            ingest_message_record(
                                self.inbound_sender.as_ref(),
                                &self.local_store,
                                &mut message,
                            );
                            hydrated.push(message);
                        }
                        messages = hydrated;
                        retain_strictly_older_messages(&mut messages, &before_id);
                    }
                }
                let next_cursor = next_older_cursor(
                    &self.local_store,
                    &conversation_id,
                    &messages,
                    fetched_page_has_more,
                );
                schedule_message_emoji_source_syncs(self.inbound_sender.as_ref(), &messages);
                let mut events = reaction_delete_events;
                events.push(BackendEvent::MessagesPrepended {
                    key: crate::state::state::TimelineKey::Conversation(conversation_id),
                    messages,
                    cursor: next_cursor,
                });
                Ok(events)
            }
            RoutedBackendCommand::JumpToMessage {
                account_id: _,
                conversation,
                message_id,
            } => {
                self.ensure_listener_started();
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let target_id = MessageId::new(message_id);
                let mut older_messages = self
                    .local_store
                    .load_messages_before(&conversation_id, Some(&target_id), LOAD_OLDER_PAGE_SIZE)
                    .unwrap_or_default();
                retain_strictly_older_messages(&mut older_messages, &target_id);
                let mut newer_messages = self
                    .local_store
                    .load_messages_after(&conversation_id, &target_id, LOAD_OLDER_PAGE_SIZE)
                    .unwrap_or_default();
                retain_strictly_newer_messages(&mut newer_messages, &target_id);
                let mut target_message = self
                    .local_store
                    .get_message(&conversation_id, &target_id)
                    .ok()
                    .flatten();
                let mut reaction_delete_events = Vec::new();
                let mut fetched_page_has_more = false;
                let needs_remote_fetch = target_message.is_none()
                    || messages_have_non_text_placeholders(&older_messages)
                    || messages_have_non_text_placeholders(&newer_messages)
                    || target_message.as_ref().is_some_and(is_placeholder_message);

                if needs_remote_fetch
                    && let Some(raw_conversation_id) =
                        provider_ref_to_conversation_id_bytes(&conversation)
                    && let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let conversation_id_for_fetch = conversation_id.clone();
                    let target_id_for_fetch = target_id.clone();
                    if let Ok((older_page, newer_page)) = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        let older_page = fetch_thread_messages_before_anchor(
                            &mut client,
                            &conversation_id_for_fetch,
                            &raw_conversation_id,
                            &target_id_for_fetch,
                            LOAD_OLDER_PAGE_SIZE,
                        )
                        .await?;
                        let newer_page = fetch_thread_messages_after_anchor(
                            &mut client,
                            &conversation_id_for_fetch,
                            &raw_conversation_id,
                            &target_id_for_fetch,
                            LOAD_OLDER_PAGE_SIZE,
                        )
                        .await?;
                        Ok::<(ThreadPage, ThreadPage), io::Error>((older_page, newer_page))
                    }) {
                        fetched_page_has_more =
                            page_may_have_more_older_messages(&older_page, LOAD_OLDER_PAGE_SIZE);
                        let mut reaction_deltas = older_page.reaction_deltas;
                        reaction_deltas.extend(newer_page.reaction_deltas);
                        persist_reaction_deltas(
                            self.inbound_sender.as_ref(),
                            &self.local_store,
                            &reaction_deltas,
                        );
                        reaction_delete_events = reaction_op_delete_events(&reaction_deltas);

                        let mut fetched_messages = older_page.messages;
                        fetched_messages.extend(newer_page.messages);
                        normalize_message_records(&mut fetched_messages);
                        strip_reaction_delete_tombstones(
                            &self.local_store,
                            &conversation_id,
                            &mut fetched_messages,
                        );
                        apply_regular_delete_tombstones(
                            &self.local_store,
                            &conversation_id,
                            self.inbound_sender.as_ref(),
                            &mut fetched_messages,
                        );
                        let message_ids = fetched_messages
                            .iter()
                            .map(|message| message.id.clone())
                            .collect::<Vec<_>>();
                        let loaded_reactions = self
                            .local_store
                            .load_message_reactions_for_messages(&conversation_id, &message_ids)
                            .unwrap_or_default();
                        let mut fetched_older = Vec::new();
                        let mut fetched_newer = Vec::new();
                        let mut fetched_target = None;
                        for mut message in fetched_messages {
                            message.reactions =
                                domain_message_reactions(loaded_reactions.get(&message.id));
                            ingest_message_record(
                                self.inbound_sender.as_ref(),
                                &self.local_store,
                                &mut message,
                            );
                            match compare_message_ids(&message.id, &target_id) {
                                std::cmp::Ordering::Less => fetched_older.push(message),
                                std::cmp::Ordering::Equal => fetched_target = Some(message),
                                std::cmp::Ordering::Greater => fetched_newer.push(message),
                            }
                        }
                        normalize_message_records(&mut fetched_older);
                        normalize_message_records(&mut fetched_newer);
                        if !fetched_older.is_empty()
                            || !fetched_newer.is_empty()
                            || fetched_target.is_some()
                        {
                            older_messages = fetched_older;
                            newer_messages = fetched_newer;
                            target_message = fetched_target.or_else(|| {
                                self.local_store
                                    .get_message(&conversation_id, &target_id)
                                    .ok()
                                    .flatten()
                            });
                        }
                    }
                }

                let mut messages =
                    Vec::with_capacity(older_messages.len() + newer_messages.len() + 1);
                messages.extend(older_messages);
                if let Some(mut target) = target_message {
                    target.reactions = self
                        .local_store
                        .load_message_reactions_for_messages(
                            &conversation_id,
                            std::slice::from_ref(&target.id),
                        )
                        .ok()
                        .and_then(|loaded| loaded.get(&target.id).cloned())
                        .map(|cached| domain_message_reactions(Some(&cached)))
                        .unwrap_or_else(|| target.reactions.clone());
                    hydrate_thread_metadata(&self.local_store, &mut target);
                    messages.push(target);
                }
                messages.extend(newer_messages);
                normalize_message_records(&mut messages);

                let older_cursor = next_older_cursor(
                    &self.local_store,
                    &conversation_id,
                    &messages,
                    fetched_page_has_more,
                );
                let newer_cursor = messages.last().map(|message| message.id.0.clone());
                schedule_message_emoji_source_syncs(self.inbound_sender.as_ref(), &messages);
                let mut events = reaction_delete_events;
                events.push(BackendEvent::TimelineReplaced {
                    conversation_id,
                    messages,
                    older_cursor,
                    newer_cursor,
                });
                Ok(events)
            }
            RoutedBackendCommand::SendMessage {
                op_id,
                account_id: _,
                conversation,
                client_message_id,
                text,
                attachments: _,
                reply_to,
            } => {
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let Some(raw_conversation_id) =
                    provider_ref_to_conversation_id_bytes(&conversation)
                else {
                    return Ok(vec![BackendEvent::MessageSendFailed {
                        op_id,
                        client_message_id,
                        error: "missing provider conversation id".to_string(),
                    }]);
                };
                let client_prev = self
                    .local_store
                    .load_recent_messages_for_conversation(&conversation_id, 1)
                    .ok()
                    .and_then(|messages| {
                        messages
                            .last()
                            .and_then(|message| message.id.0.parse::<i64>().ok())
                    })
                    .unwrap_or(0);
                let reply_to_id = reply_to
                    .as_ref()
                    .and_then(provider_message_ref_to_message_id)
                    .and_then(|message_id| message_id.0.parse::<i64>().ok());
                let mut emitted = Vec::new();
                if let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let raw_for_call = raw_conversation_id.clone();
                    let text_for_call = text.clone();
                    let send_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        let inbox =
                            fetch_inbox_for_conversation_id(&mut client, &raw_for_call).await?;
                        let Some((tlf_name, _, tlf_public)) =
                            extract_team_lookup_params_from_inbox_response(&inbox)
                        else {
                            return Err(io::Error::other("missing tlf metadata"));
                        };
                        call_post_text_nonblock(
                            &mut client,
                            &raw_for_call,
                            &tlf_name,
                            tlf_public,
                            &text_for_call,
                            client_prev,
                            reply_to_id,
                        )
                        .await
                    });
                    match send_result {
                        Ok(response) => {
                            let meta = PendingSendMeta {
                                op_id,
                                client_message_id,
                                local_attachment_path: None,
                            };
                            match self.pending_outbox_sends.lock() {
                                Ok(mut pending) => emitted.extend(pending.acknowledge(&response, meta)),
                                Err(_) => emitted.push(BackendEvent::MessageSendFailed {
                                    op_id: meta.op_id,
                                    client_message_id: meta.client_message_id,
                                    error: "Could not track the Keybase send. Check the conversation before retrying.".into(),
                                }),
                            }
                        }
                        Err(error) => {
                            emitted.push(BackendEvent::MessageSendFailed {
                                op_id,
                                client_message_id,
                                error: error.to_string(),
                            });
                        }
                    }
                } else {
                    emitted.push(BackendEvent::MessageSendFailed {
                        op_id,
                        client_message_id,
                        error: "keybase socket unavailable".to_string(),
                    });
                }
                Ok(emitted)
            }
            RoutedBackendCommand::SendAttachment {
                op_id,
                account_id: _,
                conversation,
                client_message_id,
                local_path,
                filename,
                caption,
            } => {
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let Some(raw_conversation_id) =
                    provider_ref_to_conversation_id_bytes(&conversation)
                else {
                    return Ok(vec![BackendEvent::MessageSendFailed {
                        op_id,
                        client_message_id,
                        error: "missing provider conversation id".to_string(),
                    }]);
                };
                let client_prev = self
                    .local_store
                    .load_recent_messages_for_conversation(&conversation_id, 1)
                    .ok()
                    .and_then(|messages| {
                        messages
                            .last()
                            .and_then(|message| message.id.0.parse::<i64>().ok())
                    })
                    .unwrap_or(0);
                let mut emitted = Vec::new();
                if let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let raw_for_call = raw_conversation_id.clone();
                    let local_path_for_call = local_path.clone();
                    let filename_for_call = filename.clone();
                    let caption_for_call = caption.clone();
                    let send_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        let inbox =
                            fetch_inbox_for_conversation_id(&mut client, &raw_for_call).await?;
                        let Some((tlf_name, _, tlf_public)) =
                            extract_team_lookup_params_from_inbox_response(&inbox)
                        else {
                            return Err(io::Error::other("missing tlf metadata"));
                        };
                        call_post_file_attachment_nonblock(
                            &mut client,
                            &raw_for_call,
                            &tlf_name,
                            tlf_public,
                            &local_path_for_call,
                            &filename_for_call,
                            &caption_for_call,
                            client_prev,
                        )
                        .await
                    });
                    match send_result {
                        Ok(response) => {
                            let meta = PendingSendMeta {
                                op_id,
                                client_message_id,
                                local_attachment_path: Some(local_path.clone()),
                            };
                            match self.pending_outbox_sends.lock() {
                                Ok(mut pending) => emitted.extend(pending.acknowledge(&response, meta)),
                                Err(_) => emitted.push(BackendEvent::MessageSendFailed {
                                    op_id: meta.op_id,
                                    client_message_id: meta.client_message_id,
                                    error: "Could not track the Keybase send. Check the conversation before retrying.".into(),
                                }),
                            }
                        }
                        Err(error) => {
                            emitted.push(BackendEvent::MessageSendFailed {
                                op_id,
                                client_message_id,
                                error: error.to_string(),
                            });
                        }
                    }
                } else {
                    emitted.push(BackendEvent::MessageSendFailed {
                        op_id,
                        client_message_id,
                        error: "keybase socket unavailable".to_string(),
                    });
                }
                Ok(emitted)
            }
            RoutedBackendCommand::EditMessage {
                op_id,
                conversation,
                message,
                text,
                ..
            } => self.start_message_mutation(op_id, conversation, message, Some(text)),
            RoutedBackendCommand::DeleteMessage {
                op_id,
                conversation,
                message,
                ..
            } => self.start_message_mutation(op_id, conversation, message, None),
            RoutedBackendCommand::ReactToMessage {
                op_id,
                account_id: _,
                conversation,
                message,
                reaction,
            } => {
                let Some(raw_conversation_id) =
                    provider_ref_to_conversation_id_bytes(&conversation)
                else {
                    return Ok(vec![BackendEvent::ReactionFailed {
                        op_id,
                        error: "missing provider conversation id".to_string(),
                    }]);
                };
                let Some(supersedes) = provider_message_ref_to_message_id(&message)
                    .and_then(|message_id| message_id.0.parse::<i64>().ok())
                else {
                    return Ok(vec![BackendEvent::ReactionFailed {
                        op_id,
                        error: "invalid target message id".to_string(),
                    }]);
                };
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let client_prev = self
                    .local_store
                    .load_recent_messages_for_conversation(&conversation_id, 1)
                    .ok()
                    .and_then(|messages| {
                        messages
                            .last()
                            .and_then(|message| message.id.0.parse::<i64>().ok())
                    })
                    .unwrap_or(0);
                let mut emitted = Vec::new();
                if let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let raw_for_call = raw_conversation_id.clone();
                    let reaction_for_call = reaction.clone();
                    let react_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        let inbox =
                            fetch_inbox_for_conversation_id(&mut client, &raw_for_call).await?;
                        let Some((tlf_name, _, tlf_public)) =
                            extract_team_lookup_params_from_inbox_response(&inbox)
                        else {
                            return Err(io::Error::other("missing tlf metadata"));
                        };
                        call_post_reaction_nonblock(
                            &mut client,
                            &raw_for_call,
                            &tlf_name,
                            tlf_public,
                            supersedes,
                            &reaction_for_call,
                            client_prev,
                        )
                        .await
                    });
                    match react_result {
                        Ok(_) => emitted.push(BackendEvent::ReactionConfirmed { op_id }),
                        Err(error) => emitted.push(BackendEvent::ReactionFailed {
                            op_id,
                            error: error.to_string(),
                        }),
                    }
                } else {
                    emitted.push(BackendEvent::ReactionFailed {
                        op_id,
                        error: "keybase socket unavailable".to_string(),
                    });
                }
                Ok(emitted)
            }
            RoutedBackendCommand::PinMessage {
                op_id: _,
                account_id: _,
                conversation,
                message,
            } => {
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let mut emitted = Vec::new();
                let Some(raw_message_id) = provider_message_ref_to_message_id(&message)
                    .and_then(|value| value.0.parse::<i64>().ok())
                else {
                    return Ok(emitted);
                };
                if let Some(raw_conversation_id) =
                    provider_ref_to_conversation_id_bytes(&conversation)
                    && let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let raw_for_pin = raw_conversation_id.clone();
                    let path_for_pin = path.clone();
                    let pin_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path_for_pin).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        call_pin_message_local(&mut client, &raw_for_pin, raw_message_id).await
                    });
                    if pin_result.is_ok() {
                        let raw_for_inbox = raw_conversation_id.clone();
                        let path_for_inbox = path.clone();
                        if let Ok(inbox) = runtime.block_on(async move {
                            let transport =
                                FramedMsgpackTransport::connect(&path_for_inbox).await?;
                            let mut client = KeybaseRpcClient::new(transport);
                            fetch_inbox_for_conversation_id(&mut client, &raw_for_inbox).await
                        }) && let Some(pinned) =
                            extract_pinned_state_for_conversation(&inbox, &conversation_id)
                        {
                            emitted.push(BackendEvent::PinnedStateUpdated {
                                conversation_id: conversation_id.clone(),
                                pinned,
                            });
                        }
                    }
                }
                Ok(emitted)
            }
            RoutedBackendCommand::UnpinMessage {
                op_id: _,
                account_id: _,
                conversation,
            } => {
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let mut emitted = Vec::new();
                if let Some(raw_conversation_id) =
                    provider_ref_to_conversation_id_bytes(&conversation)
                    && let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let raw_for_unpin = raw_conversation_id.clone();
                    let path_for_unpin = path.clone();
                    let unpin_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path_for_unpin).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        call_unpin_message_local(&mut client, &raw_for_unpin).await
                    });
                    if unpin_result.is_ok() {
                        let raw_for_inbox = raw_conversation_id.clone();
                        let path_for_inbox = path.clone();
                        if let Ok(inbox) = runtime.block_on(async move {
                            let transport =
                                FramedMsgpackTransport::connect(&path_for_inbox).await?;
                            let mut client = KeybaseRpcClient::new(transport);
                            fetch_inbox_for_conversation_id(&mut client, &raw_for_inbox).await
                        }) && let Some(pinned) =
                            extract_pinned_state_for_conversation(&inbox, &conversation_id)
                        {
                            emitted.push(BackendEvent::PinnedStateUpdated {
                                conversation_id: conversation_id.clone(),
                                pinned,
                            });
                        }
                    }
                }
                Ok(emitted)
            }
            RoutedBackendCommand::MarkRead {
                account_id: _,
                conversation,
                message: _,
            } => {
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let read_upto = self
                    .local_store
                    .load_recent_messages_for_conversation(&conversation_id, 1)
                    .ok()
                    .and_then(|messages| messages.last().map(|message| message.id.clone()));

                let mut emitted = Vec::new();
                let mut mark_read_succeeded = false;
                if let Some(raw_conversation_id) =
                    provider_ref_to_conversation_id_bytes(&conversation)
                    && let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let raw_for_call = raw_conversation_id.clone();
                    let mark_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        call_mark_as_read_local(&mut client, &raw_for_call, None, false).await
                    });

                    match mark_result {
                        Ok(_) => {
                            mark_read_succeeded = true;
                            if let Some(sender) = self.inbound_sender.clone() {
                                send_internal(
                                    &sender,
                                    "zbase.internal.mark_read_sent",
                                    Value::Map(vec![
                                        (
                                            Value::from("conversation_id"),
                                            Value::from(conversation_id.0.clone()),
                                        ),
                                        (
                                            Value::from("message_id"),
                                            read_upto
                                                .as_ref()
                                                .map(|id| Value::from(id.0.clone()))
                                                .unwrap_or(Value::Nil),
                                        ),
                                    ]),
                                );
                            }
                        }
                        Err(error) => {
                            if let Some(sender) = self.inbound_sender.clone() {
                                send_internal(
                                    &sender,
                                    "zbase.internal.mark_read_failed",
                                    Value::from(error.to_string()),
                                );
                            }
                        }
                    }
                }

                if mark_read_succeeded {
                    let counts = HashMap::from([(
                        conversation_id.clone(),
                        ConversationBadgeCounts::default(),
                    )]);
                    self.local_store
                        .update_conversation_badges(counts.clone(), false)
                        .map_err(|error| BackendError::OperationFailed(error.to_string()))?;
                    emitted.push(BackendEvent::ConversationBadgesUpdated {
                        backend_id: self.backend_id.clone(),
                        counts,
                        complete: false,
                    });
                    emitted.push(BackendEvent::ReadMarkerUpdated {
                        conversation_id: conversation_id.clone(),
                        read_upto: read_upto.clone(),
                    });
                    let snapshot = ConversationUnreadSnapshot {
                        conversation_id: conversation_id.clone(),
                        unread_count: 0,
                        mention_count: 0,
                        read_upto,
                        activity_time: None,
                    };
                    persist_unread_snapshot(&self.local_store, &snapshot);
                    let _ = self.local_store.clear_unread_marker();
                    emitted.push(BackendEvent::ConversationUnreadChanged {
                        conversation_id: conversation_id.clone(),
                        unread_count: snapshot.unread_count,
                        mention_count: snapshot.mention_count,
                        read_upto: snapshot.read_upto,
                    });
                }
                Ok(emitted)
            }
            RoutedBackendCommand::Search {
                query_id,
                query,
                workspace_id,
                conversation_id,
                filters,
                ..
            } => {
                const QUICK_SWITCHER_REMOTE_MESSAGE_LIMIT: usize = 36;
                const FIND_IN_CHAT_MESSAGE_LIMIT: usize = 500;
                let search_limit = if query_id.0.starts_with("quick-switcher-") {
                    QUICK_SWITCHER_REMOTE_MESSAGE_LIMIT
                } else if query_id.0.starts_with("find-in-chat-") {
                    FIND_IN_CHAT_MESSAGE_LIMIT
                } else {
                    100
                };
                let hits = if let Some(conversation_id) = conversation_id {
                    self.search_index.search_conversation(
                        &conversation_id.0,
                        &query,
                        search_limit,
                    )?
                } else if let Some(workspace_id) = workspace_id {
                    self.search_index
                        .search_workspace(&workspace_id.0, &query, search_limit)?
                } else {
                    self.search_index
                        .search_workspace(WORKSPACE_ID, &query, search_limit)?
                };
                let self_username = if filters.is_empty() {
                    None
                } else {
                    self.local_store.load_account_display_name()
                };
                let mut results = Vec::new();
                let mut conversation_cache: HashMap<ConversationId, Option<ConversationSummary>> =
                    HashMap::new();
                for hit in hits {
                    let message = self
                        .local_store
                        .get_message(&hit.conversation_id, &hit.message_id)
                        .ok()
                        .flatten();
                    let conversation =
                        if let Some(cached) = conversation_cache.get(&hit.conversation_id) {
                            cached.clone()
                        } else {
                            let fetched = self
                                .local_store
                                .get_conversation(&hit.conversation_id)
                                .ok()
                                .flatten();
                            conversation_cache.insert(hit.conversation_id.clone(), fetched.clone());
                            fetched
                        };
                    if let (Some(message), Some(conversation)) = (message, conversation) {
                        if !filters
                            .iter()
                            .all(|filter| filter.matches(&message, self_username.as_deref()))
                        {
                            continue;
                        }
                        results.push(SearchResult {
                            conversation_id: hit.conversation_id.clone(),
                            route: search_result_route(&conversation),
                            snippet: hit.snippet,
                            snippet_highlight_ranges: hit.highlight_ranges,
                            message,
                        });
                    }
                }
                Ok(vec![BackendEvent::SearchResults {
                    query_id,
                    results,
                    is_complete: true,
                }])
            }
            RoutedBackendCommand::SearchUsers {
                query_id, query, ..
            } => self.schedule_user_search(query_id, query),
            RoutedBackendCommand::CreateConversation {
                op_id,
                account_id,
                participants,
                kind,
                ..
            } => {
                let mut emitted = Vec::new();
                if let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let participants_for_call = participants.clone();
                    let create_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        let create_response =
                            call_new_conversation_local(&mut client, &participants_for_call)
                                .await?;
                        let raw_conversation_id = extract_new_conversation_raw_id(&create_response)
                            .ok_or_else(|| {
                                io::Error::other(
                                    "missing conversation id in newConversationLocal response",
                                )
                            })?;
                        let inbox =
                            fetch_inbox_for_conversation_id(&mut client, &raw_conversation_id)
                                .await?;
                        Ok::<_, io::Error>((raw_conversation_id, inbox))
                    });
                    match create_result {
                        Ok((raw_conversation_id, inbox)) => {
                            let parsed = parse_inbox_conversations(&inbox, None, false);
                            let created = parsed
                                .iter()
                                .find(|item| item.raw_conversation_id == raw_conversation_id)
                                .cloned()
                                .or_else(|| parsed.into_iter().next());
                            let (summary, provider_ref) = if let Some(created) = created {
                                (created.summary, created.provider_ref)
                            } else {
                                let conversation_id = ConversationId::new(format!(
                                    "kb_conv:{}",
                                    hex_encode(&raw_conversation_id)
                                ));
                                let provider_ref =
                                    ProviderConversationRef::new(conversation_id.0.clone());
                                let title = participants
                                    .iter()
                                    .map(|participant| participant.trim())
                                    .filter(|participant| !participant.is_empty())
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                (
                                    ConversationSummary {
                                        id: conversation_id,
                                        title: if title.is_empty() {
                                            "New conversation".to_string()
                                        } else {
                                            title
                                        },
                                        kind,
                                        topic: String::new(),
                                        group: None,
                                        unread_count: 0,
                                        mention_count: 0,
                                        muted: false,
                                        last_activity_ms: 0,
                                        last_message: None,
                                    },
                                    provider_ref,
                                )
                            };
                            let conversation_binding = ConversationBinding {
                                conversation_id: summary.id.clone(),
                                backend_id: self.backend_id.clone(),
                                account_id: account_id.clone(),
                                provider_conversation_ref: provider_ref,
                            };
                            emitted.push(BackendEvent::ConversationCreated {
                                op_id,
                                workspace_id: WorkspaceId::new(WORKSPACE_ID),
                                conversation: summary,
                                conversation_binding,
                            });
                        }
                        Err(error) => return Err(error.into()),
                    }
                } else {
                    return Err(BackendError::OperationFailed(
                        "Keybase is unavailable. Please try again.".into(),
                    ));
                }
                Ok(emitted)
            }
            RoutedBackendCommand::LoadUserProfile {
                account_id,
                user_id,
            } => {
                self.ensure_listener_started();
                let Some(sender) = self.inbound_sender.clone() else {
                    return Ok(Vec::new());
                };
                let local_store = Arc::clone(&self.local_store);
                let dedupe_key = format!("load_user_profile:{}", user_id.0);
                let _ = task_runtime::spawn_task(TaskPriority::High, Some(dedupe_key), move || {
                    run_load_user_profile(sender, local_store, account_id, user_id)
                });
                Ok(Vec::new())
            }
            RoutedBackendCommand::LoadConversationMembers { conversation, .. } => {
                self.ensure_listener_started();
                let Some(sender) = self.inbound_sender.clone() else {
                    return Ok(Vec::new());
                };
                let conversation_id = canonical_conversation_id_from_provider_ref(&conversation);
                let dedupe_key = format!("load_conversation_members:{}", conversation_id.0);
                let _ = task_runtime::spawn_task(TaskPriority::High, Some(dedupe_key), move || {
                    run_load_conversation_members(sender, conversation_id);
                });
                Ok(Vec::new())
            }
            RoutedBackendCommand::RefreshParticipants {
                user_id,
                conversation_id,
                ..
            } => {
                self.ensure_listener_started();
                let Some(sender) = self.inbound_sender.clone() else {
                    return Ok(Vec::new());
                };
                let local_store = Arc::clone(&self.local_store);
                let dedupe_key = format!(
                    "refresh_participants:{}:{}",
                    user_id.0,
                    conversation_id
                        .as_ref()
                        .map(|id| id.0.as_str())
                        .unwrap_or("none")
                );
                let _ = task_runtime::spawn_task(TaskPriority::High, Some(dedupe_key), move || {
                    run_refresh_profile_presence(sender, local_store, user_id, conversation_id)
                });
                Ok(Vec::new())
            }
            RoutedBackendCommand::LoadSocialGraphList {
                user_id, list_type, ..
            } => {
                self.ensure_listener_started();
                let Some(sender) = self.inbound_sender.clone() else {
                    return Ok(Vec::new());
                };
                let local_store = Arc::clone(&self.local_store);
                let dedupe_key = format!("load_social_graph:{}:{list_type:?}", user_id.0);
                let _ = task_runtime::spawn_task(TaskPriority::High, Some(dedupe_key), move || {
                    run_load_social_graph_list(sender, local_store, user_id, list_type)
                });
                Ok(Vec::new())
            }
            RoutedBackendCommand::FollowUser { user_id, .. } => {
                self.ensure_listener_started();
                let Some(sender) = self.inbound_sender.clone() else {
                    return Ok(Vec::new());
                };
                let dedupe_key = format!("follow_user:{}", user_id.0);
                let _ = task_runtime::spawn_task(TaskPriority::High, Some(dedupe_key), move || {
                    run_follow_toggle(sender, user_id, true)
                });
                Ok(Vec::new())
            }
            RoutedBackendCommand::UnfollowUser { user_id, .. } => {
                self.ensure_listener_started();
                let Some(sender) = self.inbound_sender.clone() else {
                    return Ok(Vec::new());
                };
                let dedupe_key = format!("unfollow_user:{}", user_id.0);
                let _ = task_runtime::spawn_task(TaskPriority::High, Some(dedupe_key), move || {
                    run_follow_toggle(sender, user_id, false)
                });
                Ok(Vec::new())
            }
            RoutedBackendCommand::ResolveChannel {
                account_id,
                team_name,
                channel_name,
                workspace_id,
                ..
            } => {
                let mut emitted = Vec::new();
                if let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let team_name_for_call = team_name.clone();
                    let channel_name_for_call = channel_name.clone();
                    let resolve_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        let inbox = fetch_inbox_for_team_topic(
                            &mut client,
                            &team_name_for_call,
                            &channel_name_for_call,
                        )
                        .await?;
                        Ok::<_, io::Error>(inbox)
                    });
                    match resolve_result {
                        Ok(inbox) => {
                            let parsed = parse_inbox_conversations(&inbox, None, true);
                            let resolved = parsed
                                .iter()
                                .find(|item| {
                                    item.summary.kind == ConversationKind::Channel
                                        && item.summary.group.as_ref().is_some_and(|group| {
                                            group.id.eq_ignore_ascii_case(&team_name)
                                        })
                                        && item.summary.title.eq_ignore_ascii_case(&channel_name)
                                })
                                .cloned();
                            if let Some(resolved) = resolved {
                                let conversation_binding = ConversationBinding {
                                    conversation_id: resolved.summary.id.clone(),
                                    backend_id: self.backend_id.clone(),
                                    account_id,
                                    provider_conversation_ref: resolved.provider_ref,
                                };
                                emitted.push(BackendEvent::ChannelResolved {
                                    workspace_id,
                                    conversation: resolved.summary,
                                    conversation_binding,
                                    can_post: resolved.can_post,
                                });
                            } else {
                                tracing::warn!(
                                    "ResolveChannel: no match for {}#{} among {} parsed conversations",
                                    team_name,
                                    channel_name,
                                    parsed.len()
                                );
                                emitted.push(BackendEvent::ChannelResolveFailed {
                                    workspace_id,
                                    channel_name: channel_name.clone(),
                                    error: format!(
                                        "No matching channel found for {}#{}",
                                        team_name, channel_name
                                    ),
                                });
                            }
                        }
                        Err(error) => {
                            emitted.push(BackendEvent::ChannelResolveFailed {
                                workspace_id,
                                channel_name,
                                error: error.to_string(),
                            });
                        }
                    }
                } else {
                    emitted.push(BackendEvent::ChannelResolveFailed {
                        workspace_id,
                        channel_name,
                        error: "Unable to connect to Keybase service".to_string(),
                    });
                }
                Ok(emitted)
            }
            RoutedBackendCommand::ResolveChannelById {
                account_id,
                conversation,
                workspace_id,
            } => {
                let mut emitted = Vec::new();
                let raw_conversation_id = provider_ref_to_conversation_id_bytes(&conversation);
                if let Some(raw_id) = raw_conversation_id
                    && let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let resolve_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        fetch_inbox_for_conversation_id(&mut client, &raw_id).await
                    });
                    match resolve_result {
                        Ok(inbox) => {
                            let parsed = parse_inbox_conversations(&inbox, None, true);
                            if let Some(resolved) = parsed.into_iter().next() {
                                let conversation_binding = ConversationBinding {
                                    conversation_id: resolved.summary.id.clone(),
                                    backend_id: self.backend_id.clone(),
                                    account_id,
                                    provider_conversation_ref: resolved.provider_ref,
                                };
                                emitted.push(BackendEvent::ChannelResolved {
                                    workspace_id,
                                    conversation: resolved.summary,
                                    conversation_binding,
                                    can_post: resolved.can_post,
                                });
                            } else {
                                emitted.push(BackendEvent::ChannelResolveFailed {
                                    workspace_id,
                                    channel_name: conversation.0,
                                    error: "Channel not found by ID".to_string(),
                                });
                            }
                        }
                        Err(error) => {
                            emitted.push(BackendEvent::ChannelResolveFailed {
                                workspace_id,
                                channel_name: conversation.0,
                                error: error.to_string(),
                            });
                        }
                    }
                } else {
                    emitted.push(BackendEvent::ChannelResolveFailed {
                        workspace_id,
                        channel_name: conversation.0,
                        error: "Unable to resolve channel ID".to_string(),
                    });
                }
                Ok(emitted)
            }
            RoutedBackendCommand::SetConversationNotificationSettings {
                conversation,
                settings,
                ..
            } => {
                let raw_id =
                    provider_ref_to_conversation_id_bytes(&conversation).ok_or_else(|| {
                        BackendError::OperationFailed(
                            "invalid provider conversation reference".into(),
                        )
                    })?;
                with_keybase_client(async |client| {
                    set_conversation_notification_settings_rpc(client, &raw_id, settings).await
                })?;
                let conversation_id =
                    ConversationId::new(format!("kb_conv:{}", hex_encode(&raw_id)));
                Ok(vec![BackendEvent::ConversationNotificationSettingsSynced {
                    conversation_id,
                    settings,
                }])
            }
            RoutedBackendCommand::SetNotifySound { enabled, .. } => {
                with_keybase_client(async |client| save_notify_sound(client, enabled).await)?;
                Ok(Vec::new())
            }
            RoutedBackendCommand::LeaveChannel {
                conversation,
                workspace_id,
                ..
            } => {
                self.ensure_listener_started();
                let sender = self.inbound_sender.clone().ok_or_else(|| {
                    BackendError::OperationFailed("Keybase connection is unavailable".into())
                })?;
                channel_membership::start_leave(
                    sender,
                    Arc::clone(&self.local_store),
                    workspace_id,
                    conversation,
                );
                Ok(Vec::new())
            }
            RoutedBackendCommand::JoinChannel {
                conversation,
                workspace_id,
                ..
            } => {
                if let Some(path) = socket_path()
                    && let Ok(runtime) = Builder::new_current_thread().enable_all().build()
                {
                    let join_result = runtime.block_on(async move {
                        let transport = FramedMsgpackTransport::connect(&path).await?;
                        let mut client = KeybaseRpcClient::new(transport);
                        let raw_conversation_id = provider_ref_to_conversation_id_bytes(
                            &conversation,
                        )
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "invalid provider conversation reference",
                            )
                        })?;
                        let response =
                            call_join_conversation_by_id_local(&mut client, &raw_conversation_id)
                                .await?;
                        channel_membership::ensure_online(&response)?;
                        Ok::<_, io::Error>(raw_conversation_id)
                    });
                    match join_result {
                        Ok(raw_conversation_id) => {
                            let conversation_id = ConversationId::new(format!(
                                "kb_conv:{}",
                                hex_encode(&raw_conversation_id)
                            ));
                            self.local_store
                                .set_conversation_left(&conversation_id, false)
                                .map_err(|error| {
                                    BackendError::OperationFailed(error.to_string())
                                })?;
                            tracing::info!(
                                "JoinChannel: successfully joined {}",
                                conversation_id.0
                            );
                            return Ok(vec![BackendEvent::ChannelJoined {
                                workspace_id,
                                conversation_id,
                            }]);
                        }
                        Err(e) => {
                            tracing::warn!("JoinChannel: RPC error: {e}");
                        }
                    }
                }
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn poll_events(&mut self) -> Vec<BackendEvent> {
        let mut events = Vec::new();
        let Some(receiver) = &self.inbound_events else {
            return events;
        };
        while events.len() < MAX_EVENTS_PER_POLL {
            let Ok(event) = receiver.try_recv() else {
                break;
            };
            events.push(event);
        }
        events
    }
}

impl Drop for KeybaseBackend {
    fn drop(&mut self) {
        self.listener_shutdown.store(true, Ordering::Relaxed);
        self.sync.cancel();
    }
}

fn reconnect_delay(attempt: u32) -> Duration {
    Duration::from_millis(500u64.saturating_mul(1u64 << attempt.min(6)))
        .min(Duration::from_secs(30))
}

fn run_listener(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    pending_outbox_sends: Arc<Mutex<PendingOutboxSends>>,
    account_id: AccountId,
    shutdown: Arc<AtomicBool>,
) {
    ensure_service_running();
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = sender.send(BackendEvent::AccountDisconnected {
                account_id,
                reason: error.to_string(),
            });
            return;
        }
    };
    runtime.block_on(listen_with_reconnect(
        &sender,
        &local_store,
        &pending_outbox_sends,
        &account_id,
        &shutdown,
        socket_path,
    ));
}

async fn listen_with_reconnect(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    pending_outbox_sends: &Arc<Mutex<PendingOutboxSends>>,
    account_id: &AccountId,
    shutdown: &AtomicBool,
    resolve_socket: impl Fn() -> Option<PathBuf>,
) {
    let mut attempt = 0;
    while !shutdown.load(Ordering::Relaxed) {
        let started = Instant::now();
        let result = match resolve_socket() {
            Some(path) => {
                run_listener_session(
                    &path,
                    sender,
                    local_store,
                    pending_outbox_sends,
                    account_id,
                    shutdown,
                )
                .await
            }
            None => Err(io::Error::other("Keybase socket path missing")),
        };
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        if started.elapsed() >= Duration::from_secs(60) {
            attempt = 0;
        }
        let reason = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "Keybase notification connection closed".into());
        if sender
            .send(BackendEvent::AccountDisconnected {
                account_id: account_id.clone(),
                reason,
            })
            .is_err()
        {
            break;
        }
        let delay = reconnect_delay(attempt);
        attempt = attempt.saturating_add(1);
        let deadline = tokio::time::Instant::now() + delay;
        while tokio::time::Instant::now() < deadline && !shutdown.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn run_listener_session(
    path: &Path,
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    pending_outbox_sends: &Arc<Mutex<PendingOutboxSends>>,
    account_id: &AccountId,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    let (client, badges, callbacks, notify_sound) =
        tokio::time::timeout(Duration::from_secs(15), async {
            let transport = FramedMsgpackTransport::connect(path).await?;
            let mut client = KeybaseRpcClient::new(transport);
            // Before subscribing: a plain call would drop interleaved notifications.
            let notify_sound = fetch_notify_sound(&mut client).await;
            client
                .set_notifications(NotificationChannels::all_enabled())
                .await?;
            // Request a full snapshot on every subscription, including reconnects.
            // getBadgeState includes the daemon's locally applied read state.
            let (badges, callbacks) = client
                .call_collecting_callbacks_until_response("keybase.1.badger.getBadgeState", vec![])
                .await?;
            Ok::<_, io::Error>((client, badges, callbacks, notify_sound))
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Keybase notification subscription timed out",
            )
        })??;
    let counts = parse_badge_state(&badges)
        .ok_or_else(|| io::Error::other("invalid Keybase badge snapshot"))?;
    emit_badge_state(sender, local_store, counts, true)?;
    if let Some(enabled) = notify_sound {
        sender
            .send(BackendEvent::NotifySoundLoaded { enabled })
            .map_err(|_| io::Error::other("UI closed"))?;
    }
    sender
        .send(BackendEvent::AccountConnected {
            account_id: account_id.clone(),
        })
        .map_err(|_| io::Error::other("UI closed"))?;
    // Badge callbacks preceding the snapshot response are superseded by it.
    // Preserve other activity callbacks; their inferred counts cannot overwrite
    // the authoritative snapshot.
    let mut pending_callbacks = callbacks
        .into_iter()
        .filter(|callback| callback.method != "keybase.1.NotifyBadges.badgeState")
        .collect::<std::collections::VecDeque<_>>();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<RpcNotification>(512);
    let notification_task = tokio::spawn(async move { client.run_notification_loop(tx).await });
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        // timeout preserves the ability to cancel an otherwise idle socket.
        let message = if let Some(message) = pending_callbacks.pop_front() {
            message
        } else {
            match tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(_) => continue,
            }
        };
        let event = KeybaseNotifyEvent::from_method(&message.method, message.params);
        if event.method_name() == "keybase.1.NotifyBadges.badgeState" {
            let params = match &event {
                KeybaseNotifyEvent::Known { raw_params, .. }
                | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
            };
            if let Some(counts) =
                find_value_for_keys(params, &["badgeState"], 0).and_then(parse_badge_state)
                && let Err(error) = emit_badge_state(sender, local_store, counts, true)
            {
                notification_task.abort();
                return Err(error);
            }
        }
        maybe_handle_tracking_notify(&event, sender);
        maybe_handle_presence_notify(&event, sender);
        maybe_handle_profile_notify(&event, sender, local_store);
        maybe_handle_emoji_notify(&event, sender, local_store);
        maybe_handle_team_role_notify(&event, sender, local_store);
        channel_membership::handle_notify(&event, sender, local_store);
        maybe_handle_new_conversation_notify(&event, sender, local_store);
        maybe_handle_team_type_notify(&event, sender, local_store);
        if let Some(progress) = parse_attachment_upload_progress(&event)
            && let Some(client_message_id) = pending_outbox_sends.lock().ok().and_then(|pending| {
                pending
                    .pending
                    .get(&progress.outbox_id)
                    .map(|meta| meta.client_message_id.clone())
            })
            && sender
                .send(BackendEvent::AttachmentUploadProgress {
                    client_message_id,
                    sent_bytes: progress.sent_bytes,
                    total_bytes: progress.total_bytes,
                })
                .is_err()
        {
            break;
        }
        let mut should_refresh_inbox = notify_should_refresh_inbox(&event);
        let mut emitted_unread_for_conversation: Option<ConversationId> = None;
        if let Some(read_update) = parse_read_marker_update_from_notify(&event) {
            if sender
                .send(BackendEvent::ReadMarkerUpdated {
                    conversation_id: read_update.conversation_id.clone(),
                    read_upto: Some(read_update.read_upto.clone()),
                })
                .is_err()
            {
                break;
            }
            if let Some(snapshot) = read_update.snapshot {
                if !emit_unread_snapshot_event(sender, local_store, &snapshot) {
                    break;
                }
                emitted_unread_for_conversation = Some(snapshot.conversation_id.clone());
            } else {
                should_refresh_inbox = true;
            }
        }
        if let Some(snapshot) = parse_conversation_unread_snapshot_from_notify(&event)
            && emitted_unread_for_conversation.as_ref() != Some(&snapshot.conversation_id)
            && !emit_unread_snapshot_event(sender, local_store, &snapshot)
        {
            break;
        }
        if should_refresh_inbox && request_inbox_unread_refresh() {
            let sender_for_refresh = sender.clone();
            let local_store_for_refresh = Arc::clone(local_store);
            let _ = task_runtime::spawn_task(TaskPriority::Low, None, move || {
                loop {
                    refresh_inbox_unread_state(&sender_for_refresh, &local_store_for_refresh);
                    if !finish_inbox_unread_refresh() {
                        break;
                    }
                }
            });
        }
        for (conversation_id, users) in parse_typing_updates_from_notify(&event) {
            if sender
                .send(BackendEvent::TypingUpdated {
                    conversation_id,
                    users,
                })
                .is_err()
            {
                break;
            }
        }
        let mut handled_reaction = false;
        if let Some(reaction_delta) = parse_live_reaction_delta_from_notify(&event) {
            persist_reaction_deltas(
                Some(sender),
                local_store,
                std::slice::from_ref(&reaction_delta),
            );
            handled_reaction = true;
            let delete_events = reaction_op_delete_events(std::slice::from_ref(&reaction_delta));
            if sender
                .send(BackendEvent::MessageReactionApplied {
                    conversation_id: reaction_delta.conversation_id.clone(),
                    message_id: reaction_delta.target_message_id.clone(),
                    emoji: reaction_delta.emoji.clone(),
                    source_ref: reaction_delta.source_ref.clone(),
                    actor_id: reaction_delta.actor_id.clone(),
                    updated_ms: reaction_delta.updated_ms,
                })
                .is_err()
            {
                break;
            }
            for delete_event in delete_events {
                if sender.send(delete_event).is_err() {
                    break;
                }
            }
        }
        if !handled_reaction && let Some(mut live_message) = parse_live_message_from_notify(&event)
        {
            if let Some(reaction_removed_event) =
                reaction_removed_event_for_live_delete(local_store, &live_message)
            {
                if sender.send(reaction_removed_event).is_err() {
                    break;
                }
            } else if let Some(delete_event) =
                regular_message_delete_event(local_store, &live_message)
            {
                if sender.send(delete_event).is_err() {
                    break;
                }
            } else {
                let pending_send_meta =
                    extract_outbox_id_from_notify(&event).and_then(|outbox_id| {
                        pending_outbox_sends
                            .lock()
                            .ok()
                            .and_then(|mut pending| pending.confirm(outbox_id, &live_message))
                    });
                let live_reaction_map_deltas = parse_live_reaction_map_deltas_from_notify(
                    &event,
                    &live_message.conversation_id,
                    &live_message.id,
                );
                let username = live_message.author_id.0.clone();
                spawn_prefetch_user_profiles(sender, local_store, vec![username]);
                if message_contains_shortcode(&live_message) {
                    spawn_sync_conversation_emojis(
                        sender,
                        local_store,
                        live_message.conversation_id.clone(),
                    );
                }
                if let Some(ref meta) = pending_send_meta
                    && let Some(ref local_path) = meta.local_attachment_path
                {
                    stamp_local_attachment_path(&mut live_message, local_path);
                }
                ingest_message_record(Some(sender), local_store, &mut live_message);
                spawn_live_attachment_hydration(sender, local_store, &live_message);
                let reaction_sync_event = if live_reaction_map_deltas.is_empty() {
                    None
                } else {
                    persist_reaction_deltas(Some(sender), local_store, &live_reaction_map_deltas);
                    for delete_event in reaction_op_delete_events(&live_reaction_map_deltas) {
                        if sender.send(delete_event).is_err() {
                            break;
                        }
                    }
                    message_reaction_sync_event(
                        local_store,
                        &live_message.conversation_id,
                        std::slice::from_ref(&live_message),
                    )
                };
                if let Some(pending_send) = pending_send_meta {
                    if sender
                        .send(BackendEvent::MessageSendConfirmed {
                            op_id: pending_send.op_id,
                            client_message_id: pending_send.client_message_id,
                            server_message: live_message,
                        })
                        .is_err()
                    {
                        break;
                    }
                } else if sender
                    .send(BackendEvent::MessageUpserted(live_message))
                    .is_err()
                {
                    break;
                }
                if let Some(sync_event) = reaction_sync_event
                    && sender.send(sync_event).is_err()
                {
                    break;
                }
            }
        }
        if let Some(notification) = parse_desktop_notification_from_notify(&event)
            && sender
                .send(BackendEvent::DesktopNotification(notification))
                .is_err()
        {
            break;
        }
        if let Some(stub_event) = KeybaseBackend::map_notify_event(event)
            && sender.send(stub_event).is_err()
        {
            break;
        }
    }
    notification_task.abort();
    Err(io::Error::new(
        io::ErrorKind::ConnectionReset,
        "Keybase notification stream ended",
    ))
}

const WORKSPACE_ID: &str = "ws_primary";
const WORKSPACE_NAME: &str = "Keybase";
const PROVIDER_WORKSPACE_REF: &str = "keybase:workspace:primary";
const KEYBASE_BACKEND_ID: &str = "keybase";
const DEFAULT_ACCOUNT_ID: &str = "account_demo_keybase";
const SESSION_ID: i64 = 0;

const KEYBASE_GET_CURRENT_STATUS: &str = "keybase.1.config.getCurrentStatus";
const KEYBASE_LOAD_USER_AVATARS: &str = "keybase.1.avatars.loadUserAvatars";
const KEYBASE_LOAD_TEAM_AVATARS: &str = "keybase.1.avatars.loadTeamAvatars";
const KEYBASE_LOAD_USER: &str = "keybase.1.user.loadUser";
const KEYBASE_LOAD_USER_BY_NAME: &str = "keybase.1.user.loadUserByName";
const KEYBASE_USER_CARD: &str = "keybase.1.user.userCard";
const KEYBASE_LIST_TRACKING: &str = "keybase.1.user.listTracking";
const KEYBASE_LIST_TRACKERS_UNVERIFIED: &str = "keybase.1.user.listTrackersUnverified";
const KEYBASE_LIST_TRACKING_JSON: &str = "keybase.1.user.listTrackingJSON";
const KEYBASE_LIST_TRACKING_FALLBACK: &str = "keybase.1.track.listTracking";
const KEYBASE_IDENTIFY3: &str = "keybase.1.identify3.identify3";
const KEYBASE_IDENTIFY3_FOLLOW_USER: &str = "keybase.1.identify3.identify3FollowUser";
const KEYBASE_TEAM_GET_MEMBERS_BY_ID: &str = "keybase.1.teams.teamGetMembersByID";
const CHAT_GET_INBOX_SUMMARY_CLI_LOCAL: &str = "chat.1.local.getInboxSummaryForCLILocal";
const CHAT_GET_INBOX_AND_UNBOX_LOCAL: &str = "chat.1.local.getInboxAndUnboxLocal";
const CHAT_GET_THREAD_LOCAL: &str = "chat.1.local.getThreadLocal";
const CHAT_POST_TEXT_NONBLOCK: &str = "chat.1.local.postTextNonblock";
const CHAT_POST_FILE_ATTACHMENT_LOCAL_NONBLOCK: &str =
    "chat.1.local.postFileAttachmentLocalNonblock";
const CHAT_POST_EDIT_NONBLOCK: &str = "chat.1.local.postEditNonblock";
const CHAT_POST_DELETE_NONBLOCK: &str = "chat.1.local.postDeleteNonblock";
const CHAT_POST_REACTION_NONBLOCK: &str = "chat.1.local.postReactionNonblock";
const CHAT_DOWNLOAD_FILE_ATTACHMENT_LOCAL: &str = "chat.1.local.DownloadFileAttachmentLocal";
const CHAT_USER_EMOJIS: &str = "chat.1.local.userEmojis";
const CHAT_MARK_AS_READ_LOCAL: &str = "chat.1.local.markAsReadLocal";
const CHAT_SET_CONVERSATION_STATUS_LOCAL: &str = "chat.1.local.SetConversationStatusLocal";
const CHAT_SET_APP_NOTIFICATION_SETTINGS_LOCAL: &str =
    "chat.1.local.setAppNotificationSettingsLocal";
const CHAT_REFRESH_PARTICIPANTS_LOCAL: &str = "chat.1.local.refreshParticipants";
const CHAT_GET_PARTICIPANTS_LOCAL: &str = "chat.1.local.getParticipants";
const CHAT_GET_LAST_ACTIVE_FOR_TLF_LOCAL: &str = "chat.1.local.getLastActiveForTLF";
const CHAT_PIN_MESSAGE_LOCAL: &str = "chat.1.local.pinMessage";
const CHAT_UNPIN_MESSAGE_LOCAL: &str = "chat.1.local.unpinMessage";
const CHAT_NEW_CONVERSATION_LOCAL: &str = "chat.1.local.newConversationLocal";
const CHAT_FIND_CONVERSATIONS_LOCAL: &str = "chat.1.local.findConversationsLocal";
const CHAT_JOIN_CONVERSATION_BY_ID_LOCAL: &str = "chat.1.local.joinConversationByIDLocal";
const CHAT_TEAM_ID_OF_CONV: &str = "chat.1.remote.teamIDOfConv";
const CHAT_TEAM_ID_FROM_TLF_NAME: &str = "chat.1.local.teamIDFromTLFName";
const KEYBASE_USER_SEARCH: &str = "keybase.1.userSearch.userSearch";

const TOPIC_TYPE_CHAT: i64 = 1;
const TLF_VISIBILITY_ANY: i64 = 0;
const TLF_VISIBILITY_PRIVATE: i64 = 0;
const TLF_VISIBILITY_PUBLIC: i64 = 1;
const GET_THREAD_REASON_FOREGROUND: i64 = 2;
const IDENTIFY_BEHAVIOR_CHAT_GUI: i64 = 2;
const TEAM_MEMBERS_TYPE: i64 = 1;
const IMPTEAM_MEMBERS_TYPE: i64 = 2;
const CONVERSATION_STATUS_UNFILED: i64 = 0;
const CONVERSATION_STATUS_FAVORITE: i64 = 1;
const CONVERSATION_STATUS_MUTED: i64 = 4;
const CONVERSATION_MEMBER_STATUS_ACTIVE: i64 = 0;
const MESSAGE_ID_CONTROL_MODE_OLDER: i64 = 0;
const MESSAGE_ID_CONTROL_MODE_NEWER: i64 = 1;
const MESSAGE_TYPE_TEXT: i64 = 1;
const MESSAGE_TYPE_ATTACHMENT: i64 = 2;
const MESSAGE_TYPE_EDIT: i64 = 3;
const MESSAGE_TYPE_DELETE: i64 = 4;
const MESSAGE_TYPE_METADATA: i64 = 5;
const MESSAGE_TYPE_HEADLINE: i64 = 7;
const MESSAGE_TYPE_ATTACHMENT_UPLOADED: i64 = 8;
const MESSAGE_TYPE_JOIN: i64 = 9;
const MESSAGE_TYPE_LEAVE: i64 = 10;
const MESSAGE_TYPE_SYSTEM: i64 = 11;
const MESSAGE_TYPE_DELETE_HISTORY: i64 = 12;
const MESSAGE_TYPE_REACTION: i64 = 13;
const MESSAGE_TYPE_SEND_PAYMENT: i64 = 14;
const MESSAGE_TYPE_REQUEST_PAYMENT: i64 = 15;
const MESSAGE_TYPE_UNFURL: i64 = 16;
const MESSAGE_TYPE_FLIP: i64 = 17;
const MESSAGE_TYPE_PIN: i64 = 18;
const MESSAGE_SYSTEM_TYPE_ADDED_TO_TEAM: i64 = 0;
const MESSAGE_SYSTEM_TYPE_INVITE_ADDED_TO_TEAM: i64 = 1;
const MESSAGE_SYSTEM_TYPE_COMPLEX_TEAM: i64 = 2;
const MESSAGE_SYSTEM_TYPE_CREATE_TEAM: i64 = 3;
const MESSAGE_SYSTEM_TYPE_GIT_PUSH: i64 = 4;
const MESSAGE_SYSTEM_TYPE_CHANGE_AVATAR: i64 = 5;
const MESSAGE_SYSTEM_TYPE_CHANGE_RETENTION: i64 = 6;
const MESSAGE_SYSTEM_TYPE_BULK_ADD_TO_CONV: i64 = 7;
const MESSAGE_SYSTEM_TYPE_SBS_RESOLVE: i64 = 8;
const MESSAGE_SYSTEM_TYPE_NEW_CHANNEL: i64 = 9;

const AVATAR_FORMAT_SQUARE_128: &str = "square_192";
const PROFILE_CACHE_TTL_MS: i64 = 24 * 60 * 60 * 1000;
const EMOJI_CACHE_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const TEAM_EMOJI_DUAL_WRITE_MIGRATION_ENABLED: bool = false;
const TEAM_EMOJI_CONVERSATION_FALLBACK_READ_ENABLED: bool = true;
const LEGACY_CONVERSATION_EMOJI_FILE_RETENTION_MS: i64 = 14 * 24 * 60 * 60 * 1000;
const TEAM_ROLE_CACHE_TTL_MS: i64 = 6 * 60 * 60 * 1000;
const TEAM_ID_BYTES_LEN: usize = 16;
const CACHED_BOOTSTRAP_INITIAL_CONVERSATION_LIMIT: usize = 300;
const CACHED_BOOTSTRAP_SELECTED_MESSAGE_LIMIT: usize = 100;
const CONVERSATION_EXTENSION_BATCH_SIZE: usize = 500;
const INBOX_CACHE_PAGE_SIZE: usize = 500;
const INBOX_CACHE_MAX_CONVERSATIONS: usize = 20_000;
const CONVERSATION_OPEN_CACHE_LOAD_LIMIT: usize = 200;
const CONVERSATION_OPEN_LIVE_FETCH_PAGE_SIZE: usize = CONVERSATION_OPEN_CACHE_LOAD_LIMIT;
const ATTACHMENT_DOWNLOAD_LIMIT_PER_PAGE: usize = 8;
const CRAWL_PAGE_SIZE: usize = 200;
const THREAD_REINDEX_SCAN_LIMIT: usize = 20_000;
const THREAD_BACKFILL_OLDER_PAGE_LIMIT: usize = 8;
const THREAD_BACKFILL_NEWER_PAGE_LIMIT: usize = 200;
const REPLY_ANCESTOR_BACKFILL_PAGE_LIMIT: usize = 48;
const THREAD_EDGE_REPAIR_SCAN_LIMIT: usize = THREAD_REINDEX_SCAN_LIMIT;
const THREAD_EDGE_MIGRATION_MAX_CONVERSATIONS: usize = 100_000;
const THREAD_DEBUG_SCAN_LIMIT: usize = 20_000;
const THREAD_DEBUG_SAMPLE_LIMIT: usize = 20;
const THREAD_DEBUG_DEFAULT_TARGET_ROOT: &str = "1101";
const CRAWL_PROGRESS_EMIT_EVERY_PAGES: u64 = 10;
const LOAD_OLDER_PAGE_SIZE: usize = 100;
const MAX_EVENTS_PER_POLL: usize = 64;
const NON_TEXT_PLACEHOLDER_BODY: &str = "<non-text message>";
const LOAD_CONVERSATION_MAX_ATTEMPTS: usize = 3;
const LOAD_CONVERSATION_RETRY_BASE_DELAY_MS: u64 = 150;
const PROFILE_SYNC_COOLDOWN_MS: i64 = 15_000;
const TEAM_AVATAR_SYNC_COOLDOWN_MS: i64 = 15_000;
const EMOJI_SYNC_COOLDOWN_MS: i64 = 10_000;
const EMOJI_SOURCE_SYNC_COOLDOWN_MS: i64 = 10_000;

static REPLY_ANCESTOR_BACKFILL_IN_FLIGHT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static PROFILE_SYNC_IN_FLIGHT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static PROFILE_SYNC_LAST_START_MS: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
static TEAM_AVATAR_SYNC_IN_FLIGHT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static TEAM_AVATAR_SYNC_LAST_START_MS: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
static EMOJI_SYNC_IN_FLIGHT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static EMOJI_SYNC_LAST_START_MS: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
static EMOJI_SOURCE_SYNC_IN_FLIGHT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static EMOJI_SOURCE_SYNC_LAST_START_MS: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
static INBOX_UNREAD_REFRESH: OnceLock<Mutex<InboxUnreadRefresh>> = OnceLock::new();

#[derive(Default)]
struct InboxUnreadRefresh {
    running: bool,
    pending: bool,
}

impl InboxUnreadRefresh {
    fn request(&mut self) -> bool {
        if self.running {
            self.pending = true;
            false
        } else {
            self.running = true;
            true
        }
    }

    fn finish(&mut self) -> bool {
        if std::mem::take(&mut self.pending) {
            true
        } else {
            self.running = false;
            false
        }
    }
}
static NON_TEXT_PLACEHOLDER_LOGGED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn non_text_placeholder_key_preview(message_body: Option<&Value>) -> Vec<String> {
    let Some(Value::Map(entries)) = message_body else {
        return Vec::new();
    };
    entries
        .iter()
        .take(16)
        .map(|(key, _)| match key {
            Value::String(value) => value.as_str().unwrap_or("").to_string(),
            other => format!("{other:?}"),
        })
        .filter(|key| !key.trim().is_empty())
        .collect()
}

fn log_non_text_placeholder_once(
    conversation_id: &ConversationId,
    message_id: i64,
    message_type: Option<i64>,
    message_body: Option<&Value>,
) {
    let key = format!(
        "{}:{}",
        conversation_id.0,
        message_type.map_or_else(|| "unknown".to_string(), |kind| kind.to_string())
    );
    let set = NON_TEXT_PLACEHOLDER_LOGGED.get_or_init(|| Mutex::new(HashSet::new()));
    let mut set = set.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if !set.insert(key.clone()) {
        return;
    }

    let keys = non_text_placeholder_key_preview(message_body);
    warn!(
        target: "zbase.keybase.non_text_placeholder",
        conversation_id = %conversation_id.0,
        message_id = message_id,
        message_type = message_type,
        message_body_keys = %keys.join(","),
        "suppressing non-text placeholder message; add parser mapping for this Keybase message type"
    );
}

fn is_unset_message_type_only(message_body: Option<&Value>) -> bool {
    let Some(body) = message_body else {
        return false;
    };
    let keys = non_text_placeholder_key_preview(Some(body));
    if keys.len() != 1 {
        return false;
    }
    let key = keys[0].as_str();
    if key != "messageType" && key != "mt" && key != "t" {
        return false;
    }
    map_get_any(body, &["messageType", "mt", "t"])
        .and_then(as_i64)
        .is_some_and(|value| value == 0)
}

fn run_bootstrap(
    sender: Sender<BackendEvent>,
    backend_id: BackendId,
    account_id: AccountId,
    local_store: Arc<LocalStore>,
    sync: Arc<SyncService>,
) {
    let mut outcome = BackendTaskOutcome::new(
        &sender,
        BackendCommand::LoadBootstrap {
            account_id: account_id.clone(),
        },
    );
    send_internal(&sender, "zbase.internal.bootstrap_starting", Value::Nil);
    let _ = sender.send(BackendEvent::BootStatus("Loading local data…".to_string()));

    for conversation_id in local_store
        .left_conversation_snapshot()
        .unwrap_or_default()
        .into_keys()
    {
        let _ = sender.send(BackendEvent::ChannelLeft {
            workspace_id: WorkspaceId::new(WORKSPACE_ID),
            conversation_id,
        });
    }
    match local_store.load_bootstrap_seed(
        CACHED_BOOTSTRAP_INITIAL_CONVERSATION_LIMIT,
        CACHED_BOOTSTRAP_SELECTED_MESSAGE_LIMIT,
    ) {
        Ok(Some(seed)) => {
            let payload = bootstrap_payload_from_cache(seed, &backend_id, &account_id);
            prefetch_user_profiles_from_payload(&sender, &payload, &local_store);
            prefetch_team_avatars_from_payload(&sender, &payload, &local_store);
            sync_selected_conversation_emojis_from_payload(&sender, &local_store, &payload);
            let cached_workspace_id = payload
                .active_workspace_id
                .clone()
                .unwrap_or_else(|| WorkspaceId::new(WORKSPACE_ID));
            let cached_conversation_ids = payload
                .channels
                .iter()
                .chain(payload.direct_messages.iter())
                .map(|summary| summary.id.clone())
                .collect::<HashSet<_>>();
            let _ = sender.send(BackendEvent::BootstrapLoaded {
                source: crate::state::event::BootstrapSource::Cache,
                account_id: account_id.clone(),
                payload,
            });
            start_background_cached_conversation_extension(
                &sender,
                &local_store,
                &backend_id,
                &account_id,
                cached_workspace_id,
                cached_conversation_ids,
            );
            start_background_thread_edge_migration(&sender, &local_store);
            send_internal(&sender, "zbase.internal.bootstrap_cache_hit", Value::Nil);
        }
        Ok(None) => {
            send_internal(&sender, "zbase.internal.bootstrap_cache_miss", Value::Nil);
        }
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.bootstrap_cache_load_failed",
                Value::from(error.to_string()),
            );
        }
    }

    let _ = sender.send(BackendEvent::BootStatus(
        "Checking Keybase service…".to_string(),
    ));

    if !ensure_service_running() {
        info!(target: "zbase.service", "service not running after auto-start attempt, will try connecting anyway");
    }

    let Some(path) = socket_path() else {
        return;
    };

    let _ = sender.send(BackendEvent::BootStatus(
        "Connecting to Keybase…".to_string(),
    ));

    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.bootstrap_runtime_init_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    if let Ok(affinities) = runtime.block_on(fetch_tracking_affinities_from_service(&path)) {
        let _ = sender.send(BackendEvent::AffinitySynced { affinities });
    }

    let _ = sender.send(BackendEvent::BootStatus(
        "Loading conversations…".to_string(),
    ));

    let left_snapshot = local_store.left_conversation_snapshot().unwrap_or_default();
    let account_for_bootstrap = account_id.clone();
    let backend_for_bootstrap = backend_id.clone();
    let result = runtime.block_on(async move {
        let mut last_error = io::Error::other("Could not refresh conversations");
        for attempt in 0..3 {
            match tokio::time::timeout(
                Duration::from_secs(30),
                bootstrap_payload_from_service(
                    &path,
                    &backend_for_bootstrap,
                    &account_for_bootstrap,
                ),
            )
            .await
            {
                Ok(Ok(payload)) => return Ok(payload),
                Ok(Err(error)) => last_error = error,
                Err(_) => {
                    last_error =
                        io::Error::new(io::ErrorKind::TimedOut, "Conversation refresh timed out")
                }
            }
            tokio::time::sleep(reconnect_delay(attempt)).await;
        }
        Err(last_error)
    });

    match result {
        Ok(mut payload) => {
            // Reconcile rejoins performed in Keybase while zBase was closed, without
            // letting an inbox request started before a leave undo that newer leave.
            if let Ok(joined) = local_store.reconcile_joined_conversations(
                &left_snapshot,
                payload.channels.iter().map(|summary| summary.id.clone()),
            ) {
                for conversation_id in joined {
                    let _ = sender.send(BackendEvent::ChannelJoined {
                        workspace_id: WorkspaceId::new(WORKSPACE_ID),
                        conversation_id,
                    });
                }
            }
            let crawl_refs = payload
                .conversation_bindings
                .iter()
                .map(|binding| binding.provider_conversation_ref.clone())
                .collect::<Vec<_>>();
            if let Err(error) = local_store.persist_bootstrap_payload(&payload) {
                send_internal(
                    &sender,
                    "zbase.internal.bootstrap_cache_persist_failed",
                    Value::from(error.to_string()),
                );
                return;
            }
            outcome.complete();
            payload.selected_messages = payload
                .selected_messages
                .iter()
                .filter_map(|message| {
                    local_store
                        .get_message(&message.conversation_id, &message.id)
                        .ok()
                        .flatten()
                })
                .collect();
            prefetch_user_profiles_from_payload(&sender, &payload, &local_store);
            prefetch_team_avatars_from_payload(&sender, &payload, &local_store);
            sync_selected_conversation_emojis_from_payload(&sender, &local_store, &payload);
            let account_display_name = payload.account_display_name.clone();
            let _ = sender.send(BackendEvent::BootstrapLoaded {
                source: crate::state::event::BootstrapSource::Server,
                account_id: account_id.clone(),
                payload,
            });
            start_background_full_history_crawl(&sender, &local_store, &sync, &crawl_refs);
            start_background_inbox_conversation_cache_refresh(
                &sender,
                &local_store,
                &backend_id,
                &account_id,
                account_display_name,
                Arc::clone(&sync),
            );
            start_background_thread_edge_migration(&sender, &local_store);
            send_internal(&sender, "zbase.internal.bootstrap_loaded", Value::Nil);
        }
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.bootstrap_failed",
                Value::from(error.to_string()),
            );
        }
    }
}

fn run_load_user_profile(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    account_id: AccountId,
    user_id: UserId,
) {
    let mut outcome = BackendTaskOutcome::new(
        &sender,
        BackendCommand::LoadUserProfile {
            user_id: user_id.clone(),
        },
    );
    let Some(username) = profile_username_from_user_id(&user_id) else {
        return;
    };
    let Some(path) = socket_path() else {
        send_internal(
            &sender,
            "zbase.internal.profile_load_socket_path_missing",
            Value::from(user_id.0.clone()),
        );
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.profile_load_runtime_init_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    let result = runtime.block_on(async {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);

        let user_card = client
            .call(
                KEYBASE_USER_CARD,
                vec![Value::Map(vec![
                    (Value::from("username"), Value::from(username.clone())),
                    (Value::from("useSession"), Value::from(true)),
                ])],
            )
            .await
            .unwrap_or(Value::Nil);

        let load_user = client
            .call(
                KEYBASE_LOAD_USER_BY_NAME,
                vec![Value::Map(vec![
                    (Value::from("sessionID"), Value::from(SESSION_ID)),
                    (Value::from("username"), Value::from(username.clone())),
                ])],
            )
            .await
            .unwrap_or(Value::Nil);

        let gui_id = format!(
            "zbase-profile-{}-{}",
            sanitize_username(&username),
            now_unix_ms()
        );
        let identify3_callbacks = client
            .call_collecting_callbacks(
                KEYBASE_IDENTIFY3,
                vec![Value::Map(vec![
                    (Value::from("assertion"), Value::from(username.clone())),
                    (Value::from("guiID"), Value::from(gui_id)),
                    (Value::from("ignoreCache"), Value::from(false)),
                ])],
            )
            .await
            .map(|(_result, callbacks)| callbacks)
            .unwrap_or_default();

        let display_name = parse_user_display_name(&load_user, &username)
            .or_else(|| parse_user_display_name(&user_card, &username))
            .unwrap_or_else(|| username.clone());
        let bio = find_value_for_keys(&user_card, &["bio"], 0)
            .and_then(as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let location = find_value_for_keys(&user_card, &["location"], 0)
            .and_then(as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let title = find_value_for_keys(&load_user, &["title", "headline"], 0)
            .or_else(|| find_value_for_keys(&user_card, &["title", "headline"], 0))
            .and_then(as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let followers_count = find_value_for_keys(&user_card, &["unverifiedNumFollowers"], 0)
            .and_then(value_to_u32_allow_zero);
        let following_count = find_value_for_keys(&user_card, &["unverifiedNumFollowing"], 0)
            .and_then(value_to_u32_allow_zero);
        let team_showcase = parse_team_showcase_entries(&user_card);
        let identity_proofs = parse_identify3_proof_callbacks(&identify3_callbacks);

        let avatar_urls = fetch_user_avatar_urls(&mut client, std::slice::from_ref(&username))
            .await
            .unwrap_or_default();
        let avatar_url = avatar_urls.get(&username).cloned();
        let mut avatar_path: Option<String> = None;
        let mut avatar_asset = validated_avatar_asset(avatar_url.clone());
        if let Some(url) = avatar_url.clone()
            && let Ok(path) = download_avatar_to_cache(&username, &url).await
        {
            let local_path = path.display().to_string();
            avatar_path = Some(local_path.clone());
            avatar_asset = Some(local_path);
        }

        let you_are_following = fetch_tracking_affinities_from_service(&path)
            .await
            .ok()
            .map(|affinities| {
                affinities.contains_key(&UserId::new(user_id.0.to_ascii_lowercase()))
                    || affinities.contains_key(&user_id)
            })
            .unwrap_or(false);

        let mut sections = Vec::new();
        if !identity_proofs.is_empty() {
            sections.push(ProfileSection::IdentityProofs(identity_proofs));
        }
        sections.push(ProfileSection::SocialGraph(SocialGraph {
            followers_count,
            following_count,
            is_following_you: false,
            you_are_following,
            followers: None,
            following: None,
        }));
        if !team_showcase.is_empty() {
            sections.push(ProfileSection::TeamShowcase(team_showcase));
        }

        let affinity = if you_are_following {
            Affinity::Positive
        } else {
            Affinity::None
        };
        let presence = Presence {
            availability: Availability::Unknown,
            status_text: None,
        };
        let profile = UserProfile {
            user_id: user_id.clone(),
            username: username.clone(),
            display_name: display_name.clone(),
            avatar_asset: avatar_asset.clone(),
            presence,
            affinity,
            bio,
            location,
            title,
            sections,
        };

        let updated_ms = now_unix_ms();
        let _ = local_store.upsert_user_profile(
            &user_id,
            display_name.clone(),
            avatar_url,
            avatar_path,
            updated_ms,
        );
        Ok::<(UserProfile, String, Option<String>, i64), io::Error>((
            profile,
            display_name,
            avatar_asset,
            updated_ms,
        ))
    });

    match result {
        Ok((profile, display_name, avatar_asset, updated_ms)) => {
            let _ = sender.send(BackendEvent::UserProfileUpserted {
                user_id: profile.user_id.clone(),
                display_name,
                avatar_asset: validated_avatar_asset(avatar_asset),
                updated_ms,
            });
            outcome.complete();
            let _ = sender.send(BackendEvent::UserProfileLoaded {
                account_id,
                profile,
            });
        }
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.profile_load_failed",
                Value::Map(vec![
                    (Value::from("user_id"), Value::from(user_id.0)),
                    (Value::from("error"), Value::from(error.to_string())),
                ]),
            );
        }
    }
}

fn run_load_social_graph_list(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    user_id: UserId,
    list_type: SocialGraphListType,
) {
    let mut outcome = BackendTaskOutcome::new(
        &sender,
        BackendCommand::LoadSocialGraphList {
            user_id: user_id.clone(),
            list_type,
        },
    );
    let Some(username) = profile_username_from_user_id(&user_id) else {
        return;
    };
    let Some(path) = socket_path() else {
        send_internal(
            &sender,
            "zbase.internal.social_graph_socket_path_missing",
            Value::from(user_id.0.clone()),
        );
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.social_graph_runtime_init_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    let result = runtime.block_on(async {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);

        let response = match list_type {
            SocialGraphListType::Followers => {
                client
                    .call(
                        KEYBASE_LIST_TRACKERS_UNVERIFIED,
                        vec![Value::Map(vec![(
                            Value::from("assertion"),
                            Value::from(username.clone()),
                        )])],
                    )
                    .await?
            }
            SocialGraphListType::Following => {
                client
                    .call(
                        KEYBASE_LIST_TRACKING,
                        vec![Value::Map(vec![
                            (Value::from("assertion"), Value::from(username.clone())),
                            (Value::from("filter"), Value::from("")),
                        ])],
                    )
                    .await?
            }
        };

        let summaries = parse_user_summary_set(&response);
        let usernames = summaries
            .iter()
            .map(|(username, _)| username.clone())
            .collect::<Vec<_>>();
        if !usernames.is_empty() {
            spawn_prefetch_user_profiles(&sender, &local_store, usernames);
        }

        let affinities = fetch_tracking_affinities_from_service(&path)
            .await
            .unwrap_or_default();
        let entries = summaries
            .into_iter()
            .map(|(username, full_name)| {
                let entry_user_id = UserId::new(username.to_ascii_lowercase());
                let (cached_name, cached_avatar) =
                    load_cached_profile_summary(&local_store, &entry_user_id, &username);
                let display_name = full_name
                    .filter(|value| !value.trim().is_empty())
                    .filter(|value| !value.eq_ignore_ascii_case(&username))
                    .or_else(|| {
                        (!cached_name.trim().is_empty()
                            && !cached_name.eq_ignore_ascii_case(&username))
                        .then_some(cached_name)
                    })
                    .unwrap_or(username.clone());
                let affinity = affinities
                    .get(&entry_user_id)
                    .copied()
                    .unwrap_or(Affinity::None);
                SocialGraphEntry {
                    user_id: entry_user_id,
                    display_name,
                    avatar_asset: validated_avatar_asset(cached_avatar),
                    affinity,
                }
            })
            .collect::<Vec<_>>();
        Ok::<Vec<SocialGraphEntry>, io::Error>(entries)
    });

    match result {
        Ok(entries) => {
            outcome.complete();
            let _ = sender.send(BackendEvent::SocialGraphListLoaded {
                user_id,
                list_type,
                entries,
            });
        }
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.social_graph_load_failed",
                Value::Map(vec![
                    (Value::from("user_id"), Value::from(user_id.0)),
                    (Value::from("error"), Value::from(error.to_string())),
                ]),
            );
        }
    }
}

fn run_refresh_profile_presence(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    user_id: UserId,
    conversation_id: Option<ConversationId>,
) {
    let candidate_conversation_ids =
        candidate_profile_presence_conversations(local_store.as_ref(), conversation_id.clone(), 48);
    if candidate_conversation_ids.is_empty() {
        send_internal(
            &sender,
            "zbase.internal.refresh_participants_no_candidate_conversations",
            Value::from(user_id.0.clone()),
        );
        return;
    }
    let Some(path) = socket_path() else {
        send_internal(
            &sender,
            "zbase.internal.refresh_participants_socket_path_missing",
            Value::from(user_id.0.clone()),
        );
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.refresh_participants_runtime_init_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    let result = runtime.block_on(async {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        let mut by_user: HashMap<UserId, PresencePatch> = HashMap::new();

        if let Some(conversation_id) = conversation_id.as_ref()
            && let Some(raw_conversation_id) = provider_ref_to_conversation_id_bytes(
                &ProviderConversationRef::new(conversation_id.0.clone()),
            )
        {
            if let Ok((_result, callbacks)) = client
                .call_collecting_callbacks(
                    CHAT_REFRESH_PARTICIPANTS_LOCAL,
                    vec![Value::Map(vec![(
                        Value::from("convID"),
                        Value::Binary(raw_conversation_id.clone()),
                    )])],
                )
                .await
            {
                for patch in parse_presence_updates_from_rpc_notifications(&callbacks)
                    .into_iter()
                    .filter(|patch| patch.user_id.0.eq_ignore_ascii_case(&user_id.0))
                {
                    merge_presence_patch(&mut by_user, patch);
                }
            }
            if let Ok(Some(patch)) = fetch_profile_presence_patch_from_last_active(
                &mut client,
                &raw_conversation_id,
                &user_id,
            )
            .await
            {
                merge_presence_patch(&mut by_user, patch);
            }
        }

        for candidate in &candidate_conversation_ids {
            if conversation_id
                .as_ref()
                .map(|current| current.0.eq_ignore_ascii_case(&candidate.0))
                .unwrap_or(false)
            {
                continue;
            }
            let Some(raw_conversation_id) = provider_ref_to_conversation_id_bytes(
                &ProviderConversationRef::new(candidate.0.clone()),
            ) else {
                continue;
            };
            if let Ok(Some(patch)) = fetch_profile_presence_patch_from_last_active(
                &mut client,
                &raw_conversation_id,
                &user_id,
            )
            .await
            {
                let is_active = matches!(&patch.presence.availability, Availability::Active);
                merge_presence_patch(&mut by_user, patch);
                if is_active {
                    break;
                }
            }
        }

        Ok::<Vec<PresencePatch>, io::Error>(by_user.into_values().collect())
    });

    match result {
        Ok(patches) => {
            if !patches.is_empty() {
                let _ = sender.send(BackendEvent::PresenceUpdated {
                    account_id: AccountId::new(DEFAULT_ACCOUNT_ID),
                    users: patches,
                });
            }
        }
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.refresh_participants_failed",
                Value::Map(vec![
                    (Value::from("user_id"), Value::from(user_id.0)),
                    (Value::from("error"), Value::from(error.to_string())),
                ]),
            );
        }
    }
}

fn run_load_conversation_members(sender: Sender<BackendEvent>, conversation_id: ConversationId) {
    let Some(raw_conversation_id) = provider_ref_to_conversation_id_bytes(
        &ProviderConversationRef::new(conversation_id.0.clone()),
    ) else {
        return;
    };
    let Some(path) = socket_path() else {
        send_internal(
            &sender,
            "zbase.internal.get_participants_socket_path_missing",
            Value::from(conversation_id.0.clone()),
        );
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.get_participants_runtime_init_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    let result = runtime.block_on(async move {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        client
            .call(
                CHAT_GET_PARTICIPANTS_LOCAL,
                vec![Value::Map(vec![(
                    Value::from("convID"),
                    Value::Binary(raw_conversation_id),
                )])],
            )
            .await
            .map_err(|e| io::Error::other(e.to_string()))
    });

    let response = match result {
        Ok(response) => response,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.get_participants_failed",
                Value::Map(vec![
                    (
                        Value::from("conversation_id"),
                        Value::from(conversation_id.0.clone()),
                    ),
                    (Value::from("error"), Value::from(error.to_string())),
                ]),
            );
            return;
        }
    };

    let Some(usernames) = parse_conversation_participants_usernames(&response) else {
        send_internal(
            &sender,
            "zbase.internal.get_participants_unparseable",
            Value::Map(vec![
                (
                    Value::from("conversation_id"),
                    Value::from(conversation_id.0.clone()),
                ),
                (
                    Value::from("response_kind"),
                    Value::from(format!("{:?}", response)),
                ),
            ]),
        );
        return;
    };

    let mut members = usernames.into_iter().map(UserId::new).collect::<Vec<_>>();
    members.sort_by(|left, right| left.0.cmp(&right.0));
    members.dedup_by(|left, right| left.0.eq_ignore_ascii_case(&right.0));
    let _ = sender.send(BackendEvent::ConversationMembersUpdated {
        conversation_id,
        members,
        updated_ms: now_unix_ms(),
        is_complete: true,
    });
}

fn parse_conversation_participants_usernames(value: &Value) -> Option<Vec<String>> {
    // chat.1.local.getParticipants returns []ConversationLocalParticipant where each item has
    // { username, fullname?, contactName?, inConvName }.
    let items = if let Some(items) = as_array(value) {
        items
    } else if let Some(result) = map_get_any(value, &["result", "Result"]).and_then(as_array) {
        result
    } else {
        map_get_any(value, &["participants", "Participants"]).and_then(as_array)?
    };
    let mut usernames = Vec::new();
    for item in items {
        let Some(username) = map_get_any(item, &["username", "Username"])
            .and_then(as_str)
            .map(str::trim)
            .filter(|u| !u.is_empty())
        else {
            continue;
        };
        if let Some(canon) = canonical_username(username) {
            usernames.push(canon);
        }
    }
    usernames.sort();
    usernames.dedup();
    Some(usernames)
}

fn candidate_profile_presence_conversations(
    local_store: &LocalStore,
    preferred: Option<ConversationId>,
    limit: usize,
) -> Vec<ConversationId> {
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();
    if let Some(conversation_id) = preferred
        && seen.insert(conversation_id.0.clone())
    {
        candidates.push(conversation_id);
    }
    if let Ok(cached) = local_store.load_cached_conversation_ids(limit) {
        for conversation_id in cached {
            if seen.insert(conversation_id.0.clone()) {
                candidates.push(conversation_id);
            }
        }
    }
    candidates
}

async fn fetch_profile_presence_patch_from_last_active(
    client: &mut KeybaseRpcClient,
    raw_conversation_id: &[u8],
    user_id: &UserId,
) -> io::Result<Option<PresencePatch>> {
    let thread_result = client
        .call(
            CHAT_GET_THREAD_LOCAL,
            vec![Value::Map(vec![
                (
                    Value::from("conversationID"),
                    Value::Binary(raw_conversation_id.to_vec()),
                ),
                (
                    Value::from("reason"),
                    Value::from(GET_THREAD_REASON_FOREGROUND),
                ),
                (Value::from("query"), Value::Nil),
                (
                    Value::from("pagination"),
                    Value::Map(vec![(Value::from("num"), Value::from(1))]),
                ),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await?;
    let tlf_name = find_value_for_keys(&thread_result, &["tlfName", "tlf_name"], 0)
        .and_then(as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(tlf_name) = tlf_name
        && !tlf_name_mentions_user(&tlf_name, &user_id.0)
    {
        return Ok(None);
    }
    let Some(tlf_id) = find_value_for_keys(&thread_result, &["tlfid", "tlfID", "tlfId"], 0)
        .and_then(value_to_tlf_id_hex)
    else {
        return Ok(None);
    };
    let status_result = client
        .call(
            CHAT_GET_LAST_ACTIVE_FOR_TLF_LOCAL,
            vec![Value::Map(vec![(
                Value::from("tlfID"),
                Value::from(tlf_id),
            )])],
        )
        .await?;
    let Some(availability) = parse_last_active_status_for_user(&status_result, user_id)
        .filter(|value| *value != Availability::Unknown)
    else {
        return Ok(None);
    };
    Ok(Some(PresencePatch {
        user_id: UserId::new(user_id.0.clone()),
        presence: Presence {
            availability,
            status_text: None,
        },
    }))
}

fn parse_last_active_status_for_user(value: &Value, user_id: &UserId) -> Option<Availability> {
    let mut patches = Vec::new();
    collect_presence_patches(value, &mut patches, 0);
    if let Some(patch) = patches
        .into_iter()
        .find(|patch| patch.user_id.0.eq_ignore_ascii_case(&user_id.0))
    {
        return Some(patch.presence.availability);
    }
    parse_last_active_status_availability(value)
        .or_else(|| parse_presence_availability_from_payload(value))
}

fn value_to_tlf_id_hex(value: &Value) -> Option<String> {
    if let Some(bytes) = as_binary(value) {
        return Some(hex_encode(bytes));
    }
    let text = as_str(value)?.trim();
    if text.is_empty() {
        return None;
    }
    if text.len() % 2 == 0 && text.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Some(text.to_ascii_lowercase());
    }
    None
}

fn tlf_name_mentions_user(tlf_name: &str, user_id: &str) -> bool {
    let Some(target) = canonical_username(user_id) else {
        return false;
    };
    tlf_name
        .split(',')
        .filter_map(canonical_username)
        .any(|member| member == target)
}

fn run_follow_toggle(sender: Sender<BackendEvent>, user_id: UserId, follow: bool) {
    let Some(username) = profile_username_from_user_id(&user_id) else {
        return;
    };
    let Some(path) = socket_path() else {
        send_internal(
            &sender,
            "zbase.internal.follow_toggle_socket_path_missing",
            Value::from(user_id.0.clone()),
        );
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.follow_toggle_runtime_init_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    let result = runtime.block_on(async {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        let gui_id = format!(
            "zbase-follow-{}-{}",
            sanitize_username(&username),
            now_unix_ms()
        );
        let _ = client
            .call(
                KEYBASE_IDENTIFY3,
                vec![Value::Map(vec![
                    (Value::from("assertion"), Value::from(username.clone())),
                    (Value::from("guiID"), Value::from(gui_id.clone())),
                    (Value::from("ignoreCache"), Value::from(false)),
                ])],
            )
            .await;
        client
            .call(
                KEYBASE_IDENTIFY3_FOLLOW_USER,
                vec![Value::Map(vec![
                    (Value::from("guiID"), Value::from(gui_id)),
                    (Value::from("follow"), Value::from(follow)),
                ])],
            )
            .await?;
        Ok::<(), io::Error>(())
    });

    match result {
        Ok(()) => {
            let _ = sender.send(BackendEvent::FollowStatusChanged {
                user_id,
                you_are_following: follow,
            });
        }
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.follow_toggle_failed",
                Value::Map(vec![
                    (Value::from("user_id"), Value::from(user_id.0.clone())),
                    (Value::from("follow"), Value::from(follow)),
                    (Value::from("error"), Value::from(error.to_string())),
                ]),
            );
            let _ = sender.send(BackendEvent::FollowStatusChangeFailed {
                user_id,
                attempted_follow: follow,
                error: error.to_string(),
            });
        }
    }
}

fn profile_username_from_user_id(user_id: &UserId) -> Option<String> {
    let raw = user_id.0.trim();
    if raw.is_empty() {
        return None;
    }
    if let Some(team_name) = raw.strip_prefix("team:") {
        let trimmed = team_name.trim();
        if trimmed.is_empty() {
            return None;
        }
        return Some(trimmed.to_string());
    }
    Some(raw.to_string())
}

fn value_to_u32_allow_zero(value: &Value) -> Option<u32> {
    if let Some(raw) = as_i64(value) {
        return u32::try_from(raw).ok();
    }
    as_str(value).and_then(|raw| raw.trim().parse::<u32>().ok())
}

fn parse_team_showcase_entries(value: &Value) -> Vec<TeamShowcaseEntry> {
    find_value_for_keys(value, &["teamShowcase"], 0)
        .and_then(as_array)
        .map(|teams| {
            teams
                .iter()
                .filter_map(|team| {
                    let name = find_value_for_keys(team, &["fqName", "name"], 0)
                        .and_then(as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())?
                        .to_string();
                    let description = find_value_for_keys(team, &["description"], 0)
                        .and_then(as_str)
                        .map(str::trim)
                        .unwrap_or("")
                        .to_string();
                    let is_open = find_value_for_keys(team, &["open"], 0)
                        .and_then(value_to_bool)
                        .unwrap_or(false);
                    let members_count = find_value_for_keys(team, &["numMembers"], 0)
                        .and_then(value_to_u32_allow_zero)
                        .unwrap_or(0);
                    Some(TeamShowcaseEntry {
                        name,
                        description,
                        is_open,
                        members_count,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn parse_identity_proofs(value: &Value) -> Vec<IdentityProof> {
    let mut rows = Vec::new();
    if let Some(proofs) =
        find_value_for_keys(value, &["proofs", "assertions", "rows"], 0).and_then(as_array)
    {
        for proof in proofs {
            if let Some(parsed) = parse_identity_proof_row(proof) {
                rows.push(parsed);
            }
        }
    }
    let mut dedup = HashSet::new();
    rows.into_iter()
        .filter(|proof| {
            dedup.insert(format!(
                "{}:{}",
                proof.service_name.to_ascii_lowercase(),
                proof.service_username.to_ascii_lowercase()
            ))
        })
        .collect()
}

fn parse_identity_proof_row(value: &Value) -> Option<IdentityProof> {
    let service_name = map_get_any(value, &["key", "service", "serviceName"])
        .or_else(|| find_value_for_keys(value, &["key", "service", "serviceName"], 0))
        .and_then(as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())?
        .to_string();
    let service_username = map_get_any(
        value,
        &[
            "value",
            "username",
            "serviceUsername",
            "name",
            "proofUsername",
        ],
    )
    .or_else(|| {
        find_value_for_keys(
            value,
            &[
                "value",
                "username",
                "serviceUsername",
                "name",
                "proofUsername",
            ],
            0,
        )
    })
    .and_then(as_str)
    .map(str::trim)
    .filter(|v| !v.is_empty())?
    .to_string();
    let state = map_get_any(value, &["proofResult", "proof_result"])
        .and_then(|proof_result| map_get_any(proof_result, &["state", "s"]))
        .map(parse_proof_state)
        .or_else(|| {
            map_get_any(value, &["state", "proofState"])
                .or_else(|| find_value_for_keys(value, &["state", "proofState"], 0))
                .map(parse_proof_state)
        })
        .unwrap_or(ProofState::Unknown);
    let proof_url = map_get_any(value, &["proofURL", "proofUrl", "proof_url"])
        .or_else(|| find_value_for_keys(value, &["proofURL", "proofUrl", "proof_url"], 0))
        .and_then(as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string);
    let site_url = map_get_any(value, &["siteURL", "siteUrl", "site_url"])
        .or_else(|| find_value_for_keys(value, &["siteURL", "siteUrl", "site_url"], 0))
        .and_then(as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string);
    let icon_asset = parse_identity_proof_icon_asset(value);

    Some(IdentityProof {
        service_name,
        service_username,
        proof_url,
        site_url,
        icon_asset,
        state,
    })
}

fn parse_proof_state(value: &Value) -> ProofState {
    if let Some(raw) = as_i64(value) {
        return match raw {
            2 => ProofState::Verified,
            3 | 5 => ProofState::Broken,
            1 | 4 => ProofState::Pending,
            _ => ProofState::Unknown,
        };
    }
    let Some(text) = as_str(value) else {
        return ProofState::Unknown;
    };
    match text.trim().to_ascii_lowercase().as_str() {
        "valid" | "verified" | "ok" => ProofState::Verified,
        "error" | "broken" | "revoked" => ProofState::Broken,
        "checking" | "warning" | "pending" => ProofState::Pending,
        _ => ProofState::Unknown,
    }
}

fn parse_identify3_proof_callbacks(callbacks: &[RpcNotification]) -> Vec<IdentityProof> {
    let mut dedup: HashMap<String, IdentityProof> = HashMap::new();
    for callback in callbacks {
        if !callback.method.starts_with("keybase.1.identify3Ui.")
            || (!callback.method.contains("identify3UpdateRow")
                && !callback.method.contains("identify3Row"))
        {
            continue;
        }
        let row_payload =
            extract_identify3_row_payload(&callback.params).unwrap_or(&callback.params);
        if let Some(proof) = parse_identity_proof_row(row_payload) {
            let dedup_key = format!(
                "{}:{}",
                proof.service_name.to_ascii_lowercase(),
                proof.service_username.to_ascii_lowercase()
            );
            if let Some(existing) = dedup.get_mut(&dedup_key) {
                merge_identity_proof_row(existing, proof);
            } else {
                dedup.insert(dedup_key, proof);
            }
        }
    }
    let mut rows = dedup.into_values().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.service_name
            .cmp(&right.service_name)
            .then(left.service_username.cmp(&right.service_username))
    });
    rows
}

fn merge_identity_proof_row(existing: &mut IdentityProof, incoming: IdentityProof) {
    if proof_state_priority(&incoming.state) >= proof_state_priority(&existing.state) {
        existing.state = incoming.state;
    }
    if existing.proof_url.is_none() && incoming.proof_url.is_some() {
        existing.proof_url = incoming.proof_url;
    }
    if existing.site_url.is_none() && incoming.site_url.is_some() {
        existing.site_url = incoming.site_url;
    }
    if existing.icon_asset.is_none() && incoming.icon_asset.is_some() {
        existing.icon_asset = incoming.icon_asset;
    }
}

fn proof_state_priority(state: &ProofState) -> u8 {
    match state {
        ProofState::Unknown => 0,
        ProofState::Pending => 1,
        ProofState::Verified | ProofState::Broken => 2,
    }
}

fn extract_identify3_row_payload(params: &Value) -> Option<&Value> {
    if let Some(row) = map_get_any(params, &["row"]) {
        return Some(row);
    }
    let entries = as_array(params)?;
    for entry in entries {
        if let Some(row) = map_get_any(entry, &["row"]) {
            return Some(row);
        }
    }
    None
}

fn parse_identity_proof_icon_asset(value: &Value) -> Option<String> {
    let site_icon = map_get_any(value, &["siteIcon", "siteIcons", "icon"])
        .or_else(|| find_value_for_keys(value, &["siteIcon", "siteIcons", "icon"], 0))?;
    match site_icon {
        Value::Array(entries) => {
            let mut first_url: Option<String> = None;
            let mut smallest_width: Option<(i64, String)> = None;
            for entry in entries {
                let url = map_get_any(entry, &["url", "asset", "src"])
                    .or_else(|| map_get_any(entry, &["path"]))
                    .and_then(as_str)
                    .map(str::trim)
                    .filter(|url| !url.is_empty())
                    .map(str::to_string);
                let Some(url) = url else {
                    continue;
                };
                let width = map_get_any(entry, &["width", "w", "size"]).and_then(as_i64);
                if width == Some(16) {
                    return Some(url);
                }
                if let Some(width) = width.filter(|value| *value > 0) {
                    match &smallest_width {
                        Some((current, _)) if *current <= width => {}
                        _ => {
                            smallest_width = Some((width, url.clone()));
                        }
                    }
                }
                if first_url.is_none() {
                    first_url = Some(url);
                }
            }
            smallest_width.map(|(_, url)| url).or(first_url)
        }
        _ => map_get_any(site_icon, &["url", "asset", "src", "path"])
            .and_then(as_str)
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_string)
            .or_else(|| {
                as_str(site_icon)
                    .map(str::trim)
                    .filter(|url| !url.is_empty())
                    .map(str::to_string)
            }),
    }
}

fn parse_user_summary_set(value: &Value) -> Vec<(String, Option<String>)> {
    let users = find_value_for_keys(value, &["users"], 0)
        .and_then(as_array)
        .cloned()
        .unwrap_or_default();
    users
        .into_iter()
        .filter_map(|entry| {
            let username = find_value_for_keys(&entry, &["username"], 0)
                .and_then(as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())?
                .to_ascii_lowercase();
            let full_name = find_value_for_keys(&entry, &["fullName", "fullname", "full_name"], 0)
                .and_then(as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            Some((username, full_name))
        })
        .collect()
}

fn load_cached_profile_summary(
    local_store: &LocalStore,
    user_id: &UserId,
    fallback_username: &str,
) -> (String, Option<String>) {
    let Ok(profile) = local_store.get_user_profile(user_id) else {
        return (fallback_username.to_string(), None);
    };
    let Some(profile) = profile else {
        return (fallback_username.to_string(), None);
    };
    let display_name = if profile.display_name.trim().is_empty() {
        fallback_username.to_string()
    } else {
        profile.display_name
    };
    let avatar = profile
        .avatar_path
        .clone()
        .filter(|path| avatar_asset_path_usable(path))
        .or_else(|| profile.avatar_url.clone());
    (display_name, avatar)
}

fn start_background_cached_conversation_extension(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    backend_id: &BackendId,
    account_id: &AccountId,
    workspace_id: WorkspaceId,
    already_loaded_ids: HashSet<ConversationId>,
) {
    let sender = sender.clone();
    let sender_for_stats = sender.clone();
    let local_store = Arc::clone(local_store);
    let backend_id = backend_id.clone();
    let account_id = account_id.clone();
    let scheduled = task_runtime::spawn_task(
        TaskPriority::Low,
        Some("cached_conversation_extension".to_string()),
        move || {
            run_cached_conversation_extension(
                sender,
                local_store,
                backend_id,
                account_id,
                workspace_id,
                already_loaded_ids,
            );
        },
    );
    if !scheduled {
        send_internal(
            &sender_for_stats,
            "zbase.internal.task_runtime.deduped",
            Value::from("cached_conversation_extension"),
        );
    }
    emit_task_runtime_stats(&sender_for_stats, "cached_conversation_extension.schedule");
}

fn run_cached_conversation_extension(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    backend_id: BackendId,
    account_id: AccountId,
    workspace_id: WorkspaceId,
    already_loaded_ids: HashSet<ConversationId>,
) {
    let seed = match local_store.load_bootstrap_seed(usize::MAX, 0) {
        Ok(Some(seed)) => seed,
        Ok(None) => return,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.cache_extension_load_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    let mut channels = seed
        .channels
        .into_iter()
        .filter(|summary| !already_loaded_ids.contains(&summary.id))
        .collect::<Vec<_>>();
    let mut direct_messages = seed
        .direct_messages
        .into_iter()
        .filter(|summary| !already_loaded_ids.contains(&summary.id))
        .collect::<Vec<_>>();
    if channels.is_empty() && direct_messages.is_empty() {
        return;
    }

    let mut remaining_ids = channels
        .iter()
        .map(|summary| summary.id.clone())
        .collect::<HashSet<_>>();
    remaining_ids.extend(direct_messages.iter().map(|summary| summary.id.clone()));
    let conversation_bindings = seed
        .conversation_bindings
        .into_iter()
        .filter_map(|(conversation_id, provider_conversation_ref)| {
            if !remaining_ids.contains(&conversation_id) {
                return None;
            }
            Some(ConversationBinding {
                conversation_id,
                backend_id: backend_id.clone(),
                account_id: account_id.clone(),
                provider_conversation_ref,
            })
        })
        .collect::<Vec<_>>();

    prefetch_user_profiles_from_summaries(&sender, &local_store, &direct_messages);

    let total = channels.len() + direct_messages.len();
    if !emit_workspace_conversations_extended_in_chunks(
        &sender,
        &workspace_id,
        std::mem::take(&mut channels),
        std::mem::take(&mut direct_messages),
        conversation_bindings,
        CONVERSATION_EXTENSION_BATCH_SIZE,
    ) {
        return;
    }
    send_internal(
        &sender,
        "zbase.internal.cache_extension_emitted",
        Value::Map(vec![
            (Value::from("workspace_id"), Value::from(workspace_id.0)),
            (Value::from("count"), Value::from(total as i64)),
        ]),
    );
}

fn start_background_inbox_conversation_cache_refresh(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    backend_id: &BackendId,
    account_id: &AccountId,
    account_display_name: Option<String>,
    sync: Arc<SyncService>,
) {
    let sender = sender.clone();
    let sender_for_stats = sender.clone();
    let local_store = Arc::clone(local_store);
    let backend_id = backend_id.clone();
    let account_id = account_id.clone();
    let scheduled = task_runtime::spawn_task(
        TaskPriority::Low,
        Some("inbox_conversation_cache_refresh".to_string()),
        move || {
            run_background_inbox_conversation_cache_refresh(
                sender,
                local_store,
                backend_id,
                account_id,
                account_display_name,
                sync,
            );
        },
    );
    if !scheduled {
        send_internal(
            &sender_for_stats,
            "zbase.internal.task_runtime.deduped",
            Value::from("inbox_conversation_cache_refresh"),
        );
    }
    emit_task_runtime_stats(
        &sender_for_stats,
        "inbox_conversation_cache_refresh.schedule",
    );
}

fn run_background_inbox_conversation_cache_refresh(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    backend_id: BackendId,
    account_id: AccountId,
    account_display_name: Option<String>,
    sync: Arc<SyncService>,
) {
    send_internal(
        &sender,
        "zbase.internal.inbox_cache_refresh_start",
        Value::Nil,
    );
    let Some(path) = socket_path() else {
        send_internal(
            &sender,
            "zbase.internal.inbox_cache_refresh_socket_missing",
            Value::Nil,
        );
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.inbox_cache_refresh_runtime_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    runtime.block_on(async move {
        let transport = match FramedMsgpackTransport::connect(&path).await {
            Ok(transport) => transport,
            Err(error) => {
                send_internal(
                    &sender,
                    "zbase.internal.inbox_cache_refresh_socket_connect_failed",
                    Value::from(error.to_string()),
                );
                return;
            }
        };
        let mut client = KeybaseRpcClient::new(transport);
        let mut self_username = account_display_name
            .as_ref()
            .filter(|value| !value.trim().is_empty())
            .cloned();
        if self_username.is_none() {
            match fetch_current_status(&mut client).await {
                Ok(status) => {
                    self_username = status_username(&status);
                }
                Err(error) => {
                    send_internal(
                        &sender,
                        "zbase.internal.inbox_cache_refresh_status_failed",
                        Value::from(error.to_string()),
                    );
                }
            }
        }
        if let Err(error) =
            repair_cached_conversation_topics(&mut client, &local_store, &sender).await
        {
            send_internal(
                &sender,
                "zbase.internal.cache_topic_repair_failed",
                Value::from(error.to_string()),
            );
        }
        let workspace_id = WorkspaceId::new(WORKSPACE_ID);
        let mut seen_conversation_ids = HashSet::new();
        let mut activity_sorted_limit = INBOX_CACHE_PAGE_SIZE;
        let mut upserted_total = 0usize;

        loop {
            let inbox =
                match fetch_inbox_summary_with_limit(&mut client, activity_sorted_limit).await {
                    Ok(value) => value,
                    Err(error) => {
                        send_internal(
                            &sender,
                            "zbase.internal.inbox_cache_refresh_fetch_failed",
                            Value::from(error.to_string()),
                        );
                        break;
                    }
                };
            let mut conversations =
                parse_inbox_conversations(&inbox, self_username.as_deref(), false);
            if conversations.is_empty() {
                break;
            }
            conversations.sort_by_key(|left| std::cmp::Reverse(left.activity_time));
            let returned_count = conversations.len();
            let fresh = conversations
                .into_iter()
                .filter(|conversation| {
                    seen_conversation_ids.insert(conversation.summary.id.clone())
                })
                .collect::<Vec<_>>();
            let fresh_count = fresh.len();
            if fresh_count > 0 {
                let refs = fresh
                    .iter()
                    .map(|conversation| conversation.provider_ref.clone())
                    .collect::<Vec<_>>();
                start_background_full_history_crawl(&sender, &local_store, &sync, &refs);
                upserted_total =
                    upserted_total.saturating_add(persist_and_emit_conversation_extension_batch(
                        &sender,
                        &local_store,
                        &backend_id,
                        &account_id,
                        &workspace_id,
                        fresh,
                    ));
            }
            if returned_count < activity_sorted_limit {
                break;
            }
            if activity_sorted_limit >= INBOX_CACHE_MAX_CONVERSATIONS {
                break;
            }
            if fresh_count == 0 && returned_count <= seen_conversation_ids.len() {
                break;
            }
            let next_limit =
                (activity_sorted_limit + INBOX_CACHE_PAGE_SIZE).min(INBOX_CACHE_MAX_CONVERSATIONS);
            if next_limit == activity_sorted_limit {
                break;
            }
            activity_sorted_limit = next_limit;
        }

        send_internal(
            &sender,
            "zbase.internal.inbox_cache_refresh_done",
            Value::Map(vec![
                (Value::from("upserted"), Value::from(upserted_total as i64)),
                (
                    Value::from("known_conversations"),
                    Value::from(seen_conversation_ids.len() as i64),
                ),
            ]),
        );
    });
}

fn persist_and_emit_conversation_extension_batch(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    backend_id: &BackendId,
    account_id: &AccountId,
    workspace_id: &WorkspaceId,
    conversations: Vec<BootstrapConversation>,
) -> usize {
    if conversations.is_empty() {
        return 0;
    }

    let mut channels = Vec::new();
    let mut direct_messages = Vec::new();
    let mut conversation_bindings = Vec::new();
    for conversation in conversations {
        let summary = conversation.summary;
        let activity_time = conversation.activity_time;
        let provider_conversation_ref = conversation.provider_ref;
        let conversation_id = summary.id.clone();
        let _ = local_store.persist_conversation(&summary, activity_time);
        let _ =
            local_store.persist_conversation_binding(&conversation_id, &provider_conversation_ref);
        conversation_bindings.push(ConversationBinding {
            conversation_id: conversation_id.clone(),
            backend_id: backend_id.clone(),
            account_id: account_id.clone(),
            provider_conversation_ref,
        });
        if matches!(&summary.kind, ConversationKind::Channel) {
            channels.push(summary);
        } else {
            direct_messages.push(summary);
        }
    }

    prefetch_user_profiles_from_summaries(sender, local_store, &direct_messages);

    let total = channels.len() + direct_messages.len();
    if !emit_workspace_conversations_extended_in_chunks(
        sender,
        workspace_id,
        channels,
        direct_messages,
        conversation_bindings,
        CONVERSATION_EXTENSION_BATCH_SIZE,
    ) {
        return 0;
    }
    total
}

fn emit_workspace_conversations_extended_in_chunks(
    sender: &Sender<BackendEvent>,
    workspace_id: &WorkspaceId,
    channels: Vec<ConversationSummary>,
    direct_messages: Vec<ConversationSummary>,
    conversation_bindings: Vec<ConversationBinding>,
    chunk_size: usize,
) -> bool {
    let max_chunk_size = chunk_size.max(1);
    let mut binding_by_id = conversation_bindings
        .into_iter()
        .map(|binding| (binding.conversation_id.clone(), binding))
        .collect::<HashMap<_, _>>();

    let mut batch_channels = Vec::new();
    let mut batch_direct_messages = Vec::new();
    let mut batch_bindings = Vec::new();
    let mut batch_count = 0usize;

    for summary in channels {
        if let Some(binding) = binding_by_id.remove(&summary.id) {
            batch_bindings.push(binding);
        }
        batch_channels.push(summary);
        batch_count += 1;
        if batch_count >= max_chunk_size {
            if sender
                .send(BackendEvent::WorkspaceConversationsExtended {
                    workspace_id: workspace_id.clone(),
                    channels: std::mem::take(&mut batch_channels),
                    direct_messages: std::mem::take(&mut batch_direct_messages),
                    conversation_bindings: std::mem::take(&mut batch_bindings),
                })
                .is_err()
            {
                return false;
            }
            batch_count = 0;
        }
    }

    for summary in direct_messages {
        if let Some(binding) = binding_by_id.remove(&summary.id) {
            batch_bindings.push(binding);
        }
        batch_direct_messages.push(summary);
        batch_count += 1;
        if batch_count >= max_chunk_size {
            if sender
                .send(BackendEvent::WorkspaceConversationsExtended {
                    workspace_id: workspace_id.clone(),
                    channels: std::mem::take(&mut batch_channels),
                    direct_messages: std::mem::take(&mut batch_direct_messages),
                    conversation_bindings: std::mem::take(&mut batch_bindings),
                })
                .is_err()
            {
                return false;
            }
            batch_count = 0;
        }
    }

    if batch_count == 0 {
        return true;
    }

    sender
        .send(BackendEvent::WorkspaceConversationsExtended {
            workspace_id: workspace_id.clone(),
            channels: batch_channels,
            direct_messages: batch_direct_messages,
            conversation_bindings: batch_bindings,
        })
        .is_ok()
}

fn start_background_full_history_crawl(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    sync: &Arc<SyncService>,
    conversation_refs: &[ProviderConversationRef],
) {
    if benchmark_read_only() || !sync.request(conversation_refs) {
        return;
    }
    let sender = sender.clone();
    let store = Arc::clone(local_store);
    let sync = Arc::clone(sync);
    task_runtime::spawn_task(TaskPriority::Low, None, move || {
        let mut attempt = 0;
        while let Some(refs) = sync.take_pending() {
            let failed =
                run_background_full_history_crawl(sender.clone(), Arc::clone(&store), &sync, refs);
            if !failed.is_empty() {
                sync.request(&failed);
                let deadline = Instant::now() + reconnect_delay(attempt);
                attempt = attempt.saturating_add(1);
                while Instant::now() < deadline && !sync.is_cancelled() {
                    thread::sleep(Duration::from_millis(100));
                }
            } else {
                attempt = 0;
            }
        }
    });
}

fn run_background_full_history_crawl(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    service: &SyncService,
    conversation_refs: Vec<ProviderConversationRef>,
) -> Vec<ProviderConversationRef> {
    let mut failed = Vec::new();
    let retry_refs = conversation_refs.clone();
    send_internal(&sender, "zbase.internal.crawl.start", Value::Nil);
    let result = (|| -> io::Result<()> {
        let path = socket_path().ok_or_else(|| io::Error::other("Keybase socket unavailable"))?;
        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            for reference in conversation_refs {
                if service.is_cancelled() {
                    break;
                }
                let conversation_id = canonical_conversation_id_from_provider_ref(&reference);
                let Some(raw) = provider_ref_to_conversation_id_bytes(&reference) else {
                    continue;
                };
                // Unlike the history-complete flag, this position is independent
                // of Tantivy and advances only after a whole page is persisted.
                let sync = async {
                    let transport = FramedMsgpackTransport::connect(&path).await?;
                    let mut client = KeybaseRpcClient::new(transport);
                    let mut position = local_store.load_sync_position(&conversation_id)?;
                    let mut pages = 0;
                    loop {
                        if service.is_cancelled() {
                            return Ok(());
                        }
                        tokio::time::sleep(crawl_throttle_delay()).await;
                        let page = tokio::time::timeout(
                            Duration::from_secs(30),
                            fetch_thread_messages_after_anchor_impl(
                                &mut client,
                                &conversation_id,
                                &raw,
                                &MessageId::new(position.to_string()),
                                CRAWL_PAGE_SIZE,
                                false,
                            ),
                        )
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "History sync timed out")
                        })??;
                        if page.offline {
                            return Err(io::Error::other(
                                "Keybase is offline; history sync will resume later",
                            ));
                        }
                        let Some(next) = page
                            .progress
                            .next_position(position, page.last && page.next_cursor.is_none())?
                        else {
                            break;
                        };
                        persist_sync_page(&sender, &local_store, &conversation_id, page)?;
                        local_store.save_sync_position(&conversation_id, next)?;
                        position = next;
                        pages += 1;
                        if pages % CRAWL_PROGRESS_EMIT_EVERY_PAGES == 0 {
                            send_internal(
                                &sender,
                                "zbase.internal.crawl.conversation_progress",
                                Value::from(conversation_id.0.clone()),
                            );
                        }
                    }
                    let _ = sender.send(BackendEvent::HistorySynced {
                        conversation_id: conversation_id.clone(),
                    });
                    Ok::<_, io::Error>(())
                }
                .await;
                if let Err(error) = sync {
                    failed.push(reference);
                    send_internal(
                        &sender,
                        "zbase.internal.crawl.page_fetch_failed",
                        Value::from(format!("{}: {error}", conversation_id.0)),
                    );
                }
            }
            Ok::<_, io::Error>(())
        })
    })();
    if let Err(error) = result {
        failed = retry_refs;
        send_internal(
            &sender,
            "zbase.internal.crawl.failed",
            Value::from(error.to_string()),
        );
    }
    send_internal(&sender, "zbase.internal.crawl.finished", Value::Nil);
    failed
}

fn persist_sync_page(
    sender: &Sender<BackendEvent>,
    store: &LocalStore,
    conversation_id: &ConversationId,
    page: ThreadPage,
) -> io::Result<()> {
    enum Operation {
        Message(MessageRecord),
        Reaction(MessageReactionDelta),
        Delete(MessageRecord),
    }
    let mut operations = Vec::new();
    for message in page.messages {
        if matches!(message.event, Some(ChatEvent::MessageDeleted { .. })) {
            continue;
        }
        let id = message
            .edited
            .as_ref()
            .map(|edit| &edit.edit_id)
            .unwrap_or(&message.id)
            .0
            .parse::<u64>()
            .unwrap_or(0);
        operations.push((id, Operation::Message(message)));
    }
    for delta in page.reaction_deltas {
        let id = delta
            .op_message_id
            .as_ref()
            .unwrap_or(&delta.target_message_id)
            .0
            .parse::<u64>()
            .unwrap_or(0);
        operations.push((id, Operation::Reaction(delta)));
    }
    for message in page.deletions {
        operations.push((
            message.id.0.parse::<u64>().unwrap_or(0),
            Operation::Delete(message),
        ));
    }
    operations.sort_by_key(|(id, _)| *id);
    let mut reaction_targets = HashSet::new();
    for (_, operation) in operations {
        match operation {
            Operation::Message(mut message) => {
                hydrate_thread_metadata(store, &mut message);
                if let Some(canonical) = store.upsert_message(&message)?
                    && canonical.edited.is_some()
                {
                    let _ = sender.send(BackendEvent::MessageUpserted(canonical));
                }
            }
            Operation::Reaction(delta) => {
                store.upsert_message_reaction(
                    conversation_id,
                    &delta.target_message_id,
                    &delta.emoji,
                    delta.source_ref.as_ref(),
                    &delta.actor_id,
                    delta.updated_ms,
                )?;
                if let Some(op_id) = delta.op_message_id.as_ref() {
                    store.upsert_message_reaction_op(
                        conversation_id,
                        op_id,
                        &delta.target_message_id,
                        &delta.emoji,
                        &delta.actor_id,
                        delta.updated_ms,
                    )?;
                    store.delete_message(conversation_id, op_id)?;
                }
                remember_reaction_op(&delta);
                reaction_targets.insert(delta.target_message_id);
            }
            Operation::Delete(message) => {
                let Some(ChatEvent::MessageDeleted {
                    target_message_id: Some(target),
                }) = message.event
                else {
                    continue;
                };
                if let Some(reaction) = take_reaction_op(store, conversation_id, &target) {
                    store.delete_message_reaction(
                        conversation_id,
                        &reaction.target_message_id,
                        &reaction.emoji,
                        &reaction.actor_id,
                    )?;
                    reaction_targets.insert(reaction.target_message_id);
                } else {
                    store.delete_message(conversation_id, &target)?;
                    let _ = sender.send(BackendEvent::MessageDeleted {
                        conversation_id: conversation_id.clone(),
                        message_id: target,
                    });
                }
            }
        }
    }
    if let Some(event) = message_reaction_sync_event_for_message_ids(
        store,
        conversation_id,
        &reaction_targets.into_iter().collect::<Vec<_>>(),
    ) {
        let _ = sender.send(event);
    }
    Ok(())
}

fn start_background_thread_edge_migration(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
) {
    let already_complete = local_store
        .is_thread_edge_migration_complete()
        .unwrap_or(false);
    if already_complete {
        return;
    }
    let sender = sender.clone();
    let sender_for_stats = sender.clone();
    let local_store = Arc::clone(local_store);
    let scheduled = task_runtime::spawn_task(
        TaskPriority::Low,
        Some("thread_edge_migration".to_string()),
        move || run_background_thread_edge_migration(sender, local_store),
    );
    if !scheduled {
        emit_task_runtime_stats(&sender_for_stats, "thread_edge_migration_deduped");
    }
    emit_task_runtime_stats(&sender_for_stats, "thread_edge_migration.schedule");
}

fn run_background_thread_edge_migration(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
) {
    if local_store
        .is_thread_edge_migration_complete()
        .unwrap_or(false)
    {
        return;
    }
    send_internal(
        &sender,
        "zbase.internal.thread_edge_migration.start",
        Value::Nil,
    );
    let conversation_ids = local_store
        .load_cached_conversation_ids(THREAD_EDGE_MIGRATION_MAX_CONVERSATIONS)
        .unwrap_or_default();
    let mut migrated_conversations = 0usize;
    let mut rewritten_edges_total = 0usize;
    for conversation_id in conversation_ids {
        let already_migrated = local_store
            .is_thread_edge_conversation_migrated(&conversation_id)
            .unwrap_or(false);
        if already_migrated {
            continue;
        }
        let rewritten = local_store
            .repair_thread_edges_for_conversation(&conversation_id, THREAD_EDGE_REPAIR_SCAN_LIMIT)
            .unwrap_or(0);
        let _ = local_store.mark_thread_edge_conversation_migrated(&conversation_id);
        migrated_conversations = migrated_conversations.saturating_add(1);
        rewritten_edges_total = rewritten_edges_total.saturating_add(rewritten);
        if migrated_conversations.is_multiple_of(50) {
            send_internal(
                &sender,
                "zbase.internal.thread_edge_migration.progress",
                Value::Map(vec![
                    (
                        Value::from("conversations_migrated"),
                        Value::from(migrated_conversations as i64),
                    ),
                    (
                        Value::from("rewritten_edges"),
                        Value::from(rewritten_edges_total as i64),
                    ),
                ]),
            );
        }
    }
    let _ = local_store.mark_thread_edge_migration_complete();
    send_internal(
        &sender,
        "zbase.internal.thread_edge_migration.done",
        Value::Map(vec![
            (
                Value::from("conversations_migrated"),
                Value::from(migrated_conversations as i64),
            ),
            (
                Value::from("rewritten_edges"),
                Value::from(rewritten_edges_total as i64),
            ),
        ]),
    );
}

fn run_load_conversation(
    sender: Sender<BackendEvent>,
    conversation_ref: ProviderConversationRef,
    local_store: Arc<LocalStore>,
    queued_at: Instant,
) {
    let queue_wait_ms = queued_at.elapsed().as_millis();
    let load_started = Instant::now();
    let conversation_id = canonical_conversation_id_from_provider_ref(&conversation_ref);
    let mut outcome = BackendTaskOutcome::new(
        &sender,
        BackendCommand::LoadConversation {
            conversation_id: conversation_id.clone(),
        },
    );
    send_internal(
        &sender,
        "zbase.internal.load_conversation_starting",
        Value::from(conversation_id.0.clone()),
    );

    let Some(path) = socket_path() else {
        send_internal(
            &sender,
            "zbase.internal.load_conversation_socket_path_missing",
            Value::from(conversation_id.0.clone()),
        );
        return;
    };

    let runtime_init_started = Instant::now();
    let runtime = {
        let mut attempt = 1usize;
        loop {
            match Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => break runtime,
                Err(error)
                    if attempt < LOAD_CONVERSATION_MAX_ATTEMPTS
                        && should_retry_load_conversation_error(&error) =>
                {
                    let backoff_ms =
                        LOAD_CONVERSATION_RETRY_BASE_DELAY_MS.saturating_mul(attempt as u64);
                    send_internal(
                        &sender,
                        "zbase.internal.load_conversation_runtime_retrying",
                        Value::Map(vec![
                            (
                                Value::from("conversation_id"),
                                Value::from(conversation_id.0.clone()),
                            ),
                            (Value::from("attempt"), Value::from(attempt as i64 + 1)),
                            (Value::from("delay_ms"), Value::from(backoff_ms as i64)),
                            (Value::from("error"), Value::from(error.to_string())),
                        ]),
                    );
                    thread::sleep(Duration::from_millis(backoff_ms));
                    attempt += 1;
                }
                Err(error) => {
                    send_internal(
                        &sender,
                        "zbase.internal.load_conversation_runtime_init_failed",
                        Value::from(error.to_string()),
                    );
                    return;
                }
            }
        }
    };
    let runtime_init_ms = runtime_init_started.elapsed().as_millis();

    let service_fetch_started = Instant::now();
    let mut attempt = 1usize;
    let result = loop {
        let path_for_fetch = path.clone();
        let conversation_id_for_fetch = conversation_id.clone();
        let conversation_ref_for_fetch = conversation_ref.clone();
        let attempt_result = runtime.block_on(async move {
            load_conversation_from_service(
                &path_for_fetch,
                &conversation_id_for_fetch,
                &conversation_ref_for_fetch,
            )
            .await
        });
        match attempt_result {
            Ok(loaded) => break Ok(loaded),
            Err(error)
                if attempt < LOAD_CONVERSATION_MAX_ATTEMPTS
                    && should_retry_load_conversation_error(&error) =>
            {
                let backoff_ms =
                    LOAD_CONVERSATION_RETRY_BASE_DELAY_MS.saturating_mul(attempt as u64);
                send_internal(
                    &sender,
                    "zbase.internal.load_conversation_retrying",
                    Value::Map(vec![
                        (
                            Value::from("conversation_id"),
                            Value::from(conversation_id.0.clone()),
                        ),
                        (Value::from("attempt"), Value::from(attempt as i64 + 1)),
                        (Value::from("delay_ms"), Value::from(backoff_ms as i64)),
                        (Value::from("error"), Value::from(error.to_string())),
                    ]),
                );
                thread::sleep(Duration::from_millis(backoff_ms));
                attempt += 1;
            }
            Err(error) => break Err(error),
        }
    };

    match result {
        Ok(loaded) => {
            outcome.complete();
            let service_fetch_ms = service_fetch_started.elapsed().as_millis();
            let LoadedConversation {
                page,
                pinned_state,
                thread_fetch_ms,
                pinned_fetch_ms,
            } = loaded;

            let pinned_emit_started = Instant::now();
            if let Some(pinned) = pinned_state {
                let _ = sender.send(BackendEvent::PinnedStateUpdated {
                    conversation_id: conversation_id.clone(),
                    pinned,
                });
            }
            let pinned_emit_ms = pinned_emit_started.elapsed().as_millis();
            let ThreadPage {
                mut messages,
                reaction_deltas,
                ..
            } = page;
            let reaction_delta_count = reaction_deltas.len();

            let reaction_delta_started = Instant::now();
            for delete_event in reaction_op_delete_events(&reaction_deltas) {
                if sender.send(delete_event).is_err() {
                    break;
                }
            }
            let reaction_delta_ms = reaction_delta_started.elapsed().as_millis();

            strip_reaction_delete_tombstones(&local_store, &conversation_id, &mut messages);
            apply_regular_delete_tombstones(
                &local_store,
                &conversation_id,
                Some(&sender),
                &mut messages,
            );
            strip_reaction_delete_tombstones_from_deltas(&mut messages, &reaction_deltas);
            let reaction_hydrate_started = Instant::now();
            let message_ids = messages
                .iter()
                .map(|message| message.id.clone())
                .collect::<Vec<_>>();
            let loaded_reactions = local_store
                .load_message_reactions_for_messages(&conversation_id, &message_ids)
                .unwrap_or_default();
            for message in &mut messages {
                message.reactions = domain_message_reactions(loaded_reactions.get(&message.id));
            }
            let reaction_hydrate_ms = reaction_hydrate_started.elapsed().as_millis();
            let attachment_hydration_candidates = messages
                .iter()
                .filter(|message| {
                    message_needs_image_attachment_hydration(message)
                        || message_needs_video_attachment_hydration(message)
                        || message_needs_file_attachment_hydration(message)
                })
                .map(|message| message.id.clone())
                .collect::<Vec<_>>();
            let attachment_hydration_candidate_count = attachment_hydration_candidates.len();

            let ingest_started = Instant::now();
            let mut usernames = Vec::new();
            let mut timeline_messages = Vec::with_capacity(messages.len());
            for mut message in messages {
                usernames.push(message.author_id.0.clone());
                ingest_message_record_for_timeline_load(Some(&sender), &local_store, &mut message);
                timeline_messages.push(message);
            }
            let ingest_ms = ingest_started.elapsed().as_millis();

            let normalize_started = Instant::now();
            normalize_message_records(&mut timeline_messages);
            let older_cursor = timeline_messages
                .first()
                .map(|message| message.id.0.clone());
            let newer_cursor = timeline_messages.last().map(|message| message.id.0.clone());
            let timeline_message_count = timeline_messages.len();
            let normalize_ms = normalize_started.elapsed().as_millis();

            let timeline_emit_started = Instant::now();
            if sender
                .send(BackendEvent::TimelineReplaced {
                    conversation_id: conversation_id.clone(),
                    messages: timeline_messages,
                    older_cursor,
                    newer_cursor,
                })
                .is_err()
            {
                return;
            }
            let timeline_emit_ms = timeline_emit_started.elapsed().as_millis();
            spawn_hydrate_timeline_attachments(
                &sender,
                &local_store,
                &conversation_id,
                &conversation_ref,
                &path,
                attachment_hydration_candidates,
            );
            spawn_persist_reaction_deltas_for_timeline_load(
                &sender,
                &local_store,
                conversation_id.clone(),
                reaction_deltas,
                message_ids,
            );

            let post_sync_started = Instant::now();
            usernames.sort();
            usernames.dedup();
            if !usernames.is_empty() {
                spawn_prefetch_user_profiles(&sender, &local_store, usernames);
            }
            spawn_sync_conversation_emojis(&sender, &local_store, conversation_id.clone());
            let should_sync_team_roles = local_store
                .get_conversation(&conversation_id)
                .ok()
                .flatten()
                .is_some_and(|summary| summary.kind == ConversationKind::Channel);
            let team_roles_sync_started = Instant::now();
            if should_sync_team_roles {
                spawn_sync_conversation_team_roles(
                    &sender,
                    &local_store,
                    conversation_id.clone(),
                    conversation_ref.clone(),
                    false,
                );
            }
            let team_roles_sync_ms = team_roles_sync_started.elapsed().as_millis();
            let post_sync_ms = post_sync_started.elapsed().as_millis();
            send_internal(
                &sender,
                "zbase.internal.load_conversation_loaded",
                Value::from(conversation_id.0.clone()),
            );
            let total_ms = load_started.elapsed().as_millis();
            if load_conversation_perf_log_all_enabled()
                || queue_wait_ms >= 150
                || total_ms >= 250
                || service_fetch_ms >= 150
                || ingest_ms >= 100
            {
                warn!(
                    target: "zbase.load_conversation.perf",
                    conversation_id = %conversation_id.0,
                    queue_wait_ms = queue_wait_ms as i64,
                    total_ms = total_ms as i64,
                    runtime_init_ms = runtime_init_ms as i64,
                    service_fetch_ms = service_fetch_ms as i64,
                    thread_fetch_ms = thread_fetch_ms as i64,
                    pinned_fetch_ms = pinned_fetch_ms as i64,
                    pinned_emit_ms = pinned_emit_ms as i64,
                    reaction_delta_ms = reaction_delta_ms as i64,
                    reaction_hydrate_ms = reaction_hydrate_ms as i64,
                    ingest_ms = ingest_ms as i64,
                    normalize_ms = normalize_ms as i64,
                    timeline_emit_ms = timeline_emit_ms as i64,
                    post_sync_ms = post_sync_ms as i64,
                    team_roles_sync_ms = team_roles_sync_ms as i64,
                    message_count = timeline_message_count as i64,
                    reaction_delta_count = reaction_delta_count as i64,
                    attachment_hydration_candidates = attachment_hydration_candidate_count as i64,
                    "load_conversation_timing"
                );
            }
        }
        Err(error) => {
            let total_ms = load_started.elapsed().as_millis();
            let service_fetch_ms = service_fetch_started.elapsed().as_millis();
            warn!(
                target: "zbase.load_conversation.perf",
                conversation_id = %conversation_id.0,
                queue_wait_ms = queue_wait_ms as i64,
                total_ms = total_ms as i64,
                runtime_init_ms = runtime_init_ms as i64,
                service_fetch_ms = service_fetch_ms as i64,
                error = %error,
                "load_conversation_failed"
            );
            send_internal(
                &sender,
                "zbase.internal.load_conversation_failed",
                Value::from(error.to_string()),
            );
        }
    }
}

fn load_conversation_perf_log_all_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("ZBASE_LOAD_CONVERSATION_PERF_LOG_ALL")
            .ok()
            .is_some_and(|raw| {
                matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
    })
}

fn thread_open_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("ZBASE_THREAD_OPEN_PROFILE")
            .ok()
            .is_some_and(|raw| {
                matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
    })
}

fn spawn_hydrate_timeline_attachments(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    conversation_id: &ConversationId,
    conversation_ref: &ProviderConversationRef,
    socket_path: &Path,
    message_ids: Vec<MessageId>,
) {
    if message_ids.is_empty() {
        return;
    }
    let Some(raw_conversation_id) = provider_ref_to_conversation_id_bytes(conversation_ref) else {
        return;
    };
    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    let conversation_id = conversation_id.clone();
    let socket_path = socket_path.to_path_buf();
    let dedupe_key = format!("hydrate_attachments:{}", conversation_id.0);
    let _ = task_runtime::spawn_task(TaskPriority::Low, Some(dedupe_key), move || {
        run_hydrate_timeline_attachments(
            sender,
            local_store,
            conversation_id,
            raw_conversation_id,
            socket_path,
            message_ids,
        );
    });
}

fn run_hydrate_timeline_attachments(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    conversation_id: ConversationId,
    raw_conversation_id: Vec<u8>,
    socket_path: PathBuf,
    message_ids: Vec<MessageId>,
) {
    let candidate_count = message_ids.len();
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(_) => return,
    };
    let mut messages = message_ids
        .into_iter()
        .filter_map(|message_id| {
            local_store
                .get_message(&conversation_id, &message_id)
                .ok()
                .flatten()
        })
        .filter(|message| {
            message_needs_image_attachment_hydration(message)
                || message_needs_video_attachment_hydration(message)
                || message_needs_file_attachment_hydration(message)
        })
        .collect::<Vec<_>>();
    if messages.is_empty() {
        return;
    }
    let scanned_message_count = messages.len();
    let hydration_started = Instant::now();
    let hydrate_result = runtime.block_on(async move {
        let transport = FramedMsgpackTransport::connect(&socket_path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        let message_limit = messages.len();
        let updated_ids = hydrate_attachment_paths_for_messages(
            &mut client,
            &raw_conversation_id,
            &mut messages,
            message_limit,
        )
        .await;
        Ok::<_, io::Error>((messages, updated_ids))
    });
    let (messages, updated_ids) = match hydrate_result {
        Ok(result) => result,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.load_conversation_attachment_hydration_failed",
                Value::Map(vec![
                    (
                        Value::from("conversation_id"),
                        Value::from(conversation_id.0.clone()),
                    ),
                    (Value::from("error"), Value::from(error.to_string())),
                ]),
            );
            return;
        }
    };
    if updated_ids.is_empty() {
        return;
    }
    let updated_ids = updated_ids.into_iter().collect::<HashSet<_>>();
    let mut updated_count = 0usize;
    for message in messages {
        if !updated_ids.contains(&message.id) {
            continue;
        }
        publish_attachment_paths(&sender, &local_store, &message);
        updated_count = updated_count.saturating_add(1);
    }
    let elapsed_ms = hydration_started.elapsed().as_millis();
    if load_conversation_perf_log_all_enabled() || elapsed_ms >= 250 {
        warn!(
            target: "zbase.load_conversation.perf",
            conversation_id = %conversation_id.0,
            elapsed_ms = elapsed_ms as i64,
            candidate_count = candidate_count as i64,
            scanned_message_count = scanned_message_count as i64,
            updated_message_count = updated_count as i64,
            "load_conversation_attachment_hydration"
        );
    }
}

fn run_load_older_messages(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    conversation_id: ConversationId,
    before_id: MessageId,
    raw_conversation_id: Vec<u8>,
    path: PathBuf,
) {
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.load_older_messages_runtime_init_failed",
                Value::from(error.to_string()),
            );
            let _ = sender.send(BackendEvent::CommandFailed {
                command: Box::new(BackendCommand::LoadOlderMessages {
                    key: crate::state::state::TimelineKey::Conversation(conversation_id),
                    cursor: before_id.0,
                }),
                error: error.to_string(),
            });
            return;
        }
    };

    let mut messages;
    let reaction_delete_events;
    let fetched_page_has_more;

    let conversation_id_for_fetch = conversation_id.clone();
    let before_id_for_fetch = before_id.clone();
    match runtime.block_on(async move {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        fetch_thread_messages_before_anchor(
            &mut client,
            &conversation_id_for_fetch,
            &raw_conversation_id,
            &before_id_for_fetch,
            LOAD_OLDER_PAGE_SIZE,
        )
        .await
    }) {
        Ok(fetched) => {
            fetched_page_has_more =
                page_may_have_more_older_messages(&fetched, LOAD_OLDER_PAGE_SIZE);
            persist_reaction_deltas(Some(&sender), &local_store, &fetched.reaction_deltas);
            reaction_delete_events = reaction_op_delete_events(&fetched.reaction_deltas);
            let mut fetched_messages = fetched.messages;
            strip_reaction_delete_tombstones(&local_store, &conversation_id, &mut fetched_messages);
            apply_regular_delete_tombstones(
                &local_store,
                &conversation_id,
                Some(&sender),
                &mut fetched_messages,
            );
            let message_ids = fetched_messages
                .iter()
                .map(|message| message.id.clone())
                .collect::<Vec<_>>();
            let loaded_reactions = local_store
                .load_message_reactions_for_messages(&conversation_id, &message_ids)
                .unwrap_or_default();
            let mut hydrated = Vec::with_capacity(fetched_messages.len());
            for mut message in fetched_messages {
                message.reactions = domain_message_reactions(loaded_reactions.get(&message.id));
                ingest_message_record(Some(&sender), &local_store, &mut message);
                hydrated.push(message);
            }
            messages = hydrated;
            retain_strictly_older_messages(&mut messages, &before_id);
        }
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.load_older_messages_fetch_failed",
                Value::from(error.to_string()),
            );
            let _ = sender.send(BackendEvent::CommandFailed {
                command: Box::new(BackendCommand::LoadOlderMessages {
                    key: crate::state::state::TimelineKey::Conversation(conversation_id),
                    cursor: before_id.0,
                }),
                error: error.to_string(),
            });
            return;
        }
    }

    for delete_event in reaction_delete_events {
        if sender.send(delete_event).is_err() {
            return;
        }
    }
    let next_cursor = next_older_cursor(
        &local_store,
        &conversation_id,
        &messages,
        fetched_page_has_more,
    );
    let _ = sender.send(BackendEvent::MessagesPrepended {
        key: crate::state::state::TimelineKey::Conversation(conversation_id),
        messages,
        cursor: next_cursor,
    });
}

fn run_backfill_thread_history(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    conversation_id: ConversationId,
    requested_root_id: MessageId,
    raw_conversation_id: Vec<u8>,
    path: PathBuf,
) {
    maybe_emit_thread_debug_snapshot(
        &sender,
        &local_store,
        &conversation_id,
        &requested_root_id,
        &requested_root_id,
        "thread_backfill.start",
    );

    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                &sender,
                "zbase.internal.thread_backfill_runtime_init_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    let mut anchor = requested_root_id.clone();
    let mut seen_anchors = HashSet::new();

    for _ in 0..THREAD_BACKFILL_OLDER_PAGE_LIMIT {
        if !seen_anchors.insert(anchor.clone()) {
            break;
        }

        let conversation_id_for_fetch = conversation_id.clone();
        let anchor_for_fetch = anchor.clone();
        let path_for_fetch = path.clone();
        let raw_conversation_id_for_fetch = raw_conversation_id.clone();
        let fetch_result = runtime.block_on(async move {
            let transport = FramedMsgpackTransport::connect(&path_for_fetch).await?;
            let mut client = KeybaseRpcClient::new(transport);
            fetch_thread_messages_before_anchor_impl(
                &mut client,
                &conversation_id_for_fetch,
                &raw_conversation_id_for_fetch,
                &anchor_for_fetch,
                CRAWL_PAGE_SIZE,
                false,
            )
            .await
        });

        let Ok(page) = fetch_result else {
            break;
        };
        if page.messages.is_empty() {
            break;
        }

        persist_reaction_deltas(Some(&sender), &local_store, &page.reaction_deltas);
        for delete_event in reaction_op_delete_events(&page.reaction_deltas) {
            let _ = sender.send(delete_event);
        }

        let oldest_id = page.messages.first().map(|message| message.id.clone());
        let message_ids = page
            .messages
            .iter()
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        let loaded_reactions = local_store
            .load_message_reactions_for_messages(&conversation_id, &message_ids)
            .unwrap_or_default();
        for mut message in page.messages {
            message.reactions = domain_message_reactions(loaded_reactions.get(&message.id));
            ingest_message_record(Some(&sender), &local_store, &mut message);
        }

        let Some(oldest_id) = oldest_id else {
            break;
        };
        if oldest_id == anchor {
            break;
        }
        anchor = oldest_id;
    }

    rebuild_thread_index_from_cached_messages(
        &local_store,
        &conversation_id,
        THREAD_REINDEX_SCAN_LIMIT,
    );

    let canonical_root = local_store
        .get_message(&conversation_id, &requested_root_id)
        .ok()
        .flatten()
        .map(|message| {
            resolve_thread_root_for_message(
                &local_store,
                &conversation_id,
                &requested_root_id,
                message.reply_to.as_ref(),
            )
        })
        .unwrap_or(requested_root_id.clone());
    maybe_emit_thread_debug_snapshot(
        &sender,
        &local_store,
        &conversation_id,
        &requested_root_id,
        &canonical_root,
        "thread_backfill.after_older",
    );

    let mut newer_pivot = canonical_root.clone();
    let mut seen_newer_pivots = HashSet::new();
    for _ in 0..THREAD_BACKFILL_NEWER_PAGE_LIMIT {
        if !seen_newer_pivots.insert(newer_pivot.clone()) {
            break;
        }

        let conversation_id_for_fetch = conversation_id.clone();
        let pivot_for_fetch = newer_pivot.clone();
        let path_for_fetch = path.clone();
        let raw_conversation_id_for_fetch = raw_conversation_id.clone();
        let fetch_result = runtime.block_on(async move {
            let transport = FramedMsgpackTransport::connect(&path_for_fetch).await?;
            let mut client = KeybaseRpcClient::new(transport);
            fetch_thread_messages_after_anchor_impl(
                &mut client,
                &conversation_id_for_fetch,
                &raw_conversation_id_for_fetch,
                &pivot_for_fetch,
                CRAWL_PAGE_SIZE,
                false,
            )
            .await
        });

        let Ok(page) = fetch_result else {
            break;
        };
        if page.messages.is_empty() {
            break;
        }

        persist_reaction_deltas(Some(&sender), &local_store, &page.reaction_deltas);
        for delete_event in reaction_op_delete_events(&page.reaction_deltas) {
            let _ = sender.send(delete_event);
        }

        let mut page_messages = page.messages;
        strip_reaction_delete_tombstones(&local_store, &conversation_id, &mut page_messages);
        apply_regular_delete_tombstones(
            &local_store,
            &conversation_id,
            Some(&sender),
            &mut page_messages,
        );
        let newest_id = page_messages.last().map(|message| message.id.clone());
        let message_ids = page_messages
            .iter()
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        let loaded_reactions = local_store
            .load_message_reactions_for_messages(&conversation_id, &message_ids)
            .unwrap_or_default();
        for mut message in page_messages {
            message.reactions = domain_message_reactions(loaded_reactions.get(&message.id));
            ingest_message_record(Some(&sender), &local_store, &mut message);
        }

        let Some(newest_id) = newest_id else {
            break;
        };
        if newest_id == newer_pivot {
            break;
        }
        newer_pivot = newest_id;
    }

    rebuild_thread_index_from_cached_messages(
        &local_store,
        &conversation_id,
        THREAD_REINDEX_SCAN_LIMIT,
    );
    maybe_emit_thread_debug_snapshot(
        &sender,
        &local_store,
        &conversation_id,
        &requested_root_id,
        &canonical_root,
        "thread_backfill.after_newer",
    );

    if let Ok(messages) = local_store.load_thread_messages(&conversation_id, &canonical_root) {
        maybe_emit_thread_debug_snapshot(
            &sender,
            &local_store,
            &conversation_id,
            &requested_root_id,
            &canonical_root,
            "thread_backfill.before_emit",
        );
        let _ = sender.send(BackendEvent::MessagesPrepended {
            key: crate::state::state::TimelineKey::Thread {
                conversation_id,
                root_id: canonical_root,
                requested_root_id,
            },
            messages,
            cursor: None,
        });
    }
}

fn should_emit_thread_debug(root_id: &MessageId) -> bool {
    if let Ok(configured) = std::env::var("ZBASE_THREAD_DEBUG_ROOT_ID") {
        let value = configured.trim();
        if !value.is_empty() {
            return value == root_id.0;
        }
    }
    std::env::var("ZBASE_THREAD_DEBUG")
        .ok()
        .map(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn to_value_array(ids: &[String], limit: usize) -> Value {
    Value::Array(
        ids.iter()
            .take(limit)
            .map(|id| Value::from(id.clone()))
            .collect(),
    )
}

fn maybe_emit_thread_debug_snapshot(
    sender: &Sender<BackendEvent>,
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    requested_root: &MessageId,
    effective_root: &MessageId,
    stage: &str,
) {
    if !should_emit_thread_debug(requested_root) && !should_emit_thread_debug(effective_root) {
        return;
    }

    let indexed_messages = local_store
        .load_thread_messages(conversation_id, effective_root)
        .unwrap_or_default();
    let indexed_ids = indexed_messages
        .iter()
        .map(|message| message.id.0.clone())
        .collect::<HashSet<_>>();
    let mut indexed_ids_sorted = indexed_ids.iter().cloned().collect::<Vec<_>>();
    indexed_ids_sorted.sort_unstable();

    let scanned = local_store
        .load_messages_before(conversation_id, None, THREAD_DEBUG_SCAN_LIMIT)
        .unwrap_or_default();
    let mut direct_reply_ids = scanned
        .iter()
        .filter(|message| message.reply_to.as_ref() == Some(effective_root))
        .map(|message| message.id.0.clone())
        .collect::<Vec<_>>();
    direct_reply_ids.sort_unstable();
    direct_reply_ids.dedup();

    let mut rooted_ids = scanned
        .iter()
        .filter(|message| message.thread_root_id.as_ref() == Some(effective_root))
        .map(|message| message.id.0.clone())
        .collect::<Vec<_>>();
    rooted_ids.sort_unstable();
    rooted_ids.dedup();

    let mut missing_direct_ids = direct_reply_ids
        .iter()
        .filter(|id| !indexed_ids.contains(*id))
        .cloned()
        .collect::<Vec<_>>();
    missing_direct_ids.sort_unstable();

    let mut missing_rooted_ids = rooted_ids
        .iter()
        .filter(|id| !indexed_ids.contains(*id))
        .cloned()
        .collect::<Vec<_>>();
    missing_rooted_ids.sort_unstable();

    let advertised_reply_count = local_store
        .get_message(conversation_id, effective_root)
        .ok()
        .flatten()
        .map(|message| message.thread_reply_count as i64)
        .unwrap_or(0);

    send_internal(
        sender,
        "zbase.internal.thread_debug.snapshot",
        Value::Map(vec![
            (Value::from("stage"), Value::from(stage.to_string())),
            (
                Value::from("conversation_id"),
                Value::from(conversation_id.0.clone()),
            ),
            (
                Value::from("requested_root"),
                Value::from(requested_root.0.clone()),
            ),
            (
                Value::from("effective_root"),
                Value::from(effective_root.0.clone()),
            ),
            (
                Value::from("advertised_reply_count"),
                Value::from(advertised_reply_count),
            ),
            (
                Value::from("indexed_count"),
                Value::from(indexed_messages.len() as i64),
            ),
            (
                Value::from("direct_reply_count"),
                Value::from(direct_reply_ids.len() as i64),
            ),
            (
                Value::from("rooted_count"),
                Value::from(rooted_ids.len() as i64),
            ),
            (
                Value::from("missing_direct_count"),
                Value::from(missing_direct_ids.len() as i64),
            ),
            (
                Value::from("missing_rooted_count"),
                Value::from(missing_rooted_ids.len() as i64),
            ),
            (
                Value::from("indexed_ids"),
                to_value_array(&indexed_ids_sorted, THREAD_DEBUG_SAMPLE_LIMIT),
            ),
            (
                Value::from("direct_reply_ids"),
                to_value_array(&direct_reply_ids, THREAD_DEBUG_SAMPLE_LIMIT),
            ),
            (
                Value::from("rooted_ids"),
                to_value_array(&rooted_ids, THREAD_DEBUG_SAMPLE_LIMIT),
            ),
            (
                Value::from("missing_direct_ids"),
                to_value_array(&missing_direct_ids, THREAD_DEBUG_SAMPLE_LIMIT),
            ),
            (
                Value::from("missing_rooted_ids"),
                to_value_array(&missing_rooted_ids, THREAD_DEBUG_SAMPLE_LIMIT),
            ),
        ]),
    );
}

fn raw_conversation_id_from_canonical(conversation_id: &ConversationId) -> Option<Vec<u8>> {
    provider_ref_to_conversation_id_bytes(&ProviderConversationRef::new(conversation_id.0.clone()))
}

fn reply_ancestor_backfill_key(conversation_id: &ConversationId, parent_id: &MessageId) -> String {
    format!("{}:{}", conversation_id.0, parent_id.0)
}

fn try_mark_reply_ancestor_backfill_in_flight(key: &str) -> bool {
    let mutex = REPLY_ANCESTOR_BACKFILL_IN_FLIGHT.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(key.to_string())
}

fn clear_reply_ancestor_backfill_in_flight(key: &str) {
    let Some(mutex) = REPLY_ANCESTOR_BACKFILL_IN_FLIGHT.get() else {
        return;
    };
    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(key);
}

fn normalize_sync_key(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

fn try_mark_throttled_in_flight(
    key: &str,
    cooldown_ms: i64,
    in_flight: &OnceLock<Mutex<HashSet<String>>>,
    last_start: &OnceLock<Mutex<HashMap<String, i64>>>,
) -> bool {
    let now_ms = now_unix_ms();
    {
        let mutex = last_start.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(last_ms) = guard.get(key)
            && now_ms.saturating_sub(*last_ms) < cooldown_ms
        {
            return false;
        }
        guard.insert(key.to_string(), now_ms);
    }

    let mutex = in_flight.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.insert(key.to_string())
}

fn clear_marked_in_flight(key: &str, in_flight: &OnceLock<Mutex<HashSet<String>>>) {
    let Some(mutex) = in_flight.get() else {
        return;
    };
    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    guard.remove(key);
}

fn spawn_prefetch_user_profiles(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    usernames: Vec<String>,
) {
    let mut queued_keys = Vec::new();
    let mut queued_usernames = Vec::new();
    for username in usernames {
        let trimmed = username.trim();
        let Some(key) = normalize_sync_key(trimmed) else {
            continue;
        };
        if !try_mark_throttled_in_flight(
            &key,
            PROFILE_SYNC_COOLDOWN_MS,
            &PROFILE_SYNC_IN_FLIGHT,
            &PROFILE_SYNC_LAST_START_MS,
        ) {
            continue;
        }
        queued_keys.push(key);
        queued_usernames.push(trimmed.to_string());
    }
    if queued_usernames.is_empty() {
        return;
    }

    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    let _ = task_runtime::spawn_task(TaskPriority::Low, None, move || {
        prefetch_user_profiles(&sender, &queued_usernames, &local_store);
        for key in queued_keys {
            clear_marked_in_flight(&key, &PROFILE_SYNC_IN_FLIGHT);
        }
    });
}

fn spawn_prefetch_team_avatars(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    team_names: Vec<String>,
) {
    let mut queued_keys = Vec::new();
    let mut queued_names = Vec::new();
    for team_name in team_names {
        let trimmed = team_name.trim();
        let Some(key) = normalize_sync_key(trimmed) else {
            continue;
        };
        if !try_mark_throttled_in_flight(
            &key,
            TEAM_AVATAR_SYNC_COOLDOWN_MS,
            &TEAM_AVATAR_SYNC_IN_FLIGHT,
            &TEAM_AVATAR_SYNC_LAST_START_MS,
        ) {
            continue;
        }
        queued_keys.push(key);
        queued_names.push(trimmed.to_string());
    }
    if queued_names.is_empty() {
        return;
    }

    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    let _ = task_runtime::spawn_task(TaskPriority::Low, None, move || {
        prefetch_team_avatars(&sender, &queued_names, &local_store);
        for key in queued_keys {
            clear_marked_in_flight(&key, &TEAM_AVATAR_SYNC_IN_FLIGHT);
        }
    });
}

fn spawn_sync_conversation_emojis(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    conversation_id: ConversationId,
) {
    let key = conversation_id.0.clone();
    if key.is_empty() {
        return;
    }
    if !try_mark_throttled_in_flight(
        &key,
        EMOJI_SYNC_COOLDOWN_MS,
        &EMOJI_SYNC_IN_FLIGHT,
        &EMOJI_SYNC_LAST_START_MS,
    ) {
        return;
    }

    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    let _ = task_runtime::spawn_task(
        TaskPriority::Low,
        Some(format!("sync_conversation_emojis:{key}")),
        move || {
            sync_conversation_emojis(&sender, &local_store, &conversation_id);
            clear_marked_in_flight(&key, &EMOJI_SYNC_IN_FLIGHT);
        },
    );
}

fn spawn_sync_message_emoji_sources(sender: &Sender<BackendEvent>, message: &MessageRecord) {
    let mut queued = HashMap::<String, (EmojiSourceRef, String)>::new();
    for fragment in &message.fragments {
        let MessageFragment::Emoji {
            alias,
            source_ref: Some(source_ref),
        } = fragment
        else {
            continue;
        };
        if source_ref.backend_id.0 != KEYBASE_BACKEND_ID {
            continue;
        }
        let cache_key = source_ref.cache_key();
        queued
            .entry(cache_key)
            .or_insert_with(|| (source_ref.clone(), alias.clone()));
    }

    for (_, (source_ref, alias)) in queued {
        spawn_sync_emoji_source(sender, source_ref, alias);
    }
}

fn schedule_message_emoji_source_syncs(
    sender: Option<&Sender<BackendEvent>>,
    messages: &[MessageRecord],
) {
    let Some(sender) = sender else {
        return;
    };
    for message in messages {
        spawn_sync_message_emoji_sources(sender, message);
    }
}

fn spawn_sync_reaction_emoji_sources(
    sender: &Sender<BackendEvent>,
    deltas: &[MessageReactionDelta],
) {
    let mut queued = HashMap::<String, (EmojiSourceRef, String)>::new();
    for delta in deltas {
        let Some(source_ref) = delta.source_ref.clone() else {
            continue;
        };
        if source_ref.backend_id.0 != KEYBASE_BACKEND_ID {
            continue;
        }
        let alias = normalize_shortcode_alias(&delta.emoji).unwrap_or_else(|| delta.emoji.clone());
        queued
            .entry(source_ref.cache_key())
            .or_insert_with(|| (source_ref, alias));
    }
    for (_, (source_ref, alias)) in queued {
        spawn_sync_emoji_source(sender, source_ref, alias);
    }
}

fn spawn_sync_emoji_source(
    sender: &Sender<BackendEvent>,
    source_ref: EmojiSourceRef,
    alias: String,
) {
    let cache_key = source_ref.cache_key();
    if !try_mark_throttled_in_flight(
        &cache_key,
        EMOJI_SOURCE_SYNC_COOLDOWN_MS,
        &EMOJI_SOURCE_SYNC_IN_FLIGHT,
        &EMOJI_SOURCE_SYNC_LAST_START_MS,
    ) {
        return;
    }
    let sender = sender.clone();
    let _ = task_runtime::spawn_task(
        TaskPriority::Low,
        Some(format!("sync_emoji_source:{cache_key}")),
        move || {
            sync_message_emoji_source(&sender, &source_ref, &alias);
            clear_marked_in_flight(&cache_key, &EMOJI_SOURCE_SYNC_IN_FLIGHT);
        },
    );
}

fn sync_message_emoji_source(
    sender: &Sender<BackendEvent>,
    source_ref: &EmojiSourceRef,
    alias: &str,
) {
    let Some((raw_conversation_id, message_id)) = keybase_emoji_source_target(source_ref) else {
        return;
    };
    let Some(socket) = socket_path() else {
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(_) => return,
    };

    let sender = sender.clone();
    let source_ref = source_ref.clone();
    let alias = alias.to_string();
    runtime.block_on(async move {
        let transport = match FramedMsgpackTransport::connect(&socket).await {
            Ok(transport) => transport,
            Err(_) => return,
        };
        let mut client = KeybaseRpcClient::new(transport);
        let mut asset_path =
            download_attachment_to_cache(&mut client, &raw_conversation_id, message_id, false)
                .await;
        if asset_path.is_none() {
            asset_path =
                download_attachment_to_cache(&mut client, &raw_conversation_id, message_id, true)
                    .await;
        }
        let Some(asset_path) = asset_path else {
            return;
        };
        let _ = sender.send(BackendEvent::EmojiSourceSynced {
            source_ref,
            alias,
            unicode: None,
            asset_path: Some(asset_path),
            updated_ms: now_unix_ms(),
        });
    });
}

fn keybase_emoji_source_target(source_ref: &EmojiSourceRef) -> Option<(Vec<u8>, i64)> {
    if source_ref.backend_id.0 != KEYBASE_BACKEND_ID {
        return None;
    }
    let mut conv_hex = None::<&str>;
    let mut message_id = None::<i64>;
    for part in source_ref.ref_key.split(':') {
        if let Some(value) = part.strip_prefix("conv=") {
            conv_hex = Some(value);
        }
        if let Some(value) = part.strip_prefix("msg=") {
            message_id = value.parse::<i64>().ok();
        }
    }
    let conv_bytes = conv_hex.and_then(hex_decode)?;
    Some((conv_bytes, message_id?))
}

fn should_retry_load_conversation_error(error: &io::Error) -> bool {
    if matches!(error.raw_os_error(), Some(24 | 23 | 35)) {
        return true;
    }
    if matches!(
        error.kind(),
        io::ErrorKind::WouldBlock
            | io::ErrorKind::Interrupted
            | io::ErrorKind::TimedOut
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::NotConnected
    ) {
        return true;
    }
    let lower = error.to_string().to_ascii_lowercase();
    lower.contains("too many open files") || lower.contains("resource temporarily unavailable")
}

fn request_inbox_unread_refresh() -> bool {
    INBOX_UNREAD_REFRESH
        .get_or_init(|| Mutex::new(InboxUnreadRefresh::default()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .request()
}

fn finish_inbox_unread_refresh() -> bool {
    INBOX_UNREAD_REFRESH
        .get_or_init(|| Mutex::new(InboxUnreadRefresh::default()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .finish()
}

fn reply_ancestor_chain_cached(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    parent_id: &MessageId,
) -> bool {
    let mut current = parent_id.clone();
    let mut seen = HashSet::new();
    while seen.insert(current.clone()) {
        let Some(message) = local_store
            .get_message(conversation_id, &current)
            .ok()
            .flatten()
        else {
            return false;
        };
        let Some(next_parent) = message.reply_to else {
            return true;
        };
        current = next_parent;
    }
    true
}

fn maybe_schedule_reply_ancestor_backfill(
    sender: Option<Sender<BackendEvent>>,
    local_store: Arc<LocalStore>,
    message: &MessageRecord,
) {
    let Some(reply_to) = message.reply_to.clone() else {
        return;
    };
    if reply_ancestor_chain_cached(&local_store, &message.conversation_id, &reply_to) {
        return;
    }
    let Some(raw_conversation_id) = raw_conversation_id_from_canonical(&message.conversation_id)
    else {
        return;
    };
    let Some(path) = socket_path() else {
        return;
    };

    let in_flight_key = reply_ancestor_backfill_key(&message.conversation_id, &reply_to);
    if !try_mark_reply_ancestor_backfill_in_flight(&in_flight_key) {
        return;
    }

    let conversation_id = message.conversation_id.clone();
    let child_message_id = message.id.clone();
    let sender_for_stats = sender.clone();
    let task_key = in_flight_key.clone();
    let in_flight_key_for_task = in_flight_key.clone();
    let scheduled = task_runtime::spawn_task(TaskPriority::High, Some(task_key), move || {
        run_reply_ancestor_backfill(
            sender,
            local_store,
            conversation_id,
            child_message_id,
            reply_to,
            raw_conversation_id,
            path,
        );
        clear_reply_ancestor_backfill_in_flight(&in_flight_key_for_task);
    });
    if !scheduled {
        clear_reply_ancestor_backfill_in_flight(&in_flight_key);
    }
    if let Some(sender) = sender_for_stats.as_ref() {
        emit_task_runtime_stats(sender, "reply_ancestor_backfill.schedule");
    }
}

fn run_reply_ancestor_backfill(
    sender: Option<Sender<BackendEvent>>,
    local_store: Arc<LocalStore>,
    conversation_id: ConversationId,
    child_message_id: MessageId,
    required_parent_id: MessageId,
    raw_conversation_id: Vec<u8>,
    path: PathBuf,
) {
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(_) => return,
    };
    let mut anchor = child_message_id.clone();
    let mut seen_anchors = HashSet::new();
    let mut fetched_any = false;

    for _ in 0..REPLY_ANCESTOR_BACKFILL_PAGE_LIMIT {
        if reply_ancestor_chain_cached(&local_store, &conversation_id, &required_parent_id) {
            break;
        }
        if !seen_anchors.insert(anchor.clone()) {
            break;
        }

        let conversation_id_for_fetch = conversation_id.clone();
        let anchor_for_fetch = anchor.clone();
        let path_for_fetch = path.clone();
        let raw_conversation_id_for_fetch = raw_conversation_id.clone();
        let fetch_result = runtime.block_on(async move {
            let transport = FramedMsgpackTransport::connect(&path_for_fetch).await?;
            let mut client = KeybaseRpcClient::new(transport);
            fetch_thread_messages_before_anchor_impl(
                &mut client,
                &conversation_id_for_fetch,
                &raw_conversation_id_for_fetch,
                &anchor_for_fetch,
                CRAWL_PAGE_SIZE,
                false,
            )
            .await
        });

        let Ok(page) = fetch_result else {
            break;
        };
        if page.messages.is_empty() {
            break;
        }
        fetched_any = true;

        persist_reaction_deltas(sender.as_ref(), &local_store, &page.reaction_deltas);
        if let Some(sender_ref) = sender.as_ref() {
            for delete_event in reaction_op_delete_events(&page.reaction_deltas) {
                let _ = sender_ref.send(delete_event);
            }
        }

        let oldest_id = page.messages.first().map(|message| message.id.clone());
        let message_ids = page
            .messages
            .iter()
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        let loaded_reactions = local_store
            .load_message_reactions_for_messages(&conversation_id, &message_ids)
            .unwrap_or_default();
        for mut message in page.messages {
            message.reactions = domain_message_reactions(loaded_reactions.get(&message.id));
            ingest_message_record(sender.as_ref(), &local_store, &mut message);
            if let Some(sender_ref) = sender.as_ref() {
                let _ = sender_ref.send(BackendEvent::MessageUpserted(message));
            }
        }

        let Some(oldest_id) = oldest_id else {
            break;
        };
        if oldest_id == anchor {
            break;
        }
        anchor = oldest_id;
    }

    if fetched_any {
        rebuild_thread_index_from_cached_messages(
            &local_store,
            &conversation_id,
            THREAD_REINDEX_SCAN_LIMIT,
        );
    }

    if reply_ancestor_chain_cached(&local_store, &conversation_id, &required_parent_id)
        && let Ok(Some(mut child)) =
            local_store.get_message_with_reactions(&conversation_id, &child_message_id)
    {
        ingest_message_record(sender.as_ref(), &local_store, &mut child);
        if let Some(sender_ref) = sender.as_ref() {
            let _ = sender_ref.send(BackendEvent::MessageUpserted(child));
        }
    }
}

fn next_older_cursor(
    local_store: &Arc<LocalStore>,
    conversation_id: &ConversationId,
    messages: &[MessageRecord],
    fetched_page_has_more: bool,
) -> Option<String> {
    messages
        .first()
        .map(|message| MessageId::new(message.id.0.clone()))
        .and_then(|anchor| {
            if fetched_page_has_more {
                return Some(anchor.0);
            }
            let local_has_more = local_store
                .has_messages_before(conversation_id, &anchor)
                .ok()
                .unwrap_or(false);
            // Keep one extra page probe when we served a full page from cache:
            // this allows falling through to a live service fetch if local crawl
            // data is incomplete.
            let likely_has_more = local_has_more || messages.len() >= LOAD_OLDER_PAGE_SIZE;
            likely_has_more.then_some(anchor.0)
        })
}

fn compare_message_ids(left: &MessageId, right: &MessageId) -> std::cmp::Ordering {
    let left_num = left.0.parse::<u64>().ok();
    let right_num = right.0.parse::<u64>().ok();

    match (left_num, right_num) {
        (Some(left_num), Some(right_num)) => {
            left_num.cmp(&right_num).then_with(|| left.0.cmp(&right.0))
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.0.cmp(&right.0),
    }
}

fn message_id_is_before(candidate: &MessageId, pivot: &MessageId) -> bool {
    compare_message_ids(candidate, pivot).is_lt()
}

fn message_id_is_after(candidate: &MessageId, pivot: &MessageId) -> bool {
    compare_message_ids(candidate, pivot).is_gt()
}

fn retain_strictly_older_messages(messages: &mut Vec<MessageRecord>, before_id: &MessageId) {
    messages.retain(|message| message_id_is_before(&message.id, before_id));
    messages.sort_by(|left, right| compare_message_ids(&left.id, &right.id));
}

fn retain_strictly_newer_messages(messages: &mut Vec<MessageRecord>, after_id: &MessageId) {
    messages.retain(|message| message_id_is_after(&message.id, after_id));
    messages.sort_by(|left, right| compare_message_ids(&left.id, &right.id));
}

fn normalize_message_records(messages: &mut Vec<MessageRecord>) {
    if messages.len() <= 1 {
        return;
    }
    let mut latest_by_id = HashMap::with_capacity(messages.len());
    for mut message in std::mem::take(messages) {
        match latest_by_id.entry(message.id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(message);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if should_replace_duplicate_record(entry.get(), &message) {
                    crate::domain::message::keep_original_attachments(&mut message, entry.get());
                    entry.insert(message);
                } else {
                    crate::domain::message::keep_original_attachments(entry.get_mut(), &message);
                }
            }
        }
    }
    let mut normalized = latest_by_id.into_values().collect::<Vec<_>>();
    normalized.sort_by(|left, right| compare_message_ids(&left.id, &right.id));
    *messages = normalized;
}

fn should_replace_duplicate_record(existing: &MessageRecord, incoming: &MessageRecord) -> bool {
    match (&existing.edited, &incoming.edited) {
        (None, Some(_)) => true,
        (Some(_), None) => false,
        (Some(_), Some(_)) => {
            crate::domain::message::message_revision(incoming)
                > crate::domain::message::message_revision(existing)
        }
        (None, None) => incoming.timestamp_ms.unwrap_or(0) > existing.timestamp_ms.unwrap_or(0),
    }
}

fn reconcile_descendant_thread_roots(
    sender: Option<&Sender<BackendEvent>>,
    local_store: &Arc<LocalStore>,
    conversation_id: &ConversationId,
    ancestor_id: &MessageId,
) {
    let Ok(descendant_ids) = local_store.load_thread_descendants_bfs(conversation_id, ancestor_id)
    else {
        return;
    };
    if descendant_ids.is_empty() {
        return;
    }
    for descendant_id in descendant_ids {
        let Ok(Some(mut descendant)) =
            local_store.get_message_with_reactions(conversation_id, &descendant_id)
        else {
            continue;
        };
        let resolved_root = resolve_thread_root_for_message(
            local_store,
            conversation_id,
            &descendant.id,
            descendant.reply_to.as_ref(),
        );
        if descendant.thread_root_id.as_ref() == Some(&resolved_root) {
            continue;
        }
        descendant.thread_root_id = Some(resolved_root);
        let _ = local_store.persist_message(&descendant);
        if let Some(sender) = sender {
            let _ = sender.send(BackendEvent::MessageUpserted(descendant));
        }
    }
}

fn refresh_root_descendant_reply_count(
    sender: Option<&Sender<BackendEvent>>,
    local_store: &Arc<LocalStore>,
    conversation_id: &ConversationId,
    root_id: &MessageId,
) {
    let descendant_count = local_store
        .load_thread_descendants_bfs(conversation_id, root_id)
        .ok()
        .map(|descendants| descendants.len() as u32)
        .unwrap_or(0);
    let Ok(Some(mut root)) = local_store.get_message_with_reactions(conversation_id, root_id)
    else {
        return;
    };
    if root.thread_reply_count == descendant_count {
        return;
    }
    root.thread_reply_count = descendant_count;
    let _ = local_store.persist_message(&root);
    if let Some(sender) = sender {
        let _ = sender.send(BackendEvent::MessageUpserted(root));
    }
}

/// Spawn a background thread to download attachments that need hydration for a
/// live notification message. This ensures images are viewable immediately
/// instead of requiring a conversation reload.
fn publish_attachment_paths(
    sender: &Sender<BackendEvent>,
    local_store: &LocalStore,
    downloaded: &MessageRecord,
) {
    match local_store.patch_attachment_paths(
        &downloaded.conversation_id,
        &downloaded.id,
        &downloaded.attachments,
    ) {
        Ok(Some(latest)) => {
            let _ = sender.send(BackendEvent::MessageAttachmentsUpdated {
                conversation_id: latest.conversation_id,
                message_id: latest.id,
                attachments: latest.attachments,
            });
        }
        Ok(None) => {} // The message was deleted while its download was running.
        Err(error) => tracing::warn!(%error, "could not persist attachment paths"),
    }
}

fn spawn_live_attachment_hydration(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    message: &MessageRecord,
) {
    let needs_image = message_needs_image_attachment_hydration(message);
    let needs_video = message_needs_video_attachment_hydration(message);
    let needs_file = message_needs_file_attachment_hydration(message);
    if !needs_image && !needs_video && !needs_file {
        return;
    }
    let Some(raw_conversation_id) = conversation_id_to_raw_bytes(&message.conversation_id) else {
        return;
    };
    let Ok(_message_id_i64) = message.id.0.parse::<i64>() else {
        return;
    };
    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    let mut message = message.clone();
    std::thread::spawn(move || {
        let Some(path) = socket_path() else { return };
        let Ok(runtime) = Builder::new_current_thread().enable_all().build() else {
            return;
        };
        runtime.block_on(async {
            let Ok(transport) = FramedMsgpackTransport::connect(&path).await else {
                return;
            };
            let mut client = KeybaseRpcClient::new(transport);
            let updated_ids = hydrate_attachment_paths_for_messages(
                &mut client,
                &raw_conversation_id,
                std::slice::from_mut(&mut message),
                1,
            )
            .await;
            if !updated_ids.is_empty() {
                publish_attachment_paths(&sender, &local_store, &message);
            }
        });
    });
}

fn conversation_id_to_raw_bytes(id: &ConversationId) -> Option<Vec<u8>> {
    if let Some(hex) = id.0.strip_prefix("kb_conv:") {
        return hex_decode(hex);
    }
    if id.0.len().is_multiple_of(2) && id.0.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return hex_decode(&id.0);
    }
    None
}

/// Apply a local file path to a message's image/video attachments so that the
/// immediately-renderable local copy is persisted alongside the server's URL.
fn stamp_local_attachment_path(message: &mut MessageRecord, local_path: &str) {
    let local_source = AttachmentSource::LocalPath(local_path.to_string());
    for attachment in &mut message.attachments {
        if matches!(
            attachment.kind,
            AttachmentKind::Image | AttachmentKind::Video
        ) {
            attachment.source = Some(local_source.clone());
            if attachment.kind == AttachmentKind::Image {
                if let Some(ref mut preview) = attachment.preview {
                    preview.source = local_source.clone();
                } else {
                    attachment.preview = Some(AttachmentPreview {
                        source: local_source.clone(),
                        width: None,
                        height: None,
                    });
                }
            }
        }
    }
}

fn persist_canonical_message(
    sender: Option<&Sender<BackendEvent>>,
    local_store: &LocalStore,
    message: &mut MessageRecord,
) -> bool {
    match local_store.upsert_message(message) {
        Ok(Some(canonical)) => {
            *message = canonical;
            true
        }
        Ok(None) => {
            if let Some(sender) = sender {
                let _ = sender.send(BackendEvent::MessageDeleted {
                    conversation_id: message.conversation_id.clone(),
                    message_id: message.id.clone(),
                });
            }
            false
        }
        Err(error) => {
            tracing::error!(%error, "could not persist message");
            false
        }
    }
}

fn ingest_message_record(
    sender: Option<&Sender<BackendEvent>>,
    local_store: &Arc<LocalStore>,
    message: &mut MessageRecord,
) {
    if let Some(sender) = sender {
        spawn_sync_message_emoji_sources(sender, message);
    }
    hydrate_thread_metadata(local_store, message);
    if !persist_canonical_message(sender, local_store, message) {
        return;
    }
    // History ingestion must remain metadata-only. Attachment downloads belong
    // to live notifications and explicit timeline loads, never the index crawl.
    maybe_schedule_reply_ancestor_backfill(sender.cloned(), Arc::clone(local_store), message);
    reconcile_descendant_thread_roots(sender, local_store, &message.conversation_id, &message.id);
    let root_id = message
        .thread_root_id
        .clone()
        .unwrap_or_else(|| message.id.clone());
    refresh_root_descendant_reply_count(sender, local_store, &message.conversation_id, &root_id);
}

fn ingest_message_record_for_timeline_load(
    sender: Option<&Sender<BackendEvent>>,
    local_store: &Arc<LocalStore>,
    message: &mut MessageRecord,
) {
    // Keep conversation-open latency low by deferring expensive reply-graph repair
    // and descendant recount work to dedicated thread/repair paths.
    if let Some(sender) = sender {
        // Keep cross-team custom emojis visible on initial timeline load without
        // paying the full ingest cost used for live message processing.
        spawn_sync_message_emoji_sources(sender, message);
    }
    hydrate_thread_metadata(local_store, message);
    persist_canonical_message(sender, local_store, message);
}

fn resolve_thread_root_for_message(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    message_id: &MessageId,
    reply_to: Option<&MessageId>,
) -> MessageId {
    let mut current = reply_to.cloned().unwrap_or_else(|| message_id.clone());
    let mut seen = HashSet::new();
    while seen.insert(current.clone()) {
        let Some(cached) = local_store
            .get_message(conversation_id, &current)
            .ok()
            .flatten()
        else {
            break;
        };
        if let Some(cached_root) = cached.thread_root_id
            && cached_root != current
        {
            current = cached_root;
            continue;
        }
        if let Some(parent) = cached.reply_to {
            current = parent;
            continue;
        }
        break;
    }
    current
}

fn hydrate_thread_metadata(local_store: &LocalStore, message: &mut MessageRecord) {
    if message.reply_to.is_none() {
        message
            .thread_root_id
            .get_or_insert_with(|| message.id.clone());
        return;
    }
    let root = resolve_thread_root_for_message(
        local_store,
        &message.conversation_id,
        &message.id,
        message.reply_to.as_ref(),
    );
    message.thread_root_id = Some(root);
}

fn rebuild_thread_index_from_cached_messages(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    scan_limit: usize,
) {
    let Ok(messages) = local_store.load_messages_before(conversation_id, None, scan_limit) else {
        return;
    };
    for mut message in messages {
        hydrate_thread_metadata(local_store, &mut message);
        let _ = local_store.persist_message(&message);
    }
}

fn warm_recent_conversation_messages_for_thread(
    local_store: &Arc<LocalStore>,
    conversation_id: &ConversationId,
    raw_conversation_id: &[u8],
    socket_path: &Path,
) -> io::Result<()> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let conversation_id_for_fetch = conversation_id.clone();
    let raw_conversation_id_for_fetch = raw_conversation_id.to_vec();
    let mut page = runtime.block_on(async move {
        let transport = FramedMsgpackTransport::connect(socket_path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        fetch_thread_messages_by_raw_id_without_attachment_hydration(
            &mut client,
            &conversation_id_for_fetch,
            &raw_conversation_id_for_fetch,
        )
        .await
    })?;

    persist_reaction_deltas(None, local_store, &page.reaction_deltas);
    strip_reaction_delete_tombstones(local_store, conversation_id, &mut page.messages);
    apply_regular_delete_tombstones(local_store, conversation_id, None, &mut page.messages);
    let message_ids = page
        .messages
        .iter()
        .map(|message| message.id.clone())
        .collect::<Vec<_>>();
    let loaded_reactions = local_store
        .load_message_reactions_for_messages(conversation_id, &message_ids)
        .unwrap_or_default();
    for mut message in page.messages {
        message.reactions = domain_message_reactions(loaded_reactions.get(&message.id));
        ingest_message_record(None, local_store, &mut message);
    }

    Ok(())
}

struct LoadedConversation {
    page: ThreadPage,
    pinned_state: Option<PinnedState>,
    thread_fetch_ms: u128,
    pinned_fetch_ms: u128,
}

async fn load_conversation_from_service(
    socket: &std::path::Path,
    conversation_id: &ConversationId,
    conversation_ref: &ProviderConversationRef,
) -> io::Result<LoadedConversation> {
    let Some(raw_conversation_id) = provider_ref_to_conversation_id_bytes(conversation_ref) else {
        return Err(io::Error::other("invalid conversation provider ref"));
    };

    let transport = FramedMsgpackTransport::connect(socket).await?;
    let mut client = KeybaseRpcClient::new(transport);
    let thread_fetch_started = Instant::now();
    let page = fetch_thread_messages_by_raw_id_without_attachment_hydration(
        &mut client,
        conversation_id,
        &raw_conversation_id,
    )
    .await?;
    let thread_fetch_ms = thread_fetch_started.elapsed().as_millis();
    let pinned_fetch_started = Instant::now();
    let pinned_state = fetch_inbox_for_conversation_id(&mut client, &raw_conversation_id)
        .await
        .ok()
        .and_then(|inbox| extract_pinned_state_for_conversation(&inbox, conversation_id));
    let pinned_fetch_ms = pinned_fetch_started.elapsed().as_millis();
    Ok(LoadedConversation {
        page,
        pinned_state,
        thread_fetch_ms,
        pinned_fetch_ms,
    })
}

fn bootstrap_payload_from_cache(
    seed: CachedBootstrapSeed,
    backend_id: &BackendId,
    account_id: &AccountId,
) -> BootstrapPayload {
    let workspace_id = seed.active_workspace_id.clone();
    let conversation_bindings = seed
        .conversation_bindings
        .into_iter()
        .map(
            |(conversation_id, provider_conversation_ref)| ConversationBinding {
                conversation_id,
                backend_id: backend_id.clone(),
                account_id: account_id.clone(),
                provider_conversation_ref,
            },
        )
        .collect::<Vec<_>>();
    let message_bindings = seed
        .message_bindings
        .into_iter()
        .map(|(message_id, provider_message_ref)| MessageBinding {
            message_id,
            backend_id: backend_id.clone(),
            account_id: account_id.clone(),
            provider_message_ref,
        })
        .collect::<Vec<_>>();
    let (selected_messages, _) = strip_placeholder_messages(seed.selected_messages);

    BootstrapPayload {
        workspace_ids: vec![workspace_id.clone()],
        active_workspace_id: Some(workspace_id.clone()),
        workspace_name: seed.workspace_name,
        channels: seed.channels,
        direct_messages: seed.direct_messages,
        workspace_bindings: vec![WorkspaceBinding {
            workspace_id,
            backend_id: backend_id.clone(),
            account_id: account_id.clone(),
            provider_workspace_ref: ProviderWorkspaceRef::new(PROVIDER_WORKSPACE_REF),
        }],
        conversation_bindings,
        message_bindings,
        selected_conversation_id: seed.selected_conversation_id,
        selected_messages,
        unread_marker: seed.unread_marker,
        account_display_name: seed.account_display_name,
        conversation_notifications: Vec::new(),
    }
}

fn prefetch_user_profiles_from_payload(
    sender: &Sender<BackendEvent>,
    payload: &BootstrapPayload,
    local_store: &Arc<LocalStore>,
) {
    let mut usernames = Vec::new();
    if let Some(account) = payload.account_display_name.as_ref() {
        usernames.push(account.clone());
    }
    for message in &payload.selected_messages {
        usernames.push(message.author_id.0.clone());
    }
    for summary in &payload.direct_messages {
        usernames.extend(usernames_from_conversation_title(&summary.title));
    }
    usernames.sort();
    usernames.dedup();
    if usernames.is_empty() {
        return;
    }
    spawn_prefetch_user_profiles(sender, local_store, usernames);
}

fn prefetch_user_profiles_from_summaries(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    summaries: &[ConversationSummary],
) {
    let mut usernames = summaries
        .iter()
        .filter_map(|summary| match summary.kind {
            ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
                Some(usernames_from_conversation_title(&summary.title))
            }
            ConversationKind::Channel => None,
        })
        .flatten()
        .collect::<Vec<_>>();
    usernames.sort();
    usernames.dedup();
    if usernames.is_empty() {
        return;
    }
    spawn_prefetch_user_profiles(sender, local_store, usernames);
}

fn prefetch_team_avatars_from_payload(
    sender: &Sender<BackendEvent>,
    payload: &BootstrapPayload,
    local_store: &Arc<LocalStore>,
) {
    let mut team_names = payload
        .channels
        .iter()
        .filter_map(|summary| summary.group.as_ref().map(|group| group.id.clone()))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    team_names.sort();
    team_names.dedup();
    if team_names.is_empty() {
        return;
    }
    spawn_prefetch_team_avatars(sender, local_store, team_names);
}

fn usernames_from_conversation_title(title: &str) -> Vec<String> {
    title
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn team_profile_user_id(team_name: &str) -> UserId {
    UserId::new(format!("team:{team_name}"))
}

fn sync_selected_conversation_emojis_from_payload(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    payload: &BootstrapPayload,
) {
    let Some(conversation_id) = payload.selected_conversation_id.clone() else {
        return;
    };
    spawn_sync_conversation_emojis(sender, local_store, conversation_id);
}

fn sync_conversation_emojis(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    conversation_id: &ConversationId,
) {
    let owner_team_id = resolve_emoji_owner_team_id(local_store, conversation_id);
    let cached =
        load_cached_emojis_for_owner(local_store, conversation_id, owner_team_id.as_deref());
    if !cached.is_empty() {
        let _ = sender.send(BackendEvent::ConversationEmojisSynced {
            conversation_id: conversation_id.clone(),
            emojis: cached_emoji_entries(&cached),
        });
    }

    let now_ms = now_unix_ms();
    let latest_updated = cached
        .iter()
        .map(|emoji| emoji.updated_ms)
        .max()
        .unwrap_or(0);
    let cache_assets_complete = cached_custom_emoji_assets_complete(&cached);
    let cache_fresh = now_ms.saturating_sub(latest_updated) < EMOJI_CACHE_TTL_MS;
    if cache_fresh && !cached.is_empty() && cache_assets_complete {
        return;
    }
    if cache_fresh && !cached.is_empty() && !cache_assets_complete {
        send_internal(
            sender,
            "zbase.internal.emoji_cache_missing_assets",
            Value::Map(vec![
                (
                    Value::from("conversation_id"),
                    Value::from(conversation_id.0.clone()),
                ),
                (
                    Value::from("missing_assets"),
                    Value::from(missing_custom_emoji_assets_count(&cached) as i64),
                ),
            ]),
        );
    }

    let raw_conversation_id = provider_ref_to_conversation_id_bytes(&ProviderConversationRef::new(
        conversation_id.0.clone(),
    ));
    let Some(raw_conversation_id) = raw_conversation_id else {
        return;
    };
    let Some(socket) = socket_path() else {
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(_) => return,
    };

    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    let conversation_id = conversation_id.clone();
    let owner_team_id = owner_team_id.clone();
    let cached_assets_by_alias = cached_custom_emoji_assets_by_alias(&cached);
    runtime.block_on(async move {
        let transport = match FramedMsgpackTransport::connect(&socket).await {
            Ok(transport) => transport,
            Err(error) => {
                send_internal(
                    &sender,
                    "zbase.internal.emoji_sync_transport_failed",
                    Value::Map(vec![
                        (
                            Value::from("conversation_id"),
                            Value::from(conversation_id.0.clone()),
                        ),
                        (Value::from("error"), Value::from(error.to_string())),
                    ]),
                );
                return;
            }
        };
        let mut client = KeybaseRpcClient::new(transport);
        let mut emojis = match fetch_conversation_emojis(&mut client, &raw_conversation_id).await {
            Ok(emojis) => emojis,
            Err(error) => {
                send_internal(
                    &sender,
                    "zbase.internal.emoji_fetch_failed",
                    Value::Map(vec![
                        (
                            Value::from("conversation_id"),
                            Value::from(conversation_id.0.clone()),
                        ),
                        (Value::from("error"), Value::from(error.to_string())),
                    ]),
                );
                return;
            }
        };

        let early_entries: Vec<ConversationEmojiEntry> = emojis
            .iter()
            .map(|emoji| ConversationEmojiEntry {
                alias: emoji.alias.clone(),
                unicode: emoji.unicode.clone(),
                asset_path: emoji.asset_path.clone(),
                updated_ms: 0,
            })
            .collect();
        if !early_entries.is_empty() {
            let _ = sender.send(BackendEvent::ConversationEmojisSynced {
                conversation_id: conversation_id.clone(),
                emojis: early_entries,
            });
        }

        for emoji in &mut emojis {
            if let Some(url) = emoji.source_url.clone() {
                match download_custom_emoji_to_cache(&url).await {
                    Ok(path) => {
                        emoji.asset_path = Some(path.display().to_string());
                        emoji.updated_ms = now_unix_ms();
                    }
                    Err(error) => {
                        let alias = emoji.alias.clone();
                        send_internal(
                            &sender,
                            "zbase.internal.emoji_asset_download_failed",
                            Value::Map(vec![
                                (
                                    Value::from("conversation_id"),
                                    Value::from(conversation_id.0.clone()),
                                ),
                                (Value::from("alias"), Value::from(alias.clone())),
                                (Value::from("url"), Value::from(url)),
                                (Value::from("error"), Value::from(error.to_string())),
                            ]),
                        );
                        let alias_key = alias.to_ascii_lowercase();
                        emoji.asset_path = cached_assets_by_alias.get(&alias_key).cloned();
                        emoji.updated_ms = if emoji.asset_path.is_some() {
                            now_unix_ms()
                        } else {
                            0
                        };
                    }
                }
            } else {
                emoji.updated_ms = now_unix_ms();
            }
        }

        persist_synced_emojis_for_owner(
            &local_store,
            &conversation_id,
            owner_team_id.as_deref(),
            &emojis,
        );
        let _ = sender.send(BackendEvent::ConversationEmojisSynced {
            conversation_id: conversation_id.clone(),
            emojis: cached_emoji_entries(&emojis),
        });
    });
}

fn resolve_emoji_owner_team_id(
    local_store: &Arc<LocalStore>,
    conversation_id: &ConversationId,
) -> Option<String> {
    local_store
        .get_conversation_team_binding(conversation_id)
        .ok()
        .flatten()
        .map(|binding| binding.team_id.trim().to_string())
        .filter(|team_id| !team_id.is_empty())
}

fn load_cached_emojis_for_owner(
    local_store: &Arc<LocalStore>,
    conversation_id: &ConversationId,
    owner_team_id: Option<&str>,
) -> Vec<CachedConversationEmoji> {
    if let Some(team_id) = owner_team_id {
        let team_cached = local_store.load_team_emojis(team_id).unwrap_or_default();
        if !team_cached.is_empty() {
            return team_cached;
        }
        if TEAM_EMOJI_CONVERSATION_FALLBACK_READ_ENABLED {
            return local_store
                .load_conversation_emojis(conversation_id)
                .unwrap_or_default();
        }
        return Vec::new();
    }
    local_store
        .load_conversation_emojis(conversation_id)
        .unwrap_or_default()
}

fn persist_synced_emojis_for_owner(
    local_store: &Arc<LocalStore>,
    conversation_id: &ConversationId,
    owner_team_id: Option<&str>,
    emojis: &[CachedConversationEmoji],
) {
    if let Some(team_id) = owner_team_id {
        let team_write_succeeded = local_store.replace_team_emojis(team_id, emojis).is_ok();
        if TEAM_EMOJI_DUAL_WRITE_MIGRATION_ENABLED || !team_write_succeeded {
            let _ = local_store.replace_conversation_emojis(conversation_id, emojis);
        } else {
            let _ = local_store.clear_conversation_emojis(conversation_id);
            cleanup_legacy_conversation_emoji_files(conversation_id);
        }
        return;
    }
    let _ = local_store.replace_conversation_emojis(conversation_id, emojis);
}

fn cleanup_legacy_conversation_emoji_files(conversation_id: &ConversationId) {
    let legacy_dir = store_paths::emojis_dir().join(sanitize_username(&conversation_id.0));
    let Ok(entries) = std::fs::read_dir(&legacy_dir) else {
        return;
    };
    let cutoff_ms = now_unix_ms().saturating_sub(LEGACY_CONVERSATION_EMOJI_FILE_RETENTION_MS);
    let mut saw_recent_file = false;
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let modified_ms = modified
            .duration_since(UNIX_EPOCH)
            .map(|value| value.as_millis() as i64)
            .unwrap_or(i64::MAX);
        if modified_ms >= cutoff_ms {
            saw_recent_file = true;
            break;
        }
    }
    if !saw_recent_file {
        let _ = std::fs::remove_dir_all(legacy_dir);
    }
}

fn cached_emoji_entries(cached: &[CachedConversationEmoji]) -> Vec<ConversationEmojiEntry> {
    cached
        .iter()
        .map(|emoji| ConversationEmojiEntry {
            alias: emoji.alias.clone(),
            unicode: emoji.unicode.clone(),
            asset_path: emoji
                .asset_path
                .as_ref()
                .filter(|path| emoji_asset_path_usable(path))
                .cloned(),
            updated_ms: emoji.updated_ms,
        })
        .collect()
}

fn cached_custom_emoji_assets_complete(cached: &[CachedConversationEmoji]) -> bool {
    cached.iter().all(|emoji| {
        if emoji.source_url.is_none() {
            return true;
        }
        emoji
            .asset_path
            .as_deref()
            .is_some_and(emoji_asset_path_usable)
    })
}

fn missing_custom_emoji_assets_count(cached: &[CachedConversationEmoji]) -> usize {
    cached
        .iter()
        .filter(|emoji| {
            emoji.source_url.is_some()
                && !emoji
                    .asset_path
                    .as_deref()
                    .is_some_and(emoji_asset_path_usable)
        })
        .count()
}

fn cached_custom_emoji_assets_by_alias(
    cached: &[CachedConversationEmoji],
) -> HashMap<String, String> {
    let mut assets_by_alias = HashMap::new();
    for emoji in cached {
        if let Some(path) = emoji
            .asset_path
            .as_deref()
            .filter(|path| emoji_asset_path_usable(path))
        {
            assets_by_alias.insert(emoji.alias.to_ascii_lowercase(), path.to_string());
        }
    }
    assets_by_alias
}

fn prefetch_user_profiles(
    sender: &Sender<BackendEvent>,
    usernames: &[String],
    local_store: &Arc<LocalStore>,
) {
    let now_ms = now_unix_ms();
    let mut cached_display_names = HashMap::new();
    let mut candidates = Vec::new();
    for username in usernames
        .iter()
        .map(|value| value.trim())
        .filter(|v| !v.is_empty())
    {
        let user_id = UserId::new(username.to_string());
        let mut should_fetch = true;
        if let Ok(Some(profile)) = local_store.get_user_profile(&user_id) {
            let cached_display_name = profile.display_name.trim().to_string();
            if !cached_display_name.is_empty() {
                cached_display_names.insert(username.to_string(), cached_display_name.clone());
            }
            let fresh = now_ms.saturating_sub(profile.updated_ms) < PROFILE_CACHE_TTL_MS;
            let cached_avatar_asset = profile
                .avatar_path
                .as_ref()
                .filter(|path| avatar_asset_path_usable(path))
                .cloned();
            if cached_avatar_asset.is_some() {
                should_fetch = !fresh;
                let _ = sender.send(BackendEvent::UserProfileUpserted {
                    user_id,
                    display_name: cached_display_name,
                    avatar_asset: validated_avatar_asset(cached_avatar_asset),
                    updated_ms: profile.updated_ms,
                });
            } else if profile.avatar_url.is_some() {
                let _ = sender.send(BackendEvent::UserProfileUpserted {
                    user_id,
                    display_name: cached_display_name,
                    avatar_asset: validated_avatar_asset(profile.avatar_url.clone()),
                    updated_ms: profile.updated_ms,
                });
            }
        }

        if should_fetch {
            candidates.push(username.to_string());
        }
    }

    if candidates.is_empty() {
        return;
    }

    let Some(socket) = socket_path() else {
        send_internal(
            sender,
            "zbase.internal.profile_avatar_socket_path_missing",
            Value::from("prefetch_user_profiles"),
        );
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                sender,
                "zbase.internal.profile_avatar_runtime_init_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    runtime.block_on(async move {
        let transport = match FramedMsgpackTransport::connect(&socket).await {
            Ok(transport) => transport,
            Err(error) => {
                send_internal(
                    &sender,
                    "zbase.internal.profile_avatar_transport_failed",
                    Value::from(error.to_string()),
                );
                return;
            }
        };
        let mut client = KeybaseRpcClient::new(transport);
        let urls = match fetch_user_avatar_urls(&mut client, &candidates).await {
            Ok(urls) => urls,
            Err(error) => {
                send_internal(
                    &sender,
                    "zbase.internal.profile_avatar_fetch_failed",
                    Value::from(error.to_string()),
                );
                HashMap::new()
            }
        };
        for username in candidates {
            let user_id = UserId::new(username.clone());
            let url = urls.get(&username).cloned();
            if url.is_none() {
                send_internal(
                    &sender,
                    "zbase.internal.profile_avatar_missing_url",
                    Value::from(username.clone()),
                );
            }
            let fetched_display_name = match fetch_user_display_name(&mut client, &username).await {
                Ok(name) => name,
                Err(error) => {
                    send_internal(
                        &sender,
                        "zbase.internal.profile_display_name_fetch_failed",
                        Value::Map(vec![
                            (Value::from("username"), Value::from(username.clone())),
                            (Value::from("error"), Value::from(error.to_string())),
                        ]),
                    );
                    None
                }
            };
            let display_name = fetched_display_name
                .or_else(|| cached_display_names.get(&username).cloned())
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| username.clone());
            let mut avatar_path = None;
            let mut avatar_asset = validated_avatar_asset(url.clone());
            if let Some(url) = url.clone() {
                if let Some(local_path) = file_url_to_local_path(&url)
                    && Path::new(&local_path).exists()
                {
                    avatar_path = Some(local_path.clone());
                    avatar_asset = Some(local_path);
                }
                match download_avatar_to_cache(&username, &url).await {
                    Ok(path) => {
                        let file_path = path.display().to_string();
                        avatar_path = Some(file_path.clone());
                        avatar_asset = Some(file_path);
                    }
                    Err(error) => {
                        send_internal(
                            &sender,
                            "zbase.internal.profile_avatar_download_failed",
                            Value::Map(vec![
                                (Value::from("username"), Value::from(username.clone())),
                                (Value::from("url"), Value::from(url)),
                                (Value::from("error"), Value::from(error.to_string())),
                            ]),
                        );
                    }
                }
            }
            let updated_ms = now_unix_ms();
            let _ = local_store.upsert_user_profile(
                &user_id,
                display_name.clone(),
                url,
                avatar_path.clone(),
                updated_ms,
            );
            let (_, cached_avatar) = load_cached_profile_summary(&local_store, &user_id, &username);
            let _ = sender.send(BackendEvent::UserProfileUpserted {
                user_id,
                display_name,
                avatar_asset: validated_avatar_asset(cached_avatar.or(avatar_asset)),
                updated_ms,
            });
        }
    });
}

async fn fetch_user_display_name(
    client: &mut KeybaseRpcClient,
    username: &str,
) -> io::Result<Option<String>> {
    let load_user_by_name_args = vec![Value::Map(vec![
        (Value::from("sessionID"), Value::from(SESSION_ID)),
        (Value::from("username"), Value::from(username)),
    ])];
    let user_card_args = vec![Value::Map(vec![
        (Value::from("sessionID"), Value::from(SESSION_ID)),
        (Value::from("username"), Value::from(username)),
        (Value::from("useSession"), Value::from(true)),
    ])];

    let mut saw_successful_response = false;
    let mut last_error: Option<io::Error> = None;

    match client
        .call(KEYBASE_LOAD_USER_BY_NAME, load_user_by_name_args)
        .await
    {
        Ok(response) => {
            saw_successful_response = true;
            if let Some(name) = parse_user_display_name(&response, username) {
                return Ok(Some(name));
            }
        }
        Err(error) => {
            last_error = Some(error);
        }
    }

    match client.call(KEYBASE_USER_CARD, user_card_args).await {
        Ok(response) => {
            saw_successful_response = true;
            if let Some(name) = parse_user_display_name(&response, username) {
                return Ok(Some(name));
            }
        }
        Err(error) => {
            last_error = Some(error);
        }
    }

    if saw_successful_response {
        Ok(None)
    } else if let Some(error) = last_error {
        Err(error)
    } else {
        Ok(None)
    }
}

fn prefetch_team_avatars(
    sender: &Sender<BackendEvent>,
    team_names: &[String],
    local_store: &Arc<LocalStore>,
) {
    let now_ms = now_unix_ms();
    let mut candidates = Vec::new();
    for team_name in team_names
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let profile_id = team_profile_user_id(team_name);
        let mut should_fetch = true;
        if let Ok(Some(profile)) = local_store.get_user_profile(&profile_id) {
            let fresh = now_ms.saturating_sub(profile.updated_ms) < PROFILE_CACHE_TTL_MS;
            let cached_avatar_asset = profile
                .avatar_path
                .as_ref()
                .filter(|path| avatar_asset_path_usable(path))
                .cloned();
            if fresh && cached_avatar_asset.is_some() {
                should_fetch = false;
                let _ = sender.send(BackendEvent::UserProfileUpserted {
                    user_id: profile_id,
                    display_name: profile.display_name,
                    avatar_asset: validated_avatar_asset(cached_avatar_asset),
                    updated_ms: profile.updated_ms,
                });
            } else if fresh {
                let _ = sender.send(BackendEvent::UserProfileUpserted {
                    user_id: profile_id,
                    display_name: profile.display_name,
                    avatar_asset: validated_avatar_asset(profile.avatar_url.clone()),
                    updated_ms: profile.updated_ms,
                });
            }
        }
        if should_fetch {
            candidates.push(team_name.to_string());
        }
    }
    if candidates.is_empty() {
        return;
    }

    let Some(socket) = socket_path() else {
        send_internal(
            sender,
            "zbase.internal.team_avatar_socket_path_missing",
            Value::from("prefetch_team_avatars"),
        );
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                sender,
                "zbase.internal.team_avatar_runtime_init_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };

    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    runtime.block_on(async move {
        let transport = match FramedMsgpackTransport::connect(&socket).await {
            Ok(transport) => transport,
            Err(error) => {
                send_internal(
                    &sender,
                    "zbase.internal.team_avatar_transport_failed",
                    Value::from(error.to_string()),
                );
                return;
            }
        };
        let mut client = KeybaseRpcClient::new(transport);
        let urls = match fetch_team_avatar_urls(&mut client, &candidates).await {
            Ok(urls) => urls,
            Err(error) => {
                send_internal(
                    &sender,
                    "zbase.internal.team_avatar_fetch_failed",
                    Value::from(error.to_string()),
                );
                return;
            }
        };
        for team_name in candidates {
            let profile_id = team_profile_user_id(&team_name);
            let url = urls.get(&team_name).cloned();
            if url.is_none() {
                send_internal(
                    &sender,
                    "zbase.internal.team_avatar_missing_url",
                    Value::from(team_name.clone()),
                );
            }
            let mut avatar_path = None;
            let mut avatar_asset = validated_avatar_asset(url.clone());
            if let Some(url) = url.clone() {
                match download_avatar_to_cache(&team_name, &url).await {
                    Ok(path) => {
                        let file_path = path.display().to_string();
                        avatar_path = Some(file_path.clone());
                        avatar_asset = Some(file_path);
                    }
                    Err(error) => {
                        send_internal(
                            &sender,
                            "zbase.internal.team_avatar_download_failed",
                            Value::Map(vec![
                                (Value::from("team"), Value::from(team_name.clone())),
                                (Value::from("url"), Value::from(url)),
                                (Value::from("error"), Value::from(error.to_string())),
                            ]),
                        );
                    }
                }
            }
            let updated_ms = now_unix_ms();
            let _ = local_store.upsert_user_profile(
                &profile_id,
                team_name.clone(),
                url,
                avatar_path.clone(),
                updated_ms,
            );
            let _ = sender.send(BackendEvent::UserProfileUpserted {
                user_id: profile_id,
                display_name: team_name,
                avatar_asset: validated_avatar_asset(avatar_asset),
                updated_ms,
            });
        }
    });
}

async fn fetch_user_avatar_urls(
    client: &mut KeybaseRpcClient,
    usernames: &[String],
) -> io::Result<std::collections::HashMap<String, String>> {
    let names = Value::Array(usernames.iter().cloned().map(Value::from).collect());
    let formats = Value::Array(vec![Value::from(AVATAR_FORMAT_SQUARE_128)]);
    let result = client
        .call(
            KEYBASE_LOAD_USER_AVATARS,
            vec![Value::Map(vec![
                (Value::from("names"), names),
                (Value::from("formats"), formats),
            ])],
        )
        .await?;

    let mut map = std::collections::HashMap::new();
    let Some(picmap) = map_get_any(&result, &["picmap", "p"]) else {
        return Ok(map);
    };
    let Value::Map(entries) = picmap else {
        return Ok(map);
    };
    for (name_key, format_map) in entries {
        let Some(username) = name_key.as_str() else {
            continue;
        };
        let Value::Map(formats) = format_map else {
            continue;
        };
        for (format_key, url_val) in formats {
            if !value_matches_avatar_format(format_key, AVATAR_FORMAT_SQUARE_128) {
                continue;
            }
            if let Some(url) = extract_avatar_url(url_val) {
                map.insert(username.to_string(), url.to_string());
            }
        }
    }
    Ok(map)
}

async fn fetch_team_avatar_urls(
    client: &mut KeybaseRpcClient,
    team_names: &[String],
) -> io::Result<std::collections::HashMap<String, String>> {
    let names = Value::Array(team_names.iter().cloned().map(Value::from).collect());
    let formats = Value::Array(vec![Value::from(AVATAR_FORMAT_SQUARE_128)]);
    let result = client
        .call(
            KEYBASE_LOAD_TEAM_AVATARS,
            vec![Value::Map(vec![
                (Value::from("names"), names),
                (Value::from("formats"), formats),
            ])],
        )
        .await?;

    let mut map = std::collections::HashMap::new();
    let Some(picmap) = map_get_any(&result, &["picmap", "p"]) else {
        return Ok(map);
    };
    let Value::Map(entries) = picmap else {
        return Ok(map);
    };
    for (name_key, format_map) in entries {
        let Some(team_name) = name_key.as_str() else {
            continue;
        };
        let Value::Map(formats) = format_map else {
            continue;
        };
        for (format_key, url_val) in formats {
            if !value_matches_avatar_format(format_key, AVATAR_FORMAT_SQUARE_128) {
                continue;
            }
            if let Some(url) = extract_avatar_url(url_val) {
                map.insert(team_name.to_string(), url.to_string());
            }
        }
    }
    Ok(map)
}

async fn fetch_conversation_emojis(
    client: &mut KeybaseRpcClient,
    raw_conversation_id: &[u8],
) -> io::Result<Vec<CachedConversationEmoji>> {
    let opts = Value::Map(vec![
        (Value::from("getCreationInfo"), Value::from(false)),
        (Value::from("getAliases"), Value::from(true)),
        (Value::from("onlyInTeam"), Value::from(false)),
    ]);
    let result = client
        .call(
            CHAT_USER_EMOJIS,
            vec![Value::Map(vec![
                (Value::from("opts"), opts),
                (
                    Value::from("convID"),
                    Value::Binary(raw_conversation_id.to_vec()),
                ),
            ])],
        )
        .await?;
    Ok(parse_user_emojis(&result))
}

fn parse_user_emojis(value: &Value) -> Vec<CachedConversationEmoji> {
    let mut parsed = Vec::new();
    let Some(user_emojis) = map_get_any(value, &["emojis", "e"]) else {
        return parsed;
    };
    let groups = map_get_any(user_emojis, &["emojis", "e"])
        .and_then(as_array)
        .cloned()
        .unwrap_or_default();
    for group in groups {
        let group_emojis = map_get_any(&group, &["emojis", "e"])
            .and_then(as_array)
            .cloned()
            .unwrap_or_default();
        for emoji in group_emojis {
            let alias = map_get_any(&emoji, &["alias", "a"])
                .and_then(as_str)
                .map(str::trim)
                .unwrap_or("");
            if alias.is_empty() {
                continue;
            }
            let mut unicode = None;
            let mut source_url = None;

            for source_value in preferred_emoji_source_values(&emoji) {
                if source_value.starts_with("http://") || source_value.starts_with("https://") {
                    source_url = source_url.or(Some(source_value));
                } else if !source_value.is_ascii() {
                    unicode = unicode.or(Some(source_value));
                }
            }

            parsed.push(CachedConversationEmoji {
                alias: alias.to_string(),
                unicode,
                source_url,
                asset_path: None,
                updated_ms: 0,
            });
        }
    }
    parsed.sort_by(|left, right| left.alias.cmp(&right.alias));
    parsed.dedup_by(|left, right| left.alias.eq_ignore_ascii_case(&right.alias));
    parsed
}

fn preferred_emoji_source_values(emoji: &Value) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(source) = map_get_any(emoji, &["source", "s"]).and_then(extract_emoji_source_string)
    {
        values.push(source);
    }
    if let Some(source) =
        map_get_any(emoji, &["noAnimSource", "nas"]).and_then(extract_emoji_source_string)
        && !values.iter().any(|candidate| candidate == &source)
    {
        values.push(source);
    }
    values
}

fn extract_emoji_source_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => text
            .as_str()
            .map(str::to_string)
            .or_else(|| Some(text.to_string().trim_matches('"').to_string())),
        Value::Map(entries) => {
            for (key, inner) in entries {
                if let Some(key_name) = key.as_str()
                    && matches!(key_name, "httpsrv" | "str" | "url" | "u" | "s")
                    && let Some(text) = inner.as_str()
                {
                    return Some(text.to_string());
                }
            }
            for (_, inner) in entries {
                if let Some(found) = extract_emoji_source_string(inner) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(values) => values.iter().find_map(extract_emoji_source_string),
        _ => None,
    }
}

fn value_matches_avatar_format(value: &Value, expected: &str) -> bool {
    if value.as_str() == Some(expected) {
        return true;
    }
    match value {
        Value::Map(entries) => entries
            .iter()
            .any(|(_, inner)| value_matches_avatar_format(inner, expected)),
        Value::Array(values) => values
            .iter()
            .any(|inner| value_matches_avatar_format(inner, expected)),
        _ => false,
    }
}

fn extract_avatar_url(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    match value {
        Value::Map(entries) => {
            for (key, inner) in entries {
                if let Some(key_name) = key.as_str()
                    && matches!(key_name, "url" | "u" | "str" | "s" | "httpsrv")
                    && let Some(url) = extract_avatar_url(inner)
                {
                    return Some(url);
                }
            }
            for (_, inner) in entries {
                if let Some(url) = extract_avatar_url(inner) {
                    return Some(url);
                }
            }
            None
        }
        Value::Array(values) => values.iter().find_map(extract_avatar_url),
        _ => None,
    }
}

async fn download_avatar_to_cache(username: &str, url: &str) -> io::Result<PathBuf> {
    let (bytes, ext) = if let Some(local_path) = file_url_to_local_path(url) {
        let bytes = std::fs::read(local_path)?;
        let ext = image_extension_for_bytes(&bytes).unwrap_or("jpg");
        (bytes, ext)
    } else {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(io::Error::other("unsupported avatar URL scheme"));
        }
        let response = reqwest::get(url)
            .await
            .map_err(io::Error::other)?
            .error_for_status()
            .map_err(io::Error::other)?;
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = response.bytes().await.map_err(io::Error::other)?.to_vec();
        let ext = image_extension_from_content_type(&content_type)
            .or_else(|| image_extension_for_bytes(&bytes))
            .unwrap_or("jpg");
        (bytes, ext)
    };

    let dir = store_paths::avatars_dir().join(sanitize_username(username));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{AVATAR_FORMAT_SQUARE_128}.{ext}"));
    let tmp = dir.join(format!("{AVATAR_FORMAT_SQUARE_128}.{ext}.tmp"));
    std::fs::write(&tmp, bytes)?;
    let _ = std::fs::rename(&tmp, &path);
    Ok(path)
}

fn image_extension_from_content_type(content_type: &str) -> Option<&'static str> {
    let lower = content_type.to_ascii_lowercase();
    if lower.contains("png") {
        Some("png")
    } else if lower.contains("jpeg") || lower.contains("jpg") {
        Some("jpg")
    } else if lower.contains("gif") {
        Some("gif")
    } else if lower.contains("webp") {
        Some("webp")
    } else {
        None
    }
}

fn image_extension_for_bytes(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("png")
    } else if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        Some("jpg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("gif")
    } else if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some("webp")
    } else {
        None
    }
}

async fn download_custom_emoji_to_cache(url: &str) -> io::Result<PathBuf> {
    let response = reqwest::get(url)
        .await
        .map_err(io::Error::other)?
        .error_for_status()
        .map_err(io::Error::other)?;
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = response.bytes().await.map_err(io::Error::other)?.to_vec();
    let ext = image_extension_for_bytes(&bytes)
        .or_else(|| image_extension_from_content_type(&content_type))
        .unwrap_or("jpg");
    write_hashed_emoji_asset(&bytes, ext, &store_paths::emoji_assets_dir())
}

fn write_hashed_emoji_asset(bytes: &[u8], ext: &str, assets_dir: &Path) -> io::Result<PathBuf> {
    std::fs::create_dir_all(assets_dir)?;
    let hash = emoji_asset_sha256_hex(bytes);
    let filename = format!("{hash}.{ext}");
    let path = assets_dir.join(&filename);
    if path.exists() {
        return Ok(path);
    }

    let tmp = assets_dir.join(format!("{filename}.{}.tmp", now_unix_ms()));
    std::fs::write(&tmp, bytes)?;
    match std::fs::rename(&tmp, &path) {
        Ok(()) => Ok(path),
        Err(error) => {
            if path.exists() {
                let _ = std::fs::remove_file(&tmp);
                return Ok(path);
            }
            let _ = std::fs::remove_file(&tmp);
            Err(error)
        }
    }
}

fn emoji_asset_sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sanitize_username(username: &str) -> String {
    username
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn validated_avatar_asset(asset: Option<String>) -> Option<String> {
    let asset = asset?;
    let trimmed = asset.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Some(asset);
    }
    if Path::new(trimmed).exists() {
        return Some(asset);
    }
    None
}

fn avatar_asset_path_usable(path: &str) -> bool {
    let path_ref = Path::new(path);
    if !path_ref.exists() {
        return false;
    }
    let ext = path_ref
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());
    matches!(
        ext.as_deref(),
        Some("png" | "jpg" | "jpeg" | "gif" | "webp" | "svg")
    )
}

fn emoji_asset_path_usable(path: &str) -> bool {
    let path_ref = Path::new(path);
    if !path_ref.exists() {
        return false;
    }
    let ext = path_ref
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());
    matches!(
        ext.as_deref(),
        Some("png" | "jpg" | "jpeg" | "gif" | "webp")
    )
}

fn file_url_to_local_path(url: &str) -> Option<String> {
    let raw = url.strip_prefix("file://")?;
    let path = if let Some(rest) = raw.strip_prefix("localhost/") {
        format!("/{rest}")
    } else {
        raw.to_string()
    };
    if path.is_empty() {
        return None;
    }
    Some(percent_decode(&path))
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (from_hex(bytes[index + 1]), from_hex(bytes[index + 2]))
        {
            out.push((hi << 4 | lo) as char);
            index += 3;
            continue;
        }
        out.push(bytes[index] as char);
        index += 1;
    }
    out
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn extract_message_timestamp_ms(value: &Value) -> Option<i64> {
    const TIMESTAMP_KEYS: &[&str] = &[
        "sent_at_ms",
        "sentAtMs",
        "sentAtMS",
        "sent_at",
        "sentAt",
        "ctimeMs",
        "ctimeMS",
        "ctime",
    ];
    const MIN_REASONABLE_TIMESTAMP_MS: i64 = 946_684_800_000; // 2000-01-01T00:00:00Z
    const MAX_FUTURE_SKEW_MS: i64 = 7 * 24 * 60 * 60 * 1000;

    let mut raw_candidates = Vec::new();
    collect_i64_values_for_keys(value, TIMESTAMP_KEYS, 0, &mut raw_candidates);
    if raw_candidates.is_empty() {
        return None;
    }

    let now_ms = now_unix_ms();
    let mut best_any: Option<i64> = None;
    let mut best_plausible: Option<i64> = None;

    for candidate in raw_candidates.into_iter().filter_map(normalize_epoch_ms) {
        if best_any.is_none_or(|current| candidate > current) {
            best_any = Some(candidate);
        }
        if candidate >= MIN_REASONABLE_TIMESTAMP_MS
            && candidate <= now_ms.saturating_add(MAX_FUTURE_SKEW_MS)
            && best_plausible.is_none_or(|current| candidate > current)
        {
            best_plausible = Some(candidate);
        }
    }

    best_plausible.or(best_any)
}

fn normalize_epoch_ms(raw: i64) -> Option<i64> {
    if raw <= 0 {
        return None;
    }
    let normalized = if raw >= 1_000_000_000_000_000_000 {
        raw / 1_000_000
    } else if raw >= 1_000_000_000_000_000 {
        raw / 1_000
    } else if raw >= 1_000_000_000_000 {
        raw
    } else {
        raw.saturating_mul(1000)
    };
    Some(normalized)
}

fn maybe_handle_new_conversation_notify(
    event: &KeybaseNotifyEvent,
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
) {
    let Some(conv_value) = extract_conv_from_notify(event) else {
        return;
    };
    let self_username = local_store.load_account_display_name();
    let Some(mut parsed) = parse_inbox_ui_item(conv_value, self_username.as_deref()) else {
        return;
    };
    let conversation_id = parsed.summary.id.clone();
    // Metadata updates are not membership changes. Inactive/stale payloads must not
    // resurrect a channel; explicit join notifications clear the local leave marker.
    if map_get(conv_value, "memberStatus")
        .and_then(as_i64)
        .is_some_and(|status| status != CONVERSATION_MEMBER_STATUS_ACTIVE)
        || local_store
            .is_conversation_left(&conversation_id)
            .unwrap_or(true)
    {
        return;
    }
    if let Some(existing) = local_store
        .get_conversation(&conversation_id)
        .ok()
        .flatten()
    {
        if let Some(group) = &parsed.summary.group
            && group.team_type != ConversationTeamType::Unknown
            && existing
                .group
                .as_ref()
                .is_some_and(|old| old.id == group.id && old.team_type != group.team_type)
        {
            apply_team_type_change(
                sender,
                local_store,
                &conversation_id,
                group.team_type.clone(),
            );
        }
        parsed.summary.preserve_known_metadata(&existing);
        parsed.summary.unread_count = existing.unread_count;
        parsed.summary.mention_count = existing.mention_count;
        if parsed.summary == existing {
            return;
        }
    }
    tracing::debug!(
        conversation_id = %conversation_id.0,
        title = %parsed.summary.title,
        kind = ?parsed.summary.kind,
        "updated conversation metadata from notification"
    );
    let workspace_id = WorkspaceId::new(WORKSPACE_ID);
    let provider_ref = parsed.provider_ref.clone();
    let _ = local_store.persist_conversation(&parsed.summary, parsed.activity_time);
    let _ = local_store.persist_conversation_binding(&conversation_id, &provider_ref);
    let (channels, direct_messages) = if matches!(parsed.summary.kind, ConversationKind::Channel) {
        (vec![parsed.summary], Vec::new())
    } else {
        (Vec::new(), vec![parsed.summary])
    };
    let binding = ConversationBinding {
        conversation_id,
        backend_id: BackendId::new(KEYBASE_BACKEND_ID),
        account_id: AccountId::new(DEFAULT_ACCOUNT_ID),
        provider_conversation_ref: provider_ref,
    };
    let _ = sender.send(BackendEvent::WorkspaceConversationsExtended {
        workspace_id,
        channels,
        direct_messages,
        conversation_bindings: vec![binding],
    });
}

fn maybe_handle_team_type_notify(
    event: &KeybaseNotifyEvent,
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
) {
    if event.method_name() != "chat.1.NotifyChat.NewChatActivity" {
        return;
    }
    let params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    let Some(activity) = find_value_for_keys(params, &["activity", "a"], 0) else {
        return;
    };
    if map_get_any(activity, &["activityType", "t"]).and_then(as_i64) != Some(8) {
        return;
    }
    let Some(update) = map_get_any(activity, &["teamtype", "teamType"]) else {
        return;
    };
    let team_type = parse_team_type(update);
    if team_type == ConversationTeamType::Unknown {
        return;
    }
    if let Some(raw_id) = map_get(update, "convID").and_then(value_to_conversation_id_bytes) {
        apply_team_type_change(
            sender,
            local_store,
            &ConversationId::new(format!("kb_conv:{}", hex_encode(&raw_id))),
            team_type,
        );
    }
}

fn apply_team_type_change(
    sender: &Sender<BackendEvent>,
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    team_type: ConversationTeamType,
) {
    let Some(team_id) = local_store
        .get_conversation(conversation_id)
        .ok()
        .flatten()
        .and_then(|summary| summary.group.map(|group| group.id))
    else {
        return;
    };
    let mut channels = Vec::new();
    let mut bindings = Vec::new();
    for id in local_store
        .load_cached_conversation_ids(100_000)
        .unwrap_or_default()
    {
        let Some(mut summary) = local_store.get_conversation(&id).ok().flatten() else {
            continue;
        };
        let Some(group) = summary.group.as_mut().filter(|group| group.id == team_id) else {
            continue;
        };
        if group.team_type == team_type {
            continue;
        }
        group.team_type = team_type.clone();
        if local_store
            .persist_conversation(&summary, summary.last_activity_ms)
            .is_err()
        {
            continue;
        }
        bindings.push(ConversationBinding {
            conversation_id: id.clone(),
            backend_id: BackendId::new(KEYBASE_BACKEND_ID),
            account_id: AccountId::new(DEFAULT_ACCOUNT_ID),
            provider_conversation_ref: ProviderConversationRef::new(id.0),
        });
        channels.push(summary);
    }
    if !channels.is_empty() {
        let _ = sender.send(BackendEvent::WorkspaceConversationsExtended {
            workspace_id: WorkspaceId::new(WORKSPACE_ID),
            channels,
            direct_messages: vec![],
            conversation_bindings: bindings,
        });
    }
}

fn maybe_handle_emoji_notify(
    event: &KeybaseNotifyEvent,
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
) {
    let method = event.method_name().to_ascii_lowercase();
    if !method.contains("emoji") {
        return;
    }

    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    let mut conversation_ids = collect_conversation_ids(raw_params);
    conversation_ids.sort_by(|left, right| left.0.cmp(&right.0));
    conversation_ids.dedup_by(|left, right| left.0 == right.0);

    for conversation_id in conversation_ids {
        spawn_sync_conversation_emojis(sender, local_store, conversation_id);
    }
}

fn collect_conversation_ids(value: &Value) -> Vec<ConversationId> {
    fn walk(value: &Value, depth: usize, output: &mut Vec<ConversationId>) {
        if depth > 8 {
            return;
        }
        match value {
            Value::Map(entries) => {
                for (key, inner) in entries {
                    if let Some(key_name) = key.as_str()
                        && matches!(
                            key_name,
                            "convID"
                                | "convId"
                                | "conversationID"
                                | "conversationId"
                                | "conversation_id"
                        )
                        && let Some(bytes) = value_to_conversation_id_bytes(inner)
                    {
                        output.push(ConversationId::new(format!(
                            "kb_conv:{}",
                            hex_encode(&bytes)
                        )));
                    }
                    walk(inner, depth + 1, output);
                }
            }
            Value::Array(values) => {
                for inner in values {
                    walk(inner, depth + 1, output);
                }
            }
            _ => {}
        }
    }

    let mut output = Vec::new();
    walk(value, 0, &mut output);
    output
}

fn maybe_handle_team_role_notify(
    event: &KeybaseNotifyEvent,
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
) {
    if event.method_name() != "keybase.1.NotifyTeam.teamRoleMapChanged" {
        return;
    }
    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    let mut team_ids = collect_team_ids(raw_params);
    team_ids.sort();
    team_ids.dedup();
    if team_ids.is_empty() {
        return;
    }

    for team_id in team_ids {
        let Ok(conversation_ids) = local_store.load_conversation_ids_for_team(&team_id) else {
            continue;
        };
        for conversation_id in conversation_ids {
            let conversation_ref = ProviderConversationRef::new(conversation_id.0.clone());
            spawn_sync_conversation_team_roles(
                sender,
                local_store,
                conversation_id,
                conversation_ref,
                true,
            );
        }
    }
}

fn collect_team_ids(value: &Value) -> Vec<String> {
    fn walk(value: &Value, depth: usize, output: &mut Vec<String>) {
        if depth > 8 {
            return;
        }
        match value {
            Value::Map(entries) => {
                for (key, inner) in entries {
                    if let Some(key_name) = key.as_str()
                        && matches!(key_name, "teamID" | "teamId" | "team_id")
                        && let Some(bytes) = value_to_team_id_bytes(inner)
                    {
                        output.push(canonical_team_id_from_bytes(&bytes));
                    }
                    walk(inner, depth + 1, output);
                }
            }
            Value::Array(values) => {
                for inner in values {
                    walk(inner, depth + 1, output);
                }
            }
            _ => {}
        }
    }

    let mut output = Vec::new();
    walk(value, 0, &mut output);
    output
}

fn collect_usernames_from_notify(raw_params: &Value, keys: &[&str]) -> Vec<String> {
    let mut usernames = Vec::new();
    for key in keys {
        if let Some(Value::Array(values)) = find_value_for_keys(raw_params, &[*key], 0) {
            for value in values {
                if let Some(name) = value.as_str()
                    && let Some(normalized) = canonical_username(name)
                {
                    usernames.push(normalized);
                }
            }
        }
    }
    usernames.sort();
    usernames.dedup();
    usernames
}

fn canonical_username(value: &str) -> Option<String> {
    let normalized = value.trim().trim_start_matches('@').to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    if normalized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        Some(normalized)
    } else {
        None
    }
}

fn collect_usernames_from_tracking_payload(value: &Value, output: &mut Vec<String>, depth: usize) {
    if depth > 8 {
        return;
    }
    match value {
        Value::Array(values) => {
            for inner in values {
                if let Some(name) = inner.as_str().and_then(canonical_username) {
                    output.push(name);
                    continue;
                }
                collect_usernames_from_tracking_payload(inner, output, depth + 1);
            }
        }
        Value::Map(entries) => {
            for (key, inner) in entries {
                if let Some(key_name) = key.as_str()
                    && matches!(
                        key_name,
                        "username" | "name" | "assertion" | "user" | "them" | "followee"
                    )
                    && let Some(name) = inner.as_str().and_then(canonical_username)
                {
                    output.push(name);
                }
                collect_usernames_from_tracking_payload(inner, output, depth + 1);
            }
        }
        _ => {}
    }
}

fn parse_tracking_followees(value: &Value) -> Vec<String> {
    let mut usernames = Vec::new();
    for key in ["followees", "tracking", "trackees", "users"] {
        if let Some(found) = find_value_for_keys(value, &[key], 0) {
            collect_usernames_from_tracking_payload(found, &mut usernames, 0);
            if !usernames.is_empty() {
                break;
            }
        }
    }
    if usernames.is_empty() {
        collect_usernames_from_tracking_payload(value, &mut usernames, 0);
    }
    usernames.sort();
    usernames.dedup();
    usernames
}

async fn fetch_tracking_affinities_from_service(
    path: &Path,
) -> io::Result<HashMap<UserId, Affinity>> {
    let transport = FramedMsgpackTransport::connect(path).await?;
    let mut client = KeybaseRpcClient::new(transport);
    let args = vec![Value::Map(vec![(
        Value::from("sessionID"),
        Value::from(SESSION_ID),
    )])];
    let methods = [
        KEYBASE_LIST_TRACKING,
        KEYBASE_LIST_TRACKING_JSON,
        KEYBASE_LIST_TRACKING_FALLBACK,
    ];
    for method in methods {
        if let Ok(response) = client.call(method, args.clone()).await {
            let followees = parse_tracking_followees(&response);
            return Ok(followees
                .into_iter()
                .map(|username| (UserId::new(username), Affinity::Positive))
                .collect());
        }
    }
    Err(io::Error::other("tracking list RPC unavailable"))
}

fn maybe_handle_presence_notify(event: &KeybaseNotifyEvent, sender: &Sender<BackendEvent>) {
    let users = parse_presence_updates_from_notify(event);
    if users.is_empty() {
        return;
    }
    let _ = sender.send(BackendEvent::PresenceUpdated {
        account_id: AccountId::new(DEFAULT_ACCOUNT_ID),
        users,
    });
}

fn parse_presence_updates_from_notify(event: &KeybaseNotifyEvent) -> Vec<PresencePatch> {
    if event.method_name() != "chat.1.NotifyChat.ChatParticipantsInfo" {
        return Vec::new();
    }

    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };

    let mut by_user: HashMap<UserId, PresencePatch> = HashMap::new();
    let mut extracted = Vec::new();
    collect_presence_patches(raw_params, &mut extracted, 0);
    for patch in extracted {
        merge_presence_patch(&mut by_user, patch);
    }

    let mut patches = by_user.into_values().collect::<Vec<_>>();
    patches.sort_by(|left, right| left.user_id.0.cmp(&right.user_id.0));
    patches
}

fn parse_presence_updates_from_rpc_notifications(
    notifications: &[RpcNotification],
) -> Vec<PresencePatch> {
    let mut by_user: HashMap<UserId, PresencePatch> = HashMap::new();
    for notification in notifications {
        if notification.method != "chat.1.NotifyChat.ChatParticipantsInfo" {
            continue;
        }
        let event =
            KeybaseNotifyEvent::from_method(&notification.method, notification.params.clone());
        for patch in parse_presence_updates_from_notify(&event) {
            merge_presence_patch(&mut by_user, patch);
        }
    }
    let mut patches = by_user.into_values().collect::<Vec<_>>();
    patches.sort_by(|left, right| left.user_id.0.cmp(&right.user_id.0));
    patches
}

fn collect_presence_patches(value: &Value, output: &mut Vec<PresencePatch>, depth: usize) {
    if depth > 8 {
        return;
    }
    match value {
        Value::Map(entries) => {
            let username = map_get_any(value, &["username", "user", "name", "assertion"])
                .or_else(|| {
                    find_value_for_keys(value, &["username", "user", "name", "assertion"], 0)
                })
                .and_then(as_str)
                .and_then(canonical_username);
            let availability = parse_presence_availability_from_payload(value);
            if let (Some(username), Some(availability)) = (username, availability) {
                let status_text = map_get_any(
                    value,
                    &[
                        "statusText",
                        "status_text",
                        "statusMessage",
                        "status_message",
                    ],
                )
                .or_else(|| {
                    find_value_for_keys(
                        value,
                        &[
                            "statusText",
                            "status_text",
                            "statusMessage",
                            "status_message",
                        ],
                        0,
                    )
                })
                .and_then(as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(str::to_string);
                output.push(PresencePatch {
                    user_id: UserId::new(username),
                    presence: Presence {
                        availability,
                        status_text,
                    },
                });
            }
            for (key, inner) in entries {
                if let Some(username) = username_from_presence_map_key(key)
                    && let Some(availability) =
                        parse_presence_availability_from_payload_shallow(inner)
                {
                    output.push(PresencePatch {
                        user_id: UserId::new(username),
                        presence: Presence {
                            availability,
                            status_text: None,
                        },
                    });
                }
                collect_presence_patches(inner, output, depth + 1);
            }
        }
        Value::Array(values) => {
            for inner in values {
                collect_presence_patches(inner, output, depth + 1);
            }
        }
        _ => {}
    }
}

fn parse_presence_availability_from_payload(value: &Value) -> Option<Availability> {
    if let Some(last_active_status) =
        map_get_any(value, &["lastActiveStatus", "last_active_status"])
            .or_else(|| find_value_for_keys(value, &["lastActiveStatus", "last_active_status"], 0))
            .and_then(parse_last_active_status_availability)
    {
        return Some(last_active_status);
    }
    map_get_any(
        value,
        &[
            "availability",
            "presence",
            "online",
            "isOnline",
            "active",
            "isActive",
            "status",
            "presenceState",
            "presence_state",
            "lastActiveStatus",
            "last_active_status",
        ],
    )
    .or_else(|| {
        find_value_for_keys(
            value,
            &[
                "availability",
                "presence",
                "online",
                "isOnline",
                "active",
                "isActive",
                "status",
                "presenceState",
                "presence_state",
                "lastActiveStatus",
                "last_active_status",
            ],
            0,
        )
    })
    .and_then(|raw| {
        parse_last_active_status_availability(raw).or_else(|| parse_presence_availability(raw))
    })
}

fn parse_presence_availability_from_payload_shallow(value: &Value) -> Option<Availability> {
    if let Some(last_active_status) =
        map_get_any(value, &["lastActiveStatus", "last_active_status"])
            .and_then(parse_last_active_status_availability)
    {
        return Some(last_active_status);
    }
    map_get_any(
        value,
        &[
            "availability",
            "presence",
            "online",
            "isOnline",
            "active",
            "isActive",
            "status",
            "presenceState",
            "presence_state",
            "lastActiveStatus",
            "last_active_status",
        ],
    )
    .or(match value {
        Value::Boolean(_) | Value::Integer(_) | Value::String(_) => Some(value),
        _ => None,
    })
    .and_then(|raw| {
        parse_last_active_status_availability(raw).or_else(|| parse_presence_availability(raw))
    })
}

fn parse_last_active_status_availability(value: &Value) -> Option<Availability> {
    if let Some(raw) = as_i64(value) {
        return match raw {
            0 => Some(Availability::Offline),
            1 => Some(Availability::Active),
            2 => Some(Availability::Away),
            _ => None,
        };
    }
    let text = as_str(value)?.trim().to_ascii_lowercase();
    match text.as_str() {
        "none" | "none_0" => Some(Availability::Offline),
        "active" | "active_1" => Some(Availability::Active),
        "recently_active" | "recentlyactive" | "recently_active_2" => Some(Availability::Away),
        _ => None,
    }
}

fn username_from_presence_map_key(value: &Value) -> Option<String> {
    let key = value.as_str()?;
    let username = canonical_username(key)?;
    if username.len() >= 32 && username.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return None;
    }
    if matches!(
        username.as_str(),
        "participants"
            | "status"
            | "lastactivestatus"
            | "availability"
            | "presence"
            | "active"
            | "isonline"
            | "assertion"
            | "username"
            | "name"
            | "user"
            | "conv_id"
            | "convid"
            | "conversationid"
    ) {
        return None;
    }
    Some(username)
}

fn merge_presence_patch(by_user: &mut HashMap<UserId, PresencePatch>, patch: PresencePatch) {
    let next_priority = presence_priority(&patch.presence.availability);
    match by_user.get_mut(&patch.user_id) {
        Some(existing) => {
            let current_priority = presence_priority(&existing.presence.availability);
            if next_priority > current_priority {
                *existing = patch;
                return;
            }
            if next_priority == current_priority
                && existing.presence.status_text.is_none()
                && patch.presence.status_text.is_some()
            {
                existing.presence.status_text = patch.presence.status_text;
            }
        }
        None => {
            by_user.insert(patch.user_id.clone(), patch);
        }
    }
}

fn parse_presence_availability(value: &Value) -> Option<Availability> {
    if let Some(raw) = as_bool(value) {
        return Some(if raw {
            Availability::Active
        } else {
            Availability::Offline
        });
    }
    if let Some(raw) = as_i64(value) {
        return match raw {
            0 => Some(Availability::Offline),
            1 => Some(Availability::Away),
            2..=4 => Some(Availability::Active),
            5 => Some(Availability::DoNotDisturb),
            _ => None,
        };
    }
    let text = as_str(value)?.trim().to_ascii_lowercase();
    match text.as_str() {
        "active" | "active_1" | "available" | "present" | "online" | "foregroundactive"
        | "backgroundactive" => Some(Availability::Active),
        "away" | "idle" | "inactive" | "recently_active" | "recentlyactive"
        | "recently_active_2" => Some(Availability::Away),
        "dnd" | "busy" | "do_not_disturb" | "donotdisturb" => Some(Availability::DoNotDisturb),
        "offline" | "none_0" => Some(Availability::Offline),
        _ => None,
    }
}

fn presence_priority(value: &Availability) -> u8 {
    match value {
        Availability::Active => 4,
        Availability::DoNotDisturb => 3,
        Availability::Away => 2,
        Availability::Offline => 1,
        Availability::Unknown => 0,
    }
}

fn maybe_handle_tracking_notify(event: &KeybaseNotifyEvent, sender: &Sender<BackendEvent>) {
    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    match event.method_name() {
        "keybase.1.NotifyTracking.trackingChanged" => {
            let Some(username) = find_value_for_keys(raw_params, &["username"], 0)
                .and_then(as_str)
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_ascii_lowercase)
            else {
                return;
            };
            let is_tracking = find_value_for_keys(raw_params, &["isTracking"], 0)
                .and_then(as_bool)
                .unwrap_or(false);
            let affinity = if is_tracking {
                Affinity::Positive
            } else {
                Affinity::None
            };
            let _ = sender.send(BackendEvent::AffinityChanged {
                user_id: UserId::new(username),
                affinity,
            });
        }
        "keybase.1.NotifyTracking.trackingInfo" => {
            let followees = collect_usernames_from_notify(raw_params, &["followees"]);
            let affinities = followees
                .into_iter()
                .map(|username| (UserId::new(username), Affinity::Positive))
                .collect::<HashMap<_, _>>();
            let _ = sender.send(BackendEvent::AffinitySynced { affinities });
        }
        "keybase.1.NotifyUsers.identifyUpdate" => {
            let broken_usernames = collect_usernames_from_notify(raw_params, &["brokenUsernames"]);
            let ok_usernames = collect_usernames_from_notify(raw_params, &["okUsernames"]);
            let mut by_user = HashMap::new();
            for username in ok_usernames {
                by_user.insert(UserId::new(username), Affinity::Positive);
            }
            for username in broken_usernames {
                by_user.insert(UserId::new(username), Affinity::Broken);
            }
            for (user_id, affinity) in by_user {
                let _ = sender.send(BackendEvent::AffinityChanged { user_id, affinity });
            }
        }
        _ => {}
    }
}

fn maybe_handle_profile_notify(
    event: &KeybaseNotifyEvent,
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
) {
    match event.method_name() {
        "keybase.1.NotifyUsers.identifyUpdate" => {
            let raw_params = match event {
                KeybaseNotifyEvent::Known { raw_params, .. }
                | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
            };
            let usernames =
                collect_usernames_from_notify(raw_params, &["okUsernames", "brokenUsernames"]);
            if usernames.is_empty() {
                return;
            }
            spawn_prefetch_user_profiles(sender, local_store, usernames);
        }
        "keybase.1.NotifyUsers.userChanged" => {
            let raw_params = match event {
                KeybaseNotifyEvent::Known { raw_params, .. }
                | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
            };
            let uid = find_value_for_keys(raw_params, &["uid", "UID"], 0)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let Some(uid) = uid else {
                return;
            };
            let sender = sender.clone();
            let local_store = Arc::clone(local_store);
            let dedupe_key = format!("resolve_username_from_uid:{uid}");
            let _ = task_runtime::spawn_task(TaskPriority::Low, Some(dedupe_key), move || {
                if let Some(username) = resolve_username_from_uid(&uid) {
                    spawn_prefetch_user_profiles(&sender, &local_store, vec![username]);
                }
            });
        }
        "keybase.1.NotifyUsers.webOfTrustChanged" => {
            let raw_params = match event {
                KeybaseNotifyEvent::Known { raw_params, .. }
                | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
            };
            let username = find_value_for_keys(raw_params, &["username"], 0)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let Some(username) = username else {
                return;
            };
            spawn_prefetch_user_profiles(sender, local_store, vec![username]);
        }
        "keybase.1.NotifyTeam.avatarUpdated" => {
            let raw_params = match event {
                KeybaseNotifyEvent::Known { raw_params, .. }
                | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
            };
            let typ = find_value_for_keys(raw_params, &["typ"], 0).and_then(as_i64);
            let name = find_value_for_keys(raw_params, &["name"], 0)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let Some(name) = name else {
                return;
            };
            match typ {
                // AvatarUpdateType.USER
                Some(1) => spawn_prefetch_user_profiles(sender, local_store, vec![name]),
                // AvatarUpdateType.TEAM
                Some(2) => spawn_prefetch_team_avatars(sender, local_store, vec![name]),
                _ => {}
            }
        }
        _ => {}
    }
}

fn resolve_username_from_uid(uid: &str) -> Option<String> {
    let socket = socket_path()?;
    let runtime = Builder::new_current_thread().enable_all().build().ok()?;
    runtime.block_on(async move {
        let transport = FramedMsgpackTransport::connect(&socket).await.ok()?;
        let mut client = KeybaseRpcClient::new(transport);
        let user = client
            .call(
                KEYBASE_LOAD_USER,
                vec![Value::Map(vec![
                    (Value::from("sessionID"), Value::from(SESSION_ID)),
                    (Value::from("uid"), Value::from(uid)),
                ])],
            )
            .await
            .ok()?;
        map_get_any(&user, &["username", "n"])
            .and_then(as_str)
            .map(str::to_string)
    })
}

fn parse_user_display_name(value: &Value, username: &str) -> Option<String> {
    for key in [
        "fullName",
        "fullname",
        "full_name",
        "displayName",
        "displayname",
        "display_name",
        "fullNameRaw",
        "fullNameUntrusted",
        "name",
    ] {
        let Some(name) = find_value_for_keys(value, &[key], 0).and_then(as_str) else {
            continue;
        };
        let trimmed = name.trim();
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case(username) {
            continue;
        }
        return Some(trimmed.to_string());
    }
    None
}

fn search_result_route(conversation: &ConversationSummary) -> Route {
    let workspace_id = WorkspaceId::new(WORKSPACE_ID);
    match conversation.kind {
        ConversationKind::Channel => Route::Channel {
            workspace_id: workspace_id.clone(),
            channel_id: ChannelId::new(conversation.id.0.clone()),
        },
        ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
            Route::DirectMessage {
                workspace_id,
                dm_id: DmId::new(conversation.id.0.clone()),
            }
        }
    }
}

fn message_plain_text(message: &MessageRecord) -> String {
    message
        .fragments
        .iter()
        .map(|fragment| match fragment {
            MessageFragment::Text(text)
            | MessageFragment::Code { text, .. }
            | MessageFragment::Quote(text) => text.clone(),
            MessageFragment::InlineCode(text) => format!("`{text}`"),
            MessageFragment::Emoji { alias, .. } => format!(":{alias}:"),
            MessageFragment::Mention(user_id) => format!("@{}", user_id.0),
            MessageFragment::ChannelMention { name } => format!("#{name}"),
            MessageFragment::BroadcastMention(BroadcastKind::Here) => "@here".to_string(),
            MessageFragment::BroadcastMention(BroadcastKind::All) => "@channel".to_string(),
            MessageFragment::Link { display, .. } => display.clone(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn message_attachment_filename_tokens(message: &MessageRecord) -> String {
    message
        .attachments
        .iter()
        .map(|attachment| attachment.name.trim())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn message_contains_shortcode(message: &MessageRecord) -> bool {
    message.fragments.iter().any(|fragment| match fragment {
        MessageFragment::Text(text)
        | MessageFragment::Code { text, .. }
        | MessageFragment::Quote(text) => {
            text.contains(':') && text.chars().filter(|ch| *ch == ':').count() >= 2
        }
        MessageFragment::InlineCode(_) => false,
        MessageFragment::Emoji { .. } => true,
        MessageFragment::Mention(_)
        | MessageFragment::ChannelMention { .. }
        | MessageFragment::BroadcastMention(_)
        | MessageFragment::Link { .. } => false,
    })
}

fn messages_have_non_text_placeholders(messages: &[MessageRecord]) -> bool {
    messages.iter().any(is_placeholder_message)
}

fn is_placeholder_non_text_message(message: &MessageRecord) -> bool {
    message.event.is_none() && message_plain_text(message).trim() == NON_TEXT_PLACEHOLDER_BODY
}

fn is_placeholder_link_preview_message(message: &MessageRecord) -> bool {
    message.event.is_none()
        && message.attachments.is_empty()
        && !message.link_previews.is_empty()
        && is_link_preview_placeholder_text(&message_plain_text(message))
}

fn is_placeholder_message(message: &MessageRecord) -> bool {
    is_placeholder_non_text_message(message) || is_placeholder_link_preview_message(message)
}

fn strip_placeholder_messages(messages: Vec<MessageRecord>) -> (Vec<MessageRecord>, usize) {
    let original_len = messages.len();
    let filtered = messages
        .into_iter()
        .filter(|message| !is_placeholder_message(message))
        .collect::<Vec<_>>();
    let removed_count = original_len.saturating_sub(filtered.len());
    (filtered, removed_count)
}

fn strip_reaction_delete_tombstones(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    messages: &mut Vec<MessageRecord>,
) {
    messages.retain(|message| {
        if let Some(ChatEvent::MessageDeleted {
            target_message_id: Some(target),
        }) = &message.event
            && local_store
                .get_message_reaction_op(conversation_id, target)
                .ok()
                .flatten()
                .is_some()
        {
            let _ = local_store.delete_message(conversation_id, &message.id);
            return false;
        }
        true
    });
}

fn apply_regular_delete_tombstones(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    sender: Option<&Sender<BackendEvent>>,
    messages: &mut Vec<MessageRecord>,
) {
    let mut deleted_ids = HashSet::new();
    for message in messages.iter() {
        if let Some(ChatEvent::MessageDeleted {
            target_message_id: Some(target),
        }) = &message.event
            && local_store
                .get_message_reaction_op(conversation_id, target)
                .ok()
                .flatten()
                .is_none()
        {
            let _ = local_store.delete_message(conversation_id, target);
            deleted_ids.insert(target.clone());
        }
    }
    if deleted_ids.is_empty() {
        return;
    }
    messages.retain(|message| {
        if deleted_ids.contains(&message.id) {
            return false;
        }
        if let Some(ChatEvent::MessageDeleted {
            target_message_id: Some(target),
        }) = &message.event
        {
            return !deleted_ids.contains(target);
        }
        true
    });
    if let Some(sender) = sender {
        for message_id in &deleted_ids {
            let _ = sender.send(BackendEvent::MessageDeleted {
                conversation_id: conversation_id.clone(),
                message_id: message_id.clone(),
            });
        }
    }
}

fn strip_reaction_delete_tombstones_from_deltas(
    messages: &mut Vec<MessageRecord>,
    deltas: &[MessageReactionDelta],
) {
    let mut delete_ids = HashSet::new();
    for delta in deltas {
        if let Some(op_message_id) = delta.op_message_id.as_ref() {
            delete_ids.insert(op_message_id.clone());
        }
    }
    if delete_ids.is_empty() {
        return;
    }
    messages.retain(|message| !delete_ids.contains(&message.id));
}

fn spawn_persist_reaction_deltas_for_timeline_load(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    conversation_id: ConversationId,
    deltas: Vec<MessageReactionDelta>,
    message_ids: Vec<MessageId>,
) {
    if deltas.is_empty() || message_ids.is_empty() {
        return;
    }
    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    let _ = task_runtime::spawn_task(TaskPriority::Low, None, move || {
        run_persist_reaction_deltas_for_timeline_load(
            sender,
            local_store,
            conversation_id,
            deltas,
            message_ids,
        )
    });
}

fn run_persist_reaction_deltas_for_timeline_load(
    sender: Sender<BackendEvent>,
    local_store: Arc<LocalStore>,
    conversation_id: ConversationId,
    deltas: Vec<MessageReactionDelta>,
    message_ids: Vec<MessageId>,
) {
    let started = Instant::now();
    persist_reaction_deltas(Some(&sender), &local_store, &deltas);
    let Some(event) =
        message_reaction_sync_event_for_message_ids(&local_store, &conversation_id, &message_ids)
    else {
        return;
    };
    let _ = sender.send(event);
    let elapsed_ms = started.elapsed().as_millis();
    if load_conversation_perf_log_all_enabled() || elapsed_ms >= 250 {
        warn!(
            target: "zbase.load_conversation.perf",
            conversation_id = %conversation_id.0,
            elapsed_ms = elapsed_ms as i64,
            reaction_delta_count = deltas.len() as i64,
            message_count = message_ids.len() as i64,
            "load_conversation_reaction_delta_persist"
        );
    }
}

fn persist_reaction_deltas(
    sender: Option<&Sender<BackendEvent>>,
    local_store: &LocalStore,
    deltas: &[MessageReactionDelta],
) {
    for delta in deltas {
        let _ = local_store.upsert_message_reaction(
            &delta.conversation_id,
            &delta.target_message_id,
            &delta.emoji,
            delta.source_ref.as_ref(),
            &delta.actor_id,
            delta.updated_ms,
        );
        remember_reaction_op(delta);
        if let Some(op_message_id) = &delta.op_message_id {
            let _ = local_store.upsert_message_reaction_op(
                &delta.conversation_id,
                op_message_id,
                &delta.target_message_id,
                &delta.emoji,
                &delta.actor_id,
                delta.updated_ms,
            );
            let _ = local_store.delete_message(&delta.conversation_id, op_message_id);
        }
    }
    if let Some(sender) = sender {
        spawn_sync_reaction_emoji_sources(sender, deltas);
    }
}

#[derive(Clone, Debug)]
struct ReactionOpIndexEntry {
    target_message_id: MessageId,
    emoji: String,
    actor_id: UserId,
}

fn reaction_op_index() -> &'static Mutex<HashMap<String, ReactionOpIndexEntry>> {
    static INDEX: OnceLock<Mutex<HashMap<String, ReactionOpIndexEntry>>> = OnceLock::new();
    INDEX.get_or_init(|| Mutex::new(HashMap::new()))
}

fn reaction_op_index_key(conversation_id: &ConversationId, op_message_id: &MessageId) -> String {
    format!("{}:{}", conversation_id.0, op_message_id.0)
}

fn remember_reaction_op(delta: &MessageReactionDelta) {
    let Some(op_message_id) = &delta.op_message_id else {
        return;
    };
    let Ok(mut index) = reaction_op_index().lock() else {
        return;
    };
    index.insert(
        reaction_op_index_key(&delta.conversation_id, op_message_id),
        ReactionOpIndexEntry {
            target_message_id: delta.target_message_id.clone(),
            emoji: delta.emoji.clone(),
            actor_id: delta.actor_id.clone(),
        },
    );
}

fn take_reaction_op(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    op_message_id: &MessageId,
) -> Option<ReactionOpIndexEntry> {
    let key = reaction_op_index_key(conversation_id, op_message_id);
    let Ok(index) = reaction_op_index().lock() else {
        let stored = local_store
            .get_message_reaction_op(conversation_id, op_message_id)
            .ok()
            .flatten()?;
        return Some(ReactionOpIndexEntry {
            target_message_id: stored.0,
            emoji: stored.1,
            actor_id: stored.2,
        });
    };
    if let Some(entry) = index.get(&key) {
        return Some(entry.clone());
    }
    drop(index);
    let stored = local_store
        .get_message_reaction_op(conversation_id, op_message_id)
        .ok()
        .flatten()?;
    let entry = ReactionOpIndexEntry {
        target_message_id: stored.0,
        emoji: stored.1,
        actor_id: stored.2,
    };
    if let Ok(mut index) = reaction_op_index().lock() {
        index.insert(key, entry.clone());
    }
    Some(entry)
}

fn reaction_removed_event_for_live_delete(
    local_store: &LocalStore,
    message: &MessageRecord,
) -> Option<BackendEvent> {
    let ChatEvent::MessageDeleted {
        target_message_id: Some(target_message_id),
    } = message.event.as_ref()?
    else {
        return None;
    };
    let removed = take_reaction_op(local_store, &message.conversation_id, target_message_id)?;
    let _ = local_store.delete_message(&message.conversation_id, target_message_id);
    let _ = local_store.delete_message_reaction(
        &message.conversation_id,
        &removed.target_message_id,
        &removed.emoji,
        &removed.actor_id,
    );
    Some(BackendEvent::MessageReactionRemoved {
        conversation_id: message.conversation_id.clone(),
        message_id: removed.target_message_id,
        emoji: removed.emoji,
        actor_id: removed.actor_id,
    })
}

fn regular_message_delete_event(
    local_store: &LocalStore,
    message: &MessageRecord,
) -> Option<BackendEvent> {
    let ChatEvent::MessageDeleted {
        target_message_id: Some(target_message_id),
    } = message.event.as_ref()?
    else {
        return None;
    };
    let _ = local_store.delete_message(&message.conversation_id, target_message_id);
    Some(BackendEvent::MessageDeleted {
        conversation_id: message.conversation_id.clone(),
        message_id: target_message_id.clone(),
    })
}

fn reaction_op_delete_events(deltas: &[MessageReactionDelta]) -> Vec<BackendEvent> {
    let mut seen = std::collections::HashSet::new();
    deltas
        .iter()
        .filter_map(|delta| {
            let op_message_id = delta.op_message_id.clone()?;
            let dedupe_key = format!("{}:{}", delta.conversation_id.0, op_message_id.0);
            if !seen.insert(dedupe_key) {
                return None;
            }
            Some(BackendEvent::MessageDeleted {
                conversation_id: delta.conversation_id.clone(),
                message_id: op_message_id,
            })
        })
        .collect()
}

fn message_reaction_sync_event(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    messages: &[MessageRecord],
) -> Option<BackendEvent> {
    if messages.is_empty() {
        return None;
    }
    let message_ids = messages
        .iter()
        .map(|message| message.id.clone())
        .collect::<Vec<_>>();
    let loaded = local_store
        .load_message_reactions_for_messages(conversation_id, &message_ids)
        .ok()?;
    let reactions_by_message = message_ids
        .into_iter()
        .map(|message_id| MessageReactionsForMessage {
            message_id: message_id.clone(),
            reactions: aggregate_message_reactions(loaded.get(&message_id)),
        })
        .collect::<Vec<_>>();
    Some(BackendEvent::MessageReactionsSynced {
        conversation_id: conversation_id.clone(),
        reactions_by_message,
    })
}

fn message_reaction_sync_event_for_message_ids(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    message_ids: &[MessageId],
) -> Option<BackendEvent> {
    if message_ids.is_empty() {
        return None;
    }
    let loaded = local_store
        .load_message_reactions_for_messages(conversation_id, message_ids)
        .ok()?;
    let reactions_by_message = message_ids
        .iter()
        .map(|message_id| MessageReactionsForMessage {
            message_id: message_id.clone(),
            reactions: aggregate_message_reactions(loaded.get(message_id)),
        })
        .collect::<Vec<_>>();
    Some(BackendEvent::MessageReactionsSynced {
        conversation_id: conversation_id.clone(),
        reactions_by_message,
    })
}

fn aggregate_message_reactions(
    records: Option<&Vec<CachedMessageReaction>>,
) -> Vec<MessageReactionEntry> {
    let mut by_emoji: std::collections::HashMap<
        String,
        (
            std::collections::HashSet<UserId>,
            Option<EmojiSourceRef>,
            i64,
        ),
    > = std::collections::HashMap::new();
    for record in records.into_iter().flatten() {
        let entry = by_emoji
            .entry(record.emoji.clone())
            .or_insert_with(|| (std::collections::HashSet::new(), None, 0));
        entry.0.insert(UserId::new(record.actor_id.clone()));
        if entry.1.is_none() {
            entry.1 = record.source_ref.as_ref().map(|source_ref| EmojiSourceRef {
                backend_id: BackendId::new(source_ref.backend_id.clone()),
                ref_key: source_ref.ref_key.clone(),
            });
        }
        entry.2 = entry.2.max(record.updated_ms);
    }

    let mut reactions = by_emoji
        .into_iter()
        .map(|(emoji, (actor_ids, source_ref, updated_ms))| {
            let mut actor_ids = actor_ids.into_iter().collect::<Vec<_>>();
            actor_ids.sort_by(|left, right| left.0.cmp(&right.0));
            MessageReactionEntry {
                emoji,
                source_ref,
                actor_ids,
                updated_ms,
            }
        })
        .collect::<Vec<_>>();
    reactions.sort_by(|left, right| left.emoji.cmp(&right.emoji));
    reactions
}

fn domain_message_reactions(records: Option<&Vec<CachedMessageReaction>>) -> Vec<MessageReaction> {
    aggregate_message_reactions(records)
        .into_iter()
        .map(|reaction| MessageReaction {
            emoji: reaction.emoji,
            source_ref: reaction.source_ref,
            actor_ids: reaction.actor_ids,
        })
        .collect()
}

fn team_roles_event_from_cache(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
) -> Option<BackendEvent> {
    let team_binding = local_store
        .get_conversation_team_binding(conversation_id)
        .ok()
        .flatten()?;
    let canonical_team_id = canonical_team_id_if_valid(&team_binding.team_id)?;
    let cached_roles = local_store
        .get_team_role_map(&canonical_team_id)
        .ok()
        .flatten()?;
    let role_index = cached_team_role_map_to_role_index(&cached_roles);
    Some(team_roles_backend_event(
        conversation_id,
        &canonical_team_id,
        &role_index,
        cached_roles.updated_ms,
    ))
}

fn spawn_sync_conversation_team_roles(
    sender: &Sender<BackendEvent>,
    local_store: &Arc<LocalStore>,
    conversation_id: ConversationId,
    conversation_ref: ProviderConversationRef,
    force_refresh: bool,
) {
    let sender = sender.clone();
    let local_store = Arc::clone(local_store);
    let dedupe_key = format!("team_roles:{}", conversation_id.0);
    let _ = task_runtime::spawn_task(TaskPriority::Low, Some(dedupe_key), move || {
        sync_conversation_team_roles(
            &sender,
            &local_store,
            &conversation_id,
            &conversation_ref,
            force_refresh,
        );
    });
}

fn sync_conversation_team_roles(
    sender: &Sender<BackendEvent>,
    local_store: &LocalStore,
    conversation_id: &ConversationId,
    conversation_ref: &ProviderConversationRef,
    force_refresh: bool,
) {
    let now_ms = now_unix_ms();
    let mut team_id = local_store
        .get_conversation_team_binding(conversation_id)
        .ok()
        .flatten()
        .and_then(|binding| {
            canonical_team_id_if_valid(&binding.team_id).or_else(|| {
                send_internal(
                    sender,
                    "zbase.internal.team_roles.invalid_cached_team_id",
                    Value::Map(vec![
                        (
                            Value::from("conversation_id"),
                            Value::from(conversation_id.0.clone()),
                        ),
                        (Value::from("team_id"), Value::from(binding.team_id)),
                    ]),
                );
                None
            })
        });

    match resolve_team_id_from_conversation_ref_local(conversation_ref) {
        Ok(Some(resolved_team_id)) => {
            let local_team_id = canonical_team_id_if_valid(&resolved_team_id);
            if let Some(local_team_id) = local_team_id {
                if team_id.as_ref() != Some(&local_team_id) {
                    send_internal(
                        sender,
                        "zbase.internal.team_roles.team_id_rebound",
                        Value::Map(vec![
                            (
                                Value::from("conversation_id"),
                                Value::from(conversation_id.0.clone()),
                            ),
                            (
                                Value::from("old_team_id"),
                                team_id
                                    .as_ref()
                                    .map(|value| Value::from(value.clone()))
                                    .unwrap_or(Value::Nil),
                            ),
                            (
                                Value::from("new_team_id"),
                                Value::from(local_team_id.clone()),
                            ),
                        ]),
                    );
                } else {
                    send_internal(
                        sender,
                        "zbase.internal.team_roles.resolve_team_local_conversation",
                        Value::Map(vec![(
                            Value::from("conversation_id"),
                            Value::from(conversation_id.0.clone()),
                        )]),
                    );
                }
                team_id = Some(local_team_id);
            }
        }
        Ok(None) => {}
        Err(error) => {
            send_internal(
                sender,
                "zbase.internal.team_roles.resolve_team_local_conversation_failed",
                Value::Map(vec![
                    (
                        Value::from("conversation_id"),
                        Value::from(conversation_id.0.clone()),
                    ),
                    (Value::from("error"), Value::from(error.to_string())),
                ]),
            );
        }
    }

    if team_id.is_none() {
        match resolve_team_id_from_cached_conversation(local_store, conversation_id) {
            Ok(Some(resolved_team_id)) => {
                let local_team_id = canonical_team_id_if_valid(&resolved_team_id);
                if let Some(local_team_id) = local_team_id {
                    if team_id.as_ref() != Some(&local_team_id) {
                        send_internal(
                            sender,
                            "zbase.internal.team_roles.team_id_rebound",
                            Value::Map(vec![
                                (
                                    Value::from("conversation_id"),
                                    Value::from(conversation_id.0.clone()),
                                ),
                                (
                                    Value::from("old_team_id"),
                                    team_id
                                        .as_ref()
                                        .map(|value| Value::from(value.clone()))
                                        .unwrap_or(Value::Nil),
                                ),
                                (
                                    Value::from("new_team_id"),
                                    Value::from(local_team_id.clone()),
                                ),
                            ]),
                        );
                    } else {
                        send_internal(
                            sender,
                            "zbase.internal.team_roles.resolve_team_local",
                            Value::Map(vec![(
                                Value::from("conversation_id"),
                                Value::from(conversation_id.0.clone()),
                            )]),
                        );
                    }
                    team_id = Some(local_team_id);
                }
            }
            Ok(None) => {}
            Err(error) => {
                send_internal(
                    sender,
                    "zbase.internal.team_roles.resolve_team_local_failed",
                    Value::Map(vec![
                        (
                            Value::from("conversation_id"),
                            Value::from(conversation_id.0.clone()),
                        ),
                        (Value::from("error"), Value::from(error.to_string())),
                    ]),
                );
            }
        }
    }

    if team_id.is_none() {
        match resolve_team_id_for_conversation(conversation_ref) {
            Ok(Some(resolved_team_id)) => {
                team_id = canonical_team_id_if_valid(&resolved_team_id);
            }
            Ok(None) => {}
            Err(error) => {
                send_internal(
                    sender,
                    "zbase.internal.team_roles.resolve_team_failed",
                    Value::Map(vec![
                        (
                            Value::from("conversation_id"),
                            Value::from(conversation_id.0.clone()),
                        ),
                        (Value::from("error"), Value::from(error.to_string())),
                    ]),
                );
            }
        }
    }

    let Some(team_id) = team_id else {
        send_internal(
            sender,
            "zbase.internal.team_roles.resolve_team_missing",
            Value::Map(vec![(
                Value::from("conversation_id"),
                Value::from(conversation_id.0.clone()),
            )]),
        );
        return;
    };
    let _ = local_store.upsert_conversation_team_binding(conversation_id, &team_id, now_ms);

    let cached_role_map = local_store.get_team_role_map(&team_id).ok().flatten();
    if !force_refresh
        && let Some(cached_role_map) = cached_role_map.as_ref()
        && !should_refresh_team_role_cache(cached_role_map, now_ms)
    {
        let cached_roles = cached_team_role_map_to_role_index(cached_role_map);
        let _ = sender.send(team_roles_backend_event(
            conversation_id,
            &team_id,
            &cached_roles,
            cached_role_map.updated_ms,
        ));
        return;
    }

    match fetch_team_roles_for_team(&team_id) {
        Ok(roles) => {
            let updated_ms = now_unix_ms();
            let _ = local_store.upsert_team_role_map(&team_id, &roles, updated_ms);
            let _ = sender.send(team_roles_backend_event(
                conversation_id,
                &team_id,
                &roles,
                updated_ms,
            ));
        }
        Err(error) => {
            send_internal(
                sender,
                "zbase.internal.team_roles.fetch_failed",
                Value::Map(vec![
                    (
                        Value::from("conversation_id"),
                        Value::from(conversation_id.0.clone()),
                    ),
                    (Value::from("team_id"), Value::from(team_id.clone())),
                    (Value::from("error"), Value::from(error.to_string())),
                ]),
            );
            if let Some(cached_role_map) = cached_role_map {
                let cached_roles = cached_team_role_map_to_role_index(&cached_role_map);
                let _ = sender.send(team_roles_backend_event(
                    conversation_id,
                    &team_id,
                    &cached_roles,
                    cached_role_map.updated_ms,
                ));
            }
        }
    }
}

fn should_refresh_team_role_cache(cached: &CachedTeamRoleMap, now_ms: i64) -> bool {
    now_ms.saturating_sub(cached.updated_ms) > TEAM_ROLE_CACHE_TTL_MS
}

fn cached_team_role_map_to_role_index(cached: &CachedTeamRoleMap) -> HashMap<UserId, i64> {
    let mut roles = HashMap::new();
    for entry in &cached.roles {
        let username = entry.user_id.trim();
        if username.is_empty() {
            continue;
        }
        roles.insert(UserId::new(username.to_ascii_lowercase()), entry.role);
    }
    roles
}

fn team_roles_backend_event(
    conversation_id: &ConversationId,
    team_id: &str,
    roles: &HashMap<UserId, i64>,
    updated_ms: i64,
) -> BackendEvent {
    let mut entries = roles
        .iter()
        .filter_map(|(user_id, role)| {
            team_role_kind_from_raw(*role).map(|role_kind| TeamRoleEntry {
                user_id: user_id.clone(),
                role: role_kind,
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.user_id.0.cmp(&right.user_id.0));
    BackendEvent::TeamRolesUpdated {
        conversation_id: conversation_id.clone(),
        team_id: team_id.to_string(),
        roles: entries,
        updated_ms,
    }
}

fn team_role_kind_from_raw(role: i64) -> Option<TeamRoleKind> {
    match role {
        0..=2 => Some(TeamRoleKind::Member),
        3 => Some(TeamRoleKind::Admin),
        4 => Some(TeamRoleKind::Owner),
        _ => None,
    }
}

fn resolve_team_id_for_conversation(
    conversation_ref: &ProviderConversationRef,
) -> io::Result<Option<String>> {
    let Some(raw_conversation_id) = provider_ref_to_conversation_id_bytes(conversation_ref) else {
        return Ok(None);
    };
    let Some(path) = socket_path() else {
        return Ok(None);
    };
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let team_id_bytes = runtime.block_on(async move {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        call_team_id_of_conv(&mut client, &raw_conversation_id).await
    })?;
    Ok(team_id_bytes
        .map(|bytes| canonical_team_id_from_bytes(&bytes))
        .and_then(|team_id| canonical_team_id_if_valid(&team_id)))
}

fn extract_team_lookup_params_from_inbox_response(value: &Value) -> Option<(String, i64, bool)> {
    let conversations = map_get_any(value, &["conversations", "c"]).and_then(as_array)?;
    let conversation = conversations.first()?;
    let info = map_get_any(conversation, &["info", "i", "conv", "conversation"])?;
    let tlf_name = map_get_any(info, &["tlfName", "n"])
        .and_then(as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)?;
    let members_type = map_get_any(info, &["membersType", "m"])
        .and_then(as_i64)
        .unwrap_or(TEAM_MEMBERS_TYPE);
    let tlf_public = map_get_any(info, &["isPublic", "public"])
        .and_then(as_bool)
        .or_else(|| {
            map_get_any(info, &["visibility", "v"])
                .and_then(as_i64)
                .map(|raw| raw == 1)
        })
        .unwrap_or(false);
    Some((tlf_name, members_type, tlf_public))
}

fn resolve_team_id_from_conversation_ref_local(
    conversation_ref: &ProviderConversationRef,
) -> io::Result<Option<String>> {
    let Some(raw_conversation_id) = provider_ref_to_conversation_id_bytes(conversation_ref) else {
        return Ok(None);
    };
    let Some(path) = socket_path() else {
        return Ok(None);
    };
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let team_id_bytes = runtime.block_on(async move {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        let inbox = fetch_inbox_for_conversation_id(&mut client, &raw_conversation_id).await?;
        let Some((tlf_name, members_type, tlf_public)) =
            extract_team_lookup_params_from_inbox_response(&inbox)
        else {
            return Ok(None);
        };

        let first =
            call_team_id_from_tlf_name(&mut client, &tlf_name, members_type, tlf_public).await?;
        if first.is_some() {
            return Ok(first);
        }

        call_team_id_from_tlf_name(&mut client, &tlf_name, members_type, !tlf_public).await
    })?;
    Ok(team_id_bytes
        .map(|bytes| canonical_team_id_from_bytes(&bytes))
        .and_then(|team_id| canonical_team_id_if_valid(&team_id)))
}

fn resolve_team_id_from_cached_conversation(
    local_store: &LocalStore,
    conversation_id: &ConversationId,
) -> io::Result<Option<String>> {
    let Some(summary) = local_store.get_conversation(conversation_id)? else {
        return Ok(None);
    };
    if summary.kind != ConversationKind::Channel {
        return Ok(None);
    }
    let Some(tlf_name) = summary
        .group
        .as_ref()
        .map(|group| group.id.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
    else {
        return Ok(None);
    };
    let Some(path) = socket_path() else {
        return Ok(None);
    };
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let team_id_bytes = runtime.block_on(async move {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        let private =
            call_team_id_from_tlf_name(&mut client, &tlf_name, TEAM_MEMBERS_TYPE, false).await?;
        if private.is_some() {
            return Ok(private);
        }
        call_team_id_from_tlf_name(&mut client, &tlf_name, TEAM_MEMBERS_TYPE, true).await
    })?;
    Ok(team_id_bytes
        .map(|bytes| canonical_team_id_from_bytes(&bytes))
        .and_then(|team_id| canonical_team_id_if_valid(&team_id)))
}

fn fetch_team_roles_for_team(team_id: &str) -> io::Result<HashMap<UserId, i64>> {
    let Some(canonical_team_id) = canonical_team_id_if_valid(team_id) else {
        return Err(io::Error::other("invalid team ID"));
    };
    let rpc_team_id = canonical_team_id
        .strip_prefix("kb_team:")
        .unwrap_or(&canonical_team_id)
        .to_string();
    let Some(path) = socket_path() else {
        return Err(io::Error::other("keybase socket path missing"));
    };
    let runtime = Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async move {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        let response = call_team_get_members_by_id(&mut client, &rpc_team_id).await?;
        Ok(parse_team_member_roles(&response))
    })
}

fn parse_team_member_roles(value: &Value) -> HashMap<UserId, i64> {
    let members = if let Some(items) = as_array(value) {
        items.clone()
    } else if let Some(grouped) = map_get_any(value, &["members", "details"]) {
        if let Some(items) = as_array(grouped) {
            items.clone()
        } else {
            collect_team_member_details_from_grouped_map(grouped)
        }
    } else {
        Vec::new()
    };

    let mut roles = HashMap::new();
    for member in members {
        let username = find_value_for_keys(&member, &["username", "name", "n"], 0)
            .and_then(as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let role = find_value_for_keys(&member, &["role", "r"], 0).and_then(parse_team_role_raw);
        let status = find_value_for_keys(&member, &["status", "s"], 0)
            .and_then(parse_team_member_status_raw);
        if status.is_some_and(|value| value != 0) {
            continue;
        }
        let (Some(username), Some(role)) = (username, role) else {
            continue;
        };
        roles.insert(UserId::new(username.to_ascii_lowercase()), role);
    }
    roles
}

fn collect_team_member_details_from_grouped_map(value: &Value) -> Vec<Value> {
    let mut members = Vec::new();
    for key in [
        "owners",
        "admins",
        "writers",
        "readers",
        "bots",
        "restrictedBots",
    ] {
        if let Some(entries) = map_get(value, key).and_then(as_array) {
            members.extend(entries.iter().cloned());
        }
    }
    members
}

fn parse_team_role_raw(value: &Value) -> Option<i64> {
    as_i64(value).or_else(|| {
        as_str(value).and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "owner" | "owner_4" => Some(4),
            "admin" | "admin_3" => Some(3),
            "writer" | "writer_2" => Some(2),
            "reader" | "reader_1" => Some(1),
            "none" | "none_0" => Some(0),
            _ => None,
        })
    })
}

fn parse_team_member_status_raw(value: &Value) -> Option<i64> {
    as_i64(value).or_else(|| {
        as_str(value).and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "active" | "active_0" => Some(0),
            "reset" | "reset_1" => Some(1),
            "deleted" | "deleted_2" => Some(2),
            _ => None,
        })
    })
}

fn extract_notify_activity_root(value: &Value) -> Option<&Value> {
    find_value_for_keys(value, &["activity", "a"], 0)
}

fn extract_notify_incoming_message_root(value: &Value) -> Option<&Value> {
    let activity = extract_notify_activity_root(value)?;
    map_get_any(activity, &["incomingMessage", "i", "im"])
        .or_else(|| find_value_for_keys(activity, &["incomingMessage", "i", "im"], 0))
}

fn extract_notify_incoming_ui_message(value: &Value) -> Option<&Value> {
    let incoming = extract_notify_incoming_message_root(value)?;
    map_get_any(incoming, &["message", "m"])
        .or_else(|| find_value_for_keys(incoming, &["message", "m"], 0))
}

fn extract_notify_incoming_valid_message(value: &Value) -> Option<&Value> {
    let message = extract_notify_incoming_ui_message(value)?;
    if let Some(valid) = map_get_any(message, &["valid", "v"]) {
        return Some(valid);
    }
    let state = map_get_any(message, &["state", "s"])
        .and_then(as_i64)
        .or_else(|| find_value_for_keys(message, &["state", "s"], 0).and_then(as_i64));
    if state != Some(0) {
        return None;
    }
    find_value_for_keys(message, &["valid", "v"], 0)
}

fn parse_live_message_from_notify(event: &KeybaseNotifyEvent) -> Option<MessageRecord> {
    if event.method_name() != "chat.1.NotifyChat.NewChatActivity" {
        return None;
    }

    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };

    let incoming_message = extract_notify_incoming_message_root(raw_params)?;
    let preferred_root = extract_notify_incoming_valid_message(raw_params)?;
    let conversation_root = incoming_message;
    let conv_keys = &[
        "convID",
        "convId",
        "conversationID",
        "conversationId",
        "conversation_id",
    ];
    let conversation_bytes =
        find_conversation_id_bytes_for_keys(conversation_root, conv_keys, 0)
            .or_else(|| find_conversation_id_bytes_for_keys(preferred_root, conv_keys, 0))?;
    let conversation_id =
        ConversationId::new(format!("kb_conv:{}", hex_encode(&conversation_bytes)));
    let message_id = extract_live_message_id_from_valid_message(preferred_root)?;
    let message_id_num = message_id.0.parse::<i64>().ok();
    let message_body = map_get_any(preferred_root, &["messageBody", "b"])
        .or_else(|| find_value_for_keys(preferred_root, &["messageBody", "content"], 0));
    let reply_to = extract_reply_to_message_id(preferred_root, message_body)
        .or_else(|| extract_reply_to_message_id(conversation_root, message_body));
    let thread_reply_count = extract_reply_children_count(preferred_root)
        .max(extract_reply_children_count(conversation_root));
    let mut attachments = message_body
        .map(extract_message_attachments)
        .unwrap_or_default();
    apply_message_attachment_url_hints(preferred_root, &mut attachments);
    apply_message_attachment_url_hints(conversation_root, &mut attachments);
    let message_type = message_body.and_then(message_type_from_message_body);
    if is_unset_message_type_only(message_body) {
        return None;
    }
    if message_type == Some(MESSAGE_TYPE_REACTION) || message_type == Some(MESSAGE_TYPE_UNFURL) {
        return None;
    }
    let event = extract_chat_event(message_body, preferred_root);
    let text_body = message_body
        .and_then(extract_text_body)
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty());
    let mut link_previews =
        extract_link_previews(preferred_root, message_body, text_body.as_deref());
    if link_previews.is_empty() {
        link_previews =
            extract_link_previews(conversation_root, message_body, text_body.as_deref());
    }
    if text_body.is_none() && attachments.is_empty() && is_unfurl_only_message(message_body) {
        return None;
    }
    let has_unfurl_payload = is_unfurl_only_message(message_body)
        || root_contains_unfurls(preferred_root)
        || root_contains_unfurls(conversation_root);
    if attachments.is_empty()
        && text_body
            .as_deref()
            .is_some_and(is_link_preview_placeholder_text)
        && has_unfurl_payload
    {
        return None;
    }
    let body = text_body
        .clone()
        .or_else(|| {
            attachments
                .is_empty()
                .then(|| message_body.and_then(extract_non_text_fallback_body))
                .flatten()
        })
        .or_else(|| {
            (!attachments.is_empty())
                .then(|| message_body.and_then(extract_attachment_caption))
                .flatten()
        })
        .or_else(|| find_body_string(preferred_root, 0).map(|text| text.trim().to_string()))
        .or_else(|| find_body_string(conversation_root, 0).map(|text| text.trim().to_string()))
        .filter(|text| !text.is_empty());
    let body = body.unwrap_or_else(|| default_body_for_message(message_body, &attachments));
    if event.is_none()
        && attachments.is_empty()
        && body.trim() == NON_TEXT_PLACEHOLDER_BODY
        && let Some(message_id_num) = message_id_num
    {
        log_non_text_placeholder_once(&conversation_id, message_id_num, message_type, message_body);
    }
    if event.is_none()
        && attachments.is_empty()
        && link_previews.is_empty()
        && body.trim() == NON_TEXT_PLACEHOLDER_BODY
    {
        return None;
    }
    let decorated_body = extract_decorated_text_body(preferred_root)
        .or_else(|| extract_decorated_text_body(conversation_root))
        .or_else(|| message_body.and_then(extract_decorated_text_body));
    let mention_metadata = parse_mention_metadata(preferred_root, message_body);
    let mut emoji_source_refs = parse_emoji_source_refs(preferred_root, message_body);
    merge_emoji_source_refs(
        &mut emoji_source_refs,
        parse_emoji_source_refs(conversation_root, message_body),
    );
    merge_emoji_source_refs(
        &mut emoji_source_refs,
        parse_emoji_source_refs(raw_params, message_body),
    );
    let fragments = fragments_from_message_body(
        &body,
        decorated_body.as_deref(),
        Some(&mention_metadata),
        emoji_source_refs,
    );
    let author = map_get_any(preferred_root, &["senderUsername", "su"])
        .and_then(as_str)
        .map(str::to_string)
        .or_else(|| find_sender_username(preferred_root))
        .or_else(|| find_sender_username(conversation_root))
        .unwrap_or_else(|| "unknown".to_string());
    let timestamp_ms = extract_message_timestamp_ms(preferred_root)
        .or_else(|| extract_message_timestamp_ms(conversation_root));
    let server_header_live =
        map_get_any(preferred_root, &["serverHeader", "s"]).unwrap_or(&Value::Nil);
    let (record_id, edited) = if message_type == Some(MESSAGE_TYPE_EDIT) {
        if let Some(target_id) = message_body.and_then(extract_edit_target_message_id) {
            (
                target_id,
                Some(EditMeta {
                    edit_id: message_id.clone(),
                    edited_at_ms: timestamp_ms,
                }),
            )
        } else {
            (message_id.clone(), None)
        }
    } else {
        let edited = if is_message_superseded(server_header_live, preferred_root)
            || is_message_superseded(server_header_live, conversation_root)
        {
            let superseded_by =
                map_get_any(server_header_live, &["supersededBy", "superseded_by", "sb"])
                    .and_then(as_i64)
                    .filter(|&id| id > 0)
                    .or_else(|| {
                        map_get_any(preferred_root, &["supersedes"])
                            .and_then(as_i64)
                            .filter(|&id| id > 0)
                    })
                    .or_else(|| {
                        map_get_any(conversation_root, &["supersedes"])
                            .and_then(as_i64)
                            .filter(|&id| id > 0)
                    })
                    .map(|id| MessageId::new(id.to_string()))
                    .unwrap_or_else(|| message_id.clone());
            Some(EditMeta {
                edit_id: superseded_by,
                edited_at_ms: None,
            })
        } else {
            None
        };
        (message_id.clone(), edited)
    };

    Some(MessageRecord {
        id: record_id,
        conversation_id,
        author_id: UserId::new(author),
        reply_to,
        thread_root_id: None,
        timestamp_ms,
        event,
        link_previews,
        permalink: message_id_num
            .and_then(|message_id_num| {
                build_keybase_permalink(preferred_root, message_id_num)
                    .or_else(|| build_keybase_permalink(conversation_root, message_id_num))
            })
            .unwrap_or_default(),
        fragments,
        source_text: Some(body),
        attachments,
        reactions: Vec::new(),
        thread_reply_count,
        send_state: MessageSendState::Sent,
        edited,
    })
}

fn parse_live_reaction_delta_from_notify(
    event: &KeybaseNotifyEvent,
) -> Option<MessageReactionDelta> {
    if event.method_name() != "chat.1.NotifyChat.NewChatActivity" {
        return None;
    }

    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };

    let incoming_message = extract_notify_incoming_message_root(raw_params);
    let preferred_root = extract_notify_incoming_valid_message(raw_params).unwrap_or(raw_params);
    let conversation_root = incoming_message.unwrap_or(raw_params);
    let conversation_id = find_value_for_keys(
        conversation_root,
        &[
            "convID",
            "convId",
            "conversationID",
            "conversationId",
            "conversation_id",
        ],
        0,
    )
    .or_else(|| {
        find_value_for_keys(
            preferred_root,
            &[
                "convID",
                "convId",
                "conversationID",
                "conversationId",
                "conversation_id",
            ],
            0,
        )
    })
    .and_then(value_to_conversation_id_bytes)
    .map(|bytes| ConversationId::new(format!("kb_conv:{}", hex_encode(&bytes))))?;
    let message_body = map_get_any(preferred_root, &["messageBody", "b"])
        .or_else(|| find_value_for_keys(preferred_root, &["messageBody", "content"], 0))
        .or_else(|| find_value_for_keys(raw_params, &["messageBody", "content"], 0));
    let author = map_get_any(preferred_root, &["senderUsername", "su"])
        .and_then(as_str)
        .map(str::to_string)
        .or_else(|| find_sender_username(raw_params))
        .or_else(|| {
            find_value_for_keys(raw_params, &["username"], 0)
                .and_then(as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| "unknown".to_string());
    let op_message_id = extract_live_event_message_id(preferred_root)
        .or_else(|| extract_live_event_message_id(raw_params));
    parse_reaction_delta(
        &conversation_id,
        message_body,
        author,
        extract_message_timestamp_ms(preferred_root)
            .or_else(|| extract_message_timestamp_ms(raw_params)),
        op_message_id,
    )
}

fn extract_live_event_message_id(value: &Value) -> Option<MessageId> {
    find_value_for_keys(value, &["messageID", "messageId", "msgID", "msgId"], 0)
        .or_else(|| find_value_for_keys(value, &["id"], 0))
        .and_then(parse_message_id_from_value)
}

fn extract_live_message_id_from_valid_message(value: &Value) -> Option<MessageId> {
    map_get_any(value, &["serverHeader", "s"])
        .and_then(|header| map_get_any(header, &["messageID", "messageId", "m"]))
        .and_then(parse_message_id_from_value)
        .or_else(|| {
            map_get_any(value, &["messageID", "messageId", "msgID", "msgId", "id"])
                .and_then(parse_message_id_from_value)
        })
}

/// The banner Keybase wants for an incoming message. The service has already
/// applied the conversation's notification settings and set
/// `displayDesktopNotification`; reactions open the message reacted to.
fn parse_desktop_notification_from_notify(
    event: &KeybaseNotifyEvent,
) -> Option<DesktopNotification> {
    if event.method_name() != "chat.1.NotifyChat.NewChatActivity" {
        return None;
    }
    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    let incoming = extract_notify_incoming_message_root(raw_params)?;
    if map_get(incoming, "displayDesktopNotification").and_then(as_bool) != Some(true) {
        return None;
    }
    let snippet = map_get(incoming, "desktopNotificationSnippet")
        .and_then(as_str)
        .map(str::trim)
        .filter(|snippet| !snippet.is_empty())?;
    let valid = extract_notify_incoming_valid_message(raw_params)?;
    let conversation_bytes =
        find_conversation_id_bytes_for_keys(incoming, &["convID", "convId"], 0)?;
    let message_id = parse_live_reaction_delta_from_notify(event)
        .map(|reaction| reaction.target_message_id)
        .or_else(|| extract_live_message_id_from_valid_message(valid))?;
    let author = map_get_any(valid, &["senderUsername", "su"])
        .and_then(as_str)
        .unwrap_or_default()
        .to_string();
    Some(DesktopNotification {
        conversation_id: ConversationId::new(format!(
            "kb_conv:{}",
            hex_encode(&conversation_bytes)
        )),
        message_id,
        author,
        snippet: snippet.to_string(),
        conversation: extract_conv_from_notify(event)
            .and_then(|conv| parse_inbox_ui_item(conv, None))
            .map(|conversation| conversation.summary),
    })
}

const NOTIFY_SOUND_PATH: &str = "notifySound";

/// Keybase's "Play a sound for new messages" setting: off when never set,
/// `None` when it could not be read, so a failed read keeps the last value.
async fn fetch_notify_sound(client: &mut KeybaseRpcClient) -> Option<bool> {
    let args = vec![Value::Map(vec![(
        Value::from("path"),
        Value::from(NOTIFY_SOUND_PATH),
    )])];
    let value = client
        .call("keybase.1.config.guiGetValue", args)
        .await
        .ok()?;
    Some(map_get(&value, "b").and_then(as_bool).unwrap_or(false))
}

/// Saves the sound setting where the Keybase app keeps it. Each app reads
/// it when it starts or reconnects.
async fn save_notify_sound(client: &mut KeybaseRpcClient, enabled: bool) -> io::Result<()> {
    client
        .call(
            "keybase.1.config.guiSetValue",
            vec![Value::Map(vec![
                (Value::from("path"), Value::from(NOTIFY_SOUND_PATH)),
                (
                    Value::from("value"),
                    Value::Map(vec![
                        (Value::from("isNull"), Value::from(false)),
                        (Value::from("b"), Value::from(enabled)),
                    ]),
                ),
            ])],
        )
        .await
        .map(|_| ())
}

/// One-off commands that answer before `execute` returns, so they finish in
/// the order the UI sent them. Bounded like the listener's own setup.
const ONE_OFF_RPC_TIMEOUT: Duration = Duration::from_secs(15);

fn with_keybase_client<T>(
    call: impl AsyncFnOnce(&mut KeybaseRpcClient) -> io::Result<T>,
) -> Result<T, BackendError> {
    let path = socket_path()
        .ok_or_else(|| BackendError::OperationFailed("Keybase connection is unavailable".into()))?;
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let result = runtime.block_on(async move {
        tokio::time::timeout(ONE_OFF_RPC_TIMEOUT, async move {
            let transport = FramedMsgpackTransport::connect(&path).await?;
            let mut client = KeybaseRpcClient::new(transport);
            call(&mut client).await
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Keybase did not answer"))?
    })?;
    Ok(result)
}

fn parse_live_reaction_map_deltas_from_notify(
    event: &KeybaseNotifyEvent,
    conversation_id: &ConversationId,
    message_id: &MessageId,
) -> Vec<MessageReactionDelta> {
    if event.method_name() != "chat.1.NotifyChat.NewChatActivity" {
        return Vec::new();
    }
    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    let preferred_root = extract_notify_incoming_valid_message(raw_params).unwrap_or(raw_params);
    parse_reaction_deltas_for_target(
        preferred_root,
        conversation_id,
        message_id,
        extract_message_timestamp_ms(preferred_root)
            .or_else(|| extract_message_timestamp_ms(raw_params)),
    )
}

fn parse_reaction_delta_from_thread(
    valid: &Value,
    conversation_id: &ConversationId,
) -> Option<MessageReactionDelta> {
    let op_message_id = map_get_any(valid, &["serverHeader", "s"])
        .and_then(|header| map_get_any(header, &["messageID", "messageId", "m"]))
        .and_then(as_i64)
        .map(|id| MessageId::new(id.to_string()));
    let message_body = map_get_any(valid, &["messageBody", "b"]);
    let author = map_get_any(valid, &["senderUsername", "su"])
        .and_then(as_str)
        .unwrap_or("unknown")
        .to_string();
    let timestamp_ms = map_get_any(valid, &["serverHeader", "s"])
        .and_then(extract_message_timestamp_ms)
        .or_else(|| extract_message_timestamp_ms(valid));
    parse_reaction_delta(
        conversation_id,
        message_body,
        author,
        timestamp_ms,
        op_message_id,
    )
}

fn parse_reaction_deltas_for_target(
    root: &Value,
    conversation_id: &ConversationId,
    target_message_id: &MessageId,
    fallback_updated_ms: Option<i64>,
) -> Vec<MessageReactionDelta> {
    let reaction_container = map_get_any(root, &["reactions", "r"])
        .or_else(|| find_value_for_keys(root, &["reactions"], 0));
    let Some(reaction_container) = reaction_container else {
        return Vec::new();
    };
    let reaction_entries =
        map_get_any(reaction_container, &["reactions", "r"]).unwrap_or(reaction_container);
    let Value::Map(emoji_entries) = reaction_entries else {
        return Vec::new();
    };

    let mut deltas = Vec::new();
    for (emoji_key, users_value) in emoji_entries {
        let Some(emoji) = emoji_key
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Value::Map(user_entries) = users_value else {
            continue;
        };

        for (user_key, reaction_value) in user_entries {
            let Some(username) = user_key
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let updated_ms = map_get_any(reaction_value, &["ctime", "c"])
                .and_then(as_i64)
                .and_then(normalize_epoch_ms)
                .or(fallback_updated_ms)
                .unwrap_or_else(now_unix_ms);
            let op_message_id =
                map_get_any(reaction_value, &["reactionMsgID", "reactionMsgId", "m"])
                    .and_then(as_i64)
                    .map(|id| MessageId::new(id.to_string()));

            deltas.push(MessageReactionDelta {
                conversation_id: conversation_id.clone(),
                target_message_id: target_message_id.clone(),
                op_message_id,
                emoji: emoji.to_string(),
                source_ref: parse_reaction_source_ref(reaction_value, emoji),
                actor_id: UserId::new(username.to_string()),
                updated_ms,
            });
        }
    }

    deltas.sort_by(|left, right| {
        left.emoji
            .cmp(&right.emoji)
            .then_with(|| left.actor_id.0.cmp(&right.actor_id.0))
            .then_with(|| right.source_ref.is_some().cmp(&left.source_ref.is_some()))
    });
    deltas.dedup_by(|left, right| {
        left.emoji.eq_ignore_ascii_case(&right.emoji) && left.actor_id == right.actor_id
    });
    deltas
}

fn parse_reaction_delta(
    conversation_id: &ConversationId,
    message_body: Option<&Value>,
    author: String,
    timestamp_ms: Option<i64>,
    op_message_id: Option<MessageId>,
) -> Option<MessageReactionDelta> {
    let reaction = message_body.and_then(extract_reaction_payload)?;
    let target_message_id =
        map_get_any(reaction, &["messageID", "messageId", "m"]).and_then(as_i64)?;
    let emoji = map_get_any(reaction, &["body", "b"])
        .and_then(as_str)
        .map(str::trim)
        .unwrap_or("")
        .to_string();
    if emoji.is_empty() {
        return None;
    }
    let source_ref = parse_reaction_source_ref(reaction, &emoji);

    Some(MessageReactionDelta {
        conversation_id: conversation_id.clone(),
        target_message_id: MessageId::new(target_message_id.to_string()),
        op_message_id,
        emoji,
        source_ref,
        actor_id: UserId::new(author),
        updated_ms: timestamp_ms.unwrap_or_else(now_unix_ms),
    })
}

fn parse_reaction_source_ref(payload: &Value, emoji: &str) -> Option<EmojiSourceRef> {
    let normalized_target = normalize_shortcode_alias(emoji);
    let entries = map_get_any(payload, &["emojis", "emoji", "e"])?;
    let Value::Map(emoji_entries) = entries else {
        return None;
    };

    let mut fallback_source = None;
    for (alias_key, entry) in emoji_entries {
        let alias = as_str(alias_key)
            .and_then(normalize_shortcode_alias)
            .or_else(|| {
                map_get_any(entry, &["alias", "a"])
                    .and_then(as_str)
                    .and_then(normalize_shortcode_alias)
            });

        let source = map_get_any(entry, &["source", "s"])
            .and_then(parse_reaction_source_message_ref)
            .or_else(|| parse_reaction_source_message_ref(entry));
        if source.is_none() {
            continue;
        }

        if fallback_source.is_none() {
            fallback_source = source.clone();
        }

        if alias.is_some() && alias == normalized_target {
            return source;
        }
    }
    fallback_source
}

fn parse_reaction_source_message_ref(value: &Value) -> Option<EmojiSourceRef> {
    if let Some(message) = map_get_any(value, &["message", "m"])
        && let Some(source_ref) = emoji_source_ref_from_metadata_entry(message)
    {
        return Some(source_ref);
    }
    emoji_source_ref_from_metadata_entry(value)
}

fn normalize_shortcode_alias(value: &str) -> Option<String> {
    let normalized = value.trim().trim_matches(':').trim();
    if normalized.is_empty() {
        return None;
    }
    Some(normalized.to_ascii_lowercase())
}

fn extract_reaction_payload(message_body: &Value) -> Option<&Value> {
    if let Some(message_type) = message_type_from_message_body(message_body)
        && message_type != MESSAGE_TYPE_REACTION
    {
        return None;
    }

    fn payload_record_candidate(value: &Value) -> Option<&Value> {
        let has_message_id = map_get_any(value, &["messageID", "messageId", "m"])
            .and_then(as_i64)
            .is_some();
        let has_reaction_body = map_get_any(value, &["body", "b"])
            .and_then(as_str)
            .is_some();
        if has_message_id && has_reaction_body {
            Some(value)
        } else {
            None
        }
    }

    fn payload_from_container(value: &Value) -> Option<&Value> {
        if let Some(payload) = message_variant_payload(value, MESSAGE_TYPE_REACTION) {
            return Some(payload);
        }
        if let Some(payload) = payload_record_candidate(value) {
            return Some(payload);
        }
        let Value::Map(entries) = value else {
            return None;
        };
        for (_, inner) in entries {
            if let Some(payload) = map_get_any(inner, &["reaction", "r"]) {
                return Some(payload);
            }
            if let Some(payload) = payload_record_candidate(inner) {
                return Some(payload);
            }
        }
        None
    }

    if let Some(payload) = payload_from_container(message_body) {
        return Some(payload);
    }
    map_get_any(message_body, &["body", "b"]).and_then(payload_from_container)
}

fn message_type_from_message_body(message_body: &Value) -> Option<i64> {
    if let Some(message_type) =
        map_get_any(message_body, &["messageType", "mt", "t"]).and_then(as_i64)
    {
        if message_type == 0 {
            return None;
        }
        return Some(message_type);
    }
    if let Some(message_type_name) = map_get_any(message_body, &["type"]).and_then(as_str)
        && let Some(message_type) = message_type_from_name(message_type_name)
    {
        return Some(message_type);
    }
    if let Some(body) = map_get_any(message_body, &["body", "b"]) {
        if let Some(message_type) = map_get_any(body, &["messageType", "mt", "t"]).and_then(as_i64)
        {
            return Some(message_type);
        }
        if let Some(message_type) = single_numeric_variant_key(body) {
            return Some(message_type);
        }
        if map_get_any(body, &["pin"]).is_some() {
            return Some(MESSAGE_TYPE_PIN);
        }
    }

    if map_get_any(message_body, &["reaction", "r"]).is_some() {
        return Some(MESSAGE_TYPE_REACTION);
    }
    if map_get_any(message_body, &["text", "t"]).is_some() {
        return Some(MESSAGE_TYPE_TEXT);
    }
    if map_get_any(message_body, &["attachment", "a"]).is_some() {
        return Some(MESSAGE_TYPE_ATTACHMENT);
    }
    if map_get_any(message_body, &["unfurl", "u"]).is_some() {
        return Some(MESSAGE_TYPE_UNFURL);
    }
    if map_get_any(message_body, &["edit", "e"]).is_some() {
        return Some(MESSAGE_TYPE_EDIT);
    }
    if map_get_any(message_body, &["delete", "d"]).is_some() {
        return Some(MESSAGE_TYPE_DELETE);
    }
    if map_get_any(message_body, &["deletehistory", "dh"]).is_some() {
        return Some(MESSAGE_TYPE_DELETE_HISTORY);
    }
    if map_get_any(message_body, &["join", "j"]).is_some() {
        return Some(MESSAGE_TYPE_JOIN);
    }
    if map_get_any(message_body, &["leave", "l"]).is_some() {
        return Some(MESSAGE_TYPE_LEAVE);
    }
    if map_get_any(message_body, &["system", "s"]).is_some() {
        return Some(MESSAGE_TYPE_SYSTEM);
    }
    if map_get_any(message_body, &["pin"]).is_some() {
        return Some(MESSAGE_TYPE_PIN);
    }

    map_get_any(message_body, &["body", "b"]).and_then(single_numeric_variant_key)
}

fn message_type_from_name(name: &str) -> Option<i64> {
    match name.trim().to_ascii_lowercase().as_str() {
        "text" => Some(MESSAGE_TYPE_TEXT),
        "attachment" => Some(MESSAGE_TYPE_ATTACHMENT),
        "edit" => Some(MESSAGE_TYPE_EDIT),
        "delete" => Some(MESSAGE_TYPE_DELETE),
        "metadata" => Some(MESSAGE_TYPE_METADATA),
        "headline" => Some(MESSAGE_TYPE_HEADLINE),
        "attachmentuploaded" | "attachment_uploaded" => Some(MESSAGE_TYPE_ATTACHMENT_UPLOADED),
        "join" => Some(MESSAGE_TYPE_JOIN),
        "leave" => Some(MESSAGE_TYPE_LEAVE),
        "system" => Some(MESSAGE_TYPE_SYSTEM),
        "deletehistory" | "delete_history" => Some(MESSAGE_TYPE_DELETE_HISTORY),
        "reaction" => Some(MESSAGE_TYPE_REACTION),
        "sendpayment" | "send_payment" => Some(MESSAGE_TYPE_SEND_PAYMENT),
        "requestpayment" | "request_payment" => Some(MESSAGE_TYPE_REQUEST_PAYMENT),
        "unfurl" => Some(MESSAGE_TYPE_UNFURL),
        "flip" => Some(MESSAGE_TYPE_FLIP),
        "pin" => Some(MESSAGE_TYPE_PIN),
        _ => None,
    }
}

fn message_variant_payload(message_body: &Value, message_type: i64) -> Option<&Value> {
    fn variant_payload_from_type_key(value: &Value, message_type: i64) -> Option<&Value> {
        let Value::Map(entries) = value else {
            return None;
        };
        entries.iter().find_map(|(key, value)| {
            value_to_i64_key(key)
                .filter(|variant| *variant == message_type)
                .map(|_| value)
        })
    }

    if let Some(variant_keys) = message_variant_keys(message_type)
        && let Some(payload) = map_get_any(message_body, variant_keys)
    {
        return Some(payload);
    }
    if let Some(payload) = variant_payload_from_type_key(message_body, message_type) {
        return Some(payload);
    }

    let body = map_get_any(message_body, &["body", "b"])?;
    if let Some(variant_keys) = message_variant_keys(message_type)
        && let Some(payload) = map_get_any(body, variant_keys)
    {
        return Some(payload);
    }
    variant_payload_from_type_key(body, message_type)
}

fn message_variant_keys(message_type: i64) -> Option<&'static [&'static str]> {
    match message_type {
        MESSAGE_TYPE_TEXT => Some(&["text", "t"]),
        MESSAGE_TYPE_ATTACHMENT => Some(&["attachment", "a"]),
        MESSAGE_TYPE_EDIT => Some(&["edit", "e"]),
        MESSAGE_TYPE_DELETE => Some(&["delete", "d"]),
        MESSAGE_TYPE_METADATA => Some(&["metadata"]),
        MESSAGE_TYPE_HEADLINE => Some(&["headline", "h"]),
        MESSAGE_TYPE_ATTACHMENT_UPLOADED => {
            Some(&["attachmentuploaded", "attachmentUploaded", "au"])
        }
        MESSAGE_TYPE_JOIN => Some(&["join", "j"]),
        MESSAGE_TYPE_LEAVE => Some(&["leave", "l"]),
        MESSAGE_TYPE_SYSTEM => Some(&["system", "s"]),
        MESSAGE_TYPE_DELETE_HISTORY => Some(&["deletehistory", "dh"]),
        MESSAGE_TYPE_REACTION => Some(&["reaction", "r"]),
        MESSAGE_TYPE_SEND_PAYMENT => Some(&["sendpayment"]),
        MESSAGE_TYPE_REQUEST_PAYMENT => Some(&["requestpayment"]),
        MESSAGE_TYPE_UNFURL => Some(&["unfurl", "u"]),
        MESSAGE_TYPE_FLIP => Some(&["flip"]),
        MESSAGE_TYPE_PIN => Some(&["pin"]),
        _ => None,
    }
}

fn single_numeric_variant_key(value: &Value) -> Option<i64> {
    let Value::Map(entries) = value else {
        return None;
    };
    let mut variant: Option<i64> = None;
    for (key, _) in entries {
        let Some(key_value) = value_to_i64_key(key) else {
            continue;
        };
        if let Some(existing) = variant {
            if existing != key_value {
                return None;
            }
        } else {
            variant = Some(key_value);
        }
    }
    variant
}

fn value_to_i64_key(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok()))
}

fn find_sender_username(value: &Value) -> Option<String> {
    if let Some(username) = find_value_for_keys(value, &["senderUsername"], 0).and_then(as_str) {
        return Some(username.to_string());
    }
    let sender = find_value_for_keys(value, &["sender"], 0)?;
    map_get_any(sender, &["username", "n"])
        .and_then(as_str)
        .map(str::to_string)
}

struct AttachmentUploadProgressNotify {
    outbox_id: String,
    sent_bytes: u64,
    total_bytes: u64,
}

/// `ChatAttachmentUploadProgress {outboxID, bytesComplete, bytesTotal}` for an
/// attachment this client is sending.
fn parse_attachment_upload_progress(
    event: &KeybaseNotifyEvent,
) -> Option<AttachmentUploadProgressNotify> {
    if event.method_name() != "chat.1.NotifyChat.ChatAttachmentUploadProgress" {
        return None;
    }
    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    let bytes = |keys: &[&str]| {
        find_value_for_keys(raw_params, keys, 0)
            .and_then(as_i64)
            .and_then(|raw| u64::try_from(raw).ok())
    };
    Some(AttachmentUploadProgressNotify {
        outbox_id: find_value_for_keys(raw_params, &["outboxID", "outboxId", "outbox_id"], 0)
            .and_then(outbox_id_to_string)?,
        sent_bytes: bytes(&["bytesComplete", "bytes_complete"])?,
        total_bytes: bytes(&["bytesTotal", "bytes_total"])?,
    })
}

fn extract_outbox_id_from_notify(event: &KeybaseNotifyEvent) -> Option<String> {
    if event.method_name() != "chat.1.NotifyChat.NewChatActivity" {
        return None;
    }
    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    find_value_for_keys(raw_params, &["outboxID", "outboxId", "outbox_id"], 0)
        .and_then(outbox_id_to_string)
}

fn find_value_for_keys<'a>(value: &'a Value, keys: &[&str], depth: usize) -> Option<&'a Value> {
    if depth > 8 {
        return None;
    }
    match value {
        Value::Map(entries) => {
            for (key, inner) in entries {
                if let Some(key_name) = key.as_str()
                    && keys.contains(&key_name)
                {
                    return Some(inner);
                }
            }
            for (_, inner) in entries {
                if let Some(found) = find_value_for_keys(inner, keys, depth + 1) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(values) => values
            .iter()
            .find_map(|inner| find_value_for_keys(inner, keys, depth + 1)),
        _ => None,
    }
}

fn find_conversation_id_bytes_for_keys(
    value: &Value,
    keys: &[&str],
    depth: usize,
) -> Option<Vec<u8>> {
    if depth > 8 {
        return None;
    }

    match value {
        Value::Map(entries) => {
            // Prefer the first key match that actually decodes; do not return a non-decodable
            // value, since some Keybase payloads reuse "conversationID" for nested objects.
            for (key, inner) in entries {
                if let Some(key_name) = key.as_str()
                    && keys.contains(&key_name)
                    && let Some(bytes) = value_to_conversation_id_bytes(inner)
                {
                    return Some(bytes);
                }
            }
            for (_, inner) in entries {
                if let Some(bytes) = find_conversation_id_bytes_for_keys(inner, keys, depth + 1) {
                    return Some(bytes);
                }
            }
            None
        }
        Value::Array(values) => values
            .iter()
            .find_map(|inner| find_conversation_id_bytes_for_keys(inner, keys, depth + 1)),
        _ => None,
    }
}

fn collect_i64_values_for_keys(value: &Value, keys: &[&str], depth: usize, output: &mut Vec<i64>) {
    if depth > 8 {
        return;
    }
    match value {
        Value::Map(entries) => {
            for (key, inner) in entries {
                if let Some(key_name) = key.as_str()
                    && keys.contains(&key_name)
                    && let Some(raw) = as_i64(inner)
                {
                    output.push(raw);
                }
            }
            for (_, inner) in entries {
                collect_i64_values_for_keys(inner, keys, depth + 1, output);
            }
        }
        Value::Array(values) => {
            for inner in values {
                collect_i64_values_for_keys(inner, keys, depth + 1, output);
            }
        }
        _ => {}
    }
}

fn parse_typing_updates_from_notify(
    event: &KeybaseNotifyEvent,
) -> Vec<(ConversationId, Vec<UserId>)> {
    if event.method_name() != "chat.1.NotifyChat.ChatTypingUpdate" {
        return Vec::new();
    }

    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };

    let typing_updates = find_value_for_keys(raw_params, &["typingUpdates"], 0)
        .and_then(as_array)
        .cloned()
        .unwrap_or_default();

    let mut result = Vec::new();
    for update in typing_updates {
        let conv_id_bytes = find_value_for_keys(
            &update,
            &[
                "convID",
                "convId",
                "conversationID",
                "conversationId",
                "conversation_id",
            ],
            0,
        )
        .and_then(value_to_conversation_id_bytes);
        let Some(conv_id_bytes) = conv_id_bytes else {
            continue;
        };
        let conversation_id =
            ConversationId::new(format!("kb_conv:{}", hex_encode(&conv_id_bytes)));

        let typers = find_value_for_keys(&update, &["typers"], 0)
            .and_then(as_array)
            .cloned()
            .unwrap_or_default();
        let mut users = Vec::new();
        for typer in typers {
            if let Some(username) =
                find_value_for_keys(&typer, &["username", "u"], 0).and_then(as_str)
            {
                let username = username.trim();
                if !username.is_empty() {
                    users.push(UserId::new(username.to_string()));
                }
            }
        }
        users.sort_by(|a, b| a.0.cmp(&b.0));
        users.dedup_by(|a, b| a.0 == b.0);
        result.push((conversation_id, users));
    }

    result
}

fn notify_should_refresh_inbox(event: &KeybaseNotifyEvent) -> bool {
    matches!(
        event.method_name(),
        "chat.1.NotifyChat.ChatInboxStale"
            | "chat.1.NotifyChat.ChatInboxSynced"
            | "chat.1.NotifyChat.ChatJoinedConversation"
    )
}

fn parse_badge_state(value: &Value) -> Option<HashMap<ConversationId, ConversationBadgeCounts>> {
    let conversations = map_get(value, "conversations").and_then(as_array)?;
    conversations
        .iter()
        .map(|conversation| {
            let id = map_get(conversation, "convID").and_then(value_to_conversation_id_bytes)?;
            if id.is_empty() {
                return None;
            }
            Some((
                ConversationId::new(format!("kb_conv:{}", hex_encode(&id))),
                ConversationBadgeCounts {
                    unread_count: u32::try_from(
                        map_get(conversation, "unreadMessages").and_then(as_i64)?,
                    )
                    .ok()?,
                    badge_count: u32::try_from(
                        map_get(conversation, "badgeCount").and_then(as_i64)?,
                    )
                    .ok()?,
                },
            ))
        })
        .collect()
}

fn emit_badge_state(
    sender: &Sender<BackendEvent>,
    local_store: &LocalStore,
    counts: HashMap<ConversationId, ConversationBadgeCounts>,
    complete: bool,
) -> io::Result<()> {
    local_store.update_conversation_badges(counts.clone(), complete)?;
    tracing::debug!(
        conversations = counts.len(),
        unread_conversations = counts.values().filter(|c| c.unread_count > 0).count(),
        complete,
        "keybase.badges.applied"
    );
    sender
        .send(BackendEvent::ConversationBadgesUpdated {
            backend_id: BackendId::new(KEYBASE_BACKEND_ID),
            counts,
            complete,
        })
        .map_err(|_| io::Error::other("UI closed"))
}

fn parse_read_marker_update_from_notify(
    event: &KeybaseNotifyEvent,
) -> Option<ReadMarkerNotifyUpdate> {
    if event.method_name() != "chat.1.NotifyChat.NewChatActivity" {
        return None;
    }
    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    let activity = find_value_for_keys(raw_params, &["activity", "a"], 0).unwrap_or(raw_params);
    let activity_type = map_get_any(activity, &["activityType", "t"])
        .and_then(as_i64)
        .or_else(|| find_value_for_keys(activity, &["activityType", "t"], 0).and_then(as_i64));
    if activity_type != Some(2) {
        return None;
    }
    let read_message = map_get_any(activity, &["readMessage", "rm", "read", "r"])
        .or_else(|| find_value_for_keys(activity, &["readMessage", "rm", "read"], 0))
        .unwrap_or(activity);
    let conversation_bytes = find_value_for_keys(
        read_message,
        &[
            "convID",
            "convId",
            "conversationID",
            "conversationId",
            "conversation_id",
        ],
        0,
    )
    .and_then(value_to_conversation_id_bytes)?;
    let read_upto = find_value_for_keys(
        read_message,
        &["msgID", "msgId", "messageID", "messageId", "m"],
        0,
    )
    .and_then(as_i64)
    .filter(|value| *value > 0)
    .map(|value| MessageId::new(value.to_string()))?;
    let conversation_id =
        ConversationId::new(format!("kb_conv:{}", hex_encode(&conversation_bytes)));
    let mut snapshot = map_get_any(read_message, &["conv", "conversation", "c"])
        .and_then(parse_conversation_unread_snapshot);
    if let Some(inner) = snapshot.as_mut() {
        inner.read_upto = Some(read_upto.clone());
        inner.conversation_id = conversation_id.clone();
    }
    Some(ReadMarkerNotifyUpdate {
        conversation_id,
        read_upto,
        snapshot,
    })
}

fn parse_conversation_unread_snapshot_from_notify(
    event: &KeybaseNotifyEvent,
) -> Option<ConversationUnreadSnapshot> {
    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    find_value_for_keys(raw_params, &["conv", "conversation"], 0)
        .and_then(parse_conversation_unread_snapshot)
}

fn parse_conversation_unread_snapshot(value: &Value) -> Option<ConversationUnreadSnapshot> {
    let conversation_bytes = find_value_for_keys(
        value,
        &[
            "convID",
            "convId",
            "conversationID",
            "conversationId",
            "conversation_id",
            "id",
            "i",
        ],
        0,
    )
    .and_then(value_to_conversation_id_bytes)?;
    let conversation_id =
        ConversationId::new(format!("kb_conv:{}", hex_encode(&conversation_bytes)));
    let reader_info = map_get_any(value, &["readerInfo", "ri", "r"])
        .cloned()
        .unwrap_or(Value::Nil);
    let unread_count = conversation_unread_count(value, &reader_info);
    let mention_count = conversation_mention_count(value, &reader_info);
    let read_upto = map_get_any(&reader_info, &["readMsgid", "readMsgID"])
        .and_then(as_i64)
        .or_else(|| {
            find_value_for_keys(value, &["readMsgid", "readMsgID", "msgID", "msgId"], 0)
                .and_then(as_i64)
        })
        .filter(|value| *value > 0)
        .map(|value| MessageId::new(value.to_string()));
    let activity_time = map_get_any(&reader_info, &["mtime"])
        .and_then(as_i64)
        .or_else(|| {
            find_value_for_keys(
                value,
                &["activeAtMs", "active_at_ms", "activeAt", "active_at"],
                0,
            )
            .and_then(as_i64)
        });
    Some(ConversationUnreadSnapshot {
        conversation_id,
        unread_count,
        mention_count,
        read_upto,
        activity_time,
    })
}

fn emit_unread_snapshot_event(
    sender: &Sender<BackendEvent>,
    local_store: &LocalStore,
    snapshot: &ConversationUnreadSnapshot,
) -> bool {
    persist_unread_snapshot(local_store, snapshot);
    sender
        .send(BackendEvent::ConversationUnreadChanged {
            conversation_id: snapshot.conversation_id.clone(),
            unread_count: snapshot.unread_count,
            mention_count: snapshot.mention_count,
            read_upto: snapshot.read_upto.clone(),
        })
        .is_ok()
}

fn persist_unread_snapshot(local_store: &LocalStore, snapshot: &ConversationUnreadSnapshot) {
    let Ok(Some(mut summary)) = local_store.get_conversation(&snapshot.conversation_id) else {
        return;
    };
    summary.unread_count = snapshot.unread_count;
    summary.mention_count = snapshot.mention_count;
    let activity_time = snapshot.activity_time.unwrap_or(summary.last_activity_ms);
    let _ = local_store.persist_conversation(&summary, activity_time);
}

fn refresh_inbox_unread_state(sender: &Sender<BackendEvent>, local_store: &LocalStore) {
    let Some(path) = socket_path() else {
        send_internal(
            sender,
            "zbase.internal.unread_refresh_socket_missing",
            Value::Nil,
        );
        return;
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            send_internal(
                sender,
                "zbase.internal.unread_refresh_runtime_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };
    let left_snapshot = local_store.left_conversation_snapshot().unwrap_or_default();
    let refreshed = runtime.block_on(async {
        let transport = FramedMsgpackTransport::connect(&path).await?;
        let mut client = KeybaseRpcClient::new(transport);
        let status = fetch_current_status(&mut client).await.ok();
        let self_username = status.as_ref().and_then(status_username);
        let inbox = fetch_inbox_unboxed(&mut client).await?;
        Ok::<Vec<BootstrapConversation>, io::Error>(parse_inbox_conversations(
            &inbox,
            self_username.as_deref(),
            false,
        ))
    });
    let conversations = match refreshed {
        Ok(conversations) => conversations,
        Err(error) => {
            send_internal(
                sender,
                "zbase.internal.unread_refresh_failed",
                Value::from(error.to_string()),
            );
            return;
        }
    };
    if let Ok(joined) = local_store.reconcile_joined_conversations(
        &left_snapshot,
        conversations
            .iter()
            .map(|conversation| conversation.summary.id.clone()),
    ) {
        for conversation_id in joined {
            let _ = sender.send(BackendEvent::ChannelJoined {
                workspace_id: WorkspaceId::new(WORKSPACE_ID),
                conversation_id: conversation_id.clone(),
            });
            if let Some(event) =
                channel_membership::cached_join_event(local_store, &conversation_id)
            {
                let _ = sender.send(event);
            }
        }
    }
    let mut new_channels = Vec::new();
    let mut new_direct_messages = Vec::new();
    let mut new_bindings = Vec::new();

    for conversation in conversations {
        let conversation_id = conversation.summary.id.clone();
        let previous = local_store
            .get_conversation(&conversation_id)
            .ok()
            .flatten();
        let is_new = previous.is_none();
        let _ = local_store.persist_conversation(&conversation.summary, conversation.activity_time);
        if is_new {
            let _ = local_store
                .persist_conversation_binding(&conversation_id, &conversation.provider_ref);
            new_bindings.push(ConversationBinding {
                conversation_id: conversation_id.clone(),
                backend_id: BackendId::new(KEYBASE_BACKEND_ID),
                account_id: AccountId::new(DEFAULT_ACCOUNT_ID),
                provider_conversation_ref: conversation.provider_ref.clone(),
            });
        }
        let canonical = local_store
            .get_conversation(&conversation_id)
            .ok()
            .flatten()
            .unwrap_or_else(|| conversation.summary.clone());
        if previous.as_ref() != Some(&canonical) {
            if matches!(canonical.kind, ConversationKind::Channel) {
                new_channels.push(canonical);
            } else {
                new_direct_messages.push(canonical);
            }
        }
        if sender
            .send(BackendEvent::ConversationUnreadChanged {
                conversation_id: conversation_id.clone(),
                unread_count: conversation.summary.unread_count,
                mention_count: conversation.summary.mention_count,
                read_upto: conversation.read_marker.clone(),
            })
            .is_err()
        {
            return;
        }
        if sender
            .send(BackendEvent::ReadMarkerUpdated {
                conversation_id: conversation_id.clone(),
                read_upto: conversation.read_marker,
            })
            .is_err()
        {
            return;
        }
        if sender
            .send(BackendEvent::PinnedStateUpdated {
                conversation_id,
                pinned: conversation.pinned_state,
            })
            .is_err()
        {
            return;
        }
    }

    if !new_channels.is_empty() || !new_direct_messages.is_empty() {
        let _ = sender.send(BackendEvent::WorkspaceConversationsExtended {
            workspace_id: WorkspaceId::new(WORKSPACE_ID),
            channels: new_channels,
            direct_messages: new_direct_messages,
            conversation_bindings: new_bindings,
        });
    }
}

async fn bootstrap_payload_from_service(
    socket: &std::path::Path,
    backend_id: &BackendId,
    account_id: &AccountId,
) -> io::Result<BootstrapPayload> {
    let transport = FramedMsgpackTransport::connect(socket).await?;
    let mut client = KeybaseRpcClient::new(transport);

    let status = client
        .call(
            KEYBASE_GET_CURRENT_STATUS,
            vec![Value::Map(vec![(
                Value::from("sessionID"),
                Value::from(SESSION_ID),
            )])],
        )
        .await?;
    let logged_in = status_logged_in(&status).unwrap_or(true);
    let session_valid = status_session_valid(&status).unwrap_or(true);
    if !(logged_in && session_valid) {
        return Err(io::Error::other(
            "keybase is not logged in or session invalid",
        ));
    }
    let account_display_name = status_username(&status);

    let conversations_value = fetch_inbox_unboxed(&mut client).await?;
    let mut conversations =
        parse_inbox_conversations(&conversations_value, account_display_name.as_deref(), false);
    if conversations.is_empty() {
        return Ok(BootstrapPayload {
            workspace_ids: vec![WorkspaceId::new(WORKSPACE_ID)],
            active_workspace_id: Some(WorkspaceId::new(WORKSPACE_ID)),
            workspace_name: WORKSPACE_NAME.to_string(),
            workspace_bindings: vec![WorkspaceBinding {
                workspace_id: WorkspaceId::new(WORKSPACE_ID),
                backend_id: backend_id.clone(),
                account_id: account_id.clone(),
                provider_workspace_ref: ProviderWorkspaceRef::new(PROVIDER_WORKSPACE_REF),
            }],
            account_display_name,
            ..BootstrapPayload::default()
        });
    }

    conversations.sort_by_key(|left| std::cmp::Reverse(left.activity_time));

    let selected = conversations.first().cloned();
    let selected_page = if let Some(conversation) = &selected {
        fetch_thread_messages(&mut client, conversation).await?
    } else {
        ThreadPage::default()
    };
    let selected_messages = selected_page.messages;

    let workspace_id = WorkspaceId::new(WORKSPACE_ID);
    let mut channels = Vec::new();
    let mut direct_messages = Vec::new();
    let mut conversation_bindings = Vec::new();
    let mut conversation_notifications = Vec::new();

    for conversation in &conversations {
        if let Some(notifications) = conversation.notifications {
            conversation_notifications.push((conversation.summary.id.clone(), notifications));
        }
        match conversation.summary.kind {
            ConversationKind::Channel => channels.push(conversation.summary.clone()),
            ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
                direct_messages.push(conversation.summary.clone())
            }
        }
        conversation_bindings.push(ConversationBinding {
            conversation_id: conversation.summary.id.clone(),
            backend_id: backend_id.clone(),
            account_id: account_id.clone(),
            provider_conversation_ref: conversation.provider_ref.clone(),
        });
    }

    let mut message_bindings = Vec::new();
    for message in &selected_messages {
        message_bindings.push(MessageBinding {
            message_id: message.id.clone(),
            backend_id: backend_id.clone(),
            account_id: account_id.clone(),
            provider_message_ref: ProviderMessageRef::new(message.id.0.clone()),
        });
    }

    Ok(BootstrapPayload {
        workspace_ids: vec![workspace_id.clone()],
        active_workspace_id: Some(workspace_id.clone()),
        workspace_name: WORKSPACE_NAME.to_string(),
        channels,
        direct_messages,
        workspace_bindings: vec![WorkspaceBinding {
            workspace_id,
            backend_id: backend_id.clone(),
            account_id: account_id.clone(),
            provider_workspace_ref: ProviderWorkspaceRef::new(PROVIDER_WORKSPACE_REF),
        }],
        conversation_bindings,
        message_bindings,
        selected_conversation_id: selected
            .as_ref()
            .map(|conversation| conversation.summary.id.clone()),
        selected_messages,
        unread_marker: selected.and_then(|conversation| conversation.read_marker),
        account_display_name,
        conversation_notifications,
    })
}

async fn fetch_inbox_summary_with_limit(
    client: &mut KeybaseRpcClient,
    activity_sorted_limit: usize,
) -> io::Result<Value> {
    let query = Value::Map(vec![
        (Value::from("topicType"), Value::from(TOPIC_TYPE_CHAT)),
        (Value::from("after"), Value::from("")),
        (Value::from("before"), Value::from("")),
        (Value::from("visibility"), Value::from(TLF_VISIBILITY_ANY)),
        (Value::from("status"), Value::Array(Vec::new())),
        (Value::from("convIDs"), Value::Array(Vec::new())),
        (Value::from("unreadFirst"), Value::from(false)),
        (
            Value::from("unreadFirstLimit"),
            Value::Map(vec![
                (Value::from("NumRead"), Value::from(0)),
                (Value::from("AtLeast"), Value::from(0)),
                (Value::from("AtMost"), Value::from(0)),
            ]),
        ),
        (
            Value::from("activitySortedLimit"),
            Value::from(activity_sorted_limit.clamp(1, i64::MAX as usize) as i64),
        ),
    ]);
    client
        .call(
            CHAT_GET_INBOX_SUMMARY_CLI_LOCAL,
            vec![Value::Map(vec![(Value::from("query"), query)])],
        )
        .await
}

async fn fetch_current_status(client: &mut KeybaseRpcClient) -> io::Result<Value> {
    client
        .call(
            KEYBASE_GET_CURRENT_STATUS,
            vec![Value::Map(vec![(
                Value::from("sessionID"),
                Value::from(SESSION_ID),
            )])],
        )
        .await
}

async fn fetch_inbox_unboxed(client: &mut KeybaseRpcClient) -> io::Result<Value> {
    let query = Value::Map(vec![
        (Value::from("topicType"), Value::from(TOPIC_TYPE_CHAT)),
        (
            Value::from("status"),
            Value::Array(vec![
                Value::from(CONVERSATION_STATUS_UNFILED),
                Value::from(CONVERSATION_STATUS_FAVORITE),
                Value::from(CONVERSATION_STATUS_MUTED),
            ]),
        ),
        (
            Value::from("memberStatus"),
            Value::Array(vec![Value::from(CONVERSATION_MEMBER_STATUS_ACTIVE)]),
        ),
        (Value::from("unreadOnly"), Value::from(false)),
        (Value::from("readOnly"), Value::from(false)),
        (Value::from("computeActiveList"), Value::from(false)),
    ]);
    client
        .call(
            CHAT_GET_INBOX_AND_UNBOX_LOCAL,
            vec![Value::Map(vec![
                (Value::from("query"), query),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await
}

async fn set_conversation_notification_settings_rpc(
    client: &mut KeybaseRpcClient,
    raw_id: &[u8],
    settings: ConversationNotificationSettings,
) -> io::Result<()> {
    client
        .call(
            CHAT_SET_CONVERSATION_STATUS_LOCAL,
            vec![conversation_status_rpc_params(raw_id, settings)],
        )
        .await?;
    client
        .call(
            CHAT_SET_APP_NOTIFICATION_SETTINGS_LOCAL,
            vec![app_notification_settings_rpc_params(raw_id, settings)],
        )
        .await?;
    Ok(())
}

fn conversation_status_rpc_params(
    raw_id: &[u8],
    settings: ConversationNotificationSettings,
) -> Value {
    let status = if settings.muted {
        CONVERSATION_STATUS_MUTED
    } else {
        CONVERSATION_STATUS_UNFILED
    };
    Value::Map(vec![
        (
            Value::from("conversationID"),
            Value::Binary(raw_id.to_vec()),
        ),
        (Value::from("status"), Value::from(status)),
        (
            Value::from("identifyBehavior"),
            Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
        ),
    ])
}

fn app_notification_settings_rpc_params(
    raw_id: &[u8],
    settings: ConversationNotificationSettings,
) -> Value {
    let mut notification_settings = Vec::new();
    for (device, level) in [
        (NOTIFICATION_DEVICE_DESKTOP, settings.desktop),
        (NOTIFICATION_DEVICE_MOBILE, settings.mobile),
    ] {
        notification_settings.push(Value::Map(vec![
            (Value::from("deviceType"), Value::from(device)),
            (
                Value::from("kind"),
                Value::from(NOTIFICATION_KIND_AT_MENTION),
            ),
            (
                Value::from("enabled"),
                Value::from(level == NotificationLevel::MentionsOnly),
            ),
        ]));
        notification_settings.push(Value::Map(vec![
            (Value::from("deviceType"), Value::from(device)),
            (Value::from("kind"), Value::from(NOTIFICATION_KIND_GENERIC)),
            (
                Value::from("enabled"),
                Value::from(level == NotificationLevel::All),
            ),
        ]));
    }
    Value::Map(vec![
        (Value::from("convID"), Value::Binary(raw_id.to_vec())),
        (
            Value::from("channelWide"),
            Value::from(!settings.ignore_mentions),
        ),
        (Value::from("settings"), Value::Array(notification_settings)),
    ])
}

fn all_member_statuses() -> Value {
    Value::Array(vec![
        Value::from(0i64), // ACTIVE
        Value::from(1i64), // REMOVED
        Value::from(2i64), // LEFT
        Value::from(3i64), // PREVIEW
        Value::from(4i64), // RESET
        Value::from(5i64), // NEVER_JOINED
    ])
}

async fn fetch_inbox_for_conversation_id(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
) -> io::Result<Value> {
    let query = Value::Map(vec![
        (Value::from("topicType"), Value::from(TOPIC_TYPE_CHAT)),
        (Value::from("status"), Value::Array(Vec::new())),
        (Value::from("memberStatus"), all_member_statuses()),
        (
            Value::from("convIDs"),
            Value::Array(vec![Value::Binary(conversation_id.to_vec())]),
        ),
        (Value::from("unreadOnly"), Value::from(false)),
        (Value::from("readOnly"), Value::from(false)),
        (Value::from("computeActiveList"), Value::from(false)),
    ]);
    client
        .call(
            CHAT_GET_INBOX_AND_UNBOX_LOCAL,
            vec![Value::Map(vec![
                (Value::from("query"), query),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await
}

async fn fetch_inbox_for_team_topic(
    client: &mut KeybaseRpcClient,
    team_name: &str,
    topic_name: &str,
) -> io::Result<Value> {
    let query = Value::Map(vec![
        (Value::from("topicType"), Value::from(TOPIC_TYPE_CHAT)),
        (Value::from("status"), Value::Array(Vec::new())),
        (Value::from("memberStatus"), all_member_statuses()),
        (Value::from("tlfName"), Value::from(team_name.to_string())),
        (
            Value::from("topicName"),
            Value::from(topic_name.to_string()),
        ),
        (Value::from("membersType"), Value::from(TEAM_MEMBERS_TYPE)),
        (Value::from("unreadOnly"), Value::from(false)),
        (Value::from("readOnly"), Value::from(false)),
        (Value::from("computeActiveList"), Value::from(false)),
    ]);
    client
        .call(
            CHAT_GET_INBOX_AND_UNBOX_LOCAL,
            vec![Value::Map(vec![
                (Value::from("query"), query),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await
}

async fn call_user_search(client: &mut KeybaseRpcClient, query: &str) -> io::Result<Value> {
    client
        .call(
            KEYBASE_USER_SEARCH,
            vec![Value::Map(vec![
                (Value::from("query"), Value::from(query.to_string())),
                (Value::from("service"), Value::from("keybase")),
                (Value::from("maxResults"), Value::from(32)),
                (Value::from("includeContacts"), Value::from(false)),
                (Value::from("includeServicesSummary"), Value::from(false)),
            ])],
        )
        .await
}

async fn call_new_conversation_local(
    client: &mut KeybaseRpcClient,
    participants: &[String],
) -> io::Result<Value> {
    let mut deduped = Vec::new();
    let mut seen = HashSet::new();
    for participant in participants {
        let normalized = participant.trim().to_ascii_lowercase();
        if normalized.is_empty() || !seen.insert(normalized.clone()) {
            continue;
        }
        deduped.push(normalized);
    }
    if deduped.is_empty() {
        return Err(io::Error::other(
            "missing participants for new conversation",
        ));
    }
    client
        .call(
            CHAT_NEW_CONVERSATION_LOCAL,
            vec![Value::Map(vec![
                (Value::from("tlfName"), Value::from(deduped.join(","))),
                (Value::from("topicType"), Value::from(TOPIC_TYPE_CHAT)),
                (
                    Value::from("tlfVisibility"),
                    Value::from(TLF_VISIBILITY_PRIVATE),
                ),
                (Value::from("topicName"), Value::Nil),
                (
                    Value::from("membersType"),
                    Value::from(IMPTEAM_MEMBERS_TYPE),
                ),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await
}

async fn call_find_conversations_local(
    client: &mut KeybaseRpcClient,
    team_name: &str,
    channel_name: &str,
) -> io::Result<Value> {
    client
        .call(
            CHAT_FIND_CONVERSATIONS_LOCAL,
            vec![Value::Map(vec![
                (Value::from("tlfName"), Value::from(team_name.to_string())),
                (
                    Value::from("topicName"),
                    Value::from(channel_name.to_string()),
                ),
                (Value::from("topicType"), Value::from(TOPIC_TYPE_CHAT)),
                (Value::from("membersType"), Value::from(TEAM_MEMBERS_TYPE)),
                (
                    Value::from("visibility"),
                    Value::from(TLF_VISIBILITY_PRIVATE),
                ),
            ])],
        )
        .await
}

async fn call_join_conversation_by_id_local(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
) -> io::Result<Value> {
    client
        .call(
            CHAT_JOIN_CONVERSATION_BY_ID_LOCAL,
            vec![Value::Map(vec![
                (
                    Value::from("convID"),
                    Value::Binary(conversation_id.to_vec()),
                ),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await
}

fn parse_user_search_results(response: &Value) -> Vec<UserSummary> {
    let entries = if let Some(entries) = as_array(response) {
        entries.clone()
    } else {
        find_value_for_keys(response, &["results", "users"], 0)
            .and_then(as_array)
            .cloned()
            .unwrap_or_default()
    };
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for entry in entries {
        let username = find_value_for_keys(
            &entry,
            &["keybaseUsername", "username", "name", "assertion"],
            0,
        )
        .and_then(as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .and_then(|value| {
            let normalized = value.trim();
            if normalized.is_empty() {
                None
            } else if normalized.contains('@') {
                normalized
                    .split('@')
                    .next()
                    .map(|base| base.to_ascii_lowercase())
            } else {
                Some(normalized.to_string())
            }
        });
        let Some(username) = username else {
            continue;
        };
        if !seen.insert(username.clone()) {
            continue;
        }
        let display_name = find_value_for_keys(
            &entry,
            &[
                "prettyName",
                "fullName",
                "fullname",
                "displayName",
                "display_name",
            ],
            0,
        )
        .and_then(as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| username.clone());
        let avatar_asset =
            find_value_for_keys(&entry, &["pictureUrl", "avatarUrl", "avatar_url", "pic"], 0)
                .and_then(as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
        output.push(UserSummary {
            id: UserId::new(username.clone()),
            display_name,
            title: username,
            avatar_asset,
            presence: Presence {
                availability: Availability::Unknown,
                status_text: None,
            },
            affinity: Affinity::None,
        });
    }
    output
}

fn extract_new_conversation_raw_id(response: &Value) -> Option<Vec<u8>> {
    for key in ["conv", "conversation", "uiConv", "uiConversation"] {
        let Some(container) = map_get(response, key) else {
            continue;
        };
        if let Some(bytes) = find_value_for_keys(
            container,
            &["id", "convID", "convId", "conversationID", "conversationId"],
            0,
        )
        .and_then(|value| {
            value_to_conversation_id_bytes(value)
                .or_else(|| conversation_id_bytes_from_base64_string(value))
        }) {
            return Some(bytes);
        }
    }
    find_value_for_keys(
        response,
        &["convID", "convId", "conversationID", "conversationId"],
        0,
    )
    .and_then(|value| {
        value_to_conversation_id_bytes(value)
            .or_else(|| conversation_id_bytes_from_base64_string(value))
    })
    .or_else(|| {
        find_value_for_keys(response, &["id"], 0).and_then(|value| {
            value_to_conversation_id_bytes(value)
                .or_else(|| conversation_id_bytes_from_base64_string(value))
        })
    })
}

fn extract_find_conversation_raw_id(response: &Value) -> Option<Vec<u8>> {
    let mut candidates: Vec<&Value> = Vec::new();
    if let Some(array) = map_get_any(response, &["conversations", "convs"]).and_then(as_array) {
        candidates.extend(array.iter());
    } else {
        candidates.push(response);
    }

    for candidate in candidates {
        let container =
            map_get_any(candidate, &["conv", "conversation", "info", "i"]).unwrap_or(candidate);
        if let Some(bytes) = find_value_for_keys(
            container,
            &["id", "convID", "convId", "conversationID", "conversationId"],
            0,
        )
        .and_then(|value| {
            value_to_conversation_id_bytes(value)
                .or_else(|| conversation_id_bytes_from_base64_string(value))
        }) {
            return Some(bytes);
        }
    }

    None
}

async fn call_mark_as_read_local(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
    message_id: Option<&MessageId>,
    force_unread: bool,
) -> io::Result<Value> {
    let msg_id_value = message_id
        .and_then(|value| value.0.parse::<i64>().ok())
        .map(Value::from)
        .unwrap_or(Value::Nil);
    client
        .call(
            CHAT_MARK_AS_READ_LOCAL,
            vec![Value::Map(vec![
                (Value::from("sessionID"), Value::from(SESSION_ID)),
                (
                    Value::from("conversationID"),
                    Value::Binary(conversation_id.to_vec()),
                ),
                (Value::from("msgID"), msg_id_value),
                (Value::from("forceUnread"), Value::from(force_unread)),
            ])],
        )
        .await
}

async fn call_post_text_nonblock(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
    tlf_name: &str,
    tlf_public: bool,
    body: &str,
    client_prev: i64,
    reply_to: Option<i64>,
) -> io::Result<Value> {
    client
        .call(
            CHAT_POST_TEXT_NONBLOCK,
            vec![Value::Map(vec![
                (Value::from("sessionID"), Value::from(SESSION_ID)),
                (
                    Value::from("conversationID"),
                    Value::Binary(conversation_id.to_vec()),
                ),
                (Value::from("tlfName"), Value::from(tlf_name.to_string())),
                (Value::from("tlfPublic"), Value::from(tlf_public)),
                (Value::from("body"), Value::from(body.to_string())),
                (Value::from("clientPrev"), Value::from(client_prev)),
                (
                    Value::from("replyTo"),
                    reply_to.map(Value::from).unwrap_or(Value::Nil),
                ),
                (Value::from("outboxID"), Value::Nil),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
                (Value::from("ephemeralLifetime"), Value::Nil),
            ])],
        )
        .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "Mirror the Keybase RPC parameters explicitly."
)]
async fn call_post_file_attachment_nonblock(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
    tlf_name: &str,
    tlf_public: bool,
    local_path: &str,
    _filename: &str,
    caption: &str,
    client_prev: i64,
) -> io::Result<Value> {
    let local_path = local_path.trim();
    if local_path.is_empty() {
        return Err(io::Error::other("missing local attachment path"));
    }
    let caption = caption.trim();
    let title = caption.to_string();
    let visibility = if tlf_public {
        TLF_VISIBILITY_PUBLIC
    } else {
        TLF_VISIBILITY_PRIVATE
    };
    let arg = Value::Map(vec![
        (
            Value::from("conversationID"),
            Value::Binary(conversation_id.to_vec()),
        ),
        (Value::from("tlfName"), Value::from(tlf_name.to_string())),
        (Value::from("visibility"), Value::from(visibility)),
        (Value::from("filename"), Value::from(local_path.to_string())),
        (Value::from("title"), Value::from(title)),
        (Value::from("metadata"), Value::Binary(Vec::new())),
        (
            Value::from("identifyBehavior"),
            Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
        ),
        (Value::from("callerPreview"), Value::Nil),
        (Value::from("outboxID"), Value::Nil),
        (Value::from("ephemeralLifetime"), Value::Nil),
    ]);
    client
        .call(
            CHAT_POST_FILE_ATTACHMENT_LOCAL_NONBLOCK,
            vec![Value::Map(vec![
                (Value::from("sessionID"), Value::from(SESSION_ID)),
                (Value::from("arg"), arg),
                (Value::from("clientPrev"), Value::from(client_prev)),
            ])],
        )
        .await
}

async fn call_post_edit_nonblock(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
    tlf_name: &str,
    tlf_public: bool,
    target_message_id: i64,
    body: &str,
    client_prev: i64,
) -> io::Result<Value> {
    client
        .call(
            CHAT_POST_EDIT_NONBLOCK,
            vec![Value::Map(vec![
                (
                    Value::from("conversationID"),
                    Value::Binary(conversation_id.to_vec()),
                ),
                (Value::from("tlfName"), Value::from(tlf_name.to_string())),
                (Value::from("tlfPublic"), Value::from(tlf_public)),
                (
                    Value::from("target"),
                    Value::Map(vec![(
                        Value::from("messageID"),
                        Value::from(target_message_id),
                    )]),
                ),
                (Value::from("body"), Value::from(body.to_string())),
                (Value::from("outboxID"), Value::Nil),
                (Value::from("clientPrev"), Value::from(client_prev)),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await
}

async fn call_post_delete_nonblock(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
    tlf_name: &str,
    tlf_public: bool,
    target_message_id: i64,
    client_prev: i64,
) -> io::Result<Value> {
    client
        .call(
            CHAT_POST_DELETE_NONBLOCK,
            vec![Value::Map(vec![
                (
                    Value::from("conversationID"),
                    Value::Binary(conversation_id.to_vec()),
                ),
                (Value::from("tlfName"), Value::from(tlf_name.to_string())),
                (Value::from("tlfPublic"), Value::from(tlf_public)),
                (Value::from("supersedes"), Value::from(target_message_id)),
                (Value::from("outboxID"), Value::Nil),
                (Value::from("clientPrev"), Value::from(client_prev)),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await
}

async fn call_post_reaction_nonblock(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
    tlf_name: &str,
    tlf_public: bool,
    supersedes: i64,
    body: &str,
    client_prev: i64,
) -> io::Result<Value> {
    client
        .call(
            CHAT_POST_REACTION_NONBLOCK,
            vec![Value::Map(vec![
                (
                    Value::from("conversationID"),
                    Value::Binary(conversation_id.to_vec()),
                ),
                (Value::from("tlfName"), Value::from(tlf_name.to_string())),
                (Value::from("tlfPublic"), Value::from(tlf_public)),
                (Value::from("supersedes"), Value::from(supersedes)),
                (Value::from("body"), Value::from(body.to_string())),
                (Value::from("outboxID"), Value::Nil),
                (Value::from("clientPrev"), Value::from(client_prev)),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await
}

async fn call_pin_message_local(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
    message_id: i64,
) -> io::Result<Value> {
    client
        .call(
            CHAT_PIN_MESSAGE_LOCAL,
            vec![Value::Map(vec![
                (
                    Value::from("convID"),
                    Value::Binary(conversation_id.to_vec()),
                ),
                (Value::from("msgID"), Value::from(message_id)),
            ])],
        )
        .await
}

fn extract_outbox_id_from_post_response(response: &Value) -> Option<String> {
    find_value_for_keys(response, &["outboxID", "outboxId", "outbox_id"], 0)
        .and_then(outbox_id_to_string)
}

async fn call_unpin_message_local(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
) -> io::Result<Value> {
    client
        .call(
            CHAT_UNPIN_MESSAGE_LOCAL,
            vec![Value::Map(vec![(
                Value::from("convID"),
                Value::Binary(conversation_id.to_vec()),
            )])],
        )
        .await
}

async fn call_team_id_of_conv(
    client: &mut KeybaseRpcClient,
    conversation_id: &[u8],
) -> io::Result<Option<Vec<u8>>> {
    let response = client
        .call(
            CHAT_TEAM_ID_OF_CONV,
            vec![Value::Map(vec![(
                Value::from("convID"),
                Value::Binary(conversation_id.to_vec()),
            )])],
        )
        .await?;
    Ok(value_to_team_id_bytes(&response).or_else(|| {
        find_value_for_keys(&response, &["teamID", "teamId", "team_id"], 0)
            .and_then(value_to_team_id_bytes)
    }))
}

async fn call_team_id_from_tlf_name(
    client: &mut KeybaseRpcClient,
    tlf_name: &str,
    members_type: i64,
    tlf_public: bool,
) -> io::Result<Option<Vec<u8>>> {
    let response = client
        .call(
            CHAT_TEAM_ID_FROM_TLF_NAME,
            vec![Value::Map(vec![
                (Value::from("tlfName"), Value::from(tlf_name.to_string())),
                (Value::from("membersType"), Value::from(members_type)),
                (Value::from("tlfPublic"), Value::from(tlf_public)),
            ])],
        )
        .await?;
    Ok(value_to_team_id_bytes(&response).or_else(|| {
        find_value_for_keys(&response, &["teamID", "teamId", "team_id"], 0)
            .and_then(value_to_team_id_bytes)
    }))
}

async fn call_team_get_members_by_id(
    client: &mut KeybaseRpcClient,
    team_id: &str,
) -> io::Result<Value> {
    client
        .call(
            KEYBASE_TEAM_GET_MEMBERS_BY_ID,
            vec![Value::Map(vec![
                (Value::from("sessionID"), Value::from(SESSION_ID)),
                (Value::from("id"), Value::from(team_id.to_string())),
            ])],
        )
        .await
}

async fn fetch_thread_messages(
    client: &mut KeybaseRpcClient,
    conversation: &BootstrapConversation,
) -> io::Result<ThreadPage> {
    fetch_thread_messages_by_raw_id(
        client,
        &conversation.summary.id,
        &conversation.raw_conversation_id,
    )
    .await
}

#[derive(Clone, Debug, Default)]
struct ThreadPage {
    messages: Vec<MessageRecord>,
    deletions: Vec<MessageRecord>,
    progress: PageProgress,
    offline: bool,
    reaction_deltas: Vec<MessageReactionDelta>,
    next_cursor: Option<Vec<u8>>,
    last: bool,
    saw_pagination: bool,
}

#[derive(Clone, Debug)]
struct MessageReactionDelta {
    conversation_id: ConversationId,
    target_message_id: MessageId,
    op_message_id: Option<MessageId>,
    emoji: String,
    source_ref: Option<EmojiSourceRef>,
    actor_id: UserId,
    updated_ms: i64,
}

#[derive(Clone, Debug)]
struct ConversationUnreadSnapshot {
    conversation_id: ConversationId,
    unread_count: u32,
    mention_count: u32,
    read_upto: Option<MessageId>,
    activity_time: Option<i64>,
}

#[derive(Clone, Debug)]
struct ReadMarkerNotifyUpdate {
    conversation_id: ConversationId,
    read_upto: MessageId,
    snapshot: Option<ConversationUnreadSnapshot>,
}

async fn fetch_thread_page(
    client: &mut KeybaseRpcClient,
    conversation_id: &ConversationId,
    raw_conversation_id: &[u8],
    next_cursor: Option<&[u8]>,
    page_size: usize,
) -> io::Result<ThreadPage> {
    fetch_thread_page_with_attachment_hydration(
        client,
        conversation_id,
        raw_conversation_id,
        next_cursor,
        page_size,
        true,
    )
    .await
}

async fn fetch_thread_page_with_attachment_hydration(
    client: &mut KeybaseRpcClient,
    conversation_id: &ConversationId,
    raw_conversation_id: &[u8],
    next_cursor: Option<&[u8]>,
    page_size: usize,
    hydrate_attachments: bool,
) -> io::Result<ThreadPage> {
    let mut pagination_entries = vec![(
        Value::from("num"),
        Value::from(page_size.clamp(1, i64::MAX as usize) as i64),
    )];
    if let Some(cursor) = next_cursor {
        pagination_entries.push((Value::from("next"), Value::Binary(cursor.to_vec())));
    }
    let pagination = Value::Map(pagination_entries);
    let result = client
        .call(
            CHAT_GET_THREAD_LOCAL,
            vec![Value::Map(vec![
                (
                    Value::from("conversationID"),
                    Value::Binary(raw_conversation_id.to_vec()),
                ),
                (
                    Value::from("reason"),
                    Value::from(GET_THREAD_REASON_FOREGROUND),
                ),
                (Value::from("query"), Value::Nil),
                (Value::from("pagination"), pagination),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await?;
    let mut page = parse_thread_page(&result, conversation_id);
    if hydrate_attachments {
        hydrate_thread_attachment_paths(client, raw_conversation_id, &mut page).await;
    }
    Ok(page)
}

async fn fetch_thread_messages_by_raw_id(
    client: &mut KeybaseRpcClient,
    conversation_id: &ConversationId,
    raw_conversation_id: &[u8],
) -> io::Result<ThreadPage> {
    let page = fetch_thread_page(
        client,
        conversation_id,
        raw_conversation_id,
        None,
        CONVERSATION_OPEN_LIVE_FETCH_PAGE_SIZE,
    )
    .await?;
    Ok(page)
}

async fn fetch_thread_messages_by_raw_id_without_attachment_hydration(
    client: &mut KeybaseRpcClient,
    conversation_id: &ConversationId,
    raw_conversation_id: &[u8],
) -> io::Result<ThreadPage> {
    let page = fetch_thread_page_with_attachment_hydration(
        client,
        conversation_id,
        raw_conversation_id,
        None,
        CONVERSATION_OPEN_LIVE_FETCH_PAGE_SIZE,
        false,
    )
    .await?;
    Ok(page)
}

async fn fetch_thread_messages_before_anchor(
    client: &mut KeybaseRpcClient,
    conversation_id: &ConversationId,
    raw_conversation_id: &[u8],
    before_message_id: &MessageId,
    page_size: usize,
) -> io::Result<ThreadPage> {
    fetch_thread_messages_before_anchor_impl(
        client,
        conversation_id,
        raw_conversation_id,
        before_message_id,
        page_size,
        true,
    )
    .await
}

async fn fetch_thread_messages_before_anchor_impl(
    client: &mut KeybaseRpcClient,
    conversation_id: &ConversationId,
    raw_conversation_id: &[u8],
    before_message_id: &MessageId,
    page_size: usize,
    hydrate_attachments: bool,
) -> io::Result<ThreadPage> {
    let Ok(pivot) = before_message_id.0.parse::<i64>() else {
        return Ok(ThreadPage::default());
    };
    let query = Value::Map(vec![(
        Value::from("messageIDControl"),
        Value::Map(vec![
            (Value::from("pivot"), Value::from(pivot)),
            (
                Value::from("mode"),
                Value::from(MESSAGE_ID_CONTROL_MODE_OLDER),
            ),
            (
                Value::from("num"),
                Value::from(page_size.clamp(1, i64::MAX as usize) as i64),
            ),
        ]),
    )]);
    let result = client
        .call(
            CHAT_GET_THREAD_LOCAL,
            vec![Value::Map(vec![
                (
                    Value::from("conversationID"),
                    Value::Binary(raw_conversation_id.to_vec()),
                ),
                (
                    Value::from("reason"),
                    Value::from(GET_THREAD_REASON_FOREGROUND),
                ),
                (Value::from("query"), query),
                (Value::from("pagination"), Value::Nil),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await?;
    let mut page = parse_thread_page(&result, conversation_id);
    if hydrate_attachments {
        hydrate_thread_attachment_paths(client, raw_conversation_id, &mut page).await;
    }
    Ok(page)
}

async fn fetch_thread_messages_after_anchor(
    client: &mut KeybaseRpcClient,
    conversation_id: &ConversationId,
    raw_conversation_id: &[u8],
    after_message_id: &MessageId,
    page_size: usize,
) -> io::Result<ThreadPage> {
    fetch_thread_messages_after_anchor_impl(
        client,
        conversation_id,
        raw_conversation_id,
        after_message_id,
        page_size,
        true,
    )
    .await
}

async fn fetch_thread_messages_after_anchor_impl(
    client: &mut KeybaseRpcClient,
    conversation_id: &ConversationId,
    raw_conversation_id: &[u8],
    after_message_id: &MessageId,
    page_size: usize,
    hydrate_attachments: bool,
) -> io::Result<ThreadPage> {
    let Ok(pivot) = after_message_id.0.parse::<i64>() else {
        return Ok(ThreadPage::default());
    };
    let query = Value::Map(vec![
        (Value::from("markAsRead"), Value::from(false)),
        (
            Value::from("disablePostProcessThread"),
            Value::from(!hydrate_attachments),
        ),
        (
            Value::from("messageIDControl"),
            Value::Map(vec![
                (Value::from("pivot"), Value::from(pivot)),
                (
                    Value::from("mode"),
                    Value::from(MESSAGE_ID_CONTROL_MODE_NEWER),
                ),
                (
                    Value::from("num"),
                    Value::from(page_size.clamp(1, i64::MAX as usize) as i64),
                ),
            ]),
        ),
    ]);
    let result = client
        .call(
            CHAT_GET_THREAD_LOCAL,
            vec![Value::Map(vec![
                (
                    Value::from("conversationID"),
                    Value::Binary(raw_conversation_id.to_vec()),
                ),
                (
                    Value::from("reason"),
                    Value::from(GET_THREAD_REASON_FOREGROUND),
                ),
                (Value::from("query"), query),
                (Value::from("pagination"), Value::Nil),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await?;
    let mut page = parse_thread_page(&result, conversation_id);
    if hydrate_attachments {
        hydrate_thread_attachment_paths(client, raw_conversation_id, &mut page).await;
    }
    Ok(page)
}

async fn hydrate_thread_attachment_paths(
    client: &mut KeybaseRpcClient,
    raw_conversation_id: &[u8],
    page: &mut ThreadPage,
) {
    let _ = hydrate_attachment_paths_for_messages(
        client,
        raw_conversation_id,
        &mut page.messages,
        ATTACHMENT_DOWNLOAD_LIMIT_PER_PAGE,
    )
    .await;
}

fn is_unrenderable_keybase_asset_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("http://127.0.0.1:") || lower.starts_with("http://localhost:") {
        return true;
    }
    let prefix = if lower.starts_with("https://s3.amazonaws.com/") {
        "https://s3.amazonaws.com/"
    } else if lower.starts_with("http://s3.amazonaws.com/") {
        "http://s3.amazonaws.com/"
    } else {
        return false;
    };
    if lower.contains("x-amz-signature=") || lower.contains("awsaccesskeyid=") {
        return false;
    }
    let path = &url[prefix.len()..];
    let Some(first_segment) = path.split('/').next() else {
        return false;
    };
    first_segment.len() == 64 && first_segment.chars().all(|ch| ch.is_ascii_hexdigit())
}

async fn download_attachment_to_cache(
    client: &mut KeybaseRpcClient,
    raw_conversation_id: &[u8],
    message_id: i64,
    preview: bool,
) -> Option<String> {
    let response = client
        .call(
            CHAT_DOWNLOAD_FILE_ATTACHMENT_LOCAL,
            vec![Value::Map(vec![
                (Value::from("sessionID"), Value::from(SESSION_ID)),
                (
                    Value::from("conversationID"),
                    Value::Binary(raw_conversation_id.to_vec()),
                ),
                (Value::from("messageID"), Value::from(message_id)),
                (Value::from("downloadToCache"), Value::from(true)),
                (Value::from("preview"), Value::from(preview)),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await
        .ok()?;
    let file_path = find_value_for_keys(&response, &["filePath", "file_path"], 0)
        .and_then(as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    Path::new(&file_path).exists().then_some(file_path)
}

#[derive(Clone)]
struct BootstrapConversation {
    summary: ConversationSummary,
    provider_ref: ProviderConversationRef,
    raw_conversation_id: Vec<u8>,
    can_post: bool,
    activity_time: i64,
    read_marker: Option<MessageId>,
    pinned_state: PinnedState,
    /// `None` when the payload does not carry notification info (for example an
    /// `InboxUIItem` metadata update); leave the known value alone then.
    notifications: Option<ConversationNotificationSettings>,
}

fn conversation_topic_type(value: &Value) -> Option<i64> {
    map_get(value, "topicType").and_then(as_i64).or_else(|| {
        map_get(value, "triple")
            .and_then(|triple| map_get(triple, "topicType"))
            .and_then(as_i64)
    })
}

fn is_chat_topic(value: &Value) -> bool {
    // Missing metadata is tolerated for legacy payloads; explicit internal types
    // (EMOJI, EMOJICROSS, KBFSFILEEDIT, DEV) never belong in the chat workspace.
    conversation_topic_type(value).is_none_or(|topic_type| topic_type == TOPIC_TYPE_CHAT)
}

async fn repair_cached_conversation_topics(
    client: &mut KeybaseRpcClient,
    local_store: &LocalStore,
    sender: &Sender<BackendEvent>,
) -> io::Result<()> {
    let ids = local_store.unverified_conversation_ids()?;
    for chunk in ids.chunks(200) {
        let raw_ids = chunk
            .iter()
            .filter_map(|id| {
                provider_ref_to_conversation_id_bytes(&ProviderConversationRef::new(id.0.clone()))
                    .map(Value::Binary)
            })
            .collect::<Vec<_>>();
        if raw_ids.is_empty() {
            continue;
        }
        // No topicType filter: we need metadata for historical internal entries too.
        let response = client
            .call(
                CHAT_GET_INBOX_SUMMARY_CLI_LOCAL,
                vec![Value::Map(vec![(
                    "query".into(),
                    Value::Map(vec![
                        ("convIDs".into(), Value::Array(raw_ids)),
                        ("visibility".into(), TLF_VISIBILITY_ANY.into()),
                        ("status".into(), Value::Array(Vec::new())),
                        ("memberStatus".into(), all_member_statuses()),
                    ]),
                )])],
            )
            .await?;
        let mut topics = Vec::new();
        for conversation in map_get(&response, "conversations")
            .and_then(as_array)
            .into_iter()
            .flatten()
        {
            let Some(info) = map_get(conversation, "info") else {
                continue;
            };
            let Some(raw_id) = map_get(info, "id").and_then(value_to_conversation_id_bytes) else {
                continue;
            };
            let Some(topic_type) = conversation_topic_type(info) else {
                continue;
            };
            let id = ConversationId::new(format!("kb_conv:{}", hex_encode(&raw_id)));
            if chunk.contains(&id) {
                topics.push((id, topic_type));
            }
        }
        local_store.persist_conversation_topic_types(&topics)?;
        tracing::info!(
            checked = topics.len(),
            excluded = topics.iter().filter(|(_, t)| *t != TOPIC_TYPE_CHAT).count(),
            "keybase.cache_topic_repair"
        );
        let excluded = topics
            .into_iter()
            .filter(|(_, t)| *t != TOPIC_TYPE_CHAT)
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        if !excluded.is_empty() {
            sender
                .send(BackendEvent::WorkspaceConversationsExcluded {
                    workspace_id: WorkspaceId::new(WORKSPACE_ID),
                    conversation_ids: excluded,
                })
                .map_err(|_| io::Error::other("UI closed"))?;
        }
    }
    Ok(())
}

fn parse_inbox_conversations(
    value: &Value,
    self_username: Option<&str>,
    include_non_active_channels: bool,
) -> Vec<BootstrapConversation> {
    let mut result = Vec::new();
    let conversations = map_get(value, "conversations")
        .and_then(as_array)
        .cloned()
        .unwrap_or_default();

    for conversation in conversations {
        let Some(info) = map_get_any(&conversation, &["info", "i", "conv", "conversation"]) else {
            continue;
        };
        if !is_chat_topic(info) {
            continue;
        }
        let Some(raw_id) = map_get_any(info, &["id", "i"]).and_then(value_to_conversation_id_bytes)
        else {
            continue;
        };
        let encoded_id = format!("kb_conv:{}", hex_encode(&raw_id));
        let conversation_id = ConversationId::new(encoded_id.clone());
        let provider_ref = ProviderConversationRef::new(encoded_id);
        let tlf_name = map_get_any(info, &["tlfName", "n"])
            .and_then(as_str)
            .unwrap_or("")
            .to_string();
        let topic_name = map_get_any(info, &["topicName", "t"])
            .and_then(as_str)
            .unwrap_or("")
            .to_string();
        let members_type = map_get_any(info, &["membersType", "m"])
            .and_then(as_i64)
            .unwrap_or(TEAM_MEMBERS_TYPE);
        let status = map_get_any(info, &["status", "s"])
            .and_then(as_i64)
            .unwrap_or(0);
        let reader_info = map_get_any(&conversation, &["readerInfo", "ri", "r"])
            .cloned()
            .unwrap_or(Value::Nil);
        let member_status = map_get_any(&reader_info, &["status", "s"]).and_then(as_i64);
        let read_msg = map_get_any(&reader_info, &["readMsgid", "readMsgID"]).and_then(as_i64);
        let activity_time = map_get_any(&reader_info, &["mtime"])
            .and_then(as_i64)
            .or_else(|| {
                map_get_any(
                    &conversation,
                    &["activeAtMs", "active_at_ms", "activeAt", "active_at"],
                )
                .and_then(as_i64)
            })
            .unwrap_or(0);
        let unread_count = conversation_unread_count(&conversation, &reader_info);
        let mention_count = conversation_mention_count(&conversation, &reader_info);
        let title = conversation_title(&tlf_name, &topic_name, members_type, self_username);
        let kind = conversation_kind(&tlf_name, members_type);
        let can_post = member_status == Some(CONVERSATION_MEMBER_STATUS_ACTIVE);
        if !include_non_active_channels && kind == ConversationKind::Channel && !can_post {
            continue;
        }
        let topic = if topic_name.is_empty() {
            tlf_name.clone()
        } else {
            topic_name.clone()
        };

        let group = if members_type == TEAM_MEMBERS_TYPE {
            Some(ConversationGroup {
                id: tlf_name.clone(),
                display_name: tlf_name.clone(),
                team_type: parse_team_type(info),
            })
        } else {
            None
        };

        let last_message = parse_inbox_last_message(&conversation, &conversation_id);
        let notifications = Some(parse_conversation_notification_settings(
            &conversation,
            status,
        ));
        let summary = ConversationSummary {
            id: conversation_id,
            title,
            kind,
            topic,
            group,
            unread_count,
            mention_count,
            muted: status == CONVERSATION_STATUS_MUTED,
            last_activity_ms: activity_time,
            last_message,
        };
        let pinned_state = extract_conversation_pinned_state(info);

        result.push(BootstrapConversation {
            summary,
            provider_ref,
            raw_conversation_id: raw_id,
            can_post,
            activity_time,
            read_marker: read_msg
                .filter(|value| *value > 0)
                .map(|value| MessageId::new(value.to_string())),
            pinned_state,
            notifications,
        });
    }

    result
}

/// Message types Keybase itself uses for inbox snippets.
const SNIPPET_MESSAGE_TYPES: [i64; 9] = [
    MESSAGE_TYPE_TEXT,
    MESSAGE_TYPE_ATTACHMENT,
    MESSAGE_TYPE_SYSTEM,
    MESSAGE_TYPE_DELETE_HISTORY,
    MESSAGE_TYPE_SEND_PAYMENT,
    MESSAGE_TYPE_REQUEST_PAYMENT,
    MESSAGE_TYPE_FLIP,
    MESSAGE_TYPE_HEADLINE,
    MESSAGE_TYPE_PIN,
];

/// Preview of a `ConversationLocal`'s newest message. Its `maxMessages` hold the
/// latest message of each type; the newest snippet-worthy one wins, and an edit
/// of that very message replaces its text.
fn parse_inbox_last_message(
    conversation: &Value,
    conversation_id: &ConversationId,
) -> Option<MessagePreview> {
    let items = map_get_any(conversation, &["maxMessages", "maxMsgs"]).and_then(as_array)?;
    let mut newest: Option<MessagePreview> = None;
    let mut edits = Vec::new();
    for item in items {
        let Some(valid) = extract_valid_message(item) else {
            continue;
        };
        let message_type =
            map_get_any(valid, &["messageBody", "b"]).and_then(message_type_from_message_body);
        let Some(message_type) = message_type else {
            continue;
        };
        if message_type == MESSAGE_TYPE_EDIT {
            edits.push(valid);
            continue;
        }
        if !SNIPPET_MESSAGE_TYPES.contains(&message_type) {
            continue;
        }
        let preview = parse_thread_message(valid, conversation_id, Some(message_type))
            .filter(|message| !is_placeholder_message(message))
            .and_then(|message| MessagePreview::from_message(&message));
        newest = MessagePreview::newest(newest, preview);
    }
    let newest = newest?;
    for valid in edits {
        let edited = parse_thread_message(valid, conversation_id, Some(MESSAGE_TYPE_EDIT))
            .and_then(|message| MessagePreview::from_message(&message))
            .filter(|edit| edit.message_id == newest.message_id);
        if let Some(edited) = edited {
            return Some(MessagePreview {
                timestamp_ms: newest.timestamp_ms,
                ..edited
            });
        }
    }
    Some(newest)
}

/// Keybase's preformatted snippet on an `InboxUIItem` (inbox notifications),
/// for conversations whose newest message has not been seen any other way.
fn parse_inbox_ui_snippet(value: &Value, activity_time: i64) -> Option<MessagePreview> {
    let snippet = map_get_any(value, &["snippet"])
        .and_then(as_str)
        .map(str::trim)
        .filter(|snippet| !snippet.is_empty())?;
    let message_id = map_get_any(value, &["maxVisibleMsgID", "maxMsgID"])
        .and_then(as_i64)
        .filter(|id| *id > 0)
        .map(|id| MessageId::new(id.to_string()));
    Some(MessagePreview {
        message_id,
        author_id: None,
        timestamp_ms: activity_time,
        body: PreviewBody::Snippet(snippet.to_string()),
    })
}

fn parse_team_type(value: &Value) -> ConversationTeamType {
    match map_get(value, "teamType").and_then(as_i64) {
        Some(1) => ConversationTeamType::Simple,
        Some(2) => ConversationTeamType::Complex,
        _ => ConversationTeamType::Unknown,
    }
}

/// Parse a single `InboxUIItem` (as embedded in `NewChatActivity` notifications) into a
/// `BootstrapConversation`. The `InboxUIItem` has top-level fields like `convID` (hex string),
/// `name` (tlfName), `channel` (topicName), `membersType`, `memberStatus`, `status`, `time`, etc.
fn parse_inbox_ui_item(
    value: &Value,
    self_username: Option<&str>,
) -> Option<BootstrapConversation> {
    if !is_chat_topic(value) {
        return None;
    }
    let conv_id_str = map_get_any(value, &["convID", "convId"]).and_then(as_str)?;
    let raw_id = hex_decode(conv_id_str)?;
    let encoded_id = format!("kb_conv:{}", hex_encode(&raw_id));
    let conversation_id = ConversationId::new(encoded_id.clone());
    let provider_ref = ProviderConversationRef::new(encoded_id);

    let tlf_name = map_get_any(value, &["name", "tlfName", "n"])
        .and_then(as_str)
        .unwrap_or("")
        .to_string();
    let topic_name = map_get_any(value, &["channel", "topicName", "t"])
        .and_then(as_str)
        .unwrap_or("")
        .to_string();
    let members_type = map_get_any(value, &["membersType", "m"])
        .and_then(as_i64)
        .unwrap_or(IMPTEAM_MEMBERS_TYPE);
    let status = map_get_any(value, &["status", "s"])
        .and_then(as_i64)
        .unwrap_or(0);
    let member_status = map_get_any(value, &["memberStatus"]).and_then(as_i64);
    let activity_time = map_get_any(value, &["time"]).and_then(as_i64).unwrap_or(0);
    let read_msg = map_get_any(value, &["readMsgID", "readMsgid"]).and_then(as_i64);

    let title = conversation_title(&tlf_name, &topic_name, members_type, self_username);
    let kind = conversation_kind(&tlf_name, members_type);
    let can_post = member_status == Some(CONVERSATION_MEMBER_STATUS_ACTIVE);
    let topic = if topic_name.is_empty() {
        tlf_name.clone()
    } else {
        topic_name.clone()
    };
    let group = if members_type == TEAM_MEMBERS_TYPE {
        Some(ConversationGroup {
            id: tlf_name.clone(),
            display_name: tlf_name.clone(),
            team_type: parse_team_type(value),
        })
    } else {
        None
    };

    let summary = ConversationSummary {
        id: conversation_id,
        title,
        kind,
        topic,
        group,
        unread_count: 0,
        mention_count: 0,
        muted: status == CONVERSATION_STATUS_MUTED,
        last_activity_ms: activity_time,
        last_message: parse_inbox_ui_snippet(value, activity_time),
    };
    let notifications = map_get_any(value, &["notifications", "notifs", "notificationInfo"])
        .map(|_| parse_conversation_notification_settings(value, status));

    Some(BootstrapConversation {
        summary,
        provider_ref,
        raw_conversation_id: raw_id,
        can_post,
        activity_time,
        read_marker: read_msg
            .filter(|v| *v > 0)
            .map(|v| MessageId::new(v.to_string())),
        pinned_state: PinnedState::default(),
        notifications,
    })
}

const NOTIFICATION_DEVICE_DESKTOP: i64 = 0;
const NOTIFICATION_DEVICE_MOBILE: i64 = 1;
const NOTIFICATION_KIND_GENERIC: i64 = 0;
const NOTIFICATION_KIND_AT_MENTION: i64 = 1;

/// Keybase's per-conversation notification choices: `channelWide` plus a
/// device/kind boolean matrix. Missing pieces keep Keybase's defaults.
fn parse_conversation_notification_settings(
    conversation: &Value,
    status: i64,
) -> ConversationNotificationSettings {
    let mut settings = ConversationNotificationSettings {
        muted: status == CONVERSATION_STATUS_MUTED,
        ..Default::default()
    };
    let Some(info) = map_get_any(
        conversation,
        &["notifications", "notifs", "notificationInfo"],
    ) else {
        return settings;
    };
    if matches!(info, Value::Nil) {
        return settings;
    }
    if let Some(channel_wide) =
        map_get_any(info, &["channelWide", "channelwide", "cw"]).and_then(as_bool)
    {
        settings.ignore_mentions = !channel_wide;
    }
    let Some(Value::Map(devices)) = map_get_any(info, &["settings", "s"]) else {
        return settings;
    };
    for (device, kinds) in devices {
        let Some(device) = notification_device_type(device) else {
            continue;
        };
        let Value::Map(kinds) = kinds else {
            continue;
        };
        let mut generic = None;
        let mut at_mention = None;
        for (kind, enabled) in kinds {
            let Some(kind) = notification_kind(kind) else {
                continue;
            };
            let enabled = as_bool(enabled).unwrap_or(false);
            match kind {
                NOTIFICATION_KIND_GENERIC => generic = Some(enabled),
                NOTIFICATION_KIND_AT_MENTION => at_mention = Some(enabled),
                _ => {}
            }
        }
        if generic.is_none() && at_mention.is_none() {
            continue;
        }
        let level = if generic == Some(true) {
            NotificationLevel::All
        } else if at_mention == Some(true) {
            NotificationLevel::MentionsOnly
        } else {
            NotificationLevel::Nothing
        };
        match device {
            NOTIFICATION_DEVICE_DESKTOP => settings.desktop = level,
            NOTIFICATION_DEVICE_MOBILE => settings.mobile = level,
            _ => {}
        }
    }
    settings
}

fn notification_device_type(value: &Value) -> Option<i64> {
    if let Some(device) = value_to_i64_key(value) {
        return Some(device);
    }
    let name = as_str(value)?.trim().to_ascii_lowercase();
    match name.as_str() {
        "desktop" | "desktop_0" => Some(NOTIFICATION_DEVICE_DESKTOP),
        "mobile" | "mobile_1" => Some(NOTIFICATION_DEVICE_MOBILE),
        _ => None,
    }
}

fn notification_kind(value: &Value) -> Option<i64> {
    if let Some(kind) = value_to_i64_key(value) {
        return Some(kind);
    }
    let name = as_str(value)?.trim().to_ascii_lowercase();
    match name.as_str() {
        "generic" | "generic_0" => Some(NOTIFICATION_KIND_GENERIC),
        "atmention" | "at_mention" | "atmention_1" => Some(NOTIFICATION_KIND_AT_MENTION),
        _ => None,
    }
}

/// Extract the `conv` (InboxUIItem) from a `NewChatActivity` notification.
/// The `conv` field appears inside `activity.incomingMessage.conv` or
/// `activity.newConversation.conv` or `activity.setStatus.conv`.
fn extract_conv_from_notify(event: &KeybaseNotifyEvent) -> Option<&Value> {
    if event.method_name() == "chat.1.NotifyChat.ChatJoinedConversation" {
        let params = match event {
            KeybaseNotifyEvent::Known { raw_params, .. }
            | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
        };
        return find_value_for_keys(params, &["conv"], 0);
    }

    if event.method_name() != "chat.1.NotifyChat.NewChatActivity" {
        return None;
    }
    let raw_params = match event {
        KeybaseNotifyEvent::Known { raw_params, .. }
        | KeybaseNotifyEvent::Unknown { raw_params, .. } => raw_params,
    };
    let activity = find_value_for_keys(raw_params, &["activity", "a"], 0)?;
    for container_key in &[
        "incomingMessage",
        "im",
        "i",
        "newConversation",
        "nc",
        "setStatus",
        "ss",
        "readMessage",
        "rm",
        "teamtype",
    ] {
        if let Some(container) = map_get(activity, container_key)
            && let Some(conv) = map_get_any(container, &["conv", "c"])
            && map_get_any(conv, &["convID", "convId"]).is_some()
        {
            return Some(conv);
        }
    }
    None
}

fn extract_pinned_state_for_conversation(
    inbox_response: &Value,
    conversation_id: &ConversationId,
) -> Option<PinnedState> {
    let conversations = map_get(inbox_response, "conversations").and_then(as_array)?;
    for conversation in conversations {
        let Some(info) = map_get_any(conversation, &["info", "i", "conv", "conversation"]) else {
            continue;
        };
        let Some(raw_id) = map_get_any(info, &["id", "i"]).and_then(value_to_conversation_id_bytes)
        else {
            continue;
        };
        let parsed_conversation_id =
            ConversationId::new(format!("kb_conv:{}", hex_encode(&raw_id)));
        if &parsed_conversation_id == conversation_id {
            return Some(extract_conversation_pinned_state(info));
        }
    }
    None
}

fn extract_conversation_pinned_state(info: &Value) -> PinnedState {
    let Some(pinned_message) = map_get_any(info, &["pinnedMsg", "pm"]) else {
        return PinnedState::default();
    };
    let Some(item) = parse_pinned_item(pinned_message) else {
        return PinnedState::default();
    };
    PinnedState { items: vec![item] }
}

fn parse_pinned_item(pinned_value: &Value) -> Option<PinnedItem> {
    let unboxed_message = map_get_any(pinned_value, &["message", "m"])?;
    let message_id = extract_message_id_from_unboxed_message(unboxed_message)?;
    let valid = extract_valid_message(unboxed_message).unwrap_or(unboxed_message);
    let message_body = map_get_any(valid, &["messageBody", "b"]);
    let preview_text = message_body
        .and_then(extract_text_body)
        .or_else(|| message_body.and_then(extract_non_text_fallback_body))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let author_label = map_get_any(valid, &["senderUsername", "su"])
        .and_then(as_str)
        .map(str::to_string)
        .or_else(|| find_sender_username(valid));
    let preview = if author_label.is_some() || preview_text.is_some() {
        Some(PinnedPreview {
            author_label,
            text: preview_text,
        })
    } else {
        None
    };
    let pinned_by = map_get_any(pinned_value, &["pinnerUsername", "pu"])
        .and_then(as_str)
        .map(|value| UserId::new(value.to_ascii_lowercase()));
    Some(PinnedItem {
        id: format!("message:{}", message_id.0),
        target: PinnedTarget::Message { message_id },
        pinned_by,
        pinned_at_ms: extract_message_timestamp_ms(valid),
        preview,
    })
}

const MSG_TYPE_TEXT: i64 = 1;
const MSG_TYPE_ATTACHMENT: i64 = 2;

fn conversation_unread_count(conversation: &Value, reader_info: &Value) -> u32 {
    // readMsgid lives in readerInfo (ConversationLocal from inbox) or as a
    // top-level field (InboxUIItem from notifications).
    let read_msg = map_get_any(reader_info, &["readMsgid", "readMsgID"])
        .and_then(as_i64)
        .or_else(|| map_get_any(conversation, &["readMsgID", "readMsgid"]).and_then(as_i64));

    // Prefer maxVisibleMsgID when present (InboxUIItem from notifications).
    // This is pre-computed by the Keybase server and already filters out
    // non-visible message types. Skip when 0 (default/unset).
    if let (Some(max_visible), Some(read_msg)) = (
        map_get_any(conversation, &["maxVisibleMsgID"]).and_then(as_i64),
        read_msg,
    ) && max_visible > 0
    {
        return if max_visible > read_msg { 1 } else { 0 };
    }

    // Use maxMsgSummaries (ConversationLocal from inbox) to check only TEXT
    // and ATTACHMENT messages. Returns Some(true/false) when the field exists,
    // None when it doesn't (so we can fall through to other heuristics).
    if let Some(read_msg) = read_msg
        && let Some(has_unread) = has_unread_visible_content(conversation, read_msg)
    {
        return if has_unread { 1 } else { 0 };
    }

    if let Some(unread_total) = map_get_any(
        conversation,
        &[
            "unreadMessages",
            "unreadmessages",
            "unreadCount",
            "unread_count",
            "uc",
        ],
    )
    .and_then(as_i64)
    {
        return unread_total.max(0) as u32;
    }

    // maxMsgid also advances for reactions, edits and deletes. Without visible
    // summaries it is not evidence of unread chat content; wait for badge state.
    map_get_any(conversation, &["unread", "u"])
        .and_then(as_bool)
        .map(|unread| if unread { 1 } else { 0 })
        .unwrap_or(0)
}

/// Returns Some(true) if there are unread visible content messages (TEXT or
/// ATTACHMENT), Some(false) if maxMsgSummaries is present but all visible
/// content is already read, or None if maxMsgSummaries is absent.
fn has_unread_visible_content(conversation: &Value, read_msg: i64) -> Option<bool> {
    let summaries = map_get_any(conversation, &["maxMsgSummaries", "ms"]).and_then(as_array)?;
    for summary in summaries {
        let msg_type = map_get_any(summary, &["messageType", "t"]).and_then(as_i64);
        if msg_type != Some(MSG_TYPE_TEXT) && msg_type != Some(MSG_TYPE_ATTACHMENT) {
            continue;
        }
        let msg_id = map_get_any(summary, &["msgID", "id"])
            .and_then(as_i64)
            .unwrap_or(0);
        if msg_id > read_msg {
            return Some(true);
        }
    }
    // maxMsgSummaries exists but no visible content is beyond the read marker.
    Some(false)
}

fn conversation_mention_count(conversation: &Value, reader_info: &Value) -> u32 {
    map_get_any(
        reader_info,
        &[
            "badgeCount",
            "badgecount",
            "badge_count",
            "mentionCount",
            "mentions",
        ],
    )
    .and_then(as_i64)
    .or_else(|| {
        map_get_any(
            conversation,
            &[
                "badgeCount",
                "badgecount",
                "badge_count",
                "mentionCount",
                "mentions",
            ],
        )
        .and_then(as_i64)
    })
    .unwrap_or(0)
    .max(0) as u32
}

fn history_envelope_disposition(item: &Value) -> EnvelopeDisposition {
    let id = extract_message_id_from_unboxed_message(item)
        .and_then(|id| id.0.parse::<u64>().ok())
        .filter(|id| *id > 0);
    let Some(id) = id else {
        return EnvelopeDisposition::Retry;
    };
    if extract_valid_message(item).is_some() {
        return EnvelopeDisposition::Available(id);
    }
    if let Some(placeholder) = map_get(item, "placeholder") {
        return if map_get(placeholder, "hidden").and_then(as_bool) == Some(true) {
            EnvelopeDisposition::PermanentlyUnavailable(id)
        } else {
            // A visible placeholder can represent a message still being fetched.
            EnvelopeDisposition::Retry
        };
    }
    if let Some(error) = map_get(item, "error") {
        let ephemeral = map_get(error, "isEphemeral").and_then(as_bool) == Some(true);
        let expired = map_get(error, "etime")
            .and_then(as_i64)
            .is_some_and(|etime| etime <= now_unix_ms());
        let exploded =
            map_get(error, "explodedBy").is_some_and(|value| !matches!(value, Value::Nil));
        let ephemeral_error = matches!(map_get(error, "errType").and_then(as_i64), Some(4 | 5));
        if ephemeral && ephemeral_error && (expired || exploded) {
            return EnvelopeDisposition::PermanentlyUnavailable(id);
        }
    }
    EnvelopeDisposition::Retry
}

fn parse_thread_page(value: &Value, conversation_id: &ConversationId) -> ThreadPage {
    let mut page = ThreadPage {
        offline: map_get_any(value, &["offline"])
            .and_then(as_bool)
            .unwrap_or(false),
        ..ThreadPage::default()
    };
    let Some(thread) = map_get_any(value, &["thread", "t"]) else {
        page.progress.observe(EnvelopeDisposition::Retry);
        return page;
    };
    if let Some(pagination) = map_get_any(thread, &["pagination", "p"]) {
        page.saw_pagination = true;
        page.next_cursor = map_get_any(pagination, &["next", "n"])
            .and_then(as_binary)
            .cloned();
        page.last = map_get_any(pagination, &["last", "l"])
            .and_then(as_bool)
            .unwrap_or(false);
    }
    let items: &[Value] = match map_get_any(thread, &["messages", "m"]) {
        Some(Value::Array(items)) => items,
        Some(Value::Nil) => &[],
        _ => {
            page.progress.observe(EnvelopeDisposition::Retry);
            return page;
        }
    };
    if items.is_empty() && page.next_cursor.is_some() {
        page.progress.observe(EnvelopeDisposition::Retry);
    }

    let permalink_base = extract_permalink_base(value);

    for item in items {
        page.progress.observe(history_envelope_disposition(item));
        let Some(valid) = extract_valid_message(item) else {
            continue;
        };
        let message_body = map_get_any(valid, &["messageBody", "b"]);
        let message_type = message_body.and_then(message_type_from_message_body);
        if message_type == Some(MESSAGE_TYPE_REACTION) {
            if let Some(reaction_delta) = parse_reaction_delta_from_thread(valid, conversation_id) {
                page.reaction_deltas.push(reaction_delta);
            }
            continue;
        }
        if let Some(mut message) = parse_thread_message(valid, conversation_id, message_type) {
            if message.permalink.is_empty()
                && let Some((ref tlf_name, ref topic_name)) = permalink_base
                && let Ok(msg_id) = message.id.0.parse::<i64>()
            {
                message.permalink = if topic_name == tlf_name {
                    format!("keybase://chat/{}/{}", tlf_name, msg_id)
                } else {
                    format!("keybase://chat/{}#{}/{}", tlf_name, topic_name, msg_id)
                };
            }
            page.reaction_deltas
                .extend(parse_reaction_deltas_for_target(
                    valid,
                    conversation_id,
                    &message.id,
                    extract_message_timestamp_ms(valid),
                ));
            if matches!(message.event, Some(ChatEvent::MessageDeleted { .. })) {
                page.deletions.push(message.clone());
            }
            page.messages.push(message);
        }
    }

    let reaction_op_ids: HashSet<String> = page
        .reaction_deltas
        .iter()
        .filter_map(|delta| delta.op_message_id.as_ref().map(|id| id.0.clone()))
        .collect();
    if !reaction_op_ids.is_empty() {
        page.messages.retain(|message| {
            if let Some(ChatEvent::MessageDeleted {
                target_message_id: Some(target),
            }) = &message.event
            {
                !reaction_op_ids.contains(&target.0)
            } else {
                true
            }
        });
    }

    page.messages
        .sort_by_key(|message| message.id.0.parse::<u64>().unwrap_or(0));
    page
}

fn page_may_have_more_older_messages(page: &ThreadPage, requested_page_size: usize) -> bool {
    if page.saw_pagination {
        return !page.last || page.next_cursor.is_some();
    }
    page.messages.len() >= requested_page_size
}

fn extract_reply_children_count(root: &Value) -> u32 {
    map_get_any(root, &["replies"])
        .and_then(as_array)
        .map(|values| values.len() as u32)
        .unwrap_or(0)
}

fn extract_reply_to_message_id(root: &Value, message_body: Option<&Value>) -> Option<MessageId> {
    map_get_any(root, &["replyTo"])
        .and_then(parse_reply_to_value)
        .or_else(|| {
            find_value_for_keys(root, &["replyTo", "reply_to", "replyto"], 0)
                .and_then(parse_reply_to_value)
        })
        .or_else(|| {
            let text_payload = message_body.and_then(|body| {
                message_variant_payload(body, MESSAGE_TYPE_TEXT)
                    .or_else(|| map_get_any(body, &["text", "t"]))
            })?;
            let reply_value = map_get_any(text_payload, &["replyTo", "r"])?;
            parse_reply_to_value(reply_value)
        })
}

fn parse_reply_to_value(value: &Value) -> Option<MessageId> {
    parse_message_id_from_value(value)
        .or_else(|| {
            map_get_any(value, &["messageID", "messageId", "m"])
                .and_then(parse_message_id_from_value)
        })
        .or_else(|| {
            map_get_any(value, &["id", "msgID", "msgId", "mid"])
                .and_then(parse_message_id_from_value)
        })
        .or_else(|| extract_message_id_from_unboxed_message(value))
}

fn extract_message_id_from_unboxed_message(value: &Value) -> Option<MessageId> {
    if matches!(value, Value::Nil) {
        return None;
    }
    let valid = map_get_any(value, &["valid", "v"])
        .or_else(|| map_get(value, "error"))
        .or_else(|| map_get(value, "placeholder"))
        .unwrap_or(value);
    map_get_any(valid, &["serverHeader", "s"])
        .and_then(|header| map_get_any(header, &["messageID", "messageId", "m"]))
        .or_else(|| map_get_any(valid, &["messageID", "messageId", "m"]))
        .and_then(parse_message_id_from_value)
}

fn parse_message_id_from_value(value: &Value) -> Option<MessageId> {
    as_i64(value)
        .map(|id| MessageId::new(id.to_string()))
        .or_else(|| as_str(value).and_then(parse_message_id_from_string))
}

fn parse_message_id_from_string(raw: &str) -> Option<MessageId> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        return Some(MessageId::new(trimmed.to_string()));
    }
    if let Some(parsed) = provider_message_ref_to_message_id(&ProviderMessageRef::new(trimmed)) {
        return Some(parsed);
    }
    let mut digits = String::new();
    for ch in trimmed.chars().rev() {
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else if !digits.is_empty() {
            break;
        }
    }
    if digits.is_empty() {
        return None;
    }
    let tail_digits = digits.chars().rev().collect::<String>();
    Some(MessageId::new(tail_digits))
}

fn parse_thread_message(
    valid: &Value,
    conversation_id: &ConversationId,
    message_type: Option<i64>,
) -> Option<MessageRecord> {
    let server_header = map_get_any(valid, &["serverHeader", "s"])?;
    let message_id =
        map_get_any(server_header, &["messageID", "messageId", "m"]).and_then(as_i64)?;
    let message_body = map_get_any(valid, &["messageBody", "b"]);
    let reply_to = extract_reply_to_message_id(valid, message_body);
    let thread_reply_count = extract_reply_children_count(server_header);
    let message_type =
        message_type.or_else(|| message_body.and_then(message_type_from_message_body));
    if is_unset_message_type_only(message_body) {
        return None;
    }
    if message_type == Some(MESSAGE_TYPE_REACTION) || message_type == Some(MESSAGE_TYPE_UNFURL) {
        return None;
    }
    let event = extract_chat_event(message_body, valid);
    let mut attachments = message_body
        .map(extract_message_attachments)
        .unwrap_or_default();
    apply_message_attachment_url_hints(valid, &mut attachments);
    let text_body = message_body
        .and_then(extract_text_body)
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty());
    let link_previews = extract_link_previews(valid, message_body, text_body.as_deref());
    if text_body.is_none() && attachments.is_empty() && is_unfurl_only_message(message_body) {
        return None;
    }
    let has_unfurl_payload = is_unfurl_only_message(message_body) || root_contains_unfurls(valid);
    if attachments.is_empty()
        && text_body
            .as_deref()
            .is_some_and(is_link_preview_placeholder_text)
        && has_unfurl_payload
    {
        return None;
    }
    let body = text_body
        .clone()
        .or_else(|| {
            attachments
                .is_empty()
                .then(|| message_body.and_then(extract_non_text_fallback_body))
                .flatten()
        })
        .or_else(|| {
            (!attachments.is_empty())
                .then(|| message_body.and_then(extract_attachment_caption))
                .flatten()
        })
        .or_else(|| find_body_string(valid, 0).map(|text| text.trim().to_string()))
        .filter(|text| !text.is_empty());
    let body = body.unwrap_or_else(|| default_body_for_message(message_body, &attachments));
    if event.is_none() && attachments.is_empty() && body.trim() == NON_TEXT_PLACEHOLDER_BODY {
        log_non_text_placeholder_once(conversation_id, message_id, message_type, message_body);
    }
    let decorated_body = extract_decorated_text_body(valid)
        .or_else(|| message_body.and_then(extract_decorated_text_body));
    let mention_metadata = parse_mention_metadata(valid, message_body);
    let emoji_source_refs = parse_emoji_source_refs(valid, message_body);
    let fragments = fragments_from_message_body(
        &body,
        decorated_body.as_deref(),
        Some(&mention_metadata),
        emoji_source_refs,
    );
    let author = map_get_any(valid, &["senderUsername", "su"])
        .and_then(as_str)
        .unwrap_or("unknown");
    let timestamp_ms =
        extract_message_timestamp_ms(server_header).or_else(|| extract_message_timestamp_ms(valid));
    let (record_id, edited) = if message_type == Some(MESSAGE_TYPE_EDIT) {
        if let Some(target_id) = message_body.and_then(extract_edit_target_message_id) {
            (
                target_id,
                Some(EditMeta {
                    edit_id: MessageId::new(message_id.to_string()),
                    edited_at_ms: timestamp_ms,
                }),
            )
        } else {
            (MessageId::new(message_id.to_string()), None)
        }
    } else {
        let edited = if is_message_superseded(server_header, valid) {
            let superseded_by =
                map_get_any(server_header, &["supersededBy", "superseded_by", "sb"])
                    .and_then(as_i64)
                    .filter(|&id| id > 0)
                    .map(|id| MessageId::new(id.to_string()))
                    .unwrap_or_else(|| MessageId::new(message_id.to_string()));
            Some(EditMeta {
                edit_id: superseded_by,
                edited_at_ms: None,
            })
        } else {
            None
        };
        (MessageId::new(message_id.to_string()), edited)
    };

    Some(MessageRecord {
        id: record_id,
        conversation_id: conversation_id.clone(),
        author_id: UserId::new(author),
        reply_to,
        thread_root_id: None,
        timestamp_ms,
        event,
        link_previews,
        permalink: build_keybase_permalink(valid, message_id).unwrap_or_default(),
        fragments,
        source_text: Some(body),
        attachments,
        reactions: Vec::new(),
        thread_reply_count,
        send_state: MessageSendState::Sent,
        edited,
    })
}

const KEYBASE_DECORATE_BEGIN: &str = "$>kb$";
const KEYBASE_DECORATE_END: &str = "$<kb$";

#[derive(Debug, Deserialize)]
struct KeybaseDecoratedSpan {
    typ: i64,
    #[serde(default)]
    atmention: Option<String>,
    #[serde(default)]
    channelnamemention: Option<KeybaseDecoratedChannelMention>,
    #[serde(default)]
    maybemention: Option<KeybaseDecoratedMaybeMention>,
    #[serde(default)]
    payment: Option<KeybaseDecoratedPayment>,
    #[serde(default)]
    link: Option<KeybaseDecoratedLink>,
    #[serde(default)]
    mailto: Option<KeybaseDecoratedLink>,
    #[serde(default)]
    kbfspath: Option<KeybaseDecoratedKbfsPath>,
    #[serde(default)]
    emoji: Option<KeybaseDecoratedEmoji>,
}

#[derive(Debug, Deserialize)]
struct KeybaseDecoratedChannelMention {
    name: String,
}

#[derive(Debug, Deserialize)]
struct KeybaseDecoratedMaybeMention {
    name: String,
    #[serde(default)]
    channel: String,
}

#[derive(Debug, Deserialize)]
struct KeybaseDecoratedPayment {
    #[serde(rename = "paymentText")]
    payment_text: Option<String>,
    #[serde(default)]
    username: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KeybaseDecoratedLink {
    url: String,
}

#[derive(Debug, Deserialize)]
struct KeybaseDecoratedKbfsPath {
    #[serde(rename = "rawPath")]
    raw_path: Option<String>,
    #[serde(rename = "standardPath")]
    standard_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KeybaseDecoratedEmoji {
    alias: String,
}

fn parse_decorated_body(decorated: &str) -> Vec<MessageFragment> {
    let mut fragments = Vec::new();
    let mut search_start = 0usize;
    let mut plain_start = 0usize;

    while let Some(relative_start) = decorated[search_start..].find(KEYBASE_DECORATE_BEGIN) {
        let marker_start = search_start + relative_start;
        if marker_is_escaped(decorated, marker_start) {
            search_start = marker_start + KEYBASE_DECORATE_BEGIN.len();
            continue;
        }

        push_text_fragment(&mut fragments, &decorated[plain_start..marker_start]);

        let payload_start = marker_start + KEYBASE_DECORATE_BEGIN.len();
        let Some(relative_end) = decorated[payload_start..].find(KEYBASE_DECORATE_END) else {
            push_text_fragment(&mut fragments, &decorated[marker_start..]);
            return fragments;
        };
        let payload_end = payload_start + relative_end;
        let marker_end = payload_end + KEYBASE_DECORATE_END.len();
        let payload = &decorated[payload_start..payload_end];

        let line_start = decorated[..marker_start]
            .rfind('\n')
            .map(|pos| pos + 1)
            .unwrap_or(0);
        let in_quote_line = decorated[line_start..].starts_with('>');
        if !push_decoration_fragment(&mut fragments, payload, in_quote_line) {
            push_text_fragment(&mut fragments, &decorated[marker_start..marker_end]);
        }

        plain_start = marker_end;
        search_start = marker_end;
    }

    push_text_fragment(&mut fragments, &decorated[plain_start..]);
    fragments
}

#[derive(Clone, Debug, Default)]
struct MentionParseMetadata {
    at_mentions: HashSet<String>,
    channel_name_mentions: HashSet<String>,
    channel_mention_enabled: bool,
    has_hints: bool,
}

impl MentionParseMetadata {
    fn has_hints(&self) -> bool {
        self.has_hints
    }
}

fn parse_emoji_source_refs(
    root: &Value,
    message_body: Option<&Value>,
) -> HashMap<String, VecDeque<EmojiSourceRef>> {
    let mut refs = HashMap::<String, VecDeque<EmojiSourceRef>>::new();
    let mut seen = HashSet::<String>::new();

    ingest_emoji_source_entries(
        map_get_any(root, &["text", "t"]).and_then(|text| map_get_any(text, &["emojis", "e"])),
        &mut refs,
        &mut seen,
    );
    ingest_emoji_source_entries(map_get_any(root, &["emojis", "e"]), &mut refs, &mut seen);
    ingest_emoji_source_entries(
        find_value_for_keys(root, &["emojis", "e"], 0),
        &mut refs,
        &mut seen,
    );

    ingest_emoji_source_entries(
        message_body
            .and_then(|body| map_get_any(body, &["text", "t"]))
            .and_then(|text| map_get_any(text, &["emojis", "e"])),
        &mut refs,
        &mut seen,
    );
    ingest_emoji_source_entries(
        message_body.and_then(|body| map_get_any(body, &["emojis", "e"])),
        &mut refs,
        &mut seen,
    );
    ingest_emoji_source_entries(
        message_body
            .and_then(|body| map_get_any(body, &["body", "b"]))
            .and_then(|payload| map_get_any(payload, &["text", "t"]))
            .and_then(|text| map_get_any(text, &["emojis", "e"])),
        &mut refs,
        &mut seen,
    );
    ingest_emoji_source_entries(
        message_body
            .and_then(|body| map_get_any(body, &["body", "b"]))
            .and_then(|payload| map_get_any(payload, &["emojis", "e"])),
        &mut refs,
        &mut seen,
    );
    ingest_emoji_source_entries(
        message_body
            .and_then(|body| find_value_for_keys(body, &["text", "t"], 0))
            .and_then(|text| map_get_any(text, &["emojis", "e"])),
        &mut refs,
        &mut seen,
    );
    ingest_emoji_source_entries(
        message_body.and_then(|body| find_value_for_keys(body, &["emojis", "e"], 0)),
        &mut refs,
        &mut seen,
    );

    refs
}

fn ingest_emoji_source_entries(
    emoji_entries: Option<&Value>,
    refs: &mut HashMap<String, VecDeque<EmojiSourceRef>>,
    seen: &mut HashSet<String>,
) {
    let Some(emojis) = emoji_entries.and_then(as_array) else {
        return;
    };
    for emoji in emojis {
        let alias = map_get_any(emoji, &["alias", "a"])
            .and_then(as_str)
            .map(str::trim)
            .unwrap_or("");
        if alias.is_empty() {
            continue;
        }
        let Some(source_ref) = emoji_source_ref_from_metadata_entry(emoji) else {
            continue;
        };
        let key = alias.to_ascii_lowercase();
        let dedupe_key = format!("{key}:{}", source_ref.ref_key);
        if !seen.insert(dedupe_key) {
            continue;
        }
        refs.entry(key).or_default().push_back(source_ref);
    }
}

fn merge_emoji_source_refs(
    into: &mut HashMap<String, VecDeque<EmojiSourceRef>>,
    additional: HashMap<String, VecDeque<EmojiSourceRef>>,
) {
    let mut seen = HashSet::<String>::new();
    for queue in into.values() {
        for source_ref in queue {
            seen.insert(source_ref.cache_key());
        }
    }
    for (alias, mut queue) in additional {
        let entry = into.entry(alias).or_default();
        while let Some(source_ref) = queue.pop_front() {
            if seen.insert(source_ref.cache_key()) {
                entry.push_back(source_ref);
            }
        }
    }
}

fn emoji_source_ref_from_metadata_entry(entry: &Value) -> Option<EmojiSourceRef> {
    let conv_value = find_value_for_keys(
        entry,
        &["convID", "convId", "conversationID", "conversationId"],
        0,
    )?;
    let raw_conversation_id = value_to_conversation_id_bytes(conv_value)
        .or_else(|| conversation_id_bytes_from_base64_string(conv_value))?;
    let message_id = find_value_for_keys(entry, &["messageID", "messageId", "msgID", "msgId"], 0)
        .and_then(value_to_i64_identifier)?;
    Some(EmojiSourceRef {
        backend_id: BackendId::new(KEYBASE_BACKEND_ID),
        ref_key: format!(
            "emoji:conv={}:msg={message_id}",
            hex_encode(&raw_conversation_id)
        ),
    })
}

fn conversation_id_bytes_from_base64_string(value: &Value) -> Option<Vec<u8>> {
    let encoded = as_str(value)?.trim();
    if encoded.is_empty() {
        return None;
    }
    BASE64_STANDARD.decode(encoded).ok()
}

fn value_to_i64_identifier(value: &Value) -> Option<i64> {
    as_i64(value).or_else(|| {
        as_str(value).and_then(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed.parse::<i64>().ok()
            }
        })
    })
}

fn attach_emoji_source_refs(
    fragments: &mut [MessageFragment],
    emoji_source_refs: &mut HashMap<String, VecDeque<EmojiSourceRef>>,
) {
    for fragment in fragments {
        let MessageFragment::Emoji { alias, source_ref } = fragment else {
            continue;
        };
        if source_ref.is_some() {
            continue;
        }
        let key = alias.to_ascii_lowercase();
        let Some(queue) = emoji_source_refs.get_mut(&key) else {
            continue;
        };
        if let Some(resolved) = queue.pop_front().or_else(|| queue.front().cloned()) {
            *source_ref = Some(resolved);
        }
    }
}

fn parse_mention_metadata(root: &Value, message_body: Option<&Value>) -> MentionParseMetadata {
    let mut metadata = MentionParseMetadata::default();
    let at_mentions_value = find_value_for_keys(root, &["atMentionUsernames", "atMentions"], 0)
        .or_else(|| {
            message_body.and_then(|body| {
                find_value_for_keys(body, &["atMentionUsernames", "atMentions"], 0)
            })
        });
    if at_mentions_value.is_some() {
        metadata.has_hints = true;
    }
    if let Some(at_mentions) = at_mentions_value.and_then(as_array) {
        for mention in at_mentions {
            if let Some(name) = as_str(mention).and_then(normalize_at_mention_key) {
                metadata.at_mentions.insert(name);
            }
        }
    }
    let user_mentions_value =
        message_body.and_then(|body| find_value_for_keys(body, &["userMentions", "um"], 0));
    if user_mentions_value.is_some() {
        metadata.has_hints = true;
    }
    if let Some(user_mentions) = user_mentions_value.and_then(as_array) {
        for mention in user_mentions {
            let value = map_get_any(mention, &["username", "text", "name", "u", "t"])
                .or_else(|| find_value_for_keys(mention, &["username", "text", "name"], 0));
            if let Some(name) = value.and_then(as_str).and_then(normalize_at_mention_key) {
                metadata.at_mentions.insert(name);
            }
        }
    }
    let channel_mentions_value =
        find_value_for_keys(root, &["channelNameMentions", "channelMentions"], 0).or_else(|| {
            message_body.and_then(|body| {
                find_value_for_keys(body, &["channelNameMentions", "channelMentions"], 0)
            })
        });
    if channel_mentions_value.is_some() {
        metadata.has_hints = true;
    }
    if let Some(channel_mentions) = channel_mentions_value.and_then(as_array) {
        for mention in channel_mentions {
            let value = as_str(mention)
                .or_else(|| map_get_any(mention, &["name", "topicName", "t"]).and_then(as_str))
                .or_else(|| {
                    find_value_for_keys(mention, &["name", "topicName"], 0).and_then(as_str)
                });
            if let Some(name) = value.and_then(normalize_channel_mention_key) {
                metadata.channel_name_mentions.insert(name);
            }
        }
    }
    let channel_mention_value = find_value_for_keys(root, &["channelMention"], 0).or_else(|| {
        message_body.and_then(|body| find_value_for_keys(body, &["channelMention"], 0))
    });
    if channel_mention_value.is_some() {
        metadata.has_hints = true;
    }
    metadata.channel_mention_enabled =
        channel_mention_value.and_then(parse_channel_mention_flag) == Some(true);
    metadata
}

fn normalize_at_mention_key(value: &str) -> Option<String> {
    let normalized = value.trim().trim_start_matches('@').trim();
    if normalized.is_empty() {
        return None;
    }
    Some(normalized.to_ascii_lowercase())
}

fn normalize_channel_mention_key(value: &str) -> Option<String> {
    let normalized = value.trim().trim_start_matches('#').trim();
    if normalized.is_empty() {
        return None;
    }
    Some(normalized.to_ascii_lowercase())
}

fn parse_channel_mention_flag(value: &Value) -> Option<bool> {
    if let Some(raw) = as_i64(value) {
        return Some(raw != 0);
    }
    if let Some(raw) = as_bool(value) {
        return Some(raw);
    }
    as_str(value).and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
        "none" | "0" => Some(false),
        "all" | "here" | "channel" | "everyone" | "1" | "2" => Some(true),
        _ => None,
    })
}

fn fragments_from_message_body(
    body: &str,
    decorated_body: Option<&str>,
    metadata: Option<&MentionParseMetadata>,
    mut emoji_source_refs: HashMap<String, VecDeque<EmojiSourceRef>>,
) -> Vec<MessageFragment> {
    let trimmed_body = body.trim();
    if let Some(decorated) = decorated_body
        && trimmed_body.is_empty()
        && !decorated.trim().is_empty()
    {
        // For attachment captions (and other cases where the canonical text is decorated),
        // prefer the decorated body even if it only parses as plain text.
        let mut parsed = parse_decorated_body(decorated);
        attach_emoji_source_refs(&mut parsed, &mut emoji_source_refs);
        return parsed;
    }
    if let Some(decorated) = decorated_body {
        let parsed = parse_decorated_body(decorated);
        if fragments_include_structured_spans(&parsed) {
            let mut parsed = parsed;
            attach_emoji_source_refs(&mut parsed, &mut emoji_source_refs);
            return parsed;
        }
    }
    if trimmed_body.is_empty() {
        return Vec::new();
    }
    if let Some(metadata) = metadata {
        let parsed = parse_metadata_mentions_from_text(body, metadata);
        if metadata.has_hints() || fragments_include_structured_spans(&parsed) {
            let mut parsed = parsed;
            attach_emoji_source_refs(&mut parsed, &mut emoji_source_refs);
            return parsed;
        }
    }
    let mut parsed = parse_plain_mentions_from_text(body);
    attach_emoji_source_refs(&mut parsed, &mut emoji_source_refs);
    parsed
}

fn fragments_include_structured_spans(fragments: &[MessageFragment]) -> bool {
    fragments.iter().any(|fragment| {
        !matches!(
            fragment,
            MessageFragment::Text(_)
                | MessageFragment::InlineCode(_)
                | MessageFragment::Code { .. }
                | MessageFragment::Quote(_)
        )
    })
}

fn parse_plain_mentions_from_text(body: &str) -> Vec<MessageFragment> {
    let bytes = body.as_bytes();
    let mut fragments = Vec::new();
    let mut index = 0usize;
    let mut plain_start = 0usize;

    while index < bytes.len() {
        let current = bytes[index];
        if current == b'`'
            && !is_on_quoted_line(body, index)
            && let Some(span_end) = parse_code_span_end(body, index)
        {
            index = span_end;
            continue;
        }
        if current == b'@'
            && !is_on_quoted_line(body, index)
            && mention_prefix_is_valid(body, index)
            && let Some((mention_name, span_end)) = parse_plain_mention_at(body, index)
        {
            push_text_fragment(&mut fragments, &body[plain_start..index]);
            if let Some(kind) = broadcast_kind_from_name(mention_name) {
                fragments.push(MessageFragment::BroadcastMention(kind));
            } else {
                fragments.push(MessageFragment::Mention(UserId::new(
                    mention_name.to_string(),
                )));
            }
            index = span_end;
            plain_start = span_end;
            continue;
        }
        if current == b':'
            && !is_on_quoted_line(body, index)
            && let Some((emoji_alias, span_end)) = parse_plain_emoji_shortcode_at(body, index)
        {
            push_text_fragment(&mut fragments, &body[plain_start..index]);
            fragments.push(MessageFragment::Emoji {
                alias: emoji_alias.to_string(),
                source_ref: None,
            });
            index = span_end;
            plain_start = span_end;
            continue;
        }

        let step = body[index..]
            .chars()
            .next()
            .map(|ch| ch.len_utf8())
            .unwrap_or(1);
        index += step;
    }

    push_text_fragment(&mut fragments, &body[plain_start..]);
    if fragments.is_empty() {
        fragments.push(MessageFragment::Text(body.to_string()));
    }
    fragments
}

fn parse_metadata_mentions_from_text(
    body: &str,
    metadata: &MentionParseMetadata,
) -> Vec<MessageFragment> {
    let bytes = body.as_bytes();
    let mut fragments = Vec::new();
    let mut index = 0usize;
    let mut plain_start = 0usize;

    while index < bytes.len() {
        let current = bytes[index];
        if current == b'`'
            && !is_on_quoted_line(body, index)
            && let Some(span_end) = parse_code_span_end(body, index)
        {
            index = span_end;
            continue;
        }
        if current == b'@'
            && !is_on_quoted_line(body, index)
            && mention_prefix_is_valid(body, index)
            && let Some((mention_name, span_end)) = parse_plain_mention_at(body, index)
        {
            let mention_key = normalize_at_mention_key(mention_name);
            let is_broadcast = metadata.channel_mention_enabled
                && mention_key
                    .as_deref()
                    .and_then(broadcast_kind_from_name)
                    .is_some();
            let is_known_mention = mention_key
                .as_deref()
                .map(|key| metadata.at_mentions.contains(key))
                .unwrap_or(false);
            if is_broadcast || is_known_mention {
                push_text_fragment(&mut fragments, &body[plain_start..index]);
                if let Some(kind) = mention_key.as_deref().and_then(broadcast_kind_from_name) {
                    fragments.push(MessageFragment::BroadcastMention(kind));
                } else {
                    fragments.push(MessageFragment::Mention(UserId::new(
                        mention_name.to_string(),
                    )));
                }
                index = span_end;
                plain_start = span_end;
                continue;
            }
        }
        if current == b'#'
            && !is_on_quoted_line(body, index)
            && mention_prefix_is_valid(body, index)
            && let Some((channel_name, span_end)) = parse_plain_channel_mention_at(body, index)
            && normalize_channel_mention_key(channel_name)
                .as_deref()
                .map(|name| metadata.channel_name_mentions.contains(name))
                .unwrap_or(false)
        {
            push_text_fragment(&mut fragments, &body[plain_start..index]);
            fragments.push(MessageFragment::ChannelMention {
                name: channel_name.to_string(),
            });
            index = span_end;
            plain_start = span_end;
            continue;
        }
        if current == b':'
            && !is_on_quoted_line(body, index)
            && let Some((emoji_alias, span_end)) = parse_plain_emoji_shortcode_at(body, index)
        {
            push_text_fragment(&mut fragments, &body[plain_start..index]);
            fragments.push(MessageFragment::Emoji {
                alias: emoji_alias.to_string(),
                source_ref: None,
            });
            index = span_end;
            plain_start = span_end;
            continue;
        }

        let step = body[index..]
            .chars()
            .next()
            .map(|ch| ch.len_utf8())
            .unwrap_or(1);
        index += step;
    }

    push_text_fragment(&mut fragments, &body[plain_start..]);
    if fragments.is_empty() {
        fragments.push(MessageFragment::Text(body.to_string()));
    }
    fragments
}

fn parse_plain_emoji_shortcode_at(body: &str, colon_index: usize) -> Option<(&str, usize)> {
    let start = colon_index + 1;
    if start >= body.len() {
        return None;
    }
    let first = body[start..].chars().next()?;
    if !is_plain_emoji_alias_char(first) {
        return None;
    }

    let mut end = start + first.len_utf8();
    while end < body.len() {
        let ch = body[end..].chars().next()?;
        if ch == ':' {
            let alias = &body[start..end];
            if alias.is_empty() {
                return None;
            }
            return Some((alias, end + ch.len_utf8()));
        }
        if !is_plain_emoji_alias_char(ch) {
            return None;
        }
        end += ch.len_utf8();
    }
    None
}

fn is_plain_emoji_alias_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '+' | '#')
}

fn is_on_quoted_line(body: &str, index: usize) -> bool {
    let line_start = body[..index].rfind('\n').map(|pos| pos + 1).unwrap_or(0);
    strip_quote_prefix(&body[line_start..]).is_some()
}

fn mention_prefix_is_valid(body: &str, at_index: usize) -> bool {
    if at_index == 0 {
        return true;
    }
    body[..at_index]
        .chars()
        .next_back()
        .map(|ch| {
            ch.is_whitespace()
                || matches!(
                    ch,
                    '(' | '[' | '/' | '{' | ':' | ';' | '.' | ',' | '!' | '?' | '"' | '\''
                )
        })
        .unwrap_or(true)
}

fn parse_plain_mention_at(body: &str, at_index: usize) -> Option<(&str, usize)> {
    let start = at_index + 1;
    if start >= body.len() {
        return None;
    }
    let first = body[start..].chars().next()?;
    if !first.is_ascii_alphanumeric() {
        return None;
    }

    let mut end = start + first.len_utf8();
    while end < body.len() {
        let ch = body[end..].chars().next()?;
        if is_plain_mention_char(ch) {
            end += ch.len_utf8();
        } else {
            break;
        }
    }

    let mention = body[start..end].trim_end_matches('.');
    let trimmed_end = start + mention.len();
    if mention.is_empty() {
        return None;
    }
    Some((mention, trimmed_end))
}

fn is_plain_mention_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '#' | '-')
}

fn parse_plain_channel_mention_at(body: &str, hash_index: usize) -> Option<(&str, usize)> {
    let start = hash_index + 1;
    if start >= body.len() {
        return None;
    }
    let first = body[start..].chars().next()?;
    if !first.is_ascii_alphanumeric() {
        return None;
    }

    let mut end = start + first.len_utf8();
    while end < body.len() {
        let ch = body[end..].chars().next()?;
        if is_plain_channel_mention_char(ch) {
            end += ch.len_utf8();
        } else {
            break;
        }
    }

    let channel = body[start..end].trim_end_matches('.');
    let trimmed_end = start + channel.len();
    if channel.is_empty() {
        return None;
    }
    Some((channel, trimmed_end))
}

fn is_plain_channel_mention_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')
}

fn marker_is_escaped(value: &str, marker_start: usize) -> bool {
    let bytes = value.as_bytes();
    let mut slash_count = 0usize;
    let mut cursor = marker_start;
    while cursor > 0 && bytes[cursor - 1] == b'\\' {
        slash_count += 1;
        cursor -= 1;
    }
    slash_count % 2 == 1
}

fn push_decoration_fragment(
    fragments: &mut Vec<MessageFragment>,
    encoded_payload: &str,
    in_quote_line: bool,
) -> bool {
    let Ok(decoded_bytes) = BASE64_STANDARD.decode(encoded_payload) else {
        return false;
    };
    let Ok(span) = serde_json::from_slice::<KeybaseDecoratedSpan>(&decoded_bytes) else {
        return false;
    };

    if in_quote_line {
        // Quote lines are treated as literal text: do not create structured fragments.
        // Instead, append the best-effort original textual representation.
        let mut as_text = None::<String>;
        match span.typ {
            // payment
            0 => {
                if let Some(payment_text) = span
                    .payment
                    .as_ref()
                    .and_then(|payment| payment.payment_text.as_deref())
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                {
                    as_text = Some(payment_text.to_string());
                } else if let Some(username) = span
                    .payment
                    .as_ref()
                    .and_then(|payment| payment.username.as_deref())
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                {
                    as_text = Some(format!("@{username}"));
                }
            }
            // @mention
            1 => {
                if let Some(name) = span
                    .atmention
                    .as_deref()
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                {
                    let name = name.trim_start_matches('@').trim();
                    if !name.is_empty() {
                        as_text = Some(format!("@{name}"));
                    }
                }
            }
            // #channel mention
            2 => {
                if let Some(name) = span
                    .channelnamemention
                    .as_ref()
                    .map(|mention| mention.name.trim())
                    .filter(|text| !text.is_empty())
                {
                    as_text = Some(format!("#{name}"));
                }
            }
            // maybe mention
            3 => {
                if let Some(maybe) = span.maybemention.as_ref() {
                    let name = maybe.name.trim();
                    if !name.is_empty() {
                        let channel = maybe.channel.trim();
                        if channel.is_empty() {
                            as_text = Some(format!("@{name}"));
                        } else {
                            as_text = Some(format!("@{name}#{channel}"));
                        }
                    }
                }
            }
            // link
            4 => {
                if let Some(url) = span
                    .link
                    .as_ref()
                    .map(|link| link.url.trim())
                    .filter(|text| !text.is_empty())
                {
                    as_text = Some(url.to_string());
                }
            }
            // mailto
            5 => {
                if let Some(raw_url) = span
                    .mailto
                    .as_ref()
                    .map(|link| link.url.trim())
                    .filter(|text| !text.is_empty())
                {
                    let display = raw_url.strip_prefix("mailto:").unwrap_or(raw_url);
                    as_text = Some(display.to_string());
                }
            }
            // kbfs path
            6 => {
                let path = span
                    .kbfspath
                    .as_ref()
                    .and_then(|path| path.standard_path.as_deref().or(path.raw_path.as_deref()))
                    .map(str::trim)
                    .filter(|text| !text.is_empty());
                if let Some(path) = path {
                    as_text = Some(path.to_string());
                }
            }
            // emoji
            7 => {
                if let Some(alias) = span
                    .emoji
                    .as_ref()
                    .map(|emoji| emoji.alias.trim())
                    .filter(|text| !text.is_empty())
                {
                    as_text = Some(format!(":{alias}:"));
                }
            }
            _ => {}
        }

        if let Some(text) = as_text {
            push_plain_text_fragment(fragments, &text);
            return true;
        }
        return false;
    }

    match span.typ {
        0 => {
            if let Some(payment_text) = span
                .payment
                .as_ref()
                .and_then(|payment| payment.payment_text.as_deref())
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                push_text_fragment(fragments, payment_text);
                return true;
            }
            if let Some(username) = span
                .payment
                .as_ref()
                .and_then(|payment| payment.username.as_deref())
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                push_text_fragment(fragments, &format!("@{username}"));
                return true;
            }
            false
        }
        1 => {
            let Some(mention_name) = span
                .atmention
                .as_deref()
                .map(str::trim)
                .filter(|text| !text.is_empty())
            else {
                return false;
            };
            let mention_name = mention_name.trim_start_matches('@').trim();
            if mention_name.is_empty() {
                return false;
            }
            if let Some(kind) = broadcast_kind_from_name(mention_name) {
                fragments.push(MessageFragment::BroadcastMention(kind));
                return true;
            }
            fragments.push(MessageFragment::Mention(UserId::new(
                mention_name.to_string(),
            )));
            true
        }
        2 => {
            let Some(channel_name) = span
                .channelnamemention
                .as_ref()
                .map(|mention| mention.name.trim())
                .filter(|text| !text.is_empty())
            else {
                return false;
            };
            fragments.push(MessageFragment::ChannelMention {
                name: channel_name.to_string(),
            });
            true
        }
        3 => {
            let Some(maybe_name) = span
                .maybemention
                .as_ref()
                .map(|mention| mention.name.trim())
                .filter(|text| !text.is_empty())
            else {
                return false;
            };
            let maybe_channel = span
                .maybemention
                .as_ref()
                .map(|mention| mention.channel.trim())
                .unwrap_or_default();
            if maybe_channel.is_empty() {
                push_text_fragment(fragments, &format!("@{maybe_name}"));
            } else {
                push_text_fragment(fragments, &format!("@{maybe_name}#{maybe_channel}"));
            }
            true
        }
        4 => {
            let Some(url) = span
                .link
                .as_ref()
                .map(|link| link.url.trim())
                .filter(|text| !text.is_empty())
            else {
                return false;
            };
            fragments.push(MessageFragment::Link {
                url: url.to_string(),
                display: url.to_string(),
            });
            true
        }
        5 => {
            let Some(raw_url) = span
                .mailto
                .as_ref()
                .map(|link| link.url.trim())
                .filter(|text| !text.is_empty())
            else {
                return false;
            };
            let url = if raw_url.starts_with("mailto:") {
                raw_url.to_string()
            } else {
                format!("mailto:{raw_url}")
            };
            let display = raw_url.strip_prefix("mailto:").unwrap_or(raw_url);
            fragments.push(MessageFragment::Link {
                url,
                display: display.to_string(),
            });
            true
        }
        6 => {
            let path = span
                .kbfspath
                .as_ref()
                .and_then(|path| path.standard_path.as_deref().or(path.raw_path.as_deref()))
                .map(str::trim)
                .filter(|text| !text.is_empty());
            if let Some(path) = path {
                push_text_fragment(fragments, path);
                return true;
            }
            false
        }
        7 => {
            let Some(alias) = span
                .emoji
                .as_ref()
                .map(|emoji| emoji.alias.trim())
                .filter(|text| !text.is_empty())
            else {
                return false;
            };
            fragments.push(MessageFragment::Emoji {
                alias: alias.to_string(),
                source_ref: None,
            });
            true
        }
        _ => false,
    }
}

fn broadcast_kind_from_name(name: &str) -> Option<BroadcastKind> {
    match name.trim().to_ascii_lowercase().as_str() {
        "here" => Some(BroadcastKind::Here),
        "channel" | "everyone" => Some(BroadcastKind::All),
        _ => None,
    }
}

fn extract_permalink_base(payload: &Value) -> Option<(String, String)> {
    let conversation = map_get_any(
        payload,
        &["conversation", "conv", "chatConversation", "chatConv"],
    )
    .or_else(|| map_get_any(payload, &["info", "i"]))
    .unwrap_or(payload);
    let tlf_name = map_get_any(conversation, &["tlfName", "n"])
        .or_else(|| map_get_any(payload, &["tlfName"]))
        .and_then(as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let topic_name = map_get_any(conversation, &["topicName", "t"])
        .or_else(|| map_get_any(payload, &["topicName"]))
        .and_then(as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(tlf_name);
    Some((tlf_name.to_string(), topic_name.to_string()))
}

fn build_keybase_permalink(payload: &Value, message_id: i64) -> Option<String> {
    let (tlf_name, topic_name) = extract_permalink_base(payload)?;
    if topic_name == tlf_name {
        Some(format!("keybase://chat/{}/{}", tlf_name, message_id))
    } else {
        Some(format!(
            "keybase://chat/{}#{}/{}",
            tlf_name, topic_name, message_id
        ))
    }
}

fn extract_valid_message(value: &Value) -> Option<&Value> {
    if let Some(valid) = map_get_any(value, &["valid", "v"]) {
        return Some(valid);
    }

    let state = map_get_any(value, &["state", "s"]).and_then(as_i64)?;
    if state != 0 {
        return None;
    }
    map_get_any(value, &["valid", "v"])
}

fn extract_decorated_text_body(value: &Value) -> Option<String> {
    let candidate = map_get_any(value, &["decoratedTextBody", "dtb"])
        .or_else(|| find_value_for_keys(value, &["decoratedTextBody", "dtb"], 0))?;
    extract_optional_string(candidate)
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

fn extract_optional_string(value: &Value) -> Option<String> {
    if let Some(text) = as_str(value) {
        return Some(text.to_string());
    }
    if matches!(value, Value::Nil) {
        return None;
    }
    if let Some(inner) = map_get_any(value, &["string", "s", "text", "t", "value", "v"]) {
        return as_str(inner).map(str::to_string);
    }
    None
}

fn extract_text_body(message_body: &Value) -> Option<String> {
    if let Some(text_body) =
        map_get_any(message_body, &["text", "t"]).and_then(|text| map_get_any(text, &["body", "b"]))
    {
        return as_str(text_body).map(ToString::to_string);
    }

    if let Some(payload) = map_get_any(message_body, &["body", "b"]) {
        if let Some(text_body) =
            map_get_any(payload, &["text", "t"]).and_then(|text| map_get_any(text, &["body", "b"]))
        {
            return as_str(text_body).map(ToString::to_string);
        }
        if let Some(edit_body) =
            map_get_any(payload, &["edit", "e"]).and_then(|edit| map_get_any(edit, &["body", "b"]))
        {
            return as_str(edit_body).map(ToString::to_string);
        }
        if let Some(reaction_body) = map_get_any(payload, &["reaction", "r"])
            .and_then(|reaction| map_get_any(reaction, &["body", "b"]))
        {
            return as_str(reaction_body).map(ToString::to_string);
        }
    }

    if let Some(edit_body) =
        map_get_any(message_body, &["edit", "e"]).and_then(|edit| map_get_any(edit, &["body", "b"]))
    {
        return as_str(edit_body).map(ToString::to_string);
    }

    if let Some(reaction_body) = map_get_any(message_body, &["reaction", "r"])
        .and_then(|reaction| map_get_any(reaction, &["body", "b"]))
    {
        return as_str(reaction_body).map(ToString::to_string);
    }

    find_body_string(message_body, 0)
}

fn extract_message_attachments(message_body: &Value) -> Vec<AttachmentSummary> {
    let mut attachments = Vec::new();
    for key in [
        "attachment",
        "a",
        "attachmentuploaded",
        "attachmentUploaded",
        "au",
    ] {
        let Some(container) = map_get_any(message_body, &[key]) else {
            continue;
        };
        if let Some(attachment) = asset_to_attachment(container) {
            attachments.push(attachment);
        }
    }
    attachments.sort_by(|left, right| left.name.cmp(&right.name));
    attachments.dedup_by(|left, right| left.name == right.name);
    attachments
}

fn extract_attachment_caption(message_body: &Value) -> Option<String> {
    for key in [
        "attachment",
        "a",
        "attachmentuploaded",
        "attachmentUploaded",
        "au",
    ] {
        let Some(container) = map_get_any(message_body, &[key]) else {
            continue;
        };
        let asset = map_get_any(container, &["object", "o"]).unwrap_or(container);
        let filename = map_get_any(asset, &["filename", "f"])
            .and_then(as_str)
            .map(str::trim)
            .unwrap_or("");
        let title = map_get_any(asset, &["title", "t"])
            .and_then(as_str)
            .map(str::trim)
            .unwrap_or("");
        if title.is_empty() {
            continue;
        }
        // Keybase often sets title == filename; only treat it as a caption when it differs.
        if !filename.is_empty() && title == filename {
            continue;
        }
        return Some(title.to_string());
    }
    None
}

fn apply_message_attachment_url_hints(root: &Value, attachments: &mut [AttachmentSummary]) {
    if attachments.is_empty() {
        return;
    }
    let Some(asset_url_info) = find_value_for_keys(root, &["assetUrlInfo", "asset_url_info"], 0)
    else {
        return;
    };
    let preview_source =
        attachment_source_from_value(asset_url_info, &["previewUrl", "previewURL", "preview_url"]);
    let full_source =
        attachment_source_from_value(asset_url_info, &["fullUrl", "fullURL", "full_url"]);
    if preview_source.is_none() && full_source.is_none() {
        return;
    }
    for attachment in attachments {
        if attachment.preview.is_none()
            && let Some(preview_source) = preview_source.clone().or_else(|| full_source.clone())
        {
            attachment.preview = Some(AttachmentPreview {
                source: preview_source,
                width: attachment.width,
                height: attachment.height,
            });
        }
        if attachment.source.is_none() {
            attachment.source = full_source.clone().or_else(|| preview_source.clone());
        }
    }
}

fn asset_to_attachment(value: &Value) -> Option<AttachmentSummary> {
    let asset = map_get_any(value, &["object", "o"]).unwrap_or(value);
    let filename = map_get_any(asset, &["filename", "f"])
        .and_then(as_str)
        .map(str::trim)
        .unwrap_or("");
    let title = map_get_any(asset, &["title", "t"])
        .and_then(as_str)
        .map(str::trim)
        .unwrap_or("");
    let name = if !filename.is_empty() {
        filename
    } else if !title.is_empty() {
        title
    } else {
        return None;
    };
    let mime = map_get_any(asset, &["mimeType", "mime", "m"])
        .and_then(as_str)
        .map(str::trim)
        .unwrap_or("");
    let size_bytes = map_get_any(asset, &["size", "s"])
        .and_then(as_i64)
        .unwrap_or(0)
        .max(0) as u64;
    let mime_type = (!mime.is_empty()).then(|| mime.to_string());
    let (width, height) = attachment_dimensions_from_asset(asset);
    Some(AttachmentSummary {
        name: name.to_string(),
        kind: attachment_kind(name, mime),
        mime_type,
        size_bytes,
        width,
        height,
        preview: extract_attachment_preview(value),
        duration_ms: attachment_duration_ms_from_asset(asset),
        waveform: attachment_waveform_from_asset(asset),
        source: attachment_source_from_container(value)
            .or_else(|| attachment_source_from_asset(asset)),
    })
}

fn attachment_kind(name: &str, mime: &str) -> AttachmentKind {
    let mime = mime.to_ascii_lowercase();
    if mime.starts_with("image/") {
        return AttachmentKind::Image;
    }
    if mime.starts_with("video/") {
        return AttachmentKind::Video;
    }
    if mime.starts_with("audio/") {
        return AttachmentKind::Audio;
    }
    let lower_name = name.to_ascii_lowercase();
    if lower_name.ends_with(".png")
        || lower_name.ends_with(".jpg")
        || lower_name.ends_with(".jpeg")
        || lower_name.ends_with(".gif")
        || lower_name.ends_with(".webp")
        || lower_name.ends_with(".svg")
    {
        AttachmentKind::Image
    } else if lower_name.ends_with(".mp4")
        || lower_name.ends_with(".mov")
        || lower_name.ends_with(".avi")
        || lower_name.ends_with(".mkv")
        || lower_name.ends_with(".webm")
    {
        AttachmentKind::Video
    } else if lower_name.ends_with(".mp3")
        || lower_name.ends_with(".wav")
        || lower_name.ends_with(".m4a")
        || lower_name.ends_with(".ogg")
    {
        AttachmentKind::Audio
    } else {
        AttachmentKind::File
    }
}

fn extract_attachment_preview(container: &Value) -> Option<AttachmentPreview> {
    if let Some(source) = attachment_source_from_value(
        container,
        &[
            "previewURL",
            "previewUrl",
            "thumbnailURL",
            "thumbnailUrl",
            "posterURL",
            "posterUrl",
            "url",
            "u",
        ],
    ) {
        let width = map_get_any(
            container,
            &[
                "previewWidth",
                "preview_width",
                "thumbnailWidth",
                "thumbnail_width",
                "width",
                "w",
            ],
        )
        .and_then(value_to_u32);
        let height = map_get_any(
            container,
            &[
                "previewHeight",
                "preview_height",
                "thumbnailHeight",
                "thumbnail_height",
                "height",
                "h",
            ],
        )
        .and_then(value_to_u32);
        return Some(AttachmentPreview {
            source,
            width,
            height,
        });
    }

    let preview_asset = map_get_any(container, &["preview", "p"]).or_else(|| {
        map_get_any(container, &["previews", "ps"])
            .and_then(as_array)
            .and_then(|items| items.first())
    })?;
    let preview_asset = map_get_any(preview_asset, &["object", "o"]).unwrap_or(preview_asset);
    let source = attachment_source_from_asset(preview_asset)?;
    let (width, height) = attachment_dimensions_from_asset(preview_asset);
    Some(AttachmentPreview {
        source,
        width,
        height,
    })
}

fn attachment_source_from_container(container: &Value) -> Option<AttachmentSource> {
    attachment_source_from_value(
        container,
        &[
            "fileURL",
            "fileUrl",
            "downloadURL",
            "downloadUrl",
            "url",
            "u",
            "localPath",
            "local_path",
            "filePath",
            "file_path",
            "file",
        ],
    )
}

fn attachment_source_from_asset(asset: &Value) -> Option<AttachmentSource> {
    if let Some(source) = attachment_source_from_value(
        asset,
        &[
            "url",
            "u",
            "fileURL",
            "fileUrl",
            "downloadURL",
            "downloadUrl",
            "previewURL",
            "previewUrl",
            "localPath",
            "local_path",
            "filePath",
            "file_path",
            "file",
        ],
    ) {
        return Some(source);
    }

    if let Some(location) = map_get_any(asset, &["location", "l"])
        && let Some(source) = attachment_source_from_value(
            location,
            &["url", "u", "file", "path", "localPath", "local_path"],
        )
    {
        return Some(source);
    }

    None
}

fn attachment_source_from_value(value: &Value, keys: &[&str]) -> Option<AttachmentSource> {
    map_get_any(value, keys)
        .and_then(as_str)
        .and_then(attachment_source_from_string)
}

fn attachment_source_from_string(raw: &str) -> Option<AttachmentSource> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(local_path) = file_url_to_local_path(value) {
        return Some(AttachmentSource::LocalPath(local_path));
    }
    if value.starts_with("http://") || value.starts_with("https://") {
        return Some(AttachmentSource::Url(value.to_string()));
    }
    if (value.starts_with('/') || value.starts_with("./") || value.starts_with("../"))
        && Path::new(value).exists()
    {
        return Some(AttachmentSource::LocalPath(value.to_string()));
    }
    None
}

fn attachment_dimensions_from_asset(asset: &Value) -> (Option<u32>, Option<u32>) {
    if let Some(metadata) = map_get_any(asset, &["metadata", "md"]) {
        let dimensions = attachment_dimensions_from_metadata(metadata);
        if dimensions.0.is_some() || dimensions.1.is_some() {
            return dimensions;
        }
    }
    (
        map_get_any(asset, &["width", "w"]).and_then(value_to_u32),
        map_get_any(asset, &["height", "h"]).and_then(value_to_u32),
    )
}

fn attachment_dimensions_from_metadata(metadata: &Value) -> (Option<u32>, Option<u32>) {
    let video = map_get_any(metadata, &["video", "v"]);
    let image = map_get_any(metadata, &["image", "i"]);
    let width = video
        .and_then(|video| map_get_any(video, &["width", "w", "videoWidth", "video_width"]))
        .and_then(value_to_u32)
        .or_else(|| {
            image
                .and_then(|image| map_get_any(image, &["width", "w", "imageWidth", "image_width"]))
                .and_then(value_to_u32)
        })
        .or_else(|| map_get_any(metadata, &["width", "w"]).and_then(value_to_u32));
    let height = video
        .and_then(|video| map_get_any(video, &["height", "h", "videoHeight", "video_height"]))
        .and_then(value_to_u32)
        .or_else(|| {
            image
                .and_then(|image| {
                    map_get_any(image, &["height", "h", "imageHeight", "image_height"])
                })
                .and_then(value_to_u32)
        })
        .or_else(|| map_get_any(metadata, &["height", "h"]).and_then(value_to_u32));
    (width, height)
}

fn attachment_duration_ms_from_asset(asset: &Value) -> Option<u64> {
    let metadata = map_get_any(asset, &["metadata", "md"])?;
    let video = map_get_any(metadata, &["video", "v"])?;
    map_get_any(video, &["durationMs", "duration_ms", "duration", "d"]).and_then(value_to_u64)
}

fn attachment_is_audio_asset(asset: &Value) -> bool {
    map_get_any(asset, &["metadata", "md"])
        .and_then(|metadata| map_get_any(metadata, &["video", "v"]))
        .and_then(|video| map_get_any(video, &["isAudio", "is_audio"]))
        .and_then(value_to_bool)
        .unwrap_or(false)
}

fn attachment_waveform_from_asset(asset: &Value) -> Option<Vec<f32>> {
    if !attachment_is_audio_asset(asset) {
        return None;
    }
    let metadata = map_get_any(asset, &["metadata", "md"])?;
    let image = map_get_any(metadata, &["image", "i"])?;
    let amps = map_get_any(image, &["audioAmps", "audio_amps", "amps"])?;
    let waveform = as_array(amps)?
        .iter()
        .filter_map(value_to_f32)
        .collect::<Vec<_>>();
    if waveform.is_empty() {
        None
    } else {
        Some(waveform)
    }
}

fn extract_chat_event(message_body: Option<&Value>, root: &Value) -> Option<ChatEvent> {
    let message_body = message_body?;
    let message_type = message_type_from_message_body(message_body)?;
    match message_type {
        MESSAGE_TYPE_JOIN => Some(
            extract_member_change_event(message_body, MESSAGE_TYPE_JOIN, true)
                .unwrap_or(ChatEvent::MemberJoined),
        ),
        MESSAGE_TYPE_LEAVE => Some(
            extract_member_change_event(message_body, MESSAGE_TYPE_LEAVE, false)
                .unwrap_or(ChatEvent::MemberLeft),
        ),
        MESSAGE_TYPE_DELETE => Some(ChatEvent::MessageDeleted {
            target_message_id: extract_delete_target_message_id(message_body),
        }),
        MESSAGE_TYPE_DELETE_HISTORY => Some(ChatEvent::HistoryCleared),
        MESSAGE_TYPE_HEADLINE => Some(ChatEvent::DescriptionChanged {
            description: extract_headline_description(message_body, root),
        }),
        MESSAGE_TYPE_METADATA => extract_metadata_channel_name(message_body)
            .map(|new_name| ChatEvent::ChannelRenamed { new_name })
            .or_else(|| {
                Some(ChatEvent::Other {
                    text: "changed the channel name".to_string(),
                })
            }),
        MESSAGE_TYPE_PIN => Some(ChatEvent::MessagePinned {
            target_message_id: extract_pin_target_message_id(message_body),
        }),
        MESSAGE_TYPE_SYSTEM => extract_system_subtype(message_body, root),
        _ => None,
    }
}

fn extract_member_change_event(
    message_body: &Value,
    message_type: i64,
    prefer_join: bool,
) -> Option<ChatEvent> {
    let variant_keys = if message_type == MESSAGE_TYPE_JOIN {
        &["join", "j"][..]
    } else if message_type == MESSAGE_TYPE_LEAVE {
        &["leave", "l"][..]
    } else {
        &[][..]
    };
    let payload = message_variant_payload(message_body, message_type)
        .or_else(|| map_get_any(message_body, variant_keys))?;
    let mut joiners = usernames_from_payload_list(payload, &["joiners", "j"]);
    let mut leavers = usernames_from_payload_list(payload, &["leavers", "l"]);
    dedup_usernames(&mut joiners);
    dedup_usernames(&mut leavers);

    if !joiners.is_empty() && leavers.is_empty() {
        return if joiners.len() == 1 {
            Some(ChatEvent::MemberJoined)
        } else {
            Some(ChatEvent::MembersAdded {
                user_ids: usernames_to_user_ids(joiners),
                role: None,
            })
        };
    }
    if joiners.is_empty() && !leavers.is_empty() {
        return if leavers.len() == 1 {
            Some(ChatEvent::MemberLeft)
        } else {
            Some(ChatEvent::MembersRemoved {
                user_ids: usernames_to_user_ids(leavers),
            })
        };
    }
    if !joiners.is_empty() && !leavers.is_empty() {
        let text = format!(
            "{} joined, {} left",
            member_subject(&joiners),
            member_subject(&leavers)
        );
        return Some(ChatEvent::Other { text });
    }

    if prefer_join {
        Some(ChatEvent::MemberJoined)
    } else {
        Some(ChatEvent::MemberLeft)
    }
}

fn extract_delete_target_message_id(message_body: &Value) -> Option<MessageId> {
    let payload = message_variant_payload(message_body, MESSAGE_TYPE_DELETE)
        .or_else(|| map_get_any(message_body, &["delete", "d"]))?;
    map_get_any(
        payload,
        &["messageIDs", "messageIds", "m", "messageID", "messageId"],
    )
    .and_then(first_message_id_from_value)
}

fn extract_edit_target_message_id(message_body: &Value) -> Option<MessageId> {
    let payload = message_variant_payload(message_body, MESSAGE_TYPE_EDIT)
        .or_else(|| map_get_any(message_body, &["edit", "e"]))?;
    map_get_any(payload, &["messageID", "messageId", "m"])
        .and_then(as_i64)
        .map(|id| MessageId::new(id.to_string()))
}

fn is_message_superseded(server_header: &Value, root: &Value) -> bool {
    if let Some(id) =
        map_get_any(server_header, &["supersededBy", "superseded_by", "sb"]).and_then(as_i64)
        && id > 0
    {
        return true;
    }
    if let Some(flag) = map_get_any(root, &["superseded"]).and_then(as_bool) {
        return flag;
    }
    map_get_any(root, &["supersedes"])
        .and_then(as_i64)
        .is_some_and(|id| id > 0)
}

fn extract_headline_description(message_body: &Value, root: &Value) -> Option<String> {
    let headline = message_variant_payload(message_body, MESSAGE_TYPE_HEADLINE)
        .or_else(|| map_get_any(message_body, &["headline", "h"]))
        .and_then(|payload| map_get_any(payload, &["headline", "h", "body", "b"]))
        .and_then(as_str)
        .map(str::trim)
        .map(str::to_string)
        .or_else(|| {
            extract_decorated_text_body(root)
                .map(|text| text.trim().to_string())
                .filter(|text| !text.is_empty())
        });
    headline.filter(|text| !text.is_empty())
}

fn extract_metadata_channel_name(message_body: &Value) -> Option<String> {
    message_variant_payload(message_body, MESSAGE_TYPE_METADATA)
        .or_else(|| map_get_any(message_body, &["metadata"]))
        .and_then(|payload| map_get_any(payload, &["conversationTitle", "title", "t"]))
        .and_then(as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
}

fn extract_pin_target_message_id(message_body: &Value) -> Option<MessageId> {
    let payload = message_variant_payload(message_body, MESSAGE_TYPE_PIN)
        .or_else(|| map_get_any(message_body, &["pin"]))?;
    map_get_any(payload, &["msgID", "msgId", "messageID", "messageId", "m"])
        .and_then(as_i64)
        .map(|id| MessageId::new(id.to_string()))
}

fn extract_system_subtype(message_body: &Value, _root: &Value) -> Option<ChatEvent> {
    let payload = message_variant_payload(message_body, MESSAGE_TYPE_SYSTEM)
        .or_else(|| map_get_any(message_body, &["system", "s"]))?;
    let system_type = map_get_any(payload, &["systemType", "st"])
        .and_then(as_i64)
        .or_else(|| {
            map_get_any(payload, &["type"])
                .and_then(as_str)
                .and_then(message_system_type_from_name)
        })
        .or_else(|| infer_system_type_from_payload(payload))?;

    match system_type {
        MESSAGE_SYSTEM_TYPE_ADDED_TO_TEAM => {
            let details = map_get_any(payload, &["addedtoteam", "at"]).unwrap_or(payload);
            let mut usernames = usernames_from_payload_list(details, &["bulkAdds", "bulkadds"]);
            if let Some(addee) = map_get_any(details, &["addee", "a"])
                .and_then(as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                usernames.push(addee.to_string());
            }
            dedup_usernames(&mut usernames);
            let role = map_get_any(details, &["role", "r"]).and_then(team_role_label_from_value);
            Some(ChatEvent::MembersAdded {
                user_ids: usernames_to_user_ids(usernames),
                role,
            })
        }
        MESSAGE_SYSTEM_TYPE_INVITE_ADDED_TO_TEAM => {
            let details = map_get_any(payload, &["inviteaddedtoteam", "iat"]).unwrap_or(payload);
            let mut usernames = Vec::new();
            if let Some(invitee) = map_get_any(details, &["invitee", "i"])
                .and_then(as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                usernames.push(invitee.to_string());
            }
            if let Some(addee) = map_get_any(details, &["addee", "a"])
                .and_then(as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                usernames.push(addee.to_string());
            }
            dedup_usernames(&mut usernames);
            let role = map_get_any(details, &["role", "r"]).and_then(team_role_label_from_value);
            Some(ChatEvent::MembersAdded {
                user_ids: usernames_to_user_ids(usernames),
                role,
            })
        }
        MESSAGE_SYSTEM_TYPE_BULK_ADD_TO_CONV => {
            let details = map_get_any(payload, &["bulkaddtoconv", "bac"]).unwrap_or(payload);
            let mut usernames = usernames_from_payload_list(details, &["usernames", "u"]);
            dedup_usernames(&mut usernames);
            Some(ChatEvent::MembersAdded {
                user_ids: usernames_to_user_ids(usernames),
                role: None,
            })
        }
        MESSAGE_SYSTEM_TYPE_CHANGE_AVATAR => Some(ChatEvent::AvatarChanged),
        MESSAGE_SYSTEM_TYPE_CHANGE_RETENTION => {
            let details = map_get_any(payload, &["changeretention", "cr"]).unwrap_or(payload);
            Some(ChatEvent::RetentionChanged {
                summary: retention_summary_from_payload(details),
            })
        }
        MESSAGE_SYSTEM_TYPE_CREATE_TEAM => {
            let details = map_get_any(payload, &["createteam", "ct"]).unwrap_or(payload);
            let creator = map_get_any(details, &["creator", "c"])
                .and_then(as_str)
                .unwrap_or("someone");
            let team = map_get_any(details, &["team", "t"])
                .and_then(as_str)
                .unwrap_or("the team");
            Some(ChatEvent::Other {
                text: format!("{creator} created the team {team}"),
            })
        }
        MESSAGE_SYSTEM_TYPE_COMPLEX_TEAM => {
            let details = map_get_any(payload, &["complexteam", "xt"]).unwrap_or(payload);
            let team = map_get_any(details, &["team", "t"])
                .and_then(as_str)
                .unwrap_or("the team");
            Some(ChatEvent::Other {
                text: format!("made {team} a big team"),
            })
        }
        MESSAGE_SYSTEM_TYPE_GIT_PUSH => {
            let details = map_get_any(payload, &["gitpush", "gp"]).unwrap_or(payload);
            let pusher = map_get_any(details, &["pusher", "p"])
                .and_then(as_str)
                .unwrap_or("someone");
            let repo = map_get_any(details, &["repoName", "repo", "r"])
                .and_then(as_str)
                .unwrap_or("the repository");
            let commit_count = map_get_any(details, &["refs", "rs"])
                .and_then(as_array_len)
                .unwrap_or(0);
            let text = if commit_count > 0 {
                format!(
                    "{pusher} pushed {commit_count} commit{} to {repo}",
                    if commit_count == 1 { "" } else { "s" }
                )
            } else {
                format!("{pusher} updated {repo}")
            };
            Some(ChatEvent::Other { text })
        }
        MESSAGE_SYSTEM_TYPE_SBS_RESOLVE => {
            let details = map_get_any(payload, &["sbsresolve", "sbs"]).unwrap_or(payload);
            let prover = map_get_any(details, &["prover", "p"])
                .and_then(as_str)
                .unwrap_or("someone");
            let assertion_user = map_get_any(details, &["assertionUsername", "u"])
                .and_then(as_str)
                .unwrap_or("someone");
            let assertion_service = map_get_any(details, &["assertionService", "s"])
                .and_then(as_str)
                .unwrap_or("service");
            Some(ChatEvent::Other {
                text: format!("{prover} proved {assertion_user}@{assertion_service}"),
            })
        }
        MESSAGE_SYSTEM_TYPE_NEW_CHANNEL => {
            let details = map_get_any(payload, &["newchannel", "nc"]).unwrap_or(payload);
            let name = map_get_any(details, &["nameAtCreation", "n"])
                .and_then(as_str)
                .unwrap_or("channel");
            let conv_id = map_get_any(details, &["convID", "c"])
                .and_then(value_to_conversation_id_bytes)
                .map(|bytes| ConversationId::new(format!("kb_conv:{}", hex_encode(&bytes))));
            Some(ChatEvent::ChannelCreated {
                channel_name: name.to_string(),
                conv_id,
            })
        }
        _ => Some(ChatEvent::Other {
            text: "system message".to_string(),
        }),
    }
}

fn infer_system_type_from_payload(payload: &Value) -> Option<i64> {
    if map_get_any(payload, &["addedtoteam", "at"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_ADDED_TO_TEAM);
    }
    if map_get_any(payload, &["inviteaddedtoteam", "iat"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_INVITE_ADDED_TO_TEAM);
    }
    if map_get_any(payload, &["complexteam", "xt"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_COMPLEX_TEAM);
    }
    if map_get_any(payload, &["createteam", "ct"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_CREATE_TEAM);
    }
    if map_get_any(payload, &["gitpush", "gp"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_GIT_PUSH);
    }
    if map_get_any(payload, &["changeavatar", "ca"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_CHANGE_AVATAR);
    }
    if map_get_any(payload, &["changeretention", "cr"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_CHANGE_RETENTION);
    }
    if map_get_any(payload, &["bulkaddtoconv", "bac"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_BULK_ADD_TO_CONV);
    }
    if map_get_any(payload, &["sbsresolve", "sbs"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_SBS_RESOLVE);
    }
    if map_get_any(payload, &["newchannel", "nc"]).is_some() {
        return Some(MESSAGE_SYSTEM_TYPE_NEW_CHANNEL);
    }
    None
}

fn message_system_type_from_name(name: &str) -> Option<i64> {
    match name.trim().to_ascii_lowercase().as_str() {
        "addedtoteam" => Some(MESSAGE_SYSTEM_TYPE_ADDED_TO_TEAM),
        "inviteaddedtoteam" => Some(MESSAGE_SYSTEM_TYPE_INVITE_ADDED_TO_TEAM),
        "complexteam" => Some(MESSAGE_SYSTEM_TYPE_COMPLEX_TEAM),
        "createteam" => Some(MESSAGE_SYSTEM_TYPE_CREATE_TEAM),
        "gitpush" => Some(MESSAGE_SYSTEM_TYPE_GIT_PUSH),
        "changeavatar" => Some(MESSAGE_SYSTEM_TYPE_CHANGE_AVATAR),
        "changeretention" => Some(MESSAGE_SYSTEM_TYPE_CHANGE_RETENTION),
        "bulkaddtoconv" => Some(MESSAGE_SYSTEM_TYPE_BULK_ADD_TO_CONV),
        "sbsresolve" => Some(MESSAGE_SYSTEM_TYPE_SBS_RESOLVE),
        "newchannel" => Some(MESSAGE_SYSTEM_TYPE_NEW_CHANNEL),
        _ => None,
    }
}

fn first_message_id_from_value(value: &Value) -> Option<MessageId> {
    if let Some(id) = as_i64(value) {
        return Some(MessageId::new(id.to_string()));
    }
    let Value::Array(items) = value else {
        return None;
    };
    items
        .iter()
        .find_map(as_i64)
        .map(|id| MessageId::new(id.to_string()))
}

fn as_array_len(value: &Value) -> Option<usize> {
    match value {
        Value::Array(values) => Some(values.len()),
        _ => None,
    }
}

fn team_role_label_from_value(value: &Value) -> Option<String> {
    if let Some(label) = as_str(value)
        .map(str::trim)
        .filter(|label| !label.is_empty())
    {
        return Some(label.to_ascii_lowercase());
    }
    match as_i64(value)? {
        1 => Some("reader".to_string()),
        2 => Some("writer".to_string()),
        3 => Some("admin".to_string()),
        4 => Some("owner".to_string()),
        5 => Some("bot".to_string()),
        6 => Some("restricted bot".to_string()),
        _ => None,
    }
}

fn usernames_to_user_ids(usernames: Vec<String>) -> Vec<UserId> {
    usernames.into_iter().map(UserId::new).collect()
}

fn dedup_usernames(usernames: &mut Vec<String>) {
    for username in usernames.iter_mut() {
        *username = username.trim().to_string();
    }
    usernames.retain(|username| !username.is_empty());
    usernames.sort();
    usernames.dedup();
}

fn retention_summary_from_payload(payload: &Value) -> String {
    let user = map_get_any(payload, &["user", "u"])
        .and_then(as_str)
        .unwrap_or("someone");
    let scope = if map_get_any(payload, &["isTeam"])
        .and_then(as_bool)
        .unwrap_or(false)
    {
        "team"
    } else {
        "channel"
    };
    if map_get_any(payload, &["isInherit"])
        .and_then(as_bool)
        .unwrap_or(false)
    {
        return format!("{user} changed the {scope} retention policy to inherit defaults");
    }
    format!("{user} changed the {scope} retention policy")
}

fn default_body_for_message(
    message_body: Option<&Value>,
    attachments: &[AttachmentSummary],
) -> String {
    if attachments.is_empty() {
        if let Some(message_body) = message_body
            && let Some(fallback) = extract_non_text_fallback_body(message_body)
        {
            return fallback;
        }
        "<non-text message>".to_string()
    } else if attachments
        .iter()
        .all(|attachment| attachment.kind == AttachmentKind::Image)
    {
        // For Keybase image attachments, the "caption" is stored separately (decorated text/title).
        // Avoid synthesizing placeholder text that the UI would render as a duplicate line.
        String::new()
    } else if attachments.len() == 1 {
        format!("Attachment: {}", attachments[0].name)
    } else {
        format!("{} attachments", attachments.len())
    }
}

fn extract_non_text_fallback_body(message_body: &Value) -> Option<String> {
    let message_type = message_type_from_message_body(message_body);

    if let Some(reaction_body) = extract_reaction_payload(message_body)
        .and_then(|reaction| map_get_any(reaction, &["body", "b"]))
        .and_then(as_str)
    {
        let reaction = reaction_body.trim();
        if !reaction.is_empty() {
            return Some(format!("reacted with {reaction}"));
        }
    }

    if message_type == Some(MESSAGE_TYPE_JOIN) {
        if let Some(payload) = message_variant_payload(message_body, MESSAGE_TYPE_JOIN)
            && let Some(summary) = member_change_summary_from_payload(payload)
        {
            return Some(summary);
        }
        if let Some(payload) = map_get_any(message_body, &["join", "j"])
            && let Some(summary) = member_change_summary_from_payload(payload)
        {
            return Some(summary);
        }
        return Some("joined the conversation".to_string());
    }

    if message_type == Some(MESSAGE_TYPE_LEAVE) {
        if let Some(payload) = message_variant_payload(message_body, MESSAGE_TYPE_LEAVE)
            && let Some(summary) = member_change_summary_from_payload(payload)
        {
            return Some(summary);
        }
        if let Some(payload) = map_get_any(message_body, &["leave", "l"])
            && let Some(summary) = member_change_summary_from_payload(payload)
        {
            return Some(summary);
        }
        return Some("left the conversation".to_string());
    }

    if message_type == Some(MESSAGE_TYPE_METADATA)
        || map_get_any(message_body, &["metadata"]).is_some()
    {
        if let Some(new_name) = extract_metadata_channel_name(message_body) {
            return Some(format!("renamed channel to #{new_name}"));
        }
        return Some("changed the channel name".to_string());
    }

    if message_type == Some(MESSAGE_TYPE_HEADLINE)
        || map_get_any(message_body, &["headline", "h"]).is_some()
    {
        if let Some(description) = extract_headline_description(message_body, message_body)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        {
            return Some(format!("changed the channel description: {description}"));
        }
        return Some("changed the channel description".to_string());
    }

    if message_type == Some(MESSAGE_TYPE_PIN) || map_get_any(message_body, &["pin"]).is_some() {
        if let Some(target_id) = extract_pin_target_message_id(message_body) {
            return Some(format!("pinned message {}", target_id.0));
        }
        return Some("pinned a message".to_string());
    }

    if message_type == Some(MESSAGE_TYPE_SEND_PAYMENT)
        || map_get_any(message_body, &["sendpayment"]).is_some()
    {
        return Some("sent a payment".to_string());
    }
    if message_type == Some(MESSAGE_TYPE_REQUEST_PAYMENT)
        || map_get_any(message_body, &["requestpayment"]).is_some()
    {
        return Some("requested a payment".to_string());
    }
    if message_type == Some(MESSAGE_TYPE_FLIP) || map_get_any(message_body, &["flip"]).is_some() {
        return Some("started a flip".to_string());
    }
    if message_type == Some(MESSAGE_TYPE_EDIT)
        || map_get_any(message_body, &["edit", "e"]).is_some()
    {
        return Some("edited a message".to_string());
    }
    if message_type == Some(MESSAGE_TYPE_ATTACHMENT_UPLOADED)
        || map_get_any(
            message_body,
            &["attachmentuploaded", "attachmentUploaded", "au"],
        )
        .is_some()
    {
        return Some("uploaded an attachment".to_string());
    }
    if message_type == Some(MESSAGE_TYPE_ATTACHMENT)
        || map_get_any(message_body, &["attachment", "a"]).is_some()
    {
        return Some("shared an attachment".to_string());
    }
    if message_type == Some(MESSAGE_TYPE_UNFURL)
        || map_get_any(message_body, &["unfurl", "u"]).is_some()
    {
        if let Some(preview) = extract_link_previews_from_message_body(message_body)
            .into_iter()
            .next()
        {
            if let Some(title) = preview
                .title
                .as_deref()
                .map(str::trim)
                .filter(|title| !title.is_empty())
            {
                return Some(format!("shared a link preview: {title}"));
            }
            if !preview.url.trim().is_empty() {
                return Some(format!("shared a link preview: {}", preview.url.trim()));
            }
        }
        return Some("shared a link preview".to_string());
    }

    if message_type == Some(MESSAGE_TYPE_DELETE)
        || map_get_any(message_body, &["delete", "d"]).is_some()
    {
        return Some("deleted a message".to_string());
    }
    if message_type == Some(MESSAGE_TYPE_DELETE_HISTORY)
        || map_get_any(message_body, &["deletehistory", "dh"]).is_some()
    {
        return Some("cleared chat history".to_string());
    }
    if map_get_any(message_body, &["join", "j"]).is_some() {
        return Some("joined the conversation".to_string());
    }
    if map_get_any(message_body, &["leave", "l"]).is_some() {
        return Some("left the conversation".to_string());
    }
    if message_type == Some(MESSAGE_TYPE_SYSTEM)
        || map_get_any(message_body, &["system", "s"]).is_some()
    {
        return Some("system message".to_string());
    }

    None
}

fn member_change_summary_from_payload(payload: &Value) -> Option<String> {
    let mut joiners = usernames_from_payload_list(payload, &["joiners", "j"]);
    let mut leavers = usernames_from_payload_list(payload, &["leavers", "l"]);
    joiners.sort();
    joiners.dedup();
    leavers.sort();
    leavers.dedup();

    if !joiners.is_empty() && leavers.is_empty() {
        return Some(format!(
            "{} joined the conversation",
            member_subject(&joiners)
        ));
    }
    if joiners.is_empty() && !leavers.is_empty() {
        return Some(format!(
            "{} left the conversation",
            member_subject(&leavers)
        ));
    }
    if !joiners.is_empty() && !leavers.is_empty() {
        return Some(format!(
            "{} joined, {} left",
            member_subject(&joiners),
            member_subject(&leavers)
        ));
    }
    None
}

fn usernames_from_payload_list(payload: &Value, keys: &[&str]) -> Vec<String> {
    map_get_any(payload, keys)
        .and_then(as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn member_subject(users: &[String]) -> String {
    match users {
        [] => "Someone".to_string(),
        [single] => single.clone(),
        [first, second] => format!("{first} and {second}"),
        [first, second, third] => format!("{first}, {second}, and {third}"),
        many => format!("{} members", many.len()),
    }
}

fn is_unfurl_only_message(message_body: Option<&Value>) -> bool {
    let Some(body) = message_body else {
        return false;
    };
    map_get_any(body, &["unfurl", "u"]).is_some()
        || map_get_any(body, &["body", "b"])
            .and_then(|inner| map_get_any(inner, &["unfurl", "u"]))
            .is_some()
}

fn root_contains_unfurls(root: &Value) -> bool {
    map_get_any(root, &["unfurls", "u"]).is_some()
        || find_value_for_keys(root, &["unfurls", "u"], 0).is_some()
}

fn is_link_preview_placeholder_text(text: &str) -> bool {
    let normalized = text.trim().to_ascii_lowercase();
    normalized == "shared a link preview" || normalized.starts_with("shared a link preview:")
}

fn extract_link_previews(
    root: &Value,
    message_body: Option<&Value>,
    message_text: Option<&str>,
) -> Vec<LinkPreview> {
    let mut previews = Vec::new();
    if let Some(body) = message_body {
        previews.extend(extract_link_previews_from_message_body(body));
    }
    let unfurls = map_get_any(root, &["unfurls", "u"])
        .filter(|value| matches!(value, Value::Map(_)))
        .or_else(|| find_value_for_keys(root, &["unfurls"], 0));
    if let Some(Value::Map(entries)) = unfurls {
        previews.extend(
            entries
                .iter()
                .filter_map(|(_, value)| extract_link_preview_from_result(value)),
        );
    }
    dedupe_link_previews(&mut previews);

    if let Some(message_text) = message_text {
        let matched = previews
            .iter()
            .filter(|preview| message_text_contains_url(message_text, &preview.url))
            .cloned()
            .collect::<Vec<_>>();
        if !matched.is_empty() {
            return matched;
        }
    }

    previews
}

fn extract_link_previews_from_message_body(message_body: &Value) -> Vec<LinkPreview> {
    let mut previews = Vec::new();
    let payload = map_get_any(message_body, &["unfurl", "u"]).or_else(|| {
        map_get_any(message_body, &["body", "b"])
            .and_then(|body| map_get_any(body, &["unfurl", "u"]))
    });
    let Some(payload) = payload else {
        return previews;
    };

    if let Some(result) = map_get_any(payload, &["unfurl", "u"])
        && let Some(preview) = extract_link_preview_from_result(result)
    {
        previews.push(preview);
    }
    if let Some(preview) = extract_link_preview_from_result(payload) {
        previews.push(preview);
    }
    if previews.is_empty()
        && let Value::Map(entries) = payload
    {
        previews.extend(
            entries
                .iter()
                .filter_map(|(_, value)| extract_link_preview_from_result(value)),
        );
    }
    dedupe_link_previews(&mut previews);
    previews
}

#[derive(Clone, Debug)]
struct LinkPreviewMediaDisplay {
    url: String,
    width: Option<u32>,
    height: Option<u32>,
    is_video: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct UrlMediaHints {
    width: Option<u32>,
    height: Option<u32>,
    is_video: bool,
}

fn extract_link_preview_from_result(result: &Value) -> Option<LinkPreview> {
    let url = find_value_for_keys(result, &["url"], 0)
        .and_then(as_str)
        .and_then(normalize_link_preview_url)?;
    let title = find_value_for_keys(result, &["title"], 0)
        .and_then(as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let description = find_value_for_keys(result, &["description", "desc"], 0)
        .and_then(as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let site = site_from_url(&url).or_else(|| {
        find_value_for_keys(result, &["site_name", "siteName"], 0)
            .and_then(as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    });
    let media_display = extract_preferred_media_display(result, &url);
    let url_media_hints = media_hints_from_url(&url);
    let is_video = media_display
        .as_ref()
        .map(|display| display.is_video)
        .unwrap_or(false)
        || url_media_hints.is_video
        || url_looks_like_video(&url);
    let video_url = extract_video_url(result, media_display.as_ref(), &url, is_video);
    let media_width = media_display
        .as_ref()
        .and_then(|display| display.width)
        .or(url_media_hints.width);
    let media_height = media_display
        .as_ref()
        .and_then(|display| display.height)
        .or(url_media_hints.height);
    let is_media = url_is_media(&url) || is_video || looks_like_giphy_unfurl(result, &url);
    let mut thumbnail_asset = media_display
        .as_ref()
        .map(|display| display.url.clone())
        .filter(|asset_url| asset_url != &url);
    if thumbnail_asset.is_none() && is_video {
        thumbnail_asset = media_thumbnail_fallback_url(&url);
    }

    Some(LinkPreview {
        url,
        video_url,
        title,
        site,
        description,
        thumbnail_asset,
        is_media,
        media_width,
        media_height,
        is_video,
        github_pr: None,
    })
}

fn url_is_media(url: &str) -> bool {
    let lower_url = url.to_ascii_lowercase();
    url_looks_like_image(url)
        || url_looks_like_video(url)
        || lower_url.contains("giphy.com/media")
        || lower_url.contains("imgur.com")
}

fn normalize_link_preview_url(url: &str) -> Option<String> {
    let trimmed = url.trim().trim_matches(|ch| matches!(ch, '<' | '>'));
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

fn site_from_url(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(str::to_string))
}

fn message_text_contains_url(message_text: &str, url: &str) -> bool {
    message_text
        .to_ascii_lowercase()
        .contains(&url.to_ascii_lowercase())
}

fn dedupe_link_previews(previews: &mut Vec<LinkPreview>) {
    let mut deduped = Vec::with_capacity(previews.len());
    let mut by_url = std::collections::HashMap::<String, usize>::new();
    for preview in previews.drain(..) {
        let key = preview.url.trim().to_ascii_lowercase();
        if key.is_empty() {
            continue;
        }
        if let Some(existing_index) = by_url.get(&key).copied() {
            merge_link_preview_metadata(&mut deduped[existing_index], preview);
            continue;
        }
        by_url.insert(key, deduped.len());
        deduped.push(preview);
    }
    *previews = deduped;
}

fn merge_link_preview_metadata(target: &mut LinkPreview, incoming: LinkPreview) {
    if target.title.is_none() {
        target.title = incoming.title;
    }
    if target.site.is_none() {
        target.site = incoming.site;
    }
    if target.description.is_none() {
        target.description = incoming.description;
    }
    if target.thumbnail_asset.is_none() {
        target.thumbnail_asset = incoming.thumbnail_asset;
    }
    if target.video_url.is_none() {
        target.video_url = incoming.video_url;
    }
    if target.media_width.is_none() {
        target.media_width = incoming.media_width;
    }
    if target.media_height.is_none() {
        target.media_height = incoming.media_height;
    }
    target.is_media = target.is_media || incoming.is_media;
    target.is_video = target.is_video || incoming.is_video;
}

fn extract_video_url(
    result: &Value,
    media_display: Option<&LinkPreviewMediaDisplay>,
    fallback_url: &str,
    is_video: bool,
) -> Option<String> {
    if let Some(display) = media_display
        && display.is_video
    {
        return Some(display.url.clone());
    }

    let from_metadata = find_value_for_keys(
        result,
        &[
            "video_url",
            "videoUrl",
            "video",
            "videoURL",
            "og:video",
            "og:video:url",
            "og:video:secure_url",
            "twitter:player",
            "twitter:player:stream",
        ],
        0,
    )
    .and_then(as_str)
    .and_then(normalize_link_preview_url);
    if from_metadata.is_some() {
        return from_metadata;
    }

    if is_video && url_looks_like_video(fallback_url) {
        return Some(fallback_url.to_string());
    }
    None
}

fn extract_preferred_media_display(
    result: &Value,
    fallback_url: &str,
) -> Option<LinkPreviewMediaDisplay> {
    let mut candidates = Vec::new();
    collect_media_display_candidates(result, 0, &mut candidates);
    if candidates.is_empty() {
        return None;
    }
    let fallback_host = site_from_url(fallback_url).map(|value| value.to_ascii_lowercase());
    candidates
        .into_iter()
        .max_by_key(|candidate| media_display_score(candidate, fallback_host.as_deref()))
}

fn collect_media_display_candidates(
    value: &Value,
    depth: usize,
    out: &mut Vec<LinkPreviewMediaDisplay>,
) {
    if depth > 10 {
        return;
    }
    match value {
        Value::Map(entries) => {
            let width = map_get_any(
                value,
                &[
                    "width",
                    "w",
                    "imageWidth",
                    "image_width",
                    "videoWidth",
                    "video_width",
                ],
            )
            .and_then(value_to_u32);
            let height = map_get_any(
                value,
                &[
                    "height",
                    "h",
                    "imageHeight",
                    "image_height",
                    "videoHeight",
                    "video_height",
                ],
            )
            .and_then(value_to_u32);
            let explicit_is_video = map_get_any(value, &["isVideo", "isvideo", "video"])
                .and_then(value_to_bool)
                .unwrap_or(false);
            let mime_is_video = map_get_any(value, &["mimeType", "mime_type", "mime"])
                .and_then(as_str)
                .map(|mime| mime.to_ascii_lowercase().starts_with("video/"))
                .unwrap_or(false);

            for key_group in [
                &[
                    "imageUrl",
                    "image_url",
                    "thumbnailUrl",
                    "thumbnail_url",
                    "posterUrl",
                    "poster_url",
                ][..],
                &["url", "mediaUrl", "media_url"][..],
            ] {
                let Some(candidate_url) = map_get_any(value, key_group)
                    .and_then(as_str)
                    .and_then(normalize_link_preview_url)
                else {
                    continue;
                };
                let is_video =
                    explicit_is_video || mime_is_video || url_looks_like_video(&candidate_url);
                if width.is_some() || height.is_some() || is_video || url_is_media(&candidate_url) {
                    out.push(LinkPreviewMediaDisplay {
                        url: candidate_url,
                        width,
                        height,
                        is_video,
                    });
                }
            }

            for (_, inner) in entries {
                collect_media_display_candidates(inner, depth + 1, out);
            }
        }
        Value::Array(values) => {
            for inner in values {
                collect_media_display_candidates(inner, depth + 1, out);
            }
        }
        _ => {}
    }
}

fn media_display_score(candidate: &LinkPreviewMediaDisplay, fallback_host: Option<&str>) -> i64 {
    let mut score = 0i64;
    if let (Some(width), Some(height)) = (candidate.width, candidate.height) {
        if width > 0 && height > 0 {
            score += 50;
            score += (width as i64 * height as i64) / 5_000;
        }
    } else if candidate.width.is_some() || candidate.height.is_some() {
        score += 20;
    }
    if !candidate.is_video {
        score += 40;
    } else {
        score += 12;
    }
    if url_looks_like_image(&candidate.url) {
        score += 40;
    }
    if url_looks_like_video(&candidate.url) {
        score += 8;
    }
    if let Some(host) = fallback_host {
        let url_lc = candidate.url.to_ascii_lowercase();
        if url_lc.contains(host) {
            score += 6;
        }
    }
    score
}

fn media_hints_from_url(url: &str) -> UrlMediaHints {
    let mut hints = UrlMediaHints::default();
    let fragment_params = url.split('#').nth(1).map(str::to_string);
    let query_params = Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.query().map(str::to_string));
    for params in [fragment_params, query_params] {
        let Some(params) = params else {
            continue;
        };
        for pair in params.split('&') {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or_default().trim().to_ascii_lowercase();
            let value = parts.next().unwrap_or_default().trim();
            if key.is_empty() || value.is_empty() {
                continue;
            }
            match key.as_str() {
                "width" | "w" => {
                    if hints.width.is_none() {
                        hints.width = value.parse::<u32>().ok().filter(|candidate| *candidate > 0);
                    }
                }
                "height" | "h" => {
                    if hints.height.is_none() {
                        hints.height = value.parse::<u32>().ok().filter(|candidate| *candidate > 0);
                    }
                }
                "isvideo" | "video" => {
                    hints.is_video = hints.is_video
                        || matches!(
                            value.to_ascii_lowercase().as_str(),
                            "1" | "true" | "yes" | "y"
                        );
                }
                _ => {}
            }
        }
    }
    hints
}

fn media_thumbnail_fallback_url(url: &str) -> Option<String> {
    let lower = url.to_ascii_lowercase();
    if !lower.contains("giphy.com") || !url_looks_like_video(url) {
        return None;
    }
    let base = url.split('#').next().unwrap_or(url);
    if base.to_ascii_lowercase().ends_with(".mp4") {
        return Some(format!("{}.gif", &base[..base.len().saturating_sub(4)]));
    }
    None
}

fn looks_like_giphy_unfurl(result: &Value, url: &str) -> bool {
    if url.to_ascii_lowercase().contains("giphy.com") {
        return true;
    }
    find_value_for_keys(result, &["unfurlType", "unfurl_type", "type"], 0)
        .and_then(as_str)
        .map(|kind| kind.trim().eq_ignore_ascii_case("giphy"))
        .unwrap_or(false)
}

fn value_to_u32(value: &Value) -> Option<u32> {
    if let Some(raw) = as_i64(value) {
        return u32::try_from(raw).ok().filter(|candidate| *candidate > 0);
    }
    as_str(value)
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|candidate| *candidate > 0)
}

fn value_to_u64(value: &Value) -> Option<u64> {
    if let Some(raw) = as_i64(value) {
        return u64::try_from(raw).ok().filter(|candidate| *candidate > 0);
    }
    as_str(value)
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|candidate| *candidate > 0)
}

fn value_to_f32(value: &Value) -> Option<f32> {
    if let Some(raw) = as_i64(value) {
        return Some(raw as f32);
    }
    if let Some(raw) = value.as_f64()
        && raw.is_finite()
    {
        return Some(raw as f32);
    }
    as_str(value)
        .and_then(|raw| raw.trim().parse::<f32>().ok())
        .filter(|candidate| candidate.is_finite())
}

fn value_to_bool(value: &Value) -> Option<bool> {
    if let Some(raw) = as_bool(value) {
        return Some(raw);
    }
    as_str(value).and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" => Some(true),
        "0" | "false" | "no" | "n" => Some(false),
        _ => None,
    })
}

fn url_looks_like_image(url: &str) -> bool {
    let path = url
        .split('?')
        .next()
        .and_then(|base| base.split('#').next())
        .unwrap_or(url)
        .to_ascii_lowercase();
    const IMAGE_EXTS: &[&str] = &[".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp"];
    IMAGE_EXTS.iter().any(|ext| path.ends_with(ext))
}

fn url_looks_like_video(url: &str) -> bool {
    let path = url
        .split('?')
        .next()
        .and_then(|base| base.split('#').next())
        .unwrap_or(url)
        .to_ascii_lowercase();
    const VIDEO_EXTS: &[&str] = &[".mp4", ".mov", ".webm", ".avi", ".mkv"];
    VIDEO_EXTS.iter().any(|ext| path.ends_with(ext))
        || url.to_ascii_lowercase().contains("isvideo=true")
}

fn find_body_string(value: &Value, depth: usize) -> Option<String> {
    if depth > 5 {
        return None;
    }
    match value {
        Value::Map(entries) => {
            for (key, inner) in entries {
                if matches!(key.as_str(), Some("body" | "b"))
                    && let Some(text) = as_str(inner)
                {
                    return Some(text.to_string());
                }
                if let Some(found) = find_body_string(inner, depth + 1) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(values) => values
            .iter()
            .find_map(|inner| find_body_string(inner, depth + 1)),
        _ => None,
    }
}

fn conversation_title(
    tlf_name: &str,
    topic_name: &str,
    members_type: i64,
    self_username: Option<&str>,
) -> String {
    if members_type == TEAM_MEMBERS_TYPE {
        return if topic_name.is_empty() {
            tlf_name.to_string()
        } else {
            topic_name.to_string()
        };
    }

    crate::domain::conversation::dm_peer_title(tlf_name, self_username)
}

fn conversation_kind(tlf_name: &str, members_type: i64) -> ConversationKind {
    if members_type == TEAM_MEMBERS_TYPE {
        return ConversationKind::Channel;
    }
    if members_type == IMPTEAM_MEMBERS_TYPE {
        let count = tlf_name
            .split(',')
            .filter(|name| !name.trim().is_empty())
            .count();
        return if count <= 2 {
            ConversationKind::DirectMessage
        } else {
            ConversationKind::GroupDirectMessage
        };
    }
    ConversationKind::Channel
}

fn status_logged_in(status: &Value) -> Option<bool> {
    map_get_any(status, &["loggedIn", "l"]).and_then(as_bool)
}

fn status_session_valid(status: &Value) -> Option<bool> {
    map_get_any(status, &["sessionIsValid", "v"]).and_then(as_bool)
}

fn status_username(status: &Value) -> Option<String> {
    let user = map_get_any(status, &["user", "u"])?;
    map_get_any(user, &["username", "n"])
        .and_then(as_str)
        .map(ToString::to_string)
}

fn map_get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    let Value::Map(entries) = value else {
        return None;
    };
    entries
        .iter()
        .find_map(|(k, v)| (k.as_str() == Some(key)).then_some(v))
}

fn map_get_any<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| map_get(value, key))
}

fn as_array(value: &Value) -> Option<&Vec<Value>> {
    match value {
        Value::Array(values) => Some(values),
        _ => None,
    }
}

fn as_binary(value: &Value) -> Option<&Vec<u8>> {
    match value {
        Value::Binary(bytes) => Some(bytes),
        _ => None,
    }
}

fn outbox_id_to_string(value: &Value) -> Option<String> {
    if let Some(bytes) = as_binary(value) {
        return Some(hex_encode(bytes));
    }
    let text = as_str(value)?.trim();
    if text.is_empty() {
        return None;
    }
    if text.len() % 2 == 0 && text.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Some(text.to_ascii_lowercase());
    }
    Some(text.to_string())
}

fn value_to_conversation_id_bytes(value: &Value) -> Option<Vec<u8>> {
    if let Some(bytes) = as_binary(value) {
        return Some(bytes.clone());
    }
    let text = as_str(value)?;
    if let Some(hex) = text.strip_prefix("kb_conv:") {
        return hex_decode(hex);
    }
    if text.len() % 2 == 0 && text.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return hex_decode(text);
    }
    None
}

fn value_to_team_id_bytes(value: &Value) -> Option<Vec<u8>> {
    if let Some(bytes) = as_binary(value) {
        return Some(bytes.clone());
    }
    let text = as_str(value)?;
    team_id_to_bytes(text)
}

fn canonical_team_id_if_valid(team_id: &str) -> Option<String> {
    let bytes = team_id_to_bytes(team_id)?;
    if bytes.len() != TEAM_ID_BYTES_LEN {
        return None;
    }
    Some(canonical_team_id_from_bytes(&bytes))
}

fn team_id_to_bytes(team_id: &str) -> Option<Vec<u8>> {
    let trimmed = team_id.trim();
    if let Some(hex) = trimmed.strip_prefix("kb_team:") {
        return hex_decode(hex);
    }
    if trimmed.len().is_multiple_of(2) && trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return hex_decode(trimmed);
    }
    None
}

fn canonical_team_id_from_bytes(bytes: &[u8]) -> String {
    format!("kb_team:{}", hex_encode(bytes))
}

fn provider_ref_to_conversation_id_bytes(
    conversation_ref: &ProviderConversationRef,
) -> Option<Vec<u8>> {
    if let Some(hex) = conversation_ref.0.strip_prefix("kb_conv:") {
        return hex_decode(hex);
    }
    if conversation_ref.0.len().is_multiple_of(2)
        && conversation_ref.0.chars().all(|ch| ch.is_ascii_hexdigit())
    {
        return hex_decode(&conversation_ref.0);
    }
    None
}

fn provider_message_ref_to_message_id(message_ref: &ProviderMessageRef) -> Option<MessageId> {
    let raw = message_ref.0.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.chars().all(|ch| ch.is_ascii_digit()) {
        return Some(MessageId::new(raw.to_string()));
    }
    let digits = raw
        .chars()
        .rev()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    if digits.is_empty() {
        None
    } else {
        Some(MessageId::new(digits))
    }
}

fn canonical_conversation_id_from_provider_ref(
    conversation_ref: &ProviderConversationRef,
) -> ConversationId {
    if conversation_ref.0.starts_with("kb_conv:") {
        return ConversationId::new(conversation_ref.0.clone());
    }
    if conversation_ref.0.len().is_multiple_of(2)
        && conversation_ref
            .0
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return ConversationId::new(format!(
            "kb_conv:{}",
            conversation_ref.0.to_ascii_lowercase()
        ));
    }
    ConversationId::new(conversation_ref.0.clone())
}

fn as_str(value: &Value) -> Option<&str> {
    value.as_str()
}

fn as_i64(value: &Value) -> Option<i64> {
    value.as_i64()
}

fn as_bool(value: &Value) -> Option<bool> {
    value.as_bool()
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut output = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let hi = from_hex(bytes[index])?;
        let lo = from_hex(bytes[index + 1])?;
        output.push((hi << 4) | lo);
        index += 2;
    }
    Some(output)
}

fn from_hex(ch: u8) -> Option<u8> {
    match ch {
        b'0'..=b'9' => Some(ch - b'0'),
        b'a'..=b'f' => Some(ch - b'a' + 10),
        b'A'..=b'F' => Some(ch - b'A' + 10),
        _ => None,
    }
}

fn emit_task_runtime_stats(sender: &Sender<BackendEvent>, reason: &str) {
    let stats = task_runtime::stats();
    send_internal(
        sender,
        "zbase.internal.task_runtime.stats",
        Value::Map(vec![
            (Value::from("reason"), Value::from(reason.to_string())),
            (
                Value::from("interactive_pending"),
                Value::from(stats.interactive_pending as i64),
            ),
            (
                Value::from("interactive_running"),
                Value::from(stats.interactive_running as i64),
            ),
            (
                Value::from("high_pending"),
                Value::from(stats.high_pending as i64),
            ),
            (
                Value::from("high_running"),
                Value::from(stats.high_running as i64),
            ),
            (
                Value::from("low_pending"),
                Value::from(stats.low_pending as i64),
            ),
            (
                Value::from("low_running"),
                Value::from(stats.low_running as i64),
            ),
        ]),
    );
}

fn internal_payload_preview(payload: &Value) -> Option<String> {
    match payload {
        Value::Nil => None,
        Value::Map(entries) => {
            let mut parts = Vec::new();
            for (key, value) in entries.iter().take(16) {
                let key = match key {
                    Value::String(value) => value.as_str().unwrap_or("").trim(),
                    _ => "",
                };
                if key.is_empty() {
                    continue;
                }
                let value_text = match value {
                    Value::Integer(value) => value.to_string(),
                    Value::String(value) => value.as_str().unwrap_or("").trim().to_string(),
                    Value::Boolean(value) => value.to_string(),
                    Value::F32(value) => value.to_string(),
                    Value::F64(value) => value.to_string(),
                    Value::Array(values) => format!("array({})", values.len()),
                    Value::Map(values) => format!("map({})", values.len()),
                    Value::Binary(bytes) => format!("binary({})", bytes.len()),
                    Value::Nil => "nil".to_string(),
                    _ => "value".to_string(),
                };
                parts.push(format!("{key}={value_text}"));
            }
            if parts.is_empty() {
                Some("map".to_string())
            } else {
                Some(parts.join(" "))
            }
        }
        Value::Array(values) => Some(format!("array(len={})", values.len())),
        Value::Binary(bytes) => Some(format!("binary(len={})", bytes.len())),
        Value::String(value) => Some(value.as_str().unwrap_or("").to_string()),
        _ => Some("value".to_string()),
    }
}

fn send_internal(sender: &Sender<BackendEvent>, method: &str, payload: Value) {
    let _ = sender.send(BackendEvent::KeybaseNotifyStub {
        method: method.to_string(),
        payload_preview: internal_payload_preview(&payload),
    });
}

#[cfg(test)]
mod tests {
    use super::attachment_hydration::image_attachment_hydration_flags;

    #[test]
    fn attachment_upload_progress_notifications_name_the_outbox_send() {
        let event = KeybaseNotifyEvent::from_method(
            "chat.1.NotifyChat.ChatAttachmentUploadProgress",
            Value::Map(vec![
                ("convID".into(), Value::Binary(vec![1, 2])),
                ("outboxID".into(), Value::Binary(vec![0xab, 0xcd])),
                ("bytesComplete".into(), 0.into()),
                ("bytesTotal".into(), 4096.into()),
            ]),
        );
        let progress = parse_attachment_upload_progress(&event).unwrap();
        assert_eq!(progress.outbox_id, "abcd");
        assert_eq!((progress.sent_bytes, progress.total_bytes), (0, 4096));

        let other = KeybaseNotifyEvent::from_method(
            "chat.1.NotifyChat.ChatAttachmentDownloadProgress",
            Value::Map(vec![("outboxID".into(), Value::Binary(vec![0xab]))]),
        );
        assert!(parse_attachment_upload_progress(&other).is_none());
    }
    use super::*;

    pub(super) fn temp_emoji_asset_dir(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "zbase-emoji-asset-test-{label}-{}-{}",
            std::process::id(),
            now_unix_ms()
        ));
        path
    }

    fn make_test_message(id: impl Into<String>) -> MessageRecord {
        MessageRecord {
            id: MessageId::new(id.into()),
            conversation_id: ConversationId::new("kb_conv:test"),
            author_id: UserId::new("alice"),
            reply_to: None,
            thread_root_id: None,
            timestamp_ms: None,
            event: None,
            link_previews: Vec::new(),
            permalink: String::new(),
            fragments: Vec::new(),
            source_text: None,
            attachments: Vec::new(),
            reactions: Vec::new(),
            thread_reply_count: 0,
            send_state: MessageSendState::Sent,
            edited: None,
        }
    }

    #[test]
    fn page_normalization_keeps_the_attachment_under_a_caption_edit() {
        // The server's original snapshot names its superseding edit without a
        // timestamp, so the edit record on the same page ranks newer.
        let mut original = make_test_message("1");
        original.attachments.push(test_image_attachment(None, None));
        original.edited = Some(EditMeta {
            edit_id: MessageId::new("2"),
            edited_at_ms: None,
        });
        let mut edit = make_test_message("1");
        edit.fragments = vec![MessageFragment::Text("new caption".into())];
        edit.edited = Some(EditMeta {
            edit_id: MessageId::new("2"),
            edited_at_ms: Some(2),
        });

        for mut page in [
            vec![original.clone(), edit.clone()],
            vec![edit.clone(), original.clone()],
        ] {
            normalize_message_records(&mut page);
            assert_eq!(page.len(), 1);
            assert_eq!(page[0].attachments, original.attachments);
            assert_eq!(page[0].edited.as_ref().unwrap().edited_at_ms, Some(2));
        }
    }

    fn test_image_attachment(
        source: Option<AttachmentSource>,
        preview_source: Option<AttachmentSource>,
    ) -> AttachmentSummary {
        AttachmentSummary {
            name: "paste.png".to_string(),
            kind: AttachmentKind::Image,
            mime_type: Some("image/png".to_string()),
            size_bytes: 2200,
            width: Some(64),
            height: Some(64),
            preview: preview_source.map(|source| AttachmentPreview {
                source,
                width: Some(64),
                height: Some(64),
            }),
            source,
            ..AttachmentSummary::default()
        }
    }

    // Serves a Unix socket; Windows has no equivalent.
    #[cfg(unix)]
    #[test]
    fn notification_disconnect_reconnects_and_subscribes_again() {
        let path = temp_emoji_asset_dir("listener-reconnect");
        std::fs::create_dir_all(&path).unwrap();
        let socket = PathBuf::from("/tmp").join(format!(
            "zbase-test-{}-{}.sock",
            std::process::id(),
            now_unix_ms()
        ));
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let server = tokio::spawn(async move {
                for _ in 0..2 {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut transport = FramedMsgpackTransport::from_stream(stream);
                    let request = transport.read_value().await.unwrap();
                    let parts = as_array(&request).unwrap();
                    assert_eq!(as_str(&parts[2]), Some("keybase.1.config.guiGetValue"));
                    transport
                        .write_value(&Value::Array(vec![
                            Value::from(1),
                            parts[1].clone(),
                            Value::Nil,
                            Value::Map(vec![(Value::from("b"), Value::from(true))]),
                        ]))
                        .await
                        .unwrap();
                    let request = transport.read_value().await.unwrap();
                    let parts = as_array(&request).unwrap();
                    assert_eq!(
                        as_str(&parts[2]),
                        Some("keybase.1.notifyCtl.setNotifications")
                    );
                    transport
                        .write_value(&Value::Array(vec![
                            Value::from(1),
                            parts[1].clone(),
                            Value::Nil,
                            Value::Nil,
                        ]))
                        .await
                        .unwrap();
                    let request = transport.read_value().await.unwrap();
                    let parts = as_array(&request).unwrap();
                    assert_eq!(as_str(&parts[2]), Some("keybase.1.badger.getBadgeState"));
                    let badge_state = |unread| Value::Map(vec![(Value::from("conversations"), Value::Array(vec![Value::Map(vec![
                        (Value::from("convID"), Value::Binary(vec![0xab, 0xcd])),
                        (Value::from("unreadMessages"), Value::from(unread)),
                        (Value::from("badgeCount"), Value::from(0)),
                    ])]))]);
                    transport.write_value(&Value::Array(vec![
                        Value::from(1), parts[1].clone(), Value::Nil, badge_state(5),
                    ])).await.unwrap();
                    // A read in another client arrives through NotifyBadges.
                    transport.write_value(&Value::Array(vec![
                        Value::from(2), Value::from("keybase.1.NotifyBadges.badgeState"),
                        Value::Array(vec![Value::Map(vec![(Value::from("badgeState"), badge_state(0))])]),
                    ])).await.unwrap();
                    // Dropping the stream simulates a daemon restart after subscription.
                }
            });
            let store = Arc::new(LocalStore::open_at(path.join("store")).unwrap());
            let pending = Arc::new(Mutex::new(PendingOutboxSends::default()));
            let account = AccountId::new("test-account");
            let shutdown = Arc::new(AtomicBool::new(false));
            let (sender, receiver) = mpsc::channel();
            let cancel = Arc::clone(&shutdown);
            let observer = tokio::spawn(async move {
                let mut connected = 0;
                let mut disconnected = 0;
                let mut sound_loads = 0;
                let mut badge_counts = Vec::new();
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    while let Ok(event) = receiver.try_recv() {
                        match event {
                            BackendEvent::AccountConnected { .. } => connected += 1,
                            BackendEvent::AccountDisconnected { .. } => disconnected += 1,
                            // The server answers notifySound with true.
                            BackendEvent::NotifySoundLoaded { enabled } => {
                                assert!(enabled);
                                sound_loads += 1;
                            }
                            BackendEvent::ConversationBadgesUpdated { counts, complete, .. } => {
                                assert!(complete);
                                badge_counts.push(counts[&ConversationId::new("kb_conv:abcd")].unread_count);
                            }
                            _ => {}
                        }
                    }
                    if connected == 2 && badge_counts.len() == 4 {
                        break;
                    }
                    assert!(Instant::now() < deadline, "listener did not reconnect: connected={connected}, badge_counts={badge_counts:?}");
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                assert!(disconnected >= 1);
                assert_eq!(sound_loads, 2);
                assert_eq!(badge_counts, vec![5, 0, 5, 0]);
                cancel.store(true, Ordering::Relaxed);
            });
            tokio::time::timeout(
                Duration::from_secs(6),
                listen_with_reconnect(
                    &sender,
                    &store,
                    &pending,
                    &account,
                    &shutdown,
                    || Some(socket.clone()),
                ),
            )
            .await
            .unwrap();
            observer.await.unwrap();
            server.await.unwrap();
        });
        std::fs::remove_file(socket).unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }

    // Serves a Unix socket; Windows has no equivalent.
    #[cfg(unix)]
    #[test]
    fn failed_subscription_never_reports_connected() {
        let path = temp_emoji_asset_dir("listener-rejected");
        std::fs::create_dir_all(&path).unwrap();
        let socket = PathBuf::from("/tmp").join(format!(
            "zbase-rejected-{}-{}.sock",
            std::process::id(),
            now_unix_ms()
        ));
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let listener = tokio::net::UnixListener::bind(&socket).unwrap();
                let server = tokio::spawn(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut transport = FramedMsgpackTransport::from_stream(stream);
                    let request = transport.read_value().await.unwrap();
                    let parts = as_array(&request).unwrap();
                    transport
                        .write_value(&Value::Array(vec![
                            Value::from(1),
                            parts[1].clone(),
                            Value::from("subscription rejected"),
                            Value::Nil,
                        ]))
                        .await
                        .unwrap();
                });
                let store = Arc::new(LocalStore::open_at(path.join("store")).unwrap());
                let (sender, receiver) = mpsc::channel();
                let result = run_listener_session(
                    &socket,
                    &sender,
                    &store,
                    &Arc::new(Mutex::new(PendingOutboxSends::default())),
                    &AccountId::new("test-account"),
                    &AtomicBool::new(false),
                )
                .await;
                assert!(result.is_err());
                assert!(
                    !receiver
                        .try_iter()
                        .any(|event| matches!(event, BackendEvent::AccountConnected { .. }))
                );
                server.await.unwrap();
            });
        std::fs::remove_file(socket).unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }

    fn history_envelope_page(items: Vec<Value>) -> ThreadPage {
        parse_thread_page(
            &Value::Map(vec![(
                "thread".into(),
                Value::Map(vec![
                    ("messages".into(), Value::Array(items)),
                    (
                        "pagination".into(),
                        Value::Map(vec![("last".into(), false.into())]),
                    ),
                ]),
            )]),
            &ConversationId::new("conversation"),
        )
    }

    #[test]
    fn reply_count_refresh_keeps_the_root_reactions() {
        let path = temp_emoji_asset_dir("root-reactions-refresh");
        let store = Arc::new(LocalStore::open_at(path.clone()).unwrap());
        let mut root = make_test_message("10");
        root.thread_reply_count = 3;
        store.persist_message(&root).unwrap();
        store
            .upsert_message_reaction(
                &root.conversation_id,
                &root.id,
                ":+1:",
                None,
                &UserId::new("bob"),
                now_unix_ms(),
            )
            .unwrap();

        let (sender, receiver) = mpsc::channel();
        refresh_root_descendant_reply_count(Some(&sender), &store, &root.conversation_id, &root.id);

        let Ok(BackendEvent::MessageUpserted(upserted)) = receiver.try_recv() else {
            panic!("a stale reply count should upsert the root");
        };
        assert_eq!(upserted.thread_reply_count, 0);
        assert_eq!(upserted.reactions.len(), 1);
        assert_eq!(upserted.reactions[0].emoji, ":+1:");
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn fresh_username_only_profiles_publish_cached_avatar_without_network_refresh() {
        let path = temp_emoji_asset_dir("bot-avatar-cache");
        let store = Arc::new(LocalStore::open_at(path.clone()).unwrap());
        let file = path.join("avatar.png");
        std::fs::write(&file, b"avatar").unwrap();
        store
            .upsert_user_profile(
                &UserId::new("bot"),
                "bot".into(),
                None,
                Some(file.to_string_lossy().into_owned()),
                now_unix_ms(),
            )
            .unwrap();
        let (sender, receiver) = mpsc::channel();
        prefetch_user_profiles(&sender, &["bot".into()], &store);
        assert!(
            matches!(receiver.try_recv().unwrap(), BackendEvent::UserProfileUpserted { avatar_asset: Some(asset), .. } if asset == file.to_string_lossy())
        );
        assert!(
            receiver.try_recv().is_err(),
            "fresh cache should not trigger more RPC events"
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn history_pages_of_hidden_placeholders_or_expired_errors_advance_transport_position() {
        for variant in ["placeholder", "error"] {
            let items = (1..=200)
                .map(|id| {
                    Value::Map(vec![(
                        variant.into(),
                        Value::Map(vec![
                            ("messageID".into(), id.into()),
                            ("hidden".into(), true.into()),
                            ("errType".into(), 4.into()),
                            ("isEphemeral".into(), true.into()),
                            ("etime".into(), 1.into()),
                        ]),
                    )])
                })
                .collect();
            let page = history_envelope_page(items);
            assert!(page.messages.is_empty());
            assert_eq!(
                page.progress.next_position(0, page.last).unwrap(),
                Some(200)
            );
        }
    }

    #[test]
    fn history_transient_errors_block_checkpoint_even_when_valid_messages_follow() {
        let valid = Value::Map(vec![(
            "valid".into(),
            Value::Map(vec![(
                "serverHeader".into(),
                Value::Map(vec![("messageID".into(), 201.into())]),
            )]),
        )]);
        for variant in ["error", "placeholder"] {
            let unavailable = Value::Map(vec![(
                variant.into(),
                Value::Map(vec![
                    ("messageID".into(), 200.into()),
                    ("hidden".into(), false.into()),
                    ("errType".into(), 0.into()),
                ]),
            )]);
            for items in [vec![unavailable.clone()], vec![unavailable, valid.clone()]] {
                let page = history_envelope_page(items);
                assert!(page.progress.next_position(199, true).is_err());
            }
        }
        let malformed = parse_thread_page(&Value::Nil, &ConversationId::new("conversation"));
        assert!(malformed.progress.next_position(199, true).is_err());
        let empty = history_envelope_page(vec![]);
        assert_eq!(empty.progress.next_position(201, false).unwrap(), None);
        let response = Value::Map(vec![(
            "thread".into(),
            Value::Map(vec![("messages".into(), Value::Nil)]),
        )]);
        let empty_nil = parse_thread_page(&response, &ConversationId::new("conversation"));
        assert_eq!(empty_nil.progress.next_position(201, false).unwrap(), None);
        let continuation = Value::Map(vec![(
            "thread".into(),
            Value::Map(vec![
                ("messages".into(), Value::Array(vec![])),
                (
                    "pagination".into(),
                    Value::Map(vec![("next".into(), Value::Binary(vec![1]))]),
                ),
            ]),
        )]);
        let stalled = parse_thread_page(&continuation, &ConversationId::new("conversation"));
        assert!(stalled.progress.next_position(201, false).is_err());
    }

    #[test]
    fn history_sync_applies_edits_and_deletes_in_operation_order() {
        let path = temp_emoji_asset_dir("history-sync-mutations");
        let store = LocalStore::open_at(path.clone()).unwrap();
        let conversation = ConversationId::new("history-sync-mutations");
        let mut original = make_test_message("1");
        original.conversation_id = conversation.clone();
        original.fragments = vec![MessageFragment::Text("before".into())];
        store.persist_message(&original).unwrap();
        let mut edited = original.clone();
        edited.fragments = vec![MessageFragment::Text("after".into())];
        edited.edited = Some(EditMeta {
            edit_id: MessageId::new("2"),
            edited_at_ms: Some(2),
        });
        edited.attachments.push(test_image_attachment(None, None));
        let mut deleted = make_test_message("3");
        deleted.conversation_id = conversation.clone();
        deleted.event = Some(ChatEvent::MessageDeleted {
            target_message_id: Some(original.id.clone()),
        });
        let (sender, receiver) = mpsc::channel();
        persist_sync_page(
            &sender,
            &store,
            &conversation,
            ThreadPage {
                messages: vec![edited],
                deletions: vec![deleted],
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            store
                .get_message(&conversation, &original.id)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .is_message_deleted(&conversation, &original.id)
                .unwrap()
        );
        let events = receiver.try_iter().collect::<Vec<_>>();
        assert!(events.iter().any(|event| matches!(event, BackendEvent::MessageUpserted(message) if message.edited.is_some() && message.attachments[0].source.is_none())));
        assert!(events.iter().any(|event| matches!(event, BackendEvent::MessageDeleted { message_id, .. } if message_id == &original.id)));
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn hydration_recovers_deleted_local_sources_without_changing_cached_metadata() {
        let path =
            std::env::temp_dir().join(format!("zbase-hydration-boundary-{}", std::process::id()));
        std::fs::write(&path, b"media").unwrap();
        let source = AttachmentSource::LocalPath(path.to_string_lossy().into_owned());
        let mut message = make_test_message("missing-local-media");
        message
            .attachments
            .push(test_image_attachment(Some(source.clone()), None));
        assert!(!message_needs_image_attachment_hydration(&message));
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            message.attachments[0].source,
            Some(source),
            "hydration checks do not mutate stored metadata"
        );
        assert!(message_needs_image_attachment_hydration(&message));
        message.attachments[0].kind = AttachmentKind::Video;
        assert!(message_needs_video_attachment_hydration(&message));
        message.attachments[0].kind = AttachmentKind::File;
        assert!(message_needs_file_attachment_hydration(&message));
    }

    #[test]
    fn message_needs_image_attachment_hydration_skips_renderable_image_urls() {
        let mut message = make_test_message("img-renderable");
        message.attachments.push(test_image_attachment(
            Some(AttachmentSource::Url(
                "https://example.com/images/paste.png".to_string(),
            )),
            None,
        ));

        assert!(
            !message_needs_image_attachment_hydration(&message),
            "renderable image URL should not require local hydration"
        );
        assert_eq!(image_attachment_hydration_flags(&message), (false, false));
    }

    #[test]
    fn message_needs_image_attachment_hydration_for_unrenderable_keybase_assets() {
        let unrenderable = "https://s3.amazonaws.com/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/paste.png".to_string();
        let mut message = make_test_message("img-unrenderable");
        message.attachments.push(test_image_attachment(
            Some(AttachmentSource::Url(unrenderable.clone())),
            Some(AttachmentSource::Url(unrenderable)),
        ));

        assert!(
            message_needs_image_attachment_hydration(&message),
            "Keybase hash-bucket URLs without a signature need local hydration"
        );
        assert_eq!(image_attachment_hydration_flags(&message), (true, true));
    }

    #[test]
    fn parse_identify3_proof_callbacks_prefers_row_payload_fields() {
        let callbacks = vec![RpcNotification {
            method: "keybase.1.identify3Ui.identify3UpdateRow".to_string(),
            params: Value::Array(vec![Value::Map(vec![(
                Value::from("row"),
                Value::Map(vec![
                    (Value::from("key"), Value::from("pgp")),
                    (Value::from("value"), Value::from("PEER_USER")),
                    (
                        Value::from("proofURL"),
                        Value::from("https://keybase.io/peer_user/sigchain#proof"),
                    ),
                    (
                        Value::from("proofResult"),
                        Value::Map(vec![(Value::from("state"), Value::from(2))]),
                    ),
                    (
                        Value::from("siteIcon"),
                        Value::Array(vec![
                            Value::Map(vec![
                                (Value::from("width"), Value::from(32)),
                                (Value::from("url"), Value::from("https://cdn/p.webp")),
                            ]),
                            Value::Map(vec![
                                (Value::from("width"), Value::from(16)),
                                (Value::from("url"), Value::from("https://cdn/p-16.webp")),
                            ]),
                        ]),
                    ),
                ]),
            )])]),
        }];

        let proofs = parse_identify3_proof_callbacks(&callbacks);
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].service_name, "pgp");
        assert_eq!(proofs[0].service_username, "PEER_USER");
        assert_eq!(
            proofs[0].proof_url.as_deref(),
            Some("https://keybase.io/peer_user/sigchain#proof")
        );
        assert_eq!(
            proofs[0].icon_asset.as_deref(),
            Some("https://cdn/p-16.webp")
        );
        assert_eq!(proofs[0].state, ProofState::Verified);
    }

    #[test]
    fn parse_identify3_proof_callbacks_promotes_pending_rows_to_verified() {
        let callbacks = vec![
            RpcNotification {
                method: "keybase.1.identify3Ui.identify3UpdateRow".to_string(),
                params: Value::Array(vec![Value::Map(vec![(
                    Value::from("row"),
                    Value::Map(vec![
                        (Value::from("key"), Value::from("dns")),
                        (Value::from("value"), Value::from("example.com")),
                        (Value::from("state"), Value::from(1)),
                    ]),
                )])]),
            },
            RpcNotification {
                method: "keybase.1.identify3Ui.identify3UpdateRow".to_string(),
                params: Value::Array(vec![Value::Map(vec![(
                    Value::from("row"),
                    Value::Map(vec![
                        (Value::from("key"), Value::from("dns")),
                        (Value::from("value"), Value::from("example.com")),
                        (Value::from("state"), Value::from(2)),
                        (
                            Value::from("siteIcon"),
                            Value::Array(vec![Value::Map(vec![
                                (Value::from("width"), Value::from(16)),
                                (Value::from("path"), Value::from("https://cdn/dns-16.png")),
                            ])]),
                        ),
                    ]),
                )])]),
            },
        ];

        let proofs = parse_identify3_proof_callbacks(&callbacks);
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].state, ProofState::Verified);
        assert_eq!(
            proofs[0].icon_asset.as_deref(),
            Some("https://cdn/dns-16.png")
        );
    }

    #[test]
    fn parse_presence_updates_from_notify_reads_participant_status() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.ChatParticipantsInfo".to_string(),
            raw_params: Value::Array(vec![Value::Map(vec![(
                Value::from("participants"),
                Value::Map(vec![(
                    Value::from("00003570c1fa47b06cf1ae045a9199388130854bcc009e41da07018784b933da"),
                    Value::Array(vec![Value::Map(vec![
                        (Value::from("assertion"), Value::from("peer_user")),
                        (Value::from("lastActiveStatus"), Value::from("ACTIVE_1")),
                    ])]),
                )]),
            )])]),
        };

        let patches = parse_presence_updates_from_notify(&event);
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].user_id.0, "peer_user");
        assert_eq!(patches[0].presence.availability, Availability::Active);
    }

    #[test]
    fn parse_last_active_status_maps_active_recently_and_none() {
        assert_eq!(
            parse_last_active_status_availability(&Value::from(1)),
            Some(Availability::Active)
        );
        assert_eq!(
            parse_last_active_status_availability(&Value::from(2)),
            Some(Availability::Away)
        );
        assert_eq!(
            parse_last_active_status_availability(&Value::from(0)),
            Some(Availability::Offline)
        );
    }

    #[test]
    fn parse_last_active_status_for_user_reads_username_keyed_maps() {
        let direct = Value::Map(vec![
            (Value::from("aliceexample"), Value::from(0)),
            (Value::from("peer_user"), Value::from("ACTIVE_1")),
        ]);
        assert_eq!(
            parse_last_active_status_for_user(&direct, &UserId::new("peer_user")),
            Some(Availability::Active)
        );

        let nested = Value::Map(vec![(
            Value::from("participants"),
            Value::Map(vec![(
                Value::from("peer_user"),
                Value::Map(vec![(Value::from("lastActiveStatus"), Value::from(2))]),
            )]),
        )]);
        assert_eq!(
            parse_last_active_status_for_user(&nested, &UserId::new("peer_user")),
            Some(Availability::Away)
        );
    }

    #[test]
    fn tlf_name_mentions_user_matches_dm_member_list() {
        assert!(tlf_name_mentions_user(
            "aliceexample,peer_user",
            "peer_user"
        ));
        assert!(!tlf_name_mentions_user("chia_network", "peer_user"));
    }

    #[test]
    fn strip_placeholder_messages_removes_non_text_placeholder_rows() {
        let mut placeholder = make_test_message("1");
        placeholder
            .fragments
            .push(MessageFragment::Text(NON_TEXT_PLACEHOLDER_BODY.to_string()));

        let mut plain = make_test_message("2");
        plain
            .fragments
            .push(MessageFragment::Text("hello world".to_string()));

        let (filtered, removed_count) = strip_placeholder_messages(vec![placeholder, plain]);
        assert_eq!(removed_count, 1);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id.0, "2");
    }

    #[test]
    fn strip_placeholder_messages_preserves_event_rows() {
        let mut event_row = make_test_message("3");
        event_row.event = Some(ChatEvent::Other {
            text: "changed the channel name".to_string(),
        });
        event_row
            .fragments
            .push(MessageFragment::Text(NON_TEXT_PLACEHOLDER_BODY.to_string()));

        let (filtered, removed_count) = strip_placeholder_messages(vec![event_row.clone()]);
        assert_eq!(removed_count, 0);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, event_row.id);
    }

    #[test]
    fn strip_placeholder_messages_removes_link_preview_placeholder_rows() {
        let mut placeholder = make_test_message("4");
        placeholder.fragments.push(MessageFragment::Text(
            "shared a link preview: https://giphy.com/gifs/demo".to_string(),
        ));
        placeholder.link_previews.push(LinkPreview {
            url: "https://giphy.com/gifs/demo".to_string(),
            ..Default::default()
        });

        let mut plain = make_test_message("5");
        plain.fragments.push(MessageFragment::Text(
            "https://giphy.com/gifs/demo".to_string(),
        ));
        plain.link_previews.push(LinkPreview {
            url: "https://giphy.com/gifs/demo".to_string(),
            ..Default::default()
        });

        let (filtered, removed_count) = strip_placeholder_messages(vec![placeholder, plain]);
        assert_eq!(removed_count, 1);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id.0, "5");
    }

    #[test]
    fn parse_user_display_name_prefers_full_name_keys() {
        let payload = Value::Map(vec![(
            Value::from("them"),
            Value::Map(vec![(
                Value::from("profile"),
                Value::Map(vec![(Value::from("fullName"), Value::from("Alex Example"))]),
            )]),
        )]);

        let parsed = parse_user_display_name(&payload, "alexexample9");
        assert_eq!(parsed.as_deref(), Some("Alex Example"));
    }

    #[test]
    fn parse_user_display_name_ignores_username_aliases() {
        let payload = Value::Map(vec![
            (Value::from("username"), Value::from("alexexample9")),
            (Value::from("name"), Value::from("alexexample9")),
        ]);

        let parsed = parse_user_display_name(&payload, "alexexample9");
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_user_display_name_accepts_single_word_name() {
        let payload = Value::Map(vec![(
            Value::from("them"),
            Value::Map(vec![(Value::from("name"), Value::from("Alex"))]),
        )]);

        let parsed = parse_user_display_name(&payload, "alexexample9");
        assert_eq!(parsed.as_deref(), Some("Alex"));
    }

    #[test]
    fn preferred_emoji_source_values_prioritizes_source_then_no_anim() {
        let emoji = Value::Map(vec![
            (
                Value::from("source"),
                Value::from("https://cdn.example.com/sbx-highres.png"),
            ),
            (
                Value::from("noAnimSource"),
                Value::from("https://cdn.example.com/sbx-lowres.png"),
            ),
        ]);
        let values = preferred_emoji_source_values(&emoji);
        assert_eq!(
            values,
            vec![
                "https://cdn.example.com/sbx-highres.png".to_string(),
                "https://cdn.example.com/sbx-lowres.png".to_string(),
            ]
        );
    }

    #[test]
    fn parse_user_emojis_prefers_source_over_no_anim_source() {
        let payload = Value::Map(vec![(
            Value::from("emojis"),
            Value::Map(vec![(
                Value::from("emojis"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("emojis"),
                    Value::Array(vec![Value::Map(vec![
                        (Value::from("alias"), Value::from("sbx")),
                        (
                            Value::from("source"),
                            Value::from("https://cdn.example.com/sbx-highres.png"),
                        ),
                        (
                            Value::from("noAnimSource"),
                            Value::from("https://cdn.example.com/sbx-lowres.png"),
                        ),
                    ])]),
                )])]),
            )]),
        )]);

        let parsed = parse_user_emojis(&payload);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].alias, "sbx");
        assert_eq!(
            parsed[0].source_url.as_deref(),
            Some("https://cdn.example.com/sbx-highres.png")
        );
    }

    #[test]
    fn parse_user_emojis_falls_back_to_no_anim_source_when_source_missing() {
        let payload = Value::Map(vec![(
            Value::from("emojis"),
            Value::Map(vec![(
                Value::from("emojis"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("emojis"),
                    Value::Array(vec![Value::Map(vec![
                        (Value::from("alias"), Value::from("sbx")),
                        (
                            Value::from("noAnimSource"),
                            Value::from("https://cdn.example.com/sbx-static.png"),
                        ),
                    ])]),
                )])]),
            )]),
        )]);

        let parsed = parse_user_emojis(&payload);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].alias, "sbx");
        assert_eq!(
            parsed[0].source_url.as_deref(),
            Some("https://cdn.example.com/sbx-static.png")
        );
    }

    #[test]
    fn write_hashed_emoji_asset_dedupes_identical_bytes() {
        let dir = temp_emoji_asset_dir("dedupe");
        let bytes = b"\x89PNG\r\n\x1a\nsample-image-data";

        let first = write_hashed_emoji_asset(bytes, "png", &dir).expect("write first asset");
        let second = write_hashed_emoji_asset(bytes, "png", &dir).expect("write second asset");
        assert_eq!(first, second);
        assert!(first.exists());

        let file_count = std::fs::read_dir(&dir)
            .expect("list deduped assets")
            .filter_map(Result::ok)
            .count();
        assert_eq!(file_count, 1);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn write_hashed_emoji_asset_reuses_existing_file_without_error() {
        let dir = temp_emoji_asset_dir("existing");
        std::fs::create_dir_all(&dir).expect("create asset dir");
        let bytes = b"\x89PNG\r\n\x1a\nsample-image-data";
        let hash = emoji_asset_sha256_hex(bytes);
        let existing = dir.join(format!("{hash}.png"));
        std::fs::write(&existing, bytes).expect("seed existing asset");

        let written = write_hashed_emoji_asset(bytes, "png", &dir).expect("write existing asset");
        assert_eq!(written, existing);
        assert!(written.exists());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn load_cached_emojis_for_owner_prefers_team_scope() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "zbase-emoji-owner-scope-team-{}-{}",
            std::process::id(),
            now_unix_ms()
        ));
        let local_store = Arc::new(LocalStore::open_at(path.clone()).expect("open local store"));
        let conversation_id = ConversationId::new("kb_conv:team-prefers");
        let team_id = "kb_team:one";
        local_store
            .upsert_conversation_team_binding(&conversation_id, team_id, now_unix_ms())
            .expect("upsert team binding");
        local_store
            .replace_conversation_emojis(
                &conversation_id,
                &[CachedConversationEmoji {
                    alias: "sbx".to_string(),
                    unicode: None,
                    source_url: None,
                    asset_path: Some("/tmp/conversation.png".to_string()),
                    updated_ms: now_unix_ms(),
                }],
            )
            .expect("write conversation emojis");
        local_store
            .replace_team_emojis(
                team_id,
                &[CachedConversationEmoji {
                    alias: "sbx".to_string(),
                    unicode: None,
                    source_url: None,
                    asset_path: Some("/tmp/team.png".to_string()),
                    updated_ms: now_unix_ms(),
                }],
            )
            .expect("write team emojis");

        let loaded = load_cached_emojis_for_owner(&local_store, &conversation_id, Some(team_id));
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].asset_path.as_deref(), Some("/tmp/team.png"));

        drop(local_store);
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn load_cached_emojis_for_owner_falls_back_to_conversation_scope() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "zbase-emoji-owner-scope-fallback-{}-{}",
            std::process::id(),
            now_unix_ms()
        ));
        let local_store = Arc::new(LocalStore::open_at(path.clone()).expect("open local store"));
        let conversation_id = ConversationId::new("kb_conv:team-fallback");
        let team_id = "kb_team:two";
        local_store
            .upsert_conversation_team_binding(&conversation_id, team_id, now_unix_ms())
            .expect("upsert team binding");
        local_store
            .replace_conversation_emojis(
                &conversation_id,
                &[CachedConversationEmoji {
                    alias: "sbx".to_string(),
                    unicode: None,
                    source_url: None,
                    asset_path: Some("/tmp/conversation-only.png".to_string()),
                    updated_ms: now_unix_ms(),
                }],
            )
            .expect("write conversation emojis");

        let loaded = load_cached_emojis_for_owner(&local_store, &conversation_id, Some(team_id));
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].asset_path.as_deref(),
            Some("/tmp/conversation-only.png")
        );

        drop(local_store);
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn parse_thread_page_extracts_next_cursor_and_attachment_filename() {
        let payload = Value::Map(vec![(
            Value::from("thread"),
            Value::Map(vec![
                (
                    Value::from("pagination"),
                    Value::Map(vec![
                        (Value::from("next"), Value::Binary(vec![0xCD, 0x01])),
                        (Value::from("last"), Value::from(false)),
                    ]),
                ),
                (
                    Value::from("messages"),
                    Value::Array(vec![Value::Map(vec![(
                        Value::from("valid"),
                        Value::Map(vec![
                            (
                                Value::from("serverHeader"),
                                Value::Map(vec![(Value::from("messageID"), Value::from(10))]),
                            ),
                            (Value::from("senderUsername"), Value::from("alice")),
                            (
                                Value::from("messageBody"),
                                Value::Map(vec![
                                    (Value::from("messageType"), Value::from(2)),
                                    (
                                        Value::from("attachment"),
                                        Value::Map(vec![(
                                            Value::from("object"),
                                            Value::Map(vec![(
                                                Value::from("filename"),
                                                Value::from("report.pdf"),
                                            )]),
                                        )]),
                                    ),
                                ]),
                            ),
                        ]),
                    )])]),
                ),
            ]),
        )]);

        let page = parse_thread_page(&payload, &ConversationId::new("kb_conv:test"));
        assert_eq!(page.next_cursor, Some(vec![0xCD, 0x01]));
        assert!(!page.last);
        assert!(page.saw_pagination);
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.messages[0].attachments.len(), 1);
        assert_eq!(page.messages[0].attachments[0].name, "report.pdf");
        assert_eq!(page.messages[0].fragments.len(), 1);
        let MessageFragment::Text(body) = &page.messages[0].fragments[0] else {
            panic!("expected text fragment");
        };
        assert_eq!(body, "Attachment: report.pdf");
    }

    #[test]
    fn parse_thread_page_uses_payment_fallback_text() {
        let payload = Value::Map(vec![(
            Value::from("thread"),
            Value::Map(vec![(
                Value::from("messages"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("valid"),
                    Value::Map(vec![
                        (
                            Value::from("serverHeader"),
                            Value::Map(vec![(Value::from("messageID"), Value::from(98))]),
                        ),
                        (Value::from("senderUsername"), Value::from("alice")),
                        (
                            Value::from("messageBody"),
                            Value::Map(vec![(
                                Value::from("sendpayment"),
                                Value::Map(vec![(Value::from("note"), Value::from("tip"))]),
                            )]),
                        ),
                    ]),
                )])]),
            )]),
        )]);

        let page = parse_thread_page(&payload, &ConversationId::new("kb_conv:test"));
        assert_eq!(page.messages.len(), 1);
        assert_eq!(message_plain_text(&page.messages[0]), "sent a payment");
    }

    #[test]
    fn parse_thread_page_uses_metadata_fallback_text() {
        let payload = Value::Map(vec![(
            Value::from("thread"),
            Value::Map(vec![(
                Value::from("messages"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("valid"),
                    Value::Map(vec![
                        (
                            Value::from("serverHeader"),
                            Value::Map(vec![(Value::from("messageID"), Value::from(99))]),
                        ),
                        (Value::from("senderUsername"), Value::from("alice")),
                        (
                            Value::from("messageBody"),
                            Value::Map(vec![(
                                Value::from("metadata"),
                                Value::Map(vec![(
                                    Value::from("conversationTitle"),
                                    Value::from("alerts"),
                                )]),
                            )]),
                        ),
                    ]),
                )])]),
            )]),
        )]);

        let page = parse_thread_page(&payload, &ConversationId::new("kb_conv:test"));
        assert_eq!(page.messages.len(), 1);
        assert_eq!(
            message_plain_text(&page.messages[0]),
            "renamed channel to #alerts"
        );
    }

    #[test]
    fn parse_thread_page_extracts_reaction_delta_and_skips_reaction_message_row() {
        let payload = Value::Map(vec![(
            Value::from("thread"),
            Value::Map(vec![(
                Value::from("messages"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("valid"),
                    Value::Map(vec![
                        (
                            Value::from("serverHeader"),
                            Value::Map(vec![(Value::from("messageID"), Value::from(99))]),
                        ),
                        (Value::from("senderUsername"), Value::from("alice")),
                        (
                            Value::from("messageBody"),
                            Value::Map(vec![(
                                Value::from("reaction"),
                                Value::Map(vec![
                                    (Value::from("messageID"), Value::from(42)),
                                    (Value::from("body"), Value::from(":thumbsup:")),
                                ]),
                            )]),
                        ),
                    ]),
                )])]),
            )]),
        )]);

        let page = parse_thread_page(&payload, &ConversationId::new("kb_conv:test"));
        assert!(
            page.messages.is_empty(),
            "reaction message should not render as row"
        );
        assert_eq!(page.reaction_deltas.len(), 1);
        assert_eq!(page.reaction_deltas[0].target_message_id.0, "42");
        assert_eq!(page.reaction_deltas[0].emoji, ":thumbsup:");
        assert_eq!(page.reaction_deltas[0].actor_id.0, "alice");
    }

    #[test]
    fn parse_thread_page_suppresses_unfurl_only_message() {
        let payload = Value::Map(vec![(
            Value::from("thread"),
            Value::Map(vec![(
                Value::from("messages"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("valid"),
                    Value::Map(vec![
                        (
                            Value::from("serverHeader"),
                            Value::Map(vec![(Value::from("messageID"), Value::from(150))]),
                        ),
                        (Value::from("senderUsername"), Value::from("alice")),
                        (
                            Value::from("messageBody"),
                            Value::Map(vec![(
                                Value::from("unfurl"),
                                Value::Map(vec![(
                                    Value::from("unfurl"),
                                    Value::Map(vec![
                                        (Value::from("url"), Value::from("https://example.com")),
                                        (
                                            Value::from("unfurl"),
                                            Value::Map(vec![(
                                                Value::from("generic"),
                                                Value::Map(vec![(
                                                    Value::from("title"),
                                                    Value::from("Example Link"),
                                                )]),
                                            )]),
                                        ),
                                    ]),
                                )]),
                            )]),
                        ),
                    ]),
                )])]),
            )]),
        )]);

        let page = parse_thread_page(&payload, &ConversationId::new("kb_conv:test"));
        assert!(
            page.messages.is_empty(),
            "unfurl-only messages should be suppressed"
        );
    }

    #[test]
    fn extract_link_preview_from_result_prefers_giphy_image_display() {
        let result = Value::Map(vec![
            (
                Value::from("url"),
                Value::from("https://giphy.com/gifs/demo"),
            ),
            (Value::from("unfurlType"), Value::from("giphy")),
            (
                Value::from("giphy"),
                Value::Map(vec![
                    (
                        Value::from("video"),
                        Value::Map(vec![
                            (
                                Value::from("url"),
                                Value::from("https://media4.giphy.com/media/demo/giphy.mp4"),
                            ),
                            (Value::from("height"), Value::from(270)),
                            (Value::from("width"), Value::from(360)),
                            (Value::from("isVideo"), Value::from(true)),
                        ]),
                    ),
                    (
                        Value::from("image"),
                        Value::Map(vec![
                            (
                                Value::from("url"),
                                Value::from("https://media4.giphy.com/media/demo/giphy.gif"),
                            ),
                            (Value::from("height"), Value::from(270)),
                            (Value::from("width"), Value::from(360)),
                            (Value::from("isVideo"), Value::from(false)),
                        ]),
                    ),
                ]),
            ),
        ]);

        let preview = extract_link_preview_from_result(&result).expect("preview should parse");
        assert!(preview.is_media);
        assert!(
            !preview.is_video,
            "image media should avoid video rendering"
        );
        assert_eq!(
            preview.thumbnail_asset.as_deref(),
            Some("https://media4.giphy.com/media/demo/giphy.gif")
        );
        assert_eq!(preview.media_width, Some(360));
        assert_eq!(preview.media_height, Some(270));
    }

    #[test]
    fn extract_link_preview_from_result_reads_fragment_media_hints() {
        let result = Value::Map(vec![(
            Value::from("url"),
            Value::from(
                "https://media4.giphy.com/media/demo/giphy.mp4#height=270&width=360&isvideo=true",
            ),
        )]);

        let preview = extract_link_preview_from_result(&result).expect("preview should parse");
        assert!(preview.is_media);
        assert!(preview.is_video);
        assert_eq!(preview.media_width, Some(360));
        assert_eq!(preview.media_height, Some(270));
        assert_eq!(
            preview.thumbnail_asset.as_deref(),
            Some("https://media4.giphy.com/media/demo/giphy.gif")
        );
    }

    #[test]
    fn parse_thread_page_extracts_reaction_map_deltas_from_message() {
        let payload = Value::Map(vec![(
            Value::from("thread"),
            Value::Map(vec![(
                Value::from("messages"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("valid"),
                    Value::Map(vec![
                        (
                            Value::from("serverHeader"),
                            Value::Map(vec![(Value::from("messageID"), Value::from(200))]),
                        ),
                        (Value::from("senderUsername"), Value::from("alice")),
                        (
                            Value::from("messageBody"),
                            Value::Map(vec![(
                                Value::from("text"),
                                Value::Map(vec![(Value::from("body"), Value::from("hello"))]),
                            )]),
                        ),
                        (
                            Value::from("reactions"),
                            Value::Map(vec![(
                                Value::from("reactions"),
                                Value::Map(vec![(
                                    Value::from(":party_parrot:"),
                                    Value::Map(vec![(
                                        Value::from("bob"),
                                        Value::Map(vec![
                                            (Value::from("ctime"), Value::from(1_772_823_562_i64)),
                                            (Value::from("reactionMsgID"), Value::from(201)),
                                        ]),
                                    )]),
                                )]),
                            )]),
                        ),
                    ]),
                )])]),
            )]),
        )]);

        let page = parse_thread_page(&payload, &ConversationId::new("kb_conv:test"));
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.reaction_deltas.len(), 1);
        assert_eq!(page.reaction_deltas[0].target_message_id.0, "200");
        assert_eq!(
            page.reaction_deltas[0]
                .op_message_id
                .as_ref()
                .map(|id| id.0.as_str()),
            Some("201")
        );
        assert_eq!(page.reaction_deltas[0].emoji, ":party_parrot:");
        assert_eq!(page.reaction_deltas[0].actor_id.0, "bob");
    }

    #[test]
    fn parse_thread_page_extracts_reaction_delta_from_variant_body_shape() {
        let payload = Value::Map(vec![(
            Value::from("thread"),
            Value::Map(vec![(
                Value::from("messages"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("valid"),
                    Value::Map(vec![
                        (
                            Value::from("serverHeader"),
                            Value::Map(vec![(Value::from("messageID"), Value::from(26326))]),
                        ),
                        (Value::from("senderUsername"), Value::from("fixture_peer22")),
                        (
                            Value::from("messageBody"),
                            Value::Map(vec![
                                (Value::from("messageType"), Value::from(13)),
                                (
                                    Value::from("body"),
                                    Value::Map(vec![(
                                        Value::from(13),
                                        Value::Map(vec![
                                            (Value::from("m"), Value::from(26325)),
                                            (Value::from("b"), Value::from(":100:")),
                                        ]),
                                    )]),
                                ),
                            ]),
                        ),
                    ]),
                )])]),
            )]),
        )]);

        let page = parse_thread_page(&payload, &ConversationId::new("kb_conv:test"));
        assert!(
            page.messages.is_empty(),
            "reaction message should not render as row"
        );
        assert_eq!(page.reaction_deltas.len(), 1);
        assert_eq!(page.reaction_deltas[0].target_message_id.0, "26325");
        assert_eq!(page.reaction_deltas[0].emoji, ":100:");
        assert_eq!(page.reaction_deltas[0].actor_id.0, "fixture_peer22");
    }

    #[test]
    fn page_may_have_more_older_messages_prefers_pagination_signals() {
        let mut with_pagination = ThreadPage {
            saw_pagination: true,
            last: false,
            ..ThreadPage::default()
        };
        assert!(page_may_have_more_older_messages(
            &with_pagination,
            LOAD_OLDER_PAGE_SIZE
        ));

        with_pagination.last = true;
        with_pagination.next_cursor = None;
        assert!(!page_may_have_more_older_messages(
            &with_pagination,
            LOAD_OLDER_PAGE_SIZE
        ));
    }

    #[test]
    fn page_may_have_more_older_messages_falls_back_to_page_size_without_pagination() {
        let full_page_without_pagination = ThreadPage {
            saw_pagination: false,
            messages: (0..LOAD_OLDER_PAGE_SIZE)
                .map(|index| make_test_message(index.to_string()))
                .collect(),
            ..ThreadPage::default()
        };
        assert!(page_may_have_more_older_messages(
            &full_page_without_pagination,
            LOAD_OLDER_PAGE_SIZE
        ));

        let partial_page_without_pagination = ThreadPage {
            saw_pagination: false,
            messages: Vec::new(),
            ..ThreadPage::default()
        };
        assert!(!page_may_have_more_older_messages(
            &partial_page_without_pagination,
            LOAD_OLDER_PAGE_SIZE
        ));
    }

    #[test]
    fn parse_thread_page_renders_join_message_variant_payload() {
        let payload = Value::Map(vec![(
            Value::from("thread"),
            Value::Map(vec![(
                Value::from("messages"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("valid"),
                    Value::Map(vec![
                        (
                            Value::from("serverHeader"),
                            Value::Map(vec![(Value::from("messageID"), Value::from(4001))]),
                        ),
                        (Value::from("senderUsername"), Value::from("admin")),
                        (
                            Value::from("messageBody"),
                            Value::Map(vec![
                                (Value::from("messageType"), Value::from(9)),
                                (
                                    Value::from("body"),
                                    Value::Map(vec![(
                                        Value::from(9),
                                        Value::Map(vec![
                                            (
                                                Value::from("joiners"),
                                                Value::Array(vec![Value::from("alice")]),
                                            ),
                                            (Value::from("leavers"), Value::Array(Vec::new())),
                                        ]),
                                    )]),
                                ),
                            ]),
                        ),
                    ]),
                )])]),
            )]),
        )]);

        let page = parse_thread_page(&payload, &ConversationId::new("kb_conv:test"));
        assert_eq!(page.messages.len(), 1);
        let MessageFragment::Text(body) = &page.messages[0].fragments[0] else {
            panic!("expected text fragment");
        };
        assert_eq!(body, "alice joined the conversation");
    }

    #[test]
    fn parse_thread_page_renders_leave_message() {
        let payload = Value::Map(vec![(
            Value::from("thread"),
            Value::Map(vec![(
                Value::from("messages"),
                Value::Array(vec![Value::Map(vec![(
                    Value::from("valid"),
                    Value::Map(vec![
                        (
                            Value::from("serverHeader"),
                            Value::Map(vec![(Value::from("messageID"), Value::from(4002))]),
                        ),
                        (Value::from("senderUsername"), Value::from("bob")),
                        (
                            Value::from("messageBody"),
                            Value::Map(vec![
                                (Value::from("messageType"), Value::from(10)),
                                (
                                    Value::from("body"),
                                    Value::Map(vec![(Value::from(10), Value::Map(Vec::new()))]),
                                ),
                            ]),
                        ),
                    ]),
                )])]),
            )]),
        )]);

        let page = parse_thread_page(&payload, &ConversationId::new("kb_conv:test"));
        assert_eq!(page.messages.len(), 1);
        let MessageFragment::Text(body) = &page.messages[0].fragments[0] else {
            panic!("expected text fragment");
        };
        assert_eq!(body, "left the conversation");
    }

    #[test]
    fn parse_live_reaction_delta_from_notify_content_type_reaction() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (
                    Value::from("convID"),
                    Value::Binary(vec![
                        0x00, 0x00, 0xa5, 0x2c, 0xe3, 0x89, 0x8c, 0x7f, 0x0c, 0x79, 0x50, 0x50,
                        0x4d, 0x88, 0x8b, 0xed,
                    ]),
                ),
                (Value::from("messageID"), Value::from(26326)),
                (Value::from("senderUsername"), Value::from("fixture_peer22")),
                (
                    Value::from("content"),
                    Value::Map(vec![
                        (Value::from("type"), Value::from("reaction")),
                        (
                            Value::from("reaction"),
                            Value::Map(vec![
                                (Value::from("m"), Value::from(26325)),
                                (Value::from("b"), Value::from(":100:")),
                            ]),
                        ),
                    ]),
                ),
            ]),
        };

        let delta = parse_live_reaction_delta_from_notify(&event).expect("reaction delta");
        assert_eq!(delta.target_message_id.0, "26325");
        assert_eq!(
            delta.op_message_id.as_ref().map(|id| id.0.as_str()),
            Some("26326")
        );
        assert_eq!(delta.emoji, ":100:");
        assert_eq!(delta.actor_id.0, "fixture_peer22");
        assert!(delta.source_ref.is_none());
    }

    #[test]
    fn parse_live_reaction_delta_extracts_cross_team_source_ref() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (
                    Value::from("convID"),
                    Value::Binary(vec![
                        0x00, 0x00, 0xd2, 0xea, 0x99, 0xbb, 0x8a, 0x1f, 0x0c, 0x4c, 0xe0, 0xba,
                        0x34, 0x98, 0xc5, 0x73,
                    ]),
                ),
                (Value::from("messageID"), Value::from(695)),
                (Value::from("senderUsername"), Value::from("peer_user")),
                (
                    Value::from("content"),
                    Value::Map(vec![
                        (Value::from("type"), Value::from("reaction")),
                        (
                            Value::from("reaction"),
                            Value::Map(vec![
                                (Value::from("m"), Value::from(694)),
                                (Value::from("b"), Value::from(":upvote:")),
                                (
                                    Value::from("e"),
                                    Value::Map(vec![(
                                        Value::from("upvote"),
                                        Value::Map(vec![
                                            (Value::from("alias"), Value::from("upvote")),
                                            (Value::from("isCrossTeam"), Value::from(true)),
                                            (
                                                Value::from("source"),
                                                Value::Map(vec![(
                                                    Value::from("message"),
                                                    Value::Map(vec![
                                                        (
                                                            Value::from("convID"),
                                                            Value::from(
                                                                "AADuOtIhzuifak2Ao6FnbYUYzuIn4ScHpnVjdCsiCiA=",
                                                            ),
                                                        ),
                                                        (Value::from("msgID"), Value::from(26)),
                                                    ]),
                                                )]),
                                            ),
                                        ]),
                                    )]),
                                ),
                            ]),
                        ),
                    ]),
                ),
            ]),
        };

        let delta = parse_live_reaction_delta_from_notify(&event).expect("reaction delta");
        let source_ref = delta.source_ref.expect("source ref");
        assert_eq!(source_ref.backend_id.0, KEYBASE_BACKEND_ID);
        assert_eq!(
            source_ref.ref_key,
            "emoji:conv=0000ee3ad221cee89f6a4d80a3a1676d8518cee227e12707a67563742b220a20:msg=26"
        );
    }

    #[test]
    fn parse_live_reaction_delta_uses_message_id_field_as_op_id() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (Value::from("type"), Value::from("chat")),
                (
                    Value::from("msg"),
                    Value::Map(vec![
                        (Value::from("id"), Value::from(48)),
                        (
                            Value::from("conversation_id"),
                            Value::from(
                                "0000616d72fdbc60800756697c93ac0642e32391073a9635520e2166c3821a96",
                            ),
                        ),
                        (
                            Value::from("sender"),
                            Value::Map(vec![(
                                Value::from("username"),
                                Value::from("aliceexample"),
                            )]),
                        ),
                        (
                            Value::from("content"),
                            Value::Map(vec![
                                (Value::from("type"), Value::from("reaction")),
                                (
                                    Value::from("reaction"),
                                    Value::Map(vec![
                                        (Value::from("m"), Value::from(47)),
                                        (Value::from("b"), Value::from("👍")),
                                    ]),
                                ),
                            ]),
                        ),
                    ]),
                ),
            ]),
        };

        let delta = parse_live_reaction_delta_from_notify(&event).expect("reaction delta");
        assert_eq!(delta.target_message_id.0, "47");
        assert_eq!(
            delta.op_message_id.as_ref().map(|id| id.0.as_str()),
            Some("48")
        );
        assert_eq!(delta.emoji, "👍");
        assert_eq!(delta.actor_id.0, "aliceexample");
    }

    #[test]
    fn reaction_removed_event_for_live_delete_uses_persisted_reaction_op_mapping() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "zbase-reaction-delete-map-{}-{}",
            std::process::id(),
            now_unix_ms()
        ));
        let local_store = LocalStore::open_at(path.clone()).expect("open local store");
        let conversation_id = ConversationId::new("kb_conv:test");
        let op_message_id = MessageId::new("300");
        let target_message_id = MessageId::new("200");

        local_store
            .upsert_message_reaction_op(
                &conversation_id,
                &op_message_id,
                &target_message_id,
                ":heart:",
                &UserId::new("alice"),
                1,
            )
            .expect("persist reaction op mapping");

        let mut delete_message = make_test_message("301");
        delete_message.conversation_id = conversation_id.clone();
        delete_message.event = Some(ChatEvent::MessageDeleted {
            target_message_id: Some(op_message_id.clone()),
        });

        let event = reaction_removed_event_for_live_delete(&local_store, &delete_message)
            .expect("reaction delete should map to reaction removal");
        match event {
            BackendEvent::MessageReactionRemoved {
                conversation_id: removed_conversation_id,
                message_id,
                emoji,
                actor_id,
            } => {
                assert_eq!(removed_conversation_id, conversation_id);
                assert_eq!(message_id, target_message_id);
                assert_eq!(emoji, ":heart:");
                assert_eq!(actor_id, UserId::new("alice"));
            }
            other => panic!("unexpected backend event: {other:?}"),
        }

        let duplicate = reaction_removed_event_for_live_delete(&local_store, &delete_message)
            .expect("duplicate delete notification should still map to reaction removal");
        assert!(
            matches!(duplicate, BackendEvent::MessageReactionRemoved { .. }),
            "duplicate live delete should not fall through to message tombstone rendering"
        );

        let still_present = local_store
            .get_message_reaction_op(&conversation_id, &op_message_id)
            .expect("load persisted mapping after reaction removal");
        assert!(
            still_present.is_some(),
            "mapping should remain to classify duplicate local/remote delete notifications"
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn parse_live_message_from_notify_skips_reaction_type() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (Value::from("convID"), Value::Binary(vec![0xAA, 0xBB])),
                (Value::from("messageID"), Value::from(26326)),
                (Value::from("senderUsername"), Value::from("fixture_peer22")),
                (
                    Value::from("content"),
                    Value::Map(vec![
                        (Value::from("type"), Value::from("reaction")),
                        (
                            Value::from("reaction"),
                            Value::Map(vec![
                                (Value::from("m"), Value::from(26325)),
                                (Value::from("b"), Value::from(":100:")),
                            ]),
                        ),
                    ]),
                ),
            ]),
        };

        assert!(
            parse_live_message_from_notify(&event).is_none(),
            "reaction operation should not render as standalone message row"
        );
    }

    #[test]
    fn parse_live_message_from_notify_ignores_non_incoming_activity_payloads() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (Value::from("convID"), Value::Binary(vec![0xAA, 0xBB])),
                (Value::from("messageID"), Value::from(321)),
                (Value::from("senderUsername"), Value::from("alice")),
                (
                    Value::from("activity"),
                    Value::Map(vec![(
                        Value::from("pin"),
                        Value::Map(vec![
                            (Value::from("msgID"), Value::from(11)),
                            (Value::from("body"), Value::from("very old pinned body")),
                        ]),
                    )]),
                ),
                (
                    Value::from("content"),
                    Value::Map(vec![(
                        Value::from("text"),
                        Value::Map(vec![(
                            Value::from("body"),
                            Value::from("synthetic top-level fallback body"),
                        )]),
                    )]),
                ),
            ]),
        };

        assert!(
            parse_live_message_from_notify(&event).is_none(),
            "payloads without incomingMessage.valid should not synthesize timeline rows"
        );
    }

    fn incoming_notification_event(display: bool, message_body: Value) -> KeybaseNotifyEvent {
        KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![(
                Value::from("activity"),
                Value::Map(vec![(
                    Value::from("incomingMessage"),
                    Value::Map(vec![
                        (Value::from("convID"), Value::Binary(vec![0xAA, 0xBB])),
                        (
                            Value::from("displayDesktopNotification"),
                            Value::from(display),
                        ),
                        (
                            Value::from("desktopNotificationSnippet"),
                            Value::from(" alice: hi "),
                        ),
                        (
                            Value::from("message"),
                            Value::Map(vec![
                                (Value::from("state"), Value::from(0)),
                                (
                                    Value::from("valid"),
                                    Value::Map(vec![
                                        (Value::from("messageID"), Value::from(44)),
                                        (Value::from("senderUsername"), Value::from("alice")),
                                        (Value::from("messageBody"), message_body),
                                    ]),
                                ),
                            ]),
                        ),
                    ]),
                )]),
            )]),
        }
    }

    fn notification_text_body(text: &str) -> Value {
        Value::Map(vec![
            (Value::from("messageType"), Value::from(1)),
            (
                Value::from("text"),
                Value::Map(vec![(Value::from("body"), Value::from(text))]),
            ),
        ])
    }

    #[test]
    fn desktop_notification_follows_keybase_flag_and_snippet() {
        let event = incoming_notification_event(true, notification_text_body("hi"));
        let notification = parse_desktop_notification_from_notify(&event).expect("notification");
        assert_eq!(notification.conversation_id.0, "kb_conv:aabb");
        assert_eq!(notification.message_id.0, "44");
        assert_eq!(notification.author, "alice");
        assert_eq!(notification.snippet, "alice: hi");

        let quiet = incoming_notification_event(false, notification_text_body("hi"));
        assert_eq!(parse_desktop_notification_from_notify(&quiet), None);
    }

    #[test]
    fn desktop_notification_for_reaction_opens_the_reacted_message() {
        let reaction = Value::Map(vec![
            (
                Value::from("messageType"),
                Value::from(MESSAGE_TYPE_REACTION),
            ),
            (
                Value::from("reaction"),
                Value::Map(vec![
                    (Value::from("m"), Value::from(40)),
                    (Value::from("b"), Value::from(":+1:")),
                ]),
            ),
        ]);
        let event = incoming_notification_event(true, reaction);
        let notification = parse_desktop_notification_from_notify(&event).expect("notification");
        assert_eq!(notification.message_id.0, "40");
    }

    #[test]
    fn parse_live_message_from_notify_prefers_nested_valid_payload_shape() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (Value::from("convID"), Value::Binary(vec![0xFF, 0xEE])),
                (Value::from("messageID"), Value::from(999)),
                (
                    Value::from("activity"),
                    Value::Map(vec![(
                        Value::from("incomingMessage"),
                        Value::Map(vec![
                            (Value::from("convID"), Value::Binary(vec![0xAA, 0xBB])),
                            (
                                Value::from("message"),
                                Value::Map(vec![
                                    (Value::from("state"), Value::from(0)),
                                    (
                                        Value::from("valid"),
                                        Value::Map(vec![
                                            (Value::from("messageID"), Value::from(44)),
                                            (Value::from("senderUsername"), Value::from("alice")),
                                            (
                                                Value::from("atMentionUsernames"),
                                                Value::Array(vec![Value::from("bob")]),
                                            ),
                                            (Value::from("channelMention"), Value::from(1)),
                                            (
                                                Value::from("channelNameMentions"),
                                                Value::Array(vec![Value::from("general")]),
                                            ),
                                            (
                                                Value::from("messageBody"),
                                                Value::Map(vec![(
                                                    Value::from("text"),
                                                    Value::Map(vec![(
                                                        Value::from("body"),
                                                        Value::from("hey @bob @channel #general"),
                                                    )]),
                                                )]),
                                            ),
                                        ]),
                                    ),
                                ]),
                            ),
                        ]),
                    )]),
                ),
            ]),
        };

        let message = parse_live_message_from_notify(&event).expect("expected live message");
        assert_eq!(message.id.0, "44");
        assert_eq!(message.conversation_id.0, "kb_conv:aabb");
        assert_eq!(message.author_id.0, "alice");
        assert!(message.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Mention(user_id) if user_id.0 == "bob"
        )));
        assert!(message.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::BroadcastMention(BroadcastKind::All)
        )));
        assert!(message.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::ChannelMention { name } if name == "general"
        )));
    }

    #[test]
    fn parse_live_message_from_notify_attaches_emoji_source_ref_from_top_level_content_metadata() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (
                    Value::from("activity"),
                    Value::Map(vec![(
                        Value::from("incomingMessage"),
                        Value::Map(vec![
                            (Value::from("convID"), Value::Binary(vec![0x12, 0x34])),
                            (
                                Value::from("message"),
                                Value::Map(vec![
                                    (Value::from("state"), Value::from(0)),
                                    (
                                        Value::from("valid"),
                                        Value::Map(vec![
                                            (Value::from("messageID"), Value::from(3670)),
                                            (
                                                Value::from("senderUsername"),
                                                Value::from("fixture_peer33"),
                                            ),
                                            (
                                                Value::from("messageBody"),
                                                Value::Map(vec![(
                                                    Value::from("text"),
                                                    Value::Map(vec![(
                                                        Value::from("body"),
                                                        Value::from(":sadpanda:"),
                                                    )]),
                                                )]),
                                            ),
                                        ]),
                                    ),
                                ]),
                            ),
                        ]),
                    )]),
                ),
                (
                    Value::from("content"),
                    Value::Map(vec![
                        (Value::from("type"), Value::from("text")),
                        (
                            Value::from("text"),
                            Value::Map(vec![
                                (Value::from("body"), Value::from(":sadpanda:")),
                                (
                                    Value::from("emojis"),
                                    Value::Array(vec![Value::Map(vec![
                                        (Value::from("alias"), Value::from("sadpanda")),
                                        (
                                            Value::from("convID"),
                                            Value::from(
                                                "0000e31c8f25f28d7f9ae01b5925ad65884a82767fdd75f69efa2c9ec6506812",
                                            ),
                                        ),
                                        (Value::from("messageID"), Value::from(4)),
                                    ])]),
                                ),
                            ]),
                        ),
                    ]),
                ),
            ]),
        };

        let message = parse_live_message_from_notify(&event).expect("expected live message");
        let source_ref = message
            .fragments
            .iter()
            .find_map(|fragment| match fragment {
                MessageFragment::Emoji {
                    alias,
                    source_ref: Some(source_ref),
                } if alias == "sadpanda" => Some(source_ref.clone()),
                _ => None,
            })
            .expect("expected emoji source ref");
        assert_eq!(
            source_ref.ref_key,
            "emoji:conv=0000e31c8f25f28d7f9ae01b5925ad65884a82767fdd75f69efa2c9ec6506812:msg=4"
        );
    }

    #[test]
    fn parse_live_message_from_notify_treats_pin_payload_as_system_event() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![(
                Value::from("activity"),
                Value::Map(vec![(
                    Value::from("incomingMessage"),
                    Value::Map(vec![
                        (Value::from("convID"), Value::Binary(vec![0xCA, 0xFE])),
                        (
                            Value::from("message"),
                            Value::Map(vec![
                                (Value::from("state"), Value::from(0)),
                                (
                                    Value::from("valid"),
                                    Value::Map(vec![
                                        (
                                            Value::from("serverHeader"),
                                            Value::Map(vec![(
                                                Value::from("messageID"),
                                                Value::from(200),
                                            )]),
                                        ),
                                        (Value::from("senderUsername"), Value::from("alice")),
                                        (
                                            Value::from("messageBody"),
                                            Value::Map(vec![(
                                                Value::from("pin"),
                                                Value::Map(vec![
                                                    (Value::from("msgID"), Value::from(42)),
                                                    (
                                                        Value::from("body"),
                                                        Value::from("very old message text"),
                                                    ),
                                                ]),
                                            )]),
                                        ),
                                    ]),
                                ),
                            ]),
                        ),
                    ]),
                )]),
            )]),
        };

        let message = parse_live_message_from_notify(&event).expect("expected live message");
        assert_eq!(message.id.0, "200");
        assert_eq!(message.conversation_id.0, "kb_conv:cafe");
        assert!(matches!(
            message.event.as_ref(),
            Some(ChatEvent::MessagePinned {
                target_message_id: Some(target)
            }) if target.0 == "42"
        ));
    }

    #[test]
    fn parse_live_message_from_notify_suppresses_unfurl_only_message_even_with_fallback_body() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (Value::from("convID"), Value::Binary(vec![0xAB, 0xCD])),
                (Value::from("messageID"), Value::from(150)),
                (
                    Value::from("body"),
                    Value::from("shared a link preview: https://example.com"),
                ),
                (
                    Value::from("activity"),
                    Value::Map(vec![(
                        Value::from("incomingMessage"),
                        Value::Map(vec![(
                            Value::from("message"),
                            Value::Map(vec![
                                (Value::from("state"), Value::from(0)),
                                (
                                    Value::from("valid"),
                                    Value::Map(vec![
                                        (Value::from("messageID"), Value::from(150)),
                                        (Value::from("senderUsername"), Value::from("alice")),
                                        (
                                            Value::from("messageBody"),
                                            Value::Map(vec![(
                                                Value::from("unfurl"),
                                                Value::Map(vec![(
                                                    Value::from("unfurl"),
                                                    Value::Map(vec![
                                                        (
                                                            Value::from("url"),
                                                            Value::from("https://example.com"),
                                                        ),
                                                        (
                                                            Value::from("unfurl"),
                                                            Value::Map(vec![(
                                                                Value::from("generic"),
                                                                Value::Map(vec![(
                                                                    Value::from("title"),
                                                                    Value::from("Example Link"),
                                                                )]),
                                                            )]),
                                                        ),
                                                    ]),
                                                )]),
                                            )]),
                                        ),
                                    ]),
                                ),
                            ]),
                        )]),
                    )]),
                ),
            ]),
        };

        assert!(
            parse_live_message_from_notify(&event).is_none(),
            "unfurl-only messages should not render as timeline messages"
        );
    }

    #[test]
    fn parse_live_message_from_notify_skips_unfurl_type_even_with_text_and_giphy_payload() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (Value::from("convID"), Value::Binary(vec![0xBA, 0xDC])),
                (Value::from("messageID"), Value::from(151)),
                (
                    Value::from("activity"),
                    Value::Map(vec![(
                        Value::from("incomingMessage"),
                        Value::Map(vec![(
                            Value::from("message"),
                            Value::Map(vec![
                                (Value::from("state"), Value::from(0)),
                                (
                                    Value::from("valid"),
                                    Value::Map(vec![
                                        (Value::from("messageID"), Value::from(151)),
                                        (Value::from("senderUsername"), Value::from("alice")),
                                        (
                                            Value::from("messageBody"),
                                            Value::Map(vec![
                                                (
                                                    Value::from("messageType"),
                                                    Value::from(MESSAGE_TYPE_UNFURL),
                                                ),
                                                (
                                                    Value::from("text"),
                                                    Value::Map(vec![(
                                                        Value::from("body"),
                                                        Value::from(
                                                            "shared a link preview: https://giphy.com/gifs/demo",
                                                        ),
                                                    )]),
                                                ),
                                                (
                                                    Value::from("unfurl"),
                                                    Value::Map(vec![(
                                                        Value::from("unfurl"),
                                                        Value::Map(vec![
                                                            (
                                                                Value::from("url"),
                                                                Value::from(
                                                                    "https://giphy.com/gifs/demo",
                                                                ),
                                                            ),
                                                            (
                                                                Value::from("unfurlType"),
                                                                Value::from("giphy"),
                                                            ),
                                                            (
                                                                Value::from("giphy"),
                                                                Value::Map(vec![(
                                                                    Value::from("image"),
                                                                    Value::Map(vec![(
                                                                        Value::from("url"),
                                                                        Value::from(
                                                                            "https://media.giphy.com/media/demo/giphy.gif",
                                                                        ),
                                                                    )]),
                                                                )]),
                                                            ),
                                                        ]),
                                                    )]),
                                                ),
                                            ]),
                                        ),
                                    ]),
                                ),
                            ]),
                        )]),
                    )]),
                ),
            ]),
        };

        assert!(
            parse_live_message_from_notify(&event).is_none(),
            "unfurl operation rows should be suppressed even if they include text"
        );
    }

    #[test]
    fn parse_thread_message_skips_unfurl_type_even_with_text_body() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(152))]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![
                    (Value::from("messageType"), Value::from(MESSAGE_TYPE_UNFURL)),
                    (
                        Value::from("text"),
                        Value::Map(vec![(
                            Value::from("body"),
                            Value::from("shared a link preview: https://giphy.com/gifs/demo"),
                        )]),
                    ),
                    (
                        Value::from("unfurl"),
                        Value::Map(vec![(
                            Value::from("unfurl"),
                            Value::Map(vec![(
                                Value::from("url"),
                                Value::from("https://giphy.com/gifs/demo"),
                            )]),
                        )]),
                    ),
                ]),
            ),
        ]);

        assert!(
            parse_thread_message(
                &valid,
                &ConversationId::new("kb_conv:test"),
                Some(MESSAGE_TYPE_UNFURL)
            )
            .is_none(),
            "thread parsing should suppress unfurl operation rows"
        );
    }

    #[test]
    fn parse_live_message_from_notify_suppresses_placeholder_unfurl_text_with_root_unfurls() {
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: Value::Map(vec![
                (Value::from("convID"), Value::Binary(vec![0xBE, 0xEF])),
                (Value::from("messageID"), Value::from(153)),
                (
                    Value::from("activity"),
                    Value::Map(vec![(
                        Value::from("incomingMessage"),
                        Value::Map(vec![(
                            Value::from("message"),
                            Value::Map(vec![
                                (Value::from("state"), Value::from(0)),
                                (
                                    Value::from("valid"),
                                    Value::Map(vec![
                                        (Value::from("messageID"), Value::from(153)),
                                        (Value::from("senderUsername"), Value::from("alice")),
                                        (
                                            Value::from("messageBody"),
                                            Value::Map(vec![(
                                                Value::from("body"),
                                                Value::from(
                                                    "shared a link preview: https://giphy.com/gifs/demo",
                                                ),
                                            )]),
                                        ),
                                        (
                                            Value::from("unfurls"),
                                            Value::Map(vec![(
                                                Value::from("https://giphy.com/gifs/demo"),
                                                Value::Map(vec![
                                                    (
                                                        Value::from("url"),
                                                        Value::from("https://giphy.com/gifs/demo"),
                                                    ),
                                                    (
                                                        Value::from("unfurlType"),
                                                        Value::from("giphy"),
                                                    ),
                                                    (
                                                        Value::from("giphy"),
                                                        Value::Map(vec![(
                                                            Value::from("image"),
                                                            Value::Map(vec![(
                                                                Value::from("url"),
                                                                Value::from(
                                                                    "https://media.giphy.com/media/demo/giphy.gif",
                                                                ),
                                                            )]),
                                                        )]),
                                                    ),
                                                ]),
                                            )]),
                                        ),
                                    ]),
                                ),
                            ]),
                        )]),
                    )]),
                ),
            ]),
        };

        assert!(
            parse_live_message_from_notify(&event).is_none(),
            "placeholder unfurl rows with root unfurls should be suppressed"
        );
    }

    #[test]
    fn parse_thread_message_suppresses_placeholder_unfurl_text_with_root_unfurls() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(154))]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("body"),
                    Value::from("shared a link preview: https://giphy.com/gifs/demo"),
                )]),
            ),
            (
                Value::from("unfurls"),
                Value::Map(vec![(
                    Value::from("https://giphy.com/gifs/demo"),
                    Value::Map(vec![
                        (
                            Value::from("url"),
                            Value::from("https://giphy.com/gifs/demo"),
                        ),
                        (Value::from("unfurlType"), Value::from("giphy")),
                        (
                            Value::from("giphy"),
                            Value::Map(vec![(
                                Value::from("image"),
                                Value::Map(vec![(
                                    Value::from("url"),
                                    Value::from("https://media.giphy.com/media/demo/giphy.gif"),
                                )]),
                            )]),
                        ),
                    ]),
                )]),
            ),
        ]);

        assert!(
            parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None).is_none(),
            "thread parsing should suppress placeholder unfurl rows with root unfurls"
        );
    }

    #[test]
    fn parse_thread_message_uses_metadata_mentions_when_decorated_text_missing() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(901))]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("atMentionUsernames"),
                Value::Array(vec![Value::from("bob")]),
            ),
            (Value::from("channelMention"), Value::from(1)),
            (
                Value::from("channelNameMentions"),
                Value::Array(vec![Value::from("general")]),
            ),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("text"),
                    Value::Map(vec![(
                        Value::from("body"),
                        Value::from("hello @bob @everyone #general and @ghost"),
                    )]),
                )]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        assert!(message.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Mention(user_id) if user_id.0 == "bob"
        )));
        assert!(message.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::BroadcastMention(BroadcastKind::All)
        )));
        assert!(message.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::ChannelMention { name } if name == "general"
        )));
        assert!(
            !message.fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Mention(user_id) if user_id.0 == "ghost"
            )),
            "unknown @ghost should stay plain text when metadata is available"
        );
    }

    #[test]
    fn parse_thread_message_attaches_emoji_source_ref_from_text_metadata() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(904))]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("text"),
                    Value::Map(vec![
                        (Value::from("body"), Value::from("hello :nice: world")),
                        (
                            Value::from("emojis"),
                            Value::Array(vec![Value::Map(vec![
                                (Value::from("alias"), Value::from("nice")),
                                (Value::from("isCrossTeam"), Value::from(true)),
                                (
                                    Value::from("convID"),
                                    Value::from("00112233445566778899aabbccddeeff"),
                                ),
                                (Value::from("messageID"), Value::from(54646)),
                            ])]),
                        ),
                    ]),
                )]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        let source_ref = message
            .fragments
            .iter()
            .find_map(|fragment| match fragment {
                MessageFragment::Emoji {
                    alias,
                    source_ref: Some(source_ref),
                } if alias == "nice" => Some(source_ref.clone()),
                _ => None,
            })
            .expect("expected emoji source ref");

        assert_eq!(source_ref.backend_id.0, KEYBASE_BACKEND_ID);
        assert_eq!(
            source_ref.ref_key,
            "emoji:conv=00112233445566778899aabbccddeeff:msg=54646"
        );
    }

    #[test]
    fn parse_thread_message_attaches_emoji_source_ref_from_flattened_emojis_metadata() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(905))]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![
                    (Value::from("body"), Value::from("hello :nice: world")),
                    (
                        Value::from("emojis"),
                        Value::Array(vec![Value::Map(vec![
                            (Value::from("alias"), Value::from("nice")),
                            (
                                Value::from("convID"),
                                Value::from("00112233445566778899aabbccddeeff"),
                            ),
                            (Value::from("messageID"), Value::from(14)),
                        ])]),
                    ),
                ]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        let source_ref = message
            .fragments
            .iter()
            .find_map(|fragment| match fragment {
                MessageFragment::Emoji {
                    alias,
                    source_ref: Some(source_ref),
                } if alias == "nice" => Some(source_ref.clone()),
                _ => None,
            })
            .expect("expected emoji source ref");

        assert_eq!(
            source_ref.ref_key,
            "emoji:conv=00112233445566778899aabbccddeeff:msg=14"
        );
    }

    #[test]
    fn parse_thread_message_accepts_base64_emoji_conv_id_metadata() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(906))]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("text"),
                    Value::Map(vec![
                        (Value::from("body"), Value::from("hello :nice: world")),
                        (
                            Value::from("emojis"),
                            Value::Array(vec![Value::Map(vec![
                                (Value::from("alias"), Value::from("nice")),
                                (
                                    Value::from("convID"),
                                    Value::from("AACLV/fMGdHk6fzfHZsRKYZIQa7G1WT6A934JyOkc0I="),
                                ),
                                (Value::from("messageID"), Value::from(14)),
                            ])]),
                        ),
                    ]),
                )]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        let source_ref = message
            .fragments
            .iter()
            .find_map(|fragment| match fragment {
                MessageFragment::Emoji {
                    alias,
                    source_ref: Some(source_ref),
                } if alias == "nice" => Some(source_ref.clone()),
                _ => None,
            })
            .expect("expected emoji source ref");

        assert_eq!(
            source_ref.ref_key,
            "emoji:conv=00008b57f7cc19d1e4e9fcdf1d9b1129864841aec6d564fa03ddf82723a47342:msg=14"
        );
    }

    #[test]
    fn parse_thread_message_respects_channel_mention_gating_from_metadata() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(902))]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (Value::from("channelMention"), Value::from(0)),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("text"),
                    Value::Map(vec![(Value::from("body"), Value::from("heads up @here"))]),
                )]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        assert!(
            !message
                .fragments
                .iter()
                .any(|fragment| matches!(fragment, MessageFragment::BroadcastMention(_))),
            "@here should not become broadcast when channelMention metadata is disabled"
        );
    }

    #[test]
    fn parse_thread_message_falls_back_to_plain_mentions_when_metadata_missing() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(903))]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("text"),
                    Value::Map(vec![(Value::from("body"), Value::from("hello @bob"))]),
                )]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        assert!(message.fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Mention(user_id) if user_id.0 == "bob"
        )));
    }

    #[test]
    fn parse_thread_message_extracts_reply_to_from_text_payload() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![
                    (Value::from("messageID"), Value::from(904)),
                    (
                        Value::from("replies"),
                        Value::Array(vec![Value::from(905), Value::from(906)]),
                    ),
                ]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("text"),
                    Value::Map(vec![
                        (Value::from("body"), Value::from("child message")),
                        (Value::from("replyTo"), Value::from(903)),
                    ]),
                )]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        assert_eq!(
            message.reply_to.as_ref().map(|id| id.0.as_str()),
            Some("903")
        );
        assert_eq!(message.thread_reply_count, 2);
    }

    #[test]
    fn parse_thread_message_extracts_reply_to_from_unboxed_payload() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(907))]),
            ),
            (
                Value::from("replyTo"),
                Value::Map(vec![(
                    Value::from("valid"),
                    Value::Map(vec![(
                        Value::from("serverHeader"),
                        Value::Map(vec![(Value::from("messageID"), Value::from(903))]),
                    )]),
                )]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("text"),
                    Value::Map(vec![(Value::from("body"), Value::from("child message"))]),
                )]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        assert_eq!(
            message.reply_to.as_ref().map(|id| id.0.as_str()),
            Some("903")
        );
    }

    #[test]
    fn parse_thread_message_extracts_reply_to_from_string_reference() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(908))]),
            ),
            (
                Value::from("replyTo"),
                Value::from("keybase://chat/user,user/903"),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("text"),
                    Value::Map(vec![(Value::from("body"), Value::from("child message"))]),
                )]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        assert_eq!(
            message.reply_to.as_ref().map(|id| id.0.as_str()),
            Some("903")
        );
    }

    #[test]
    fn parse_thread_message_extracts_reply_to_from_nested_msg_id() {
        let valid = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("messageID"), Value::from(909))]),
            ),
            (
                Value::from("replyTo"),
                Value::Map(vec![(Value::from("msgId"), Value::from("904"))]),
            ),
            (Value::from("senderUsername"), Value::from("alice")),
            (
                Value::from("messageBody"),
                Value::Map(vec![(
                    Value::from("text"),
                    Value::Map(vec![(Value::from("body"), Value::from("child message"))]),
                )]),
            ),
        ]);

        let message = parse_thread_message(&valid, &ConversationId::new("kb_conv:test"), None)
            .expect("expected parsed thread message");
        assert_eq!(
            message.reply_to.as_ref().map(|id| id.0.as_str()),
            Some("904")
        );
    }

    #[test]
    fn reply_ancestor_chain_cached_walks_entire_parent_chain() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "zbase-reply-chain-cached-test-{}-{}",
            std::process::id(),
            now_unix_ms()
        ));
        let local_store = LocalStore::open_at(path.clone()).expect("open local store");
        let conversation_id = ConversationId::new("kb_conv:test");

        let mut parent = make_test_message("200");
        parent.reply_to = Some(MessageId::new("150"));
        local_store
            .persist_message(&parent)
            .expect("persist parent message");

        assert!(
            !reply_ancestor_chain_cached(&local_store, &conversation_id, &MessageId::new("200")),
            "chain should be incomplete when ancestor is missing"
        );

        let root = make_test_message("150");
        local_store
            .persist_message(&root)
            .expect("persist root ancestor");

        assert!(
            reply_ancestor_chain_cached(&local_store, &conversation_id, &MessageId::new("200")),
            "chain should become complete after ancestor is indexed"
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn reply_ancestor_backfill_in_flight_dedupes_duplicate_keys() {
        let key = format!("test-key-{}-{}", std::process::id(), now_unix_ms());
        assert!(try_mark_reply_ancestor_backfill_in_flight(&key));
        assert!(!try_mark_reply_ancestor_backfill_in_flight(&key));
        clear_reply_ancestor_backfill_in_flight(&key);
        assert!(try_mark_reply_ancestor_backfill_in_flight(&key));
        clear_reply_ancestor_backfill_in_flight(&key);
    }

    #[test]
    fn parse_plain_mentions_keeps_shortcode_text_inside_inline_code() {
        let fragments = parse_plain_mentions_from_text("Use `:lol:` literally and :troll:");
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Emoji { alias, .. } if alias == "troll"
        )));
        assert!(
            !fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Emoji { alias, .. } if alias == "lol"
            )),
            "emoji shortcode inside inline code should remain plain text"
        );
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::InlineCode(code) if code == ":lol:"
        )));
    }

    #[test]
    fn parse_metadata_mentions_keeps_shortcode_text_inside_inline_code() {
        let mut metadata = MentionParseMetadata {
            has_hints: true,
            ..MentionParseMetadata::default()
        };
        metadata.at_mentions.insert("bob".to_string());

        let fragments =
            parse_metadata_mentions_from_text("hello @bob `:lol:` and :troll:", &metadata);
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Mention(user_id) if user_id.0 == "bob"
        )));
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Emoji { alias, .. } if alias == "troll"
        )));
        assert!(
            !fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Emoji { alias, .. } if alias == "lol"
            )),
            "emoji shortcode inside inline code should remain plain text"
        );
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::InlineCode(code) if code == ":lol:"
        )));
    }

    #[test]
    fn parse_plain_mentions_keeps_shortcode_text_inside_fenced_code_block() {
        let fragments = parse_plain_mentions_from_text("Use ```\n:lol:\n``` literally and :troll:");
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Code { text: code, .. } if code == ":lol:"
        )));
        assert!(
            !fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Emoji { alias, .. } if alias == "lol"
            )),
            "emoji shortcode inside fenced code block should remain code text"
        );
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Emoji { alias, .. } if alias == "troll"
        )));
    }

    #[test]
    fn parse_metadata_mentions_keeps_shortcode_text_inside_fenced_code_block() {
        let mut metadata = MentionParseMetadata {
            has_hints: true,
            ..MentionParseMetadata::default()
        };
        metadata.at_mentions.insert("bob".to_string());

        let fragments =
            parse_metadata_mentions_from_text("hello @bob ```\n:lol:\n``` and :troll:", &metadata);
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Mention(user_id) if user_id.0 == "bob"
        )));
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Code { text: code, .. } if code == ":lol:"
        )));
        assert!(
            !fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Emoji { alias, .. } if alias == "lol"
            )),
            "emoji shortcode inside fenced code block should remain code text"
        );
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Emoji { alias, .. } if alias == "troll"
        )));
    }

    #[test]
    fn parse_plain_mentions_extracts_quote_fragments() {
        let fragments =
            parse_plain_mentions_from_text("hello\n> quoted line\n> second line\nafterward");

        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Quote(quote) if quote.contains("quoted line") && quote.contains("second line")
        )));
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Text(text) if text == "hello\n"
        )));
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Text(text) if text == "afterward"
        )));
    }

    #[test]
    fn parse_plain_mentions_does_not_treat_inline_code_as_quote() {
        let fragments = parse_plain_mentions_from_text("keep `> nope`\n> yep");

        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::InlineCode(code) if code == "> nope"
        )));
        assert!(
            !fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Quote(quote) if quote.contains("nope")
            )),
            "inline code content should not become quote fragments"
        );
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Quote(quote) if quote.contains("yep")
        )));
    }

    #[test]
    fn quote_lines_are_literal_text() {
        let fragments = parse_plain_mentions_from_text("> before `code` after :events: @bob #chan");

        assert!(
            fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Quote(quote)
                    if quote.contains("before `code` after")
                        && quote.contains(":events:")
                        && quote.contains("@bob")
                        && quote.contains("#chan")
            )),
            "quote line should preserve literal text: {fragments:?}"
        );
        assert!(
            !fragments
                .iter()
                .any(|fragment| matches!(fragment, MessageFragment::InlineCode(_))),
            "quote lines should not parse inline code: {fragments:?}"
        );
        assert!(
            !fragments
                .iter()
                .any(|fragment| matches!(fragment, MessageFragment::Emoji { .. })),
            "quote lines should not parse emoji shortcodes: {fragments:?}"
        );
        assert!(
            !fragments
                .iter()
                .any(|fragment| matches!(fragment, MessageFragment::Mention(_))),
            "quote lines should not parse @mentions: {fragments:?}"
        );
        assert!(
            !fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::ChannelMention { .. } | MessageFragment::BroadcastMention(_)
            )),
            "quote lines should not parse channel/broadcast mentions: {fragments:?}"
        );
        assert!(
            !fragments
                .iter()
                .any(|fragment| matches!(fragment, MessageFragment::Link { .. })),
            "quote lines should not parse links into structured fragments: {fragments:?}"
        );
    }

    #[test]
    fn quote_context_survives_non_text_fragment_splits() {
        let mut fragments = Vec::new();
        push_plain_text_fragment(&mut fragments, "> at node:");
        fragments.push(MessageFragment::Link {
            url: "https://events".to_string(),
            display: "events".to_string(),
        });
        push_plain_text_fragment(&mut fragments, ":16248:13)\nafter");

        assert!(
            fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Quote(quote) if quote.contains("at node:")
            )),
            "leading chunk should be captured as quote: {fragments:?}"
        );
        assert!(
            fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Quote(quote) if quote.contains(":16248:13)")
            )),
            "quoted line continuation after a structured fragment should stay quoted: {fragments:?}"
        );
        assert!(
            !fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Text(text) if text.contains(":16248:13)")
            )),
            "stack-trace tail should not escape quote context: {fragments:?}"
        );
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Text(text) if text == "after"
        )));
    }

    #[test]
    fn emoji_shortcodes_are_not_parsed_on_quoted_lines() {
        let body = "> error at node:events:520:35 end\nnormal :troll: text";
        let fragments = parse_plain_mentions_from_text(body);

        assert!(
            fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Quote(quote) if quote.contains("node:events:520:35")
            )),
            "colon pairs on quoted lines should not be parsed as emoji shortcodes: {fragments:?}"
        );
        assert!(
            fragments.iter().any(|fragment| matches!(
                fragment,
                MessageFragment::Emoji { alias, .. } if alias == "troll"
            )),
            "emoji shortcodes on non-quoted lines should still be parsed: {fragments:?}"
        );
    }

    #[test]
    fn parse_plain_mentions_supports_hash_suffix_custom_emoji_alias() {
        let fragments = parse_plain_mentions_from_text("hello :troll#2: world");
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Emoji { alias, .. } if alias == "troll#2"
        )));
    }

    #[test]
    fn parse_metadata_mentions_supports_hash_suffix_custom_emoji_alias() {
        let metadata = MentionParseMetadata {
            has_hints: true,
            ..MentionParseMetadata::default()
        };
        let fragments = parse_metadata_mentions_from_text("hello :troll#2: world", &metadata);
        assert!(fragments.iter().any(|fragment| matches!(
            fragment,
            MessageFragment::Emoji { alias, .. } if alias == "troll#2"
        )));
    }

    #[test]
    fn internal_topics_never_become_sidebar_conversations() {
        for topic_type in [1i64, 2, 3, 4, 5] {
            let ui = Value::Map(vec![
                ("convID".into(), "abcd".into()),
                ("name".into(), "helperbot,aliceexample".into()),
                ("membersType".into(), IMPTEAM_MEMBERS_TYPE.into()),
                ("topicType".into(), topic_type.into()),
            ]);
            assert_eq!(
                parse_inbox_ui_item(&ui, Some("aliceexample")).is_some(),
                topic_type == TOPIC_TYPE_CHAT
            );
            let info = Value::Map(vec![
                ("id".into(), Value::Binary(vec![0xab, 0xcd])),
                ("tlfName".into(), "helperbot,aliceexample".into()),
                ("membersType".into(), IMPTEAM_MEMBERS_TYPE.into()),
                (
                    "triple".into(),
                    Value::Map(vec![("topicType".into(), topic_type.into())]),
                ),
            ]);
            let inbox = Value::Map(vec![(
                "conversations".into(),
                Value::Array(vec![Value::Map(vec![("info".into(), info)])]),
            )]);
            assert_eq!(
                parse_inbox_conversations(&inbox, Some("aliceexample"), false).len(),
                usize::from(topic_type == TOPIC_TYPE_CHAT)
            );
        }
    }

    #[test]
    fn parse_team_member_roles_extracts_active_roles() {
        let payload = Value::Array(vec![
            Value::Map(vec![
                (Value::from("username"), Value::from("alice")),
                (Value::from("role"), Value::from(4)),
                (Value::from("status"), Value::from(0)),
            ]),
            Value::Map(vec![
                (Value::from("username"), Value::from("bob")),
                (Value::from("role"), Value::from(3)),
                (Value::from("status"), Value::from(0)),
            ]),
            Value::Map(vec![
                (Value::from("username"), Value::from("carol")),
                (Value::from("role"), Value::from(2)),
                (Value::from("status"), Value::from(0)),
            ]),
            Value::Map(vec![
                (Value::from("username"), Value::from("deleted")),
                (Value::from("role"), Value::from(3)),
                (Value::from("status"), Value::from(2)),
            ]),
        ]);

        let parsed = parse_team_member_roles(&payload);
        assert_eq!(parsed.get(&UserId::new("alice")), Some(&4));
        assert_eq!(parsed.get(&UserId::new("bob")), Some(&3));
        assert_eq!(parsed.get(&UserId::new("carol")), Some(&2));
        assert!(
            !parsed.contains_key(&UserId::new("deleted")),
            "non-active team members should be ignored"
        );
    }

    #[test]
    #[ignore = "requires a running Keybase daemon and ZBASE_UNREAD_TEST_CACHE pointing to a disposable cache"]
    fn live_badge_snapshot_matches_cached_unread_counts() {
        let path = PathBuf::from(
            std::env::var("ZBASE_UNREAD_TEST_CACHE").expect("supply a disposable cache path"),
        );
        let store = LocalStore::open_at(path).unwrap();
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let value = runtime
            .block_on(async {
                let transport =
                    FramedMsgpackTransport::connect(&socket_path().expect("Keybase socket"))
                        .await?;
                KeybaseRpcClient::new(transport)
                    .call("keybase.1.badger.getBadgeState", vec![])
                    .await
            })
            .unwrap();
        let badges = crate::domain::conversation::ConversationBadgeState {
            counts: parse_badge_state(&value).expect("valid daemon badge snapshot"),
            complete: true,
        };
        let ids = store.load_cached_conversation_ids(100_000).unwrap();
        assert!(!ids.is_empty(), "cache must contain conversations");
        let mut unread = 0;
        for id in &ids {
            let summary = store.get_conversation(id).unwrap().unwrap();
            let mut expected = summary.clone();
            badges.apply(&mut expected);
            assert_eq!(
                (summary.unread_count, summary.mention_count),
                (expected.unread_count, expected.mention_count),
                "cached badge mismatch for {}",
                id.0
            );
            unread += usize::from(summary.unread_count > 0);
        }
        println!(
            "Verified {} cached conversations against live daemon badge state; {} unread",
            ids.len(),
            unread
        );
    }

    #[test]
    fn inbox_parsers_preserve_simple_and_complex_team_metadata() {
        for (raw_type, expected) in [
            (1, ConversationTeamType::Simple),
            (2, ConversationTeamType::Complex),
        ] {
            let info = Value::Map(vec![
                ("id".into(), Value::Binary(vec![0xab, 0xcd])),
                ("tlfName".into(), "finance".into()),
                ("topicName".into(), "general".into()),
                ("membersType".into(), TEAM_MEMBERS_TYPE.into()),
                ("teamType".into(), raw_type.into()),
            ]);
            let inbox = Value::Map(vec![(
                "conversations".into(),
                Value::Array(vec![Value::Map(vec![
                    ("info".into(), info),
                    (
                        "readerInfo".into(),
                        Value::Map(vec![(
                            "status".into(),
                            CONVERSATION_MEMBER_STATUS_ACTIVE.into(),
                        )]),
                    ),
                ])]),
            )]);
            let parsed = parse_inbox_conversations(&inbox, None, false);
            assert_eq!(
                parsed[0].summary.group.as_ref().unwrap().team_type,
                expected
            );
            assert_eq!(parsed[0].summary.kind, ConversationKind::Channel);
            let ui_item = Value::Map(vec![
                ("convID".into(), "abcd".into()),
                ("name".into(), "finance".into()),
                ("channel".into(), "general".into()),
                ("membersType".into(), TEAM_MEMBERS_TYPE.into()),
                ("teamType".into(), raw_type.into()),
                (
                    "memberStatus".into(),
                    CONVERSATION_MEMBER_STATUS_ACTIVE.into(),
                ),
            ]);
            assert_eq!(
                parse_inbox_ui_item(&ui_item, None)
                    .unwrap()
                    .summary
                    .group
                    .unwrap()
                    .team_type,
                expected
            );
        }
    }

    #[test]
    fn team_type_notification_promotes_all_cached_team_channels() {
        let path = temp_emoji_asset_dir("team-type-change");
        let store = Arc::new(LocalStore::open_at(path.clone()).unwrap());
        for (id, team) in [("abcd", "finance"), ("1234", "finance"), ("5678", "other")] {
            let summary = ConversationSummary {
                id: ConversationId::new(format!("kb_conv:{id}")),
                title: "general".into(),
                kind: ConversationKind::Channel,
                topic: "general".into(),
                group: Some(ConversationGroup {
                    id: team.into(),
                    display_name: team.into(),
                    team_type: ConversationTeamType::Simple,
                }),
                unread_count: 3,
                mention_count: 1,
                muted: false,
                last_activity_ms: 100,
                last_message: None,
            };
            store.persist_conversation(&summary, 100).unwrap();
        }
        let event = KeybaseNotifyEvent::from_method(
            "chat.1.NotifyChat.NewChatActivity",
            Value::Map(vec![(
                "activity".into(),
                Value::Map(vec![
                    ("activityType".into(), 8.into()),
                    (
                        "teamtype".into(),
                        Value::Map(vec![
                            ("convID".into(), Value::Binary(vec![0xab, 0xcd])),
                            ("teamType".into(), 2.into()),
                        ]),
                    ),
                ]),
            )]),
        );
        let (sender, receiver) = mpsc::channel();
        maybe_handle_team_type_notify(&event, &sender, &store);
        for id in ["abcd", "1234"] {
            let summary = store
                .get_conversation(&ConversationId::new(format!("kb_conv:{id}")))
                .unwrap()
                .unwrap();
            assert_eq!(
                summary.group.unwrap().team_type,
                ConversationTeamType::Complex
            );
            assert_eq!(
                (
                    summary.unread_count,
                    summary.mention_count,
                    summary.last_activity_ms
                ),
                (3, 1, 100)
            );
        }
        assert_eq!(
            store
                .get_conversation(&ConversationId::new("kb_conv:5678"))
                .unwrap()
                .unwrap()
                .group
                .unwrap()
                .team_type,
            ConversationTeamType::Simple
        );
        assert!(
            matches!(receiver.try_recv().unwrap(), BackendEvent::WorkspaceConversationsExtended { channels, .. } if channels.len() == 2)
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn badge_snapshot_parses_counts_and_rejects_malformed_data() {
        let value = Value::Map(vec![(
            Value::from("conversations"),
            Value::Array(vec![Value::Map(vec![
                (Value::from("convID"), Value::Binary(vec![0xab, 0xcd])),
                (Value::from("unreadMessages"), Value::from(7)),
                (Value::from("badgeCount"), Value::from(2)),
            ])]),
        )]);
        let counts = parse_badge_state(&value).unwrap();
        assert_eq!(
            counts[&ConversationId::new("kb_conv:abcd")],
            ConversationBadgeCounts {
                unread_count: 7,
                badge_count: 2
            }
        );
        assert!(parse_badge_state(&Value::Nil).is_none());
        assert!(
            parse_badge_state(&Value::Map(vec![(
                Value::from("conversations"),
                Value::Array(vec![Value::Nil])
            )]))
            .is_none()
        );
        assert!(
            parse_badge_state(&Value::Map(vec![(
                Value::from("conversations"),
                Value::Array(vec![])
            )]))
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn inbox_refresh_coalesces_bursts_without_losing_a_request() {
        let mut refresh = InboxUnreadRefresh::default();
        assert!(refresh.request());
        assert!(!refresh.request());
        assert!(!refresh.request());
        assert!(
            refresh.finish(),
            "run again after requests received during first fetch"
        );
        assert!(!refresh.request());
        assert!(
            refresh.finish(),
            "also retain a request arriving during the follow-up fetch"
        );
        assert!(!refresh.finish());
        assert!(
            refresh.request(),
            "a new worker can start after the previous one finishes"
        );
        assert!(!refresh.finish());
    }

    #[test]
    fn parse_read_marker_update_extracts_read_upto_and_unread_snapshot() {
        let payload = Value::Map(vec![(
            Value::from("activity"),
            Value::Map(vec![
                (Value::from("activityType"), Value::from(2)),
                (
                    Value::from("readMessage"),
                    Value::Map(vec![
                        (Value::from("convID"), Value::Binary(vec![0xAB, 0xCD])),
                        (Value::from("msgID"), Value::from(99)),
                        (
                            Value::from("conv"),
                            Value::Map(vec![
                                (
                                    Value::from("info"),
                                    Value::Map(vec![(
                                        Value::from("id"),
                                        Value::Binary(vec![0xAB, 0xCD]),
                                    )]),
                                ),
                                (
                                    Value::from("readerInfo"),
                                    Value::Map(vec![
                                        (Value::from("maxMsgid"), Value::from(105)),
                                        (Value::from("readMsgid"), Value::from(99)),
                                        (Value::from("badgeCount"), Value::from(2)),
                                    ]),
                                ),
                            ]),
                        ),
                    ]),
                ),
            ]),
        )]);
        let event = KeybaseNotifyEvent::Unknown {
            method: "chat.1.NotifyChat.NewChatActivity".to_string(),
            raw_params: payload,
        };

        let parsed = parse_read_marker_update_from_notify(&event).expect("read marker update");
        assert_eq!(parsed.conversation_id.0, "kb_conv:abcd");
        assert_eq!(parsed.read_upto.0, "99");
        let snapshot = parsed.snapshot.expect("snapshot");
        assert_eq!(snapshot.unread_count, 0);
        assert_eq!(snapshot.mention_count, 2);
        assert_eq!(
            snapshot.read_upto.as_ref().map(|id| id.0.as_str()),
            Some("99")
        );
    }

    fn unboxed_message(id: i64, sender: &str, ctime: i64, body: Vec<(Value, Value)>) -> Value {
        Value::Map(vec![(
            Value::from("valid"),
            Value::Map(vec![
                (
                    Value::from("serverHeader"),
                    Value::Map(vec![
                        (Value::from("messageID"), Value::from(id)),
                        (Value::from("ctime"), Value::from(ctime)),
                    ]),
                ),
                (Value::from("senderUsername"), Value::from(sender)),
                (Value::from("messageBody"), Value::Map(body)),
            ]),
        )])
    }

    fn inbox_with_max_messages(max_messages: Vec<Value>) -> Value {
        Value::Map(vec![(
            Value::from("conversations"),
            Value::Array(vec![Value::Map(vec![
                (
                    Value::from("info"),
                    Value::Map(vec![
                        (Value::from("id"), Value::Binary(vec![0x10, 0x40])),
                        (Value::from("tlfName"), Value::from("alice,bob")),
                        (Value::from("topicName"), Value::from("")),
                        (
                            Value::from("membersType"),
                            Value::from(IMPTEAM_MEMBERS_TYPE),
                        ),
                    ]),
                ),
                (
                    Value::from("readerInfo"),
                    Value::Map(vec![(
                        Value::from("status"),
                        Value::from(CONVERSATION_MEMBER_STATUS_ACTIVE),
                    )]),
                ),
                (Value::from("maxMessages"), Value::Array(max_messages)),
            ])]),
        )])
    }

    fn text_body(text: &str) -> Vec<(Value, Value)> {
        vec![
            (Value::from("messageType"), Value::from(MESSAGE_TYPE_TEXT)),
            (
                Value::from("text"),
                Value::Map(vec![(Value::from("body"), Value::from(text))]),
            ),
        ]
    }

    #[test]
    fn parse_inbox_conversations_previews_the_newest_snippet_worthy_max_message() {
        let attachment = vec![
            (
                Value::from("messageType"),
                Value::from(MESSAGE_TYPE_ATTACHMENT),
            ),
            (
                Value::from("attachment"),
                Value::Map(vec![(
                    Value::from("object"),
                    Value::Map(vec![(Value::from("filename"), Value::from("report.pdf"))]),
                )]),
            ),
        ];
        let edit = vec![
            (Value::from("messageType"), Value::from(MESSAGE_TYPE_EDIT)),
            (
                Value::from("edit"),
                Value::Map(vec![
                    (Value::from("messageID"), Value::from(41)),
                    (Value::from("body"), Value::from("hello :wave: (fixed)")),
                ]),
            ),
        ];
        let reaction = vec![
            (
                Value::from("messageType"),
                Value::from(MESSAGE_TYPE_REACTION),
            ),
            (
                Value::from("reaction"),
                Value::Map(vec![
                    (Value::from("messageID"), Value::from(41)),
                    (Value::from("body"), Value::from(":+1:")),
                ]),
            ),
        ];
        let join = vec![(Value::from("messageType"), Value::from(MESSAGE_TYPE_JOIN))];
        // Edits come first here: the result must not depend on the order.
        let inbox = inbox_with_max_messages(vec![
            unboxed_message(46, "bob", 1_700_000_006_000, edit),
            unboxed_message(40, "alice", 1_700_000_000_000, attachment),
            unboxed_message(41, "bob", 1_700_000_001_000, text_body("hello :wave:")),
            unboxed_message(45, "alice", 1_700_000_005_000, reaction),
            unboxed_message(47, "carol", 1_700_000_007_000, join),
        ]);
        let parsed = parse_inbox_conversations(&inbox, Some("alice"), false);
        let preview = parsed[0].summary.last_message.clone().expect("preview");
        assert_eq!(preview.message_id, Some(MessageId::new("41")));
        assert_eq!(preview.author_id, Some(UserId::new("bob")));
        assert_eq!(preview.timestamp_ms, 1_700_000_001_000);
        assert_eq!(
            preview.body,
            PreviewBody::Text("hello :wave: (fixed)".to_string())
        );

        // Only the attachment: its file name, not Keybase's stand-in body.
        let attachment_only = inbox_with_max_messages(vec![unboxed_message(
            40,
            "alice",
            1_700_000_000_000,
            vec![
                (
                    Value::from("messageType"),
                    Value::from(MESSAGE_TYPE_ATTACHMENT),
                ),
                (
                    Value::from("attachment"),
                    Value::Map(vec![(
                        Value::from("object"),
                        Value::Map(vec![(Value::from("filename"), Value::from("report.pdf"))]),
                    )]),
                ),
            ],
        )]);
        let parsed = parse_inbox_conversations(&attachment_only, Some("alice"), false);
        assert!(matches!(
            parsed[0].summary.last_message.as_ref().map(|preview| &preview.body),
            Some(PreviewBody::Attachment { name, caption: None, .. }) if name == "report.pdf"
        ));
        // No max messages: no preview, and nothing made up.
        let empty = inbox_with_max_messages(Vec::new());
        assert_eq!(
            parse_inbox_conversations(&empty, Some("alice"), false)[0]
                .summary
                .last_message,
            None
        );
    }

    #[test]
    fn parse_conversation_notifications_maps_keybase_wire_values() {
        let conversation = Value::Map(vec![(
            Value::from("notifications"),
            Value::Map(vec![
                (Value::from("channelWide"), Value::from(false)),
                (
                    Value::from("settings"),
                    Value::Map(vec![
                        (
                            Value::from(0i64),
                            Value::Map(vec![
                                (Value::from(0i64), Value::from(false)),
                                (Value::from(1i64), Value::from(true)),
                            ]),
                        ),
                        (
                            Value::from(1i64),
                            Value::Map(vec![
                                (Value::from(0i64), Value::from(false)),
                                (Value::from(1i64), Value::from(false)),
                            ]),
                        ),
                    ]),
                ),
            ]),
        )]);
        let parsed =
            parse_conversation_notification_settings(&conversation, CONVERSATION_STATUS_MUTED);
        assert!(parsed.muted);
        assert!(parsed.ignore_mentions);
        assert_eq!(parsed.desktop, NotificationLevel::MentionsOnly);
        assert_eq!(parsed.mobile, NotificationLevel::Nothing);

        // Conversations without notification info use Keybase's defaults.
        assert_eq!(
            parse_conversation_notification_settings(&Value::Nil, 0),
            ConversationNotificationSettings::default()
        );
    }

    #[test]
    fn conversation_notification_rpc_params_match_keybase_wire_format() {
        let settings = ConversationNotificationSettings {
            muted: true,
            ignore_mentions: true,
            desktop: NotificationLevel::All,
            mobile: NotificationLevel::MentionsOnly,
        };

        let status = conversation_status_rpc_params(&[0xab, 0xcd], settings);
        assert_eq!(
            map_get(&status, "status").and_then(as_i64),
            Some(CONVERSATION_STATUS_MUTED)
        );
        assert_eq!(
            map_get(&status, "identifyBehavior").and_then(as_i64),
            Some(IDENTIFY_BEHAVIOR_CHAT_GUI)
        );

        let params = app_notification_settings_rpc_params(&[0xab, 0xcd], settings);
        assert_eq!(
            map_get(&params, "channelWide").and_then(as_bool),
            Some(false)
        );
        let entries = map_get(&params, "settings")
            .and_then(as_array)
            .expect("settings array");
        assert_eq!(entries.len(), 4);
        let enabled = |entry: &Value| {
            (
                map_get(entry, "deviceType").and_then(as_i64),
                map_get(entry, "kind").and_then(as_i64),
                map_get(entry, "enabled").and_then(as_bool),
            )
        };
        assert_eq!(
            enabled(&entries[0]),
            (
                Some(NOTIFICATION_DEVICE_DESKTOP),
                Some(NOTIFICATION_KIND_AT_MENTION),
                Some(false)
            )
        );
        assert_eq!(
            enabled(&entries[1]),
            (
                Some(NOTIFICATION_DEVICE_DESKTOP),
                Some(NOTIFICATION_KIND_GENERIC),
                Some(true)
            )
        );
        assert_eq!(
            enabled(&entries[2]),
            (
                Some(NOTIFICATION_DEVICE_MOBILE),
                Some(NOTIFICATION_KIND_AT_MENTION),
                Some(true)
            )
        );
        assert_eq!(
            enabled(&entries[3]),
            (
                Some(NOTIFICATION_DEVICE_MOBILE),
                Some(NOTIFICATION_KIND_GENERIC),
                Some(false)
            )
        );
    }

    #[test]
    fn parse_inbox_ui_item_keeps_keybase_snippet_as_a_fallback_preview() {
        let item = Value::Map(vec![
            (Value::from("convID"), Value::from("1050")),
            (Value::from("name"), Value::from("alice,bob")),
            (
                Value::from("membersType"),
                Value::from(IMPTEAM_MEMBERS_TYPE),
            ),
            (Value::from("time"), Value::from(1_700_000_009_000i64)),
            (Value::from("snippet"), Value::from("  see you there  ")),
            (Value::from("maxVisibleMsgID"), Value::from(52)),
        ]);
        let parsed = parse_inbox_ui_item(&item, Some("alice")).expect("conversation");
        assert_eq!(
            parsed.summary.last_message,
            Some(MessagePreview {
                message_id: Some(MessageId::new("52")),
                author_id: None,
                timestamp_ms: 1_700_000_009_000,
                body: PreviewBody::Snippet("see you there".to_string()),
            })
        );
    }

    #[test]
    fn parse_inbox_conversations_does_not_infer_unread_from_nonvisible_activity() {
        let payload = Value::Map(vec![(
            Value::from("conversations"),
            Value::Array(vec![Value::Map(vec![
                (
                    Value::from("info"),
                    Value::Map(vec![
                        (Value::from("id"), Value::Binary(vec![0x10, 0x20])),
                        (Value::from("tlfName"), Value::from("alice,bob")),
                        (Value::from("topicName"), Value::from("")),
                        (
                            Value::from("membersType"),
                            Value::from(IMPTEAM_MEMBERS_TYPE),
                        ),
                        (
                            Value::from("status"),
                            Value::from(CONVERSATION_STATUS_UNFILED),
                        ),
                    ]),
                ),
                (
                    Value::from("readerInfo"),
                    Value::Map(vec![
                        (
                            Value::from("status"),
                            Value::from(CONVERSATION_MEMBER_STATUS_ACTIVE),
                        ),
                        (Value::from("maxMsgid"), Value::from(50)),
                        (Value::from("readMsgid"), Value::from(10)),
                    ]),
                ),
                (Value::from("unread"), Value::from(false)),
            ])]),
        )]);

        let parsed = parse_inbox_conversations(&payload, Some("alice"), false);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].summary.unread_count, 0);
    }

    #[test]
    fn parse_inbox_conversations_extracts_pinned_message_state() {
        let payload = Value::Map(vec![(
            Value::from("conversations"),
            Value::Array(vec![Value::Map(vec![
                (
                    Value::from("info"),
                    Value::Map(vec![
                        (Value::from("id"), Value::Binary(vec![0x10, 0x30])),
                        (Value::from("tlfName"), Value::from("alice,bob")),
                        (Value::from("topicName"), Value::from("")),
                        (
                            Value::from("membersType"),
                            Value::from(IMPTEAM_MEMBERS_TYPE),
                        ),
                        (
                            Value::from("status"),
                            Value::from(CONVERSATION_STATUS_UNFILED),
                        ),
                        (
                            Value::from("pinnedMsg"),
                            Value::Map(vec![
                                (Value::from("pinnerUsername"), Value::from("Bob")),
                                (
                                    Value::from("message"),
                                    Value::Map(vec![(
                                        Value::from("valid"),
                                        Value::Map(vec![
                                            (
                                                Value::from("serverHeader"),
                                                Value::Map(vec![
                                                    (Value::from("messageID"), Value::from(42)),
                                                    (
                                                        Value::from("ctime"),
                                                        Value::from(1_700_000_000),
                                                    ),
                                                ]),
                                            ),
                                            (Value::from("senderUsername"), Value::from("alice")),
                                            (
                                                Value::from("messageBody"),
                                                Value::Map(vec![(
                                                    Value::from("text"),
                                                    Value::Map(vec![(
                                                        Value::from("body"),
                                                        Value::from("Pinned hello"),
                                                    )]),
                                                )]),
                                            ),
                                        ]),
                                    )]),
                                ),
                            ]),
                        ),
                    ]),
                ),
                (
                    Value::from("readerInfo"),
                    Value::Map(vec![(
                        Value::from("status"),
                        Value::from(CONVERSATION_MEMBER_STATUS_ACTIVE),
                    )]),
                ),
            ])]),
        )]);

        let parsed = parse_inbox_conversations(&payload, Some("alice"), false);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].pinned_state.items.len(), 1);
        let pinned = &parsed[0].pinned_state.items[0];
        let PinnedTarget::Message { message_id } = &pinned.target;
        assert_eq!(message_id.0, "42");
        assert_eq!(
            pinned.pinned_by.as_ref().map(|value| value.0.as_str()),
            Some("bob")
        );
        assert_eq!(
            pinned
                .preview
                .as_ref()
                .and_then(|preview| preview.author_label.as_deref()),
            Some("alice")
        );
        assert_eq!(
            pinned
                .preview
                .as_ref()
                .and_then(|preview| preview.text.as_deref()),
            Some("Pinned hello")
        );
    }

    #[test]
    fn extract_message_timestamp_prefers_most_recent_plausible_candidate() {
        let payload = Value::Map(vec![
            (
                Value::from("serverHeader"),
                Value::Map(vec![(Value::from("ctime"), Value::from(1_490_000_000_i64))]),
            ),
            (
                Value::from("message"),
                Value::Map(vec![(
                    Value::from("sent_at_ms"),
                    Value::from(1_772_823_565_446_i64),
                )]),
            ),
        ]);

        let parsed = extract_message_timestamp_ms(&payload);
        assert_eq!(parsed, Some(1_772_823_565_446_i64));
    }
}
