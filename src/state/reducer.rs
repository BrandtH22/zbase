mod channel_membership;

use crate::domain::{
    affinity::Affinity,
    attachment::{
        AttachmentKind, AttachmentPreview, AttachmentSource, AttachmentSummary,
        attachment_kind_from_path,
    },
    backend::AccountId,
    channel_details::ConversationNotificationSettings,
    conversation::{ConversationKind, ConversationSummary},
    ids::{ChannelId, ConversationId, DmId, MessageId, SidebarSectionId, UserId, WorkspaceId},
    message::{MessageFragment, MessageRecord, MessageSendState},
    message_preview::{MessagePreview, PreviewBody},
    presence::Presence,
    profile::{
        ProfileSection, SocialGraph, SocialGraphEntry, SocialGraphListType, SocialListLoadState,
        UserProfile,
    },
    route::Route,
};
use crate::util::formatting::now_unix_ms;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use super::{
    action::{DraftKey, UiAction},
    effect::{BackendCommand, Effect, SearchScope},
    event::{AppEvent, BackendEvent, BootstrapSource},
    ids::{ClientMessageId, OpId, QueryId},
    state::{
        BootPhase, ConnectionState, UiSidebarRowState, UiSidebarSectionState, UiState,
        UserProfileState,
    },
};

#[derive(Clone, Debug, Default)]
pub struct ReducerOutput {
    pub effects: Vec<Effect>,
}

pub fn reduce(state: &mut UiState, event: AppEvent) -> ReducerOutput {
    match event {
        AppEvent::Ui(action) => reduce_ui_action(state, action),
        AppEvent::Backend(event) => reduce_backend_event(state, event),
        AppEvent::EffectResult(_) | AppEvent::Tick(_) => ReducerOutput::default(),
    }
}

fn reduce_ui_action(state: &mut UiState, action: UiAction) -> ReducerOutput {
    match action {
        UiAction::LeaveChannel { conversation_id } => {
            channel_membership::request_leave(state, conversation_id)
        }
        UiAction::SetConversationNotificationSettings {
            conversation_id,
            settings,
            previous,
        } => {
            apply_conversation_notification_settings(state, &conversation_id, settings);
            ReducerOutput {
                effects: vec![Effect::Backend(
                    BackendCommand::SetConversationNotificationSettings {
                        conversation_id,
                        settings,
                        previous,
                    },
                )],
            }
        }
        UiAction::SetNotifySound { enabled } => {
            let previous = std::mem::replace(&mut state.notifications.notify_sound, enabled);
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::SetNotifySound {
                    enabled,
                    previous,
                })],
            }
        }
        UiAction::StartApp => {
            state.app.boot_phase = BootPhase::HydratingLocalState;
            ReducerOutput {
                effects: vec![
                    Effect::LoadSettings,
                    Effect::Backend(BackendCommand::ConnectAccount {
                        account_id: AccountId::new("account_demo_keybase"),
                    }),
                    Effect::Backend(BackendCommand::LoadBootstrap {
                        account_id: AccountId::new("account_demo_keybase"),
                    }),
                ],
            }
        }
        UiAction::Navigate(route) => {
            state.navigation.current_route = Some(route.clone());
            state.sidebar.highlighted_route = Some(route.clone());

            let Some(conversation_id) = conversation_id_from_route(&route) else {
                return ReducerOutput::default();
            };

            let changed = state.timeline.conversation_id.as_ref() != Some(&conversation_id);
            state.timeline.conversation_id = Some(conversation_id.clone());
            state.navigation.active_thread_root = None;
            state.thread.open = false;
            state.thread.root_message_id = None;
            state.thread.replies.clear();
            state.thread.reply_draft.clear();
            state.thread.loading = false;
            if changed {
                state.timeline.messages.clear();
                state.timeline.typing_text = None;
                state.timeline.highlighted_message_id = None;
                state.timeline.older_cursor = None;
                state.timeline.newer_cursor = None;
                state.timeline.loading_older = false;
            }

            let mut effects = vec![Effect::Backend(BackendCommand::LoadConversation {
                conversation_id: conversation_id.clone(),
            })];
            if state
                .backend
                .accounts
                .values()
                .any(|account| account.capabilities.supports_conversation_members)
            {
                effects.push(Effect::Backend(BackendCommand::LoadConversationMembers {
                    conversation_id,
                }));
            }
            ReducerOutput { effects }
        }
        UiAction::NavigateQuiet(route) => {
            state.navigation.current_route = Some(route.clone());
            state.sidebar.highlighted_route = Some(route.clone());

            let Some(conversation_id) = conversation_id_from_route(&route) else {
                return ReducerOutput::default();
            };

            let changed = state.timeline.conversation_id.as_ref() != Some(&conversation_id);
            state.timeline.conversation_id = Some(conversation_id);
            state.navigation.active_thread_root = None;
            state.thread.open = false;
            state.thread.root_message_id = None;
            state.thread.replies.clear();
            state.thread.reply_draft.clear();
            state.thread.loading = false;
            if changed {
                state.timeline.messages.clear();
                state.timeline.typing_text = None;
                state.timeline.highlighted_message_id = None;
                state.timeline.older_cursor = None;
                state.timeline.newer_cursor = None;
                state.timeline.loading_older = false;
            }

            ReducerOutput::default()
        }
        UiAction::NavigateBack => ReducerOutput::default(),
        UiAction::OpenThread { root_id } => {
            state.navigation.active_thread_root = Some(root_id.clone());
            state.thread.open = true;
            if state.thread.root_message_id.as_ref() != Some(&root_id) {
                state.thread.replies.clear();
            }
            state.thread.root_message_id = Some(root_id.clone());
            state.thread.reply_draft = state
                .drafts
                .get(&DraftKey::Thread(root_id.clone()))
                .map(|draft| draft.text.clone())
                .unwrap_or_default();
            state.thread.loading = true;
            let Some(conversation_id) = state.timeline.conversation_id.clone() else {
                return ReducerOutput::default();
            };
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadThread {
                    conversation_id,
                    root_id,
                })],
            }
        }
        UiAction::CloseRightPane => {
            state.backend.profile_panel.selected_user = None;
            state.navigation.active_thread_root = None;
            state.thread.open = false;
            state.thread.root_message_id = None;
            state.thread.replies.clear();
            state.thread.reply_draft.clear();
            state.thread.loading = false;
            ReducerOutput::default()
        }
        UiAction::RefreshPeopleNames => {
            refresh_sidebar(state);
            ReducerOutput::default()
        }
        UiAction::SetSidebarFilter(filter) => {
            state.sidebar_filter = filter;
            state.sidebar.filter = state.sidebar_filter.clone();
            ReducerOutput::default()
        }
        UiAction::SetSearchQuery(query) => {
            state.search_query = query;
            state.search.query = state.search_query.clone();
            ReducerOutput::default()
        }
        UiAction::SubmitSearch => {
            let query_id = next_query_id(state);
            state.search.active_query_id = Some(query_id.clone());
            state.search.submitted_query = Some(state.search.query.trim().to_string());
            state.search.is_loading = true;
            state.search.results.clear();
            state.search.highlighted_index = None;
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::Search {
                    query_id,
                    scope: match state.navigation.current_route.as_ref() {
                        Some(Route::Channel { workspace_id, .. })
                        | Some(Route::DirectMessage { workspace_id, .. })
                        | Some(Route::Activity { workspace_id })
                        | Some(Route::Search { workspace_id, .. })
                        | Some(Route::WorkspaceHome { workspace_id })
                        | Some(Route::ActiveCall { workspace_id, .. }) => {
                            SearchScope::Workspace(workspace_id.clone())
                        }
                        Some(Route::Preferences) | None => SearchScope::Global,
                    },
                    query: state.search_query.clone(),
                    filters: state.search.filters.clone(),
                })],
            }
        }
        UiAction::OpenNewChat => {
            state.new_chat = super::state::UiNewChatState::default();
            state.new_chat.open = true;
            state.new_chat.people = super::new_chat_search::PeopleCorpus::from_state(state);
            ReducerOutput::default()
        }
        UiAction::CloseNewChat => {
            state.new_chat = super::state::UiNewChatState::default();
            ReducerOutput::default()
        }
        UiAction::NewChatSearchUsers { query } => {
            if state.new_chat.people_dirty {
                state.new_chat.people = super::new_chat_search::PeopleCorpus::from_state(state);
                state.new_chat.people_dirty = false;
            }
            state.new_chat.search_query = query.clone();
            state.new_chat.active_query_id = None;
            state.new_chat.search_results = state.new_chat.people.search(&query, 32);
            state.new_chat.is_searching = !query.trim().is_empty();
            state.new_chat.error = None;
            if query.trim().is_empty() {
                return ReducerOutput::default();
            }
            let query_id = next_query_id(state);
            state.new_chat.active_query_id = Some(query_id);
            ReducerOutput::default()
        }
        UiAction::NewChatSearchRemote { query_id } => {
            if !state.new_chat.open || state.new_chat.active_query_id.as_ref() != Some(&query_id) {
                return ReducerOutput::default();
            }
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::SearchUsers {
                    query_id,
                    query: state.new_chat.search_query.clone(),
                })],
            }
        }
        UiAction::NewChatAddParticipant { user } => {
            if state
                .new_chat
                .selected_participants
                .iter()
                .all(|entry| entry.id != user.id)
            {
                state.new_chat.selected_participants.push(user);
            }
            state.new_chat.error = None;
            ReducerOutput::default()
        }
        UiAction::NewChatRemoveParticipant { user_id } => {
            state
                .new_chat
                .selected_participants
                .retain(|entry| entry.id != user_id);
            ReducerOutput::default()
        }
        UiAction::NewChatCreate => {
            if state.new_chat.selected_participants.is_empty() {
                state.new_chat.error = Some("Select at least one participant.".to_string());
                return ReducerOutput::default();
            }
            state.new_chat.creating = true;
            state.new_chat.error = None;
            let participants = state
                .new_chat
                .selected_participants
                .iter()
                .map(|entry| entry.id.clone())
                .collect::<Vec<_>>();
            let kind = if participants.len() == 1 {
                ConversationKind::DirectMessage
            } else {
                ConversationKind::GroupDirectMessage
            };
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::CreateConversation {
                    op_id: next_op_id(state, "new-chat"),
                    participants,
                    kind,
                })],
            }
        }
        UiAction::QuickSwitcherSearch { seq, query } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::Search {
                query_id: QueryId::new(format!("quick-switcher-{seq}")),
                scope: match state.navigation.current_route.as_ref() {
                    Some(Route::Channel { workspace_id, .. })
                    | Some(Route::DirectMessage { workspace_id, .. })
                    | Some(Route::Activity { workspace_id })
                    | Some(Route::Search { workspace_id, .. })
                    | Some(Route::WorkspaceHome { workspace_id })
                    | Some(Route::ActiveCall { workspace_id, .. }) => {
                        SearchScope::Workspace(workspace_id.clone())
                    }
                    Some(Route::Preferences) | None => SearchScope::Global,
                },
                query,
                filters: Vec::new(),
            })],
        },
        UiAction::FindInChatSearch {
            seq,
            conversation_id,
            query,
        } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::Search {
                query_id: QueryId::new(format!("find-in-chat-{seq}")),
                scope: SearchScope::Conversation(conversation_id),
                query,
                filters: Vec::new(),
            })],
        },
        UiAction::ToggleSearchFilter(filter) => {
            if let Some(index) = state.search.filters.iter().position(|item| item == &filter) {
                state.search.filters.remove(index);
            } else {
                state.search.filters.push(filter);
            }
            ReducerOutput::default()
        }
        UiAction::UpdateDraft { key, text } => {
            if let DraftKey::Thread(_) = &key {
                state.thread.reply_draft = text.clone();
            }
            state.drafts.entry(key.clone()).or_default().text = text;
            ReducerOutput {
                effects: vec![Effect::PersistDraft { key }],
            }
        }
        UiAction::SendMessage { key } => {
            let Some(draft) = state.drafts.get(&key) else {
                return ReducerOutput::default();
            };
            let draft_text = draft.text.clone();
            if draft_text.trim().is_empty() {
                return ReducerOutput::default();
            }

            match key.clone() {
                DraftKey::Conversation(conversation_id) => {
                    let client_message_id = next_client_message_id(state);
                    let op_id = next_op_id(state, "send");
                    add_pending_message_to_state(
                        state,
                        &conversation_id,
                        &draft_text,
                        Vec::new(),
                        None,
                        &client_message_id,
                    );
                    ReducerOutput {
                        effects: vec![Effect::Backend(BackendCommand::SendMessage {
                            op_id,
                            draft_key: key,
                            conversation_id,
                            client_message_id,
                            text: draft_text,
                            attachments: Vec::new(),
                            reply_to: None,
                        })],
                    }
                }
                DraftKey::Thread(root_id) => {
                    let Some(conversation_id) = state.timeline.conversation_id.clone() else {
                        return ReducerOutput::default();
                    };
                    let reply_to = state
                        .thread
                        .replies
                        .last()
                        .map(|message| message.id.clone())
                        .unwrap_or_else(|| root_id.clone());
                    let client_message_id = next_client_message_id(state);
                    let op_id = next_op_id(state, "send");
                    add_pending_message_to_state(
                        state,
                        &conversation_id,
                        &draft_text,
                        Vec::new(),
                        Some(root_id.clone()),
                        &client_message_id,
                    );
                    ReducerOutput {
                        effects: vec![Effect::Backend(BackendCommand::SendMessage {
                            op_id,
                            draft_key: key,
                            conversation_id,
                            client_message_id,
                            text: draft_text,
                            attachments: Vec::new(),
                            reply_to: Some(reply_to),
                        })],
                    }
                }
            }
        }
        UiAction::DiscardFailedMessage { message_id } => {
            discard_failed_message(state, &message_id);
            ReducerOutput::default()
        }
        UiAction::EditMessage {
            conversation_id,
            message_id,
            text,
        } => {
            let text = text.trim().to_string();
            if text.is_empty() {
                return ReducerOutput::default();
            }
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::EditMessage {
                    op_id: next_op_id(state, "edit"),
                    conversation_id,
                    message_id,
                    text,
                })],
            }
        }
        UiAction::DeleteMessage {
            conversation_id,
            message_id,
        } => {
            // Keep the message visible until the backend confirms deletion.
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::DeleteMessage {
                    op_id: next_op_id(state, "delete"),
                    conversation_id,
                    message_id,
                })],
            }
        }
        UiAction::SendAttachment {
            key,
            local_path,
            filename,
            caption,
        } => {
            let local_path = local_path.trim().to_string();
            if local_path.is_empty() {
                return ReducerOutput::default();
            }

            let filename = filename.trim().to_string();
            let filename_fallback = if filename.is_empty() {
                "attachment".to_string()
            } else {
                filename
            };
            let caption = caption.trim().to_string();
            let pending_attachment_kind = attachment_kind_from_path(Path::new(&local_path));
            let pending_attachment_source = AttachmentSource::LocalPath(local_path.clone());
            let pending_attachment_preview =
                matches!(pending_attachment_kind, AttachmentKind::Image).then(|| {
                    AttachmentPreview {
                        source: pending_attachment_source.clone(),
                        width: None,
                        height: None,
                    }
                });
            let pending_attachment = AttachmentSummary {
                name: filename_fallback.clone(),
                kind: pending_attachment_kind,
                preview: pending_attachment_preview,
                source: Some(pending_attachment_source),
                ..AttachmentSummary::default()
            };

            match key.clone() {
                DraftKey::Conversation(conversation_id) => {
                    let client_message_id = next_client_message_id(state);
                    let op_id = next_op_id(state, "send-attachment");
                    add_pending_message_to_state(
                        state,
                        &conversation_id,
                        &caption,
                        vec![pending_attachment.clone()],
                        None,
                        &client_message_id,
                    );
                    ReducerOutput {
                        effects: vec![Effect::Backend(BackendCommand::SendAttachment {
                            op_id,
                            draft_key: key,
                            conversation_id,
                            client_message_id,
                            local_path: local_path.clone(),
                            filename: filename_fallback.clone(),
                            caption: caption.clone(),
                        })],
                    }
                }
                DraftKey::Thread(root_id) => {
                    let Some(conversation_id) = state.timeline.conversation_id.clone() else {
                        return ReducerOutput::default();
                    };
                    let client_message_id = next_client_message_id(state);
                    let op_id = next_op_id(state, "send-attachment");
                    add_pending_message_to_state(
                        state,
                        &conversation_id,
                        &caption,
                        vec![pending_attachment.clone()],
                        Some(root_id),
                        &client_message_id,
                    );
                    ReducerOutput {
                        effects: vec![Effect::Backend(BackendCommand::SendAttachment {
                            op_id,
                            draft_key: key,
                            conversation_id,
                            client_message_id,
                            local_path,
                            filename: filename_fallback,
                            caption,
                        })],
                    }
                }
            }
        }
        UiAction::ReactToMessage {
            conversation_id,
            message_id,
            emoji,
        } => {
            let op_id = next_op_id(state, "react");
            let previous = state
                .backend
                .message_reactions
                .get(&conversation_id)
                .and_then(|messages| messages.get(&message_id))
                .cloned()
                .unwrap_or_default();
            state.backend.pending_reactions.insert(
                op_id.clone(),
                super::state::PendingReaction {
                    conversation_id: conversation_id.clone(),
                    message_id: message_id.clone(),
                    previous,
                },
            );
            state
                .backend
                .latest_reaction_ops
                .insert((conversation_id.clone(), message_id.clone()), op_id.clone());
            let actor_id = current_user_id(state);
            toggle_runtime_reaction(
                state,
                conversation_id.clone(),
                message_id.clone(),
                emoji.clone(),
                actor_id,
                now_unix_ms(),
            );
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::ReactToMessage {
                    op_id,
                    conversation_id,
                    message_id,
                    reaction: emoji,
                })],
            }
        }
        UiAction::StartCall { conversation_id } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::StartCall {
                op_id: next_op_id(state, "call"),
                conversation_id,
            })],
        },
        UiAction::MarkConversationRead {
            conversation_id,
            message_id,
        } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::MarkRead {
                conversation_id,
                message_id,
            })],
        },
        UiAction::ReturnToLatestMessages => {
            let Some(conversation_id) = state.timeline.conversation_id.clone() else {
                return ReducerOutput::default();
            };
            state.timeline.highlighted_message_id = None;
            state.timeline.loading_older = false;
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadConversation {
                    conversation_id,
                })],
            }
        }
        UiAction::JumpToMessage {
            conversation_id,
            message_id,
        } => {
            state.timeline.conversation_id = Some(conversation_id.clone());
            state.timeline.messages.clear();
            state.timeline.highlighted_message_id = Some(message_id.clone());
            state.timeline.typing_text = None;
            state.timeline.older_cursor = None;
            state.timeline.newer_cursor = None;
            state.timeline.loading_older = true;
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::JumpToMessage {
                    conversation_id,
                    message_id,
                })],
            }
        }
        UiAction::LoadOlderMessages { key, cursor } => {
            state.timeline.loading_older = true;
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadOlderMessages {
                    key,
                    cursor,
                })],
            }
        }
        UiAction::OpenOrCreateDirectMessage { user_id } => {
            let target_user = UserId::new(user_id.0.trim().to_ascii_lowercase());
            if target_user.0.is_empty() {
                return ReducerOutput::default();
            }
            if let Some(conversation_id) = find_existing_direct_message_with_user(
                &state.workspace.direct_messages,
                &target_user,
            ) {
                let workspace_id = state
                    .workspace
                    .active_workspace_id
                    .clone()
                    .unwrap_or_else(|| WorkspaceId::new("ws_primary"));
                return reduce_ui_action(
                    state,
                    UiAction::Navigate(Route::DirectMessage {
                        workspace_id,
                        dm_id: DmId::new(conversation_id.0),
                    }),
                );
            }
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::CreateConversation {
                    op_id: next_op_id(state, "profile-message"),
                    participants: vec![target_user],
                    kind: ConversationKind::DirectMessage,
                })],
            }
        }
        UiAction::ShowUserProfileCard { user_id } | UiAction::ShowUserProfilePanel { user_id } => {
            state.backend.profile_panel.selected_user = Some(user_id.clone());
            state.backend.profile_panel.loading.insert(user_id.clone());
            let mut output = ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadUserProfile {
                    user_id: user_id.clone(),
                })],
            };
            if state.backend.profile_panel.profiles.contains_key(&user_id) {
                let list_type = state.backend.profile_panel.active_social_tab;
                output
                    .effects
                    .extend(request_social_graph_refresh(state, user_id, list_type).effects);
            }
            output
        }
        UiAction::RefreshProfilePresence {
            user_id,
            conversation_id,
        } => ReducerOutput {
            effects: vec![Effect::Backend(BackendCommand::RefreshParticipants {
                user_id,
                conversation_id,
            })],
        },
        UiAction::LoadSocialGraphList { user_id, list_type } => {
            state.backend.profile_panel.active_social_tab = list_type;
            request_social_graph_refresh(state, user_id, list_type)
        }
        UiAction::FollowUser { user_id } => {
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), Affinity::Positive);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                Affinity::Positive,
            );
            update_follow_status(&mut state.backend.profile_panel.profiles, &user_id, true);
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::FollowUser { user_id })],
            }
        }
        UiAction::UnfollowUser { user_id } => {
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), Affinity::None);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                Affinity::None,
            );
            update_follow_status(&mut state.backend.profile_panel.profiles, &user_id, false);
            ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::UnfollowUser { user_id })],
            }
        }
    }
}

fn reduce_backend_event(state: &mut UiState, event: BackendEvent) -> ReducerOutput {
    // Rebuild lazily on the next input, so background hydration does not reorder
    // the rows under a keyboard selection or repeatedly rebuild the corpus.
    if state.new_chat.open
        && matches!(
            &event,
            BackendEvent::BootstrapLoaded { .. }
                | BackendEvent::WorkspaceConversationsExtended { .. }
                | BackendEvent::UserProfileUpserted { .. }
                | BackendEvent::UserProfileLoaded { .. }
                | BackendEvent::SocialGraphListLoaded { .. }
                | BackendEvent::FollowStatusChanged { .. }
                | BackendEvent::AffinityChanged { .. }
                | BackendEvent::AffinitySynced { .. }
                | BackendEvent::TeamRolesUpdated { .. }
                | BackendEvent::ConversationMembersUpdated { .. }
        )
    {
        state.new_chat.people_dirty = true;
    }
    match event {
        BackendEvent::AccountConnected { account_id } => {
            if let Some(account) = state.backend.accounts.get_mut(&account_id) {
                account.connection_state = ConnectionState::Connected;
            }
            if !state.backend.hydrated_accounts.contains(&account_id) {
                state.app.boot_phase = BootPhase::ConnectingBackend;
            } else {
                state.app.boot_phase = BootPhase::Ready;
            }
            // A successful subscription is followed by server reconciliation. This also
            // covers daemon restarts after cached state has already been displayed.
            return ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadBootstrap {
                    account_id,
                })],
            };
        }
        BackendEvent::AccountDisconnected { account_id, .. } => {
            if let Some(account) = state.backend.accounts.get_mut(&account_id) {
                account.connection_state = ConnectionState::Disconnected;
            }
            state.app.boot_phase = BootPhase::Degraded;
        }
        BackendEvent::BootstrapLoaded {
            source,
            account_id,
            payload,
        } => {
            let first_hydration = state.backend.hydrated_accounts.insert(account_id.clone());
            if source == BootstrapSource::Cache && !first_hydration {
                return ReducerOutput::default();
            }
            let initialize_selection = first_hydration
                && state.timeline.conversation_id.is_none()
                && matches!(
                    state.navigation.current_route,
                    None | Some(Route::WorkspaceHome { .. })
                );
            let mut bootstrap_effects = Vec::new();
            if let Some(account) = state.backend.accounts.get_mut(&account_id)
                && let Some(display_name) = payload.account_display_name
            {
                account.display_name = display_name;
            }

            if state.workspace.active_workspace_id.is_none() {
                state.workspace.active_workspace_id = payload.active_workspace_id.clone();
            }
            state.workspace.workspace_name = payload.workspace_name;
            merge_workspace_conversations(state, payload.channels, payload.direct_messages);

            for binding in payload.workspace_bindings {
                state
                    .backend
                    .workspace_bindings
                    .insert(binding.workspace_id.clone(), binding);
            }
            for binding in payload.conversation_bindings {
                state
                    .backend
                    .conversation_bindings
                    .insert(binding.conversation_id.clone(), binding);
            }
            for binding in payload.message_bindings {
                state
                    .backend
                    .message_bindings
                    .insert(binding.message_id.clone(), binding);
            }
            for (conversation_id, settings) in payload.conversation_notifications {
                state
                    .backend
                    .conversation_notifications
                    .insert(conversation_id, settings);
            }

            let workspace_id = state
                .workspace
                .active_workspace_id
                .clone()
                .unwrap_or_else(|| WorkspaceId::new("ws_primary"));
            apply_authoritative_badges(state);
            state.sidebar.sections = build_sidebar_sections(
                &state.workspace.channels,
                &state.workspace.direct_messages,
                &workspace_id,
                &state.backend.user_profiles,
            );

            if initialize_selection
                && let Some(conversation_id) = payload
                    .selected_conversation_id
                    .clone()
                    .filter(|id| !state.workspace.left_conversation_ids.contains(id))
            {
                state.timeline.conversation_id = Some(conversation_id.clone());
                if let Some(route) = route_for_conversation_id(
                    &conversation_id,
                    &workspace_id,
                    &state.workspace.channels,
                    &state.workspace.direct_messages,
                ) {
                    state.navigation.current_route = Some(route.clone());
                    state.sidebar.highlighted_route = Some(route);
                }
                state.timeline.unread_marker = payload.unread_marker;
            }
            if state.timeline.conversation_id == payload.selected_conversation_id {
                for message in payload.selected_messages {
                    upsert_message_in_state(state, message);
                }
            }
            // Refresh the user's selection, never the bootstrap's default selection.
            if let Some(conversation_id) = state.timeline.conversation_id.clone() {
                bootstrap_effects.push(Effect::Backend(BackendCommand::LoadConversation {
                    conversation_id: conversation_id.clone(),
                }));
                if let Some(root_id) = state.navigation.active_thread_root.clone() {
                    bootstrap_effects.push(Effect::Backend(BackendCommand::LoadThread {
                        conversation_id,
                        root_id,
                    }));
                }
            }
            state.app.boot_phase = BootPhase::Ready;
            return ReducerOutput {
                effects: bootstrap_effects,
            };
        }
        BackendEvent::WorkspaceConversationsExtended {
            workspace_id,
            channels,
            direct_messages,
            conversation_bindings,
        } => {
            if state.workspace.active_workspace_id.is_none() {
                state.workspace.active_workspace_id = Some(workspace_id.clone());
            }
            merge_workspace_conversations(state, channels, direct_messages);
            for binding in conversation_bindings {
                state
                    .backend
                    .conversation_bindings
                    .insert(binding.conversation_id.clone(), binding);
            }
            let sidebar_workspace_id = state
                .workspace
                .active_workspace_id
                .clone()
                .unwrap_or(workspace_id);
            apply_authoritative_badges(state);
            state.sidebar.sections = build_sidebar_sections(
                &state.workspace.channels,
                &state.workspace.direct_messages,
                &sidebar_workspace_id,
                &state.backend.user_profiles,
            );
        }
        BackendEvent::WorkspaceConversationsExcluded {
            workspace_id,
            conversation_ids,
        } => {
            state
                .workspace
                .excluded_conversation_ids
                .extend(conversation_ids);
            merge_workspace_conversations(state, Vec::new(), Vec::new());
            state.sidebar.sections = build_sidebar_sections(
                &state.workspace.channels,
                &state.workspace.direct_messages,
                &workspace_id,
                &state.backend.user_profiles,
            );
            if state
                .timeline
                .conversation_id
                .as_ref()
                .is_some_and(|id| state.workspace.excluded_conversation_ids.contains(id))
            {
                if let Some(summary) = state
                    .workspace
                    .channels
                    .iter()
                    .chain(&state.workspace.direct_messages)
                    .next()
                {
                    let route = route_for_summary(summary, &workspace_id);
                    return reduce_ui_action(state, UiAction::Navigate(route));
                }
                state.timeline.conversation_id = None;
                state.timeline.messages.clear();
                state.navigation.current_route = Some(Route::WorkspaceHome { workspace_id });
                state.sidebar.highlighted_route = state.navigation.current_route.clone();
            }
        }
        BackendEvent::HistorySynced { conversation_id } => {
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                let mut effects = vec![Effect::Backend(BackendCommand::LoadConversation {
                    conversation_id: conversation_id.clone(),
                })];
                if let Some(root_id) = state.navigation.active_thread_root.clone() {
                    effects.push(Effect::Backend(BackendCommand::LoadThread {
                        conversation_id,
                        root_id,
                    }));
                }
                return ReducerOutput { effects };
            }
        }
        BackendEvent::ConversationNotificationSettingsSynced {
            conversation_id,
            settings,
        } => {
            apply_conversation_notification_settings(state, &conversation_id, settings);
        }
        BackendEvent::WorkspaceSynced { .. } | BackendEvent::ConversationLoaded { .. } => {}
        BackendEvent::MessagesPrepended {
            key,
            messages,
            cursor,
        } => match key {
            super::state::TimelineKey::Conversation(conversation_id) => {
                if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                    let previous_oldest_id = state
                        .timeline
                        .messages
                        .first()
                        .map(|message| message.id.0.clone());
                    for message in &messages {
                        apply_message_reactions(state, message);
                    }
                    let mut merged = messages;
                    merged.retain(|message| {
                        !state
                            .backend
                            .deleted_messages
                            .contains(&(conversation_id.clone(), message.id.clone()))
                    });
                    merged.extend(state.timeline.messages.clone());
                    normalize_messages_by_id(&mut merged);
                    trim_timeline_for_prepend(&mut merged);
                    recompute_thread_reply_counts(&mut merged);
                    state.timeline.messages = merged;
                    let next_oldest_id = state
                        .timeline
                        .messages
                        .first()
                        .map(|message| message.id.0.clone());
                    let no_older_progress =
                        next_oldest_id == previous_oldest_id || state.timeline.messages.is_empty();
                    state.timeline.older_cursor = if no_older_progress { None } else { cursor };
                    state.timeline.loading_older = false;
                    state.timeline.newer_cursor = state
                        .timeline
                        .messages
                        .last()
                        .map(|message| message.id.0.clone());
                }
            }
            super::state::TimelineKey::Thread {
                conversation_id,
                root_id,
                requested_root_id,
            } => {
                if state.timeline.conversation_id.as_ref() == Some(&conversation_id)
                    && state.thread.open
                    && state.navigation.active_thread_root.as_ref() == Some(&requested_root_id)
                {
                    state.thread.root_message_id = Some(root_id);
                    state.thread.loading = false;
                    state.thread.replies = messages;
                    state.thread.replies.retain(|message| {
                        !state
                            .backend
                            .deleted_messages
                            .contains(&(conversation_id.clone(), message.id.clone()))
                    });
                    normalize_messages_by_id(&mut state.thread.replies);
                }
            }
        },
        BackendEvent::TimelineReplaced {
            conversation_id,
            messages,
            older_cursor,
            newer_cursor,
        } => {
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                for message in &messages {
                    apply_message_reactions(state, message);
                }
                // TimelineReplaced is emitted by background conversation loads. A just-arrived live
                // message can land via MessageUpserted while the load is in-flight; if the eventual
                // replace page doesn't include it yet, we'd otherwise "lose" it until re-enter.
                let mut merged = messages;
                merged.retain(|message| {
                    !state
                        .backend
                        .deleted_messages
                        .contains(&(conversation_id.clone(), message.id.clone()))
                });
                merged.extend(state.timeline.messages.clone());
                normalize_messages_by_id(&mut merged);
                recompute_thread_reply_counts(&mut merged);
                trim_timeline_for_append(&mut merged);
                state.timeline.messages = merged;
                note_newest_timeline_message(state, &conversation_id);
                // Prefer computed cursors from merged state to avoid regressing past newer arrivals.
                state.timeline.older_cursor = state
                    .timeline
                    .messages
                    .first()
                    .map(|message| message.id.0.clone())
                    .or(older_cursor);
                state.timeline.newer_cursor = state
                    .timeline
                    .messages
                    .last()
                    .map(|message| message.id.0.clone())
                    .or(newer_cursor);
                state.timeline.loading_older = false;
            }
        }
        BackendEvent::MessageUpserted(message) => {
            upsert_message_in_state(state, message);
        }
        BackendEvent::MessageAttachmentsUpdated {
            conversation_id,
            message_id,
            attachments,
        } => {
            if !state
                .backend
                .deleted_messages
                .contains(&(conversation_id.clone(), message_id.clone()))
            {
                for message in state
                    .timeline
                    .messages
                    .iter_mut()
                    .chain(state.thread.replies.iter_mut())
                {
                    if message.conversation_id == conversation_id && message.id == message_id {
                        crate::domain::message::apply_attachment_paths(message, &attachments);
                    }
                }
            }
        }
        BackendEvent::CommandFailed { command, error } => {
            return reduce_command_failure(state, *command, error);
        }
        BackendEvent::MessageSendConfirmed {
            op_id: _,
            client_message_id,
            mut server_message,
        } => {
            let pending_id = pending_message_id_for_client(&client_message_id);
            let pending_snapshot = state
                .timeline
                .messages
                .iter()
                .find(|message| message.id == pending_id)
                .cloned();
            if let Some(pending_message) = pending_snapshot.as_ref() {
                merge_pending_message_attachment_sources(&mut server_message, pending_message);
            }
            replace_pending_message_id(state, &pending_id, &server_message.id);
            rename_previewed_message(state, &pending_id, &server_message.id);
            upsert_message_in_state(state, server_message);
        }
        BackendEvent::MessageSendFailed {
            op_id: _,
            client_message_id,
            error,
        } => {
            let pending_id = pending_message_id_for_client(&client_message_id);
            mark_message_failed(state, &pending_id);
            state.notifications.toasts.push(error);
        }
        BackendEvent::AttachmentUploadProgress {
            client_message_id,
            sent_bytes,
            total_bytes,
        } => {
            let pending_id = pending_message_id_for_client(&client_message_id);
            let still_pending = state
                .timeline
                .messages
                .iter()
                .chain(&state.thread.replies)
                .any(|message| {
                    message.id == pending_id && message.send_state == MessageSendState::Pending
                });
            if still_pending {
                state.timeline.upload_progress.insert(
                    pending_id,
                    crate::state::state::UploadProgress {
                        sent_bytes,
                        total_bytes,
                    },
                );
            }
        }
        BackendEvent::MessageDeleted {
            conversation_id,
            message_id,
        } => {
            state
                .backend
                .deleted_messages
                .insert((conversation_id.clone(), message_id.clone()));
            if let Some(reactions) = state.backend.message_reactions.get_mut(&conversation_id) {
                reactions.remove(&message_id);
            }
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                state
                    .timeline
                    .messages
                    .retain(|message| message.id != message_id);
                forget_deleted_preview(state, &conversation_id, &message_id);
                state
                    .thread
                    .replies
                    .retain(|message| message.id != message_id);
                state.timeline.newer_cursor = state
                    .timeline
                    .messages
                    .last()
                    .map(|message| message.id.0.clone());
                if state.timeline.older_cursor.is_some() {
                    state.timeline.older_cursor = state
                        .timeline
                        .messages
                        .first()
                        .map(|message| message.id.0.clone());
                }
            } else {
                forget_deleted_preview(state, &conversation_id, &message_id);
            }
        }
        BackendEvent::TypingUpdated {
            conversation_id,
            users,
        } => {
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                state.timeline.typing_text =
                    typing_label_for_users(&users, &state.backend.user_profiles);
            }
        }
        BackendEvent::UserProfileUpserted {
            user_id,
            display_name,
            avatar_asset,
            updated_ms,
        } => {
            let previous = state.backend.user_profiles.get(&user_id);
            let stale = previous.is_some_and(|old| old.updated_ms > updated_ms);
            let display_name = previous
                .filter(|_| stale)
                .map(|old| old.display_name.clone())
                .unwrap_or(display_name);
            let avatar_asset = if stale {
                previous
                    .and_then(|old| old.avatar_asset.clone())
                    .or(avatar_asset)
            } else {
                avatar_asset.or_else(|| previous.and_then(|old| old.avatar_asset.clone()))
            };
            let updated_ms = previous.map_or(updated_ms, |old| old.updated_ms.max(updated_ms));
            if let Some(profile) = state.backend.profile_panel.profiles.get_mut(&user_id) {
                profile.avatar_asset = avatar_asset.clone();
            }
            state.backend.user_profiles.insert(
                user_id.clone(),
                super::state::UserProfileState {
                    display_name: display_name.clone(),
                    avatar_asset: avatar_asset.clone(),
                    updated_ms,
                },
            );
            for account in state.backend.accounts.values_mut() {
                if account.display_name == user_id.0 {
                    account.avatar = avatar_asset.clone();
                }
            }
        }
        BackendEvent::UserProfileLoaded {
            account_id,
            profile,
        } => {
            let user_id = profile.user_id.clone();
            let first_profile = !state.backend.profile_panel.profiles.contains_key(&user_id);
            let mut profile = profile;
            profile.avatar_asset = profile.avatar_asset.or_else(|| {
                state
                    .backend
                    .user_profiles
                    .get(&user_id)
                    .and_then(|old| old.avatar_asset.clone())
            });
            if let Some(presence) = presence_for_user(&state.backend.user_presences, &user_id) {
                profile.presence = presence;
            }
            // Profile metadata and social rosters arrive independently. Missing
            // roster fields mean "not included", not an empty refreshed list.
            if let Some(previous) = state.backend.profile_panel.profiles.get(&user_id) {
                preserve_social_lists(&mut profile, previous);
            }
            state.backend.profile_panel.loading.remove(&user_id);
            state
                .backend
                .user_presences
                .insert(user_id.clone(), profile.presence.clone());
            state
                .backend
                .profile_panel
                .profiles
                .insert(user_id.clone(), profile.clone());
            state.backend.user_profiles.insert(
                user_id.clone(),
                super::state::UserProfileState {
                    display_name: profile.display_name.clone(),
                    avatar_asset: profile.avatar_asset.clone(),
                    updated_ms: now_unix_ms(),
                },
            );
            if let Some(account) = state.backend.accounts.get_mut(&account_id)
                && account.display_name.eq_ignore_ascii_case(&user_id.0)
            {
                account.avatar = profile.avatar_asset.clone();
            }

            if first_profile && state.backend.profile_panel.selected_user.as_ref() == Some(&user_id)
            {
                let list_type = state.backend.profile_panel.active_social_tab;
                return request_social_graph_refresh(state, user_id, list_type);
            }
        }
        BackendEvent::SocialGraphListLoaded {
            user_id,
            list_type,
            entries,
        } => {
            state
                .backend
                .profile_panel
                .social_lists
                .remove(&(user_id.clone(), list_type));
            if let Some(profile) = state.backend.profile_panel.profiles.get_mut(&user_id) {
                merge_social_graph_entries(profile, list_type, entries);
            }
        }
        BackendEvent::FollowStatusChanged {
            user_id,
            you_are_following,
        } => {
            let next_affinity = if you_are_following {
                Affinity::Positive
            } else {
                Affinity::None
            };
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), next_affinity);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                next_affinity,
            );
            update_follow_status(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                you_are_following,
            );
        }
        BackendEvent::FollowStatusChangeFailed {
            user_id,
            attempted_follow,
            error: _,
        } => {
            let reverted_follow = !attempted_follow;
            let reverted_affinity = if reverted_follow {
                Affinity::Positive
            } else {
                Affinity::None
            };
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), reverted_affinity);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                reverted_affinity,
            );
            update_follow_status(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                reverted_follow,
            );
        }
        BackendEvent::AffinityChanged { user_id, affinity } => {
            state
                .backend
                .user_affinities
                .insert(user_id.clone(), affinity);
            update_profile_affinity(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                affinity,
            );
            update_follow_status(
                &mut state.backend.profile_panel.profiles,
                &user_id,
                matches!(affinity, Affinity::Positive),
            );
        }
        BackendEvent::AffinitySynced { affinities } => {
            for (user_id, affinity) in &affinities {
                update_profile_affinity(
                    &mut state.backend.profile_panel.profiles,
                    user_id,
                    *affinity,
                );
                update_follow_status(
                    &mut state.backend.profile_panel.profiles,
                    user_id,
                    matches!(affinity, Affinity::Positive),
                );
            }
            state.backend.user_affinities = affinities;
        }
        BackendEvent::PresenceUpdated {
            account_id: _,
            users,
        } => {
            for patch in users {
                state
                    .backend
                    .user_presences
                    .insert(patch.user_id.clone(), patch.presence.clone());
                update_profile_presence(
                    &mut state.backend.profile_panel.profiles,
                    &patch.user_id,
                    patch.presence,
                );
            }
        }
        BackendEvent::ConversationEmojisSynced {
            conversation_id,
            emojis,
        } => {
            let mut index = std::collections::HashMap::new();
            for emoji in emojis {
                index.insert(
                    emoji.alias.to_ascii_lowercase(),
                    super::state::EmojiRenderState {
                        alias: emoji.alias,
                        unicode: emoji.unicode,
                        asset_path: emoji.asset_path,
                        updated_ms: emoji.updated_ms,
                    },
                );
            }
            state
                .backend
                .conversation_emojis
                .insert(conversation_id, index);
        }
        BackendEvent::EmojiSourceSynced {
            source_ref,
            alias,
            unicode,
            asset_path,
            updated_ms,
        } => {
            state.backend.emoji_sources.insert(
                source_ref.cache_key(),
                super::state::EmojiRenderState {
                    alias,
                    unicode,
                    asset_path,
                    updated_ms,
                },
            );
        }
        BackendEvent::MessageReactionsSynced {
            conversation_id,
            reactions_by_message,
        } => {
            let entry = state
                .backend
                .message_reactions
                .entry(conversation_id)
                .or_default();
            for message_reactions in reactions_by_message {
                if message_reactions.reactions.is_empty() {
                    entry.remove(&message_reactions.message_id);
                    continue;
                }
                entry.insert(
                    message_reactions.message_id,
                    message_reactions
                        .reactions
                        .into_iter()
                        .map(|reaction| super::state::MessageReactionState {
                            emoji: reaction.emoji,
                            source_ref: reaction.source_ref,
                            actor_ids: reaction.actor_ids,
                            updated_ms: reaction.updated_ms,
                        })
                        .collect(),
                );
            }
        }
        BackendEvent::PinnedStateUpdated {
            conversation_id,
            pinned,
        } => {
            state
                .backend
                .conversation_pins
                .insert(conversation_id, pinned);
        }
        BackendEvent::MessageReactionApplied {
            conversation_id,
            message_id,
            emoji,
            source_ref,
            actor_id,
            updated_ms,
        } => add_runtime_reaction(
            state,
            conversation_id,
            message_id,
            emoji,
            source_ref,
            actor_id,
            updated_ms,
        ),
        BackendEvent::MessageReactionRemoved {
            conversation_id,
            message_id,
            emoji,
            actor_id,
        } => {
            let Some(reactions_by_message) =
                state.backend.message_reactions.get_mut(&conversation_id)
            else {
                return ReducerOutput::default();
            };
            let Some(reactions) = reactions_by_message.get_mut(&message_id) else {
                return ReducerOutput::default();
            };
            if let Some(reaction) = reactions
                .iter_mut()
                .find(|entry| entry.emoji.eq_ignore_ascii_case(&emoji))
            {
                reaction.actor_ids.retain(|id| id != &actor_id);
            }
            reactions.retain(|entry| !entry.actor_ids.is_empty());
            if reactions.is_empty() {
                reactions_by_message.remove(&message_id);
            }
        }
        BackendEvent::ReactionConfirmed { op_id } => {
            state.backend.pending_reactions.remove(&op_id);
        }
        BackendEvent::ReactionFailed { op_id, error } => {
            state.notifications.toasts.push(error);
            if let Some(pending) = state.backend.pending_reactions.remove(&op_id) {
                let key = (pending.conversation_id.clone(), pending.message_id.clone());
                if state.backend.latest_reaction_ops.get(&key) == Some(&op_id) {
                    state.backend.latest_reaction_ops.remove(&key);
                    state
                        .backend
                        .message_reactions
                        .entry(pending.conversation_id.clone())
                        .or_default()
                        .insert(pending.message_id, pending.previous);
                }
                return ReducerOutput {
                    effects: vec![Effect::Backend(BackendCommand::LoadConversation {
                        conversation_id: pending.conversation_id,
                    })],
                };
            }
        }
        BackendEvent::TeamRolesUpdated {
            conversation_id,
            team_id,
            roles,
            updated_ms: _,
        } => {
            state
                .backend
                .conversation_team_ids
                .insert(conversation_id, team_id.clone());
            let mut role_index = HashMap::new();
            for role_entry in roles {
                role_index.insert(role_entry.user_id, role_entry.role);
            }
            state.backend.team_roles.insert(team_id, role_index);
        }
        BackendEvent::ConversationMembersUpdated {
            conversation_id,
            members,
            updated_ms,
            is_complete,
        } => {
            state.backend.conversation_members.insert(
                conversation_id,
                super::state::ConversationMembersState {
                    members,
                    updated_ms,
                    is_complete,
                },
            );
        }
        BackendEvent::ConversationBadgesUpdated {
            backend_id,
            counts,
            complete,
        } => {
            state
                .backend
                .conversation_badges
                .entry(backend_id)
                .or_default()
                .update(counts, complete);
            apply_authoritative_badges(state);
            let workspace_id = state
                .workspace
                .active_workspace_id
                .clone()
                .unwrap_or_else(|| WorkspaceId::new("ws_primary"));
            state.sidebar.sections = build_sidebar_sections(
                &state.workspace.channels,
                &state.workspace.direct_messages,
                &workspace_id,
                &state.backend.user_profiles,
            );
        }
        BackendEvent::ConversationUnreadChanged {
            conversation_id,
            unread_count,
            mention_count,
            read_upto,
        } => {
            remember_read_position(state, &conversation_id, read_upto.as_ref());
            let mut changed = false;
            for summary in state.workspace.channels.iter_mut() {
                if summary.id == conversation_id {
                    summary.unread_count = unread_count;
                    summary.mention_count = mention_count;
                    changed = true;
                    break;
                }
            }
            if !changed {
                for summary in state.workspace.direct_messages.iter_mut() {
                    if summary.id == conversation_id {
                        summary.unread_count = unread_count;
                        summary.mention_count = mention_count;
                        changed = true;
                        break;
                    }
                }
            }
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                state.timeline.unread_marker = read_upto;
            }
            if changed {
                let workspace_id = state
                    .workspace
                    .active_workspace_id
                    .clone()
                    .unwrap_or_else(|| WorkspaceId::new("ws_primary"));
                apply_authoritative_badges(state);
                state.sidebar.sections = build_sidebar_sections(
                    &state.workspace.channels,
                    &state.workspace.direct_messages,
                    &workspace_id,
                    &state.backend.user_profiles,
                );
            }
        }
        BackendEvent::ReadMarkerUpdated {
            conversation_id,
            read_upto,
        } => {
            remember_read_position(state, &conversation_id, read_upto.as_ref());
            if state.timeline.conversation_id.as_ref() == Some(&conversation_id) {
                state.timeline.unread_marker = read_upto;
            }
            refresh_sidebar(state);
        }
        BackendEvent::CallUpdated(_) => {}
        BackendEvent::SearchResults {
            query_id,
            results,
            is_complete,
        } => {
            if state.search.active_query_id.as_ref() != Some(&query_id) {
                return ReducerOutput::default();
            }
            state.search.results.extend(results);
            state
                .search
                .results
                .sort_by(crate::domain::search::SearchResult::cmp_for_display);
            state.search.highlighted_index = (!state.search.results.is_empty()).then_some(0);
            state.search.is_loading = !is_complete;
        }
        BackendEvent::UserSearchResults { query_id, results } => {
            if state.new_chat.active_query_id.as_ref() != Some(&query_id) {
                return ReducerOutput::default();
            }
            super::new_chat_search::merge_results(&mut state.new_chat.search_results, results);
            state.new_chat.is_searching = false;
        }
        BackendEvent::ChannelResolved {
            conversation_binding,
            ..
        } => {
            state.backend.conversation_bindings.insert(
                conversation_binding.conversation_id.clone(),
                conversation_binding,
            );
        }
        BackendEvent::ChannelResolveFailed { .. } => {}
        BackendEvent::ChannelLeft {
            workspace_id,
            conversation_id,
        } => {
            return channel_membership::channel_left(state, workspace_id, conversation_id);
        }
        BackendEvent::ChannelJoined {
            conversation_id, ..
        } => {
            // A confirmed join supersedes any in-flight leave completion.
            state
                .workspace
                .leaving_conversation_ids
                .remove(&conversation_id);
            state
                .workspace
                .leave_channel_errors
                .remove(&conversation_id);
            state
                .workspace
                .left_conversation_ids
                .remove(&conversation_id);
        }
        BackendEvent::ConversationCreated {
            op_id: _,
            workspace_id,
            conversation,
            conversation_binding,
        } => {
            state.backend.conversation_bindings.insert(
                conversation_binding.conversation_id.clone(),
                conversation_binding,
            );
            merge_workspace_conversations(state, vec![conversation.clone()], Vec::new());
            if state.workspace.active_workspace_id.is_none() {
                state.workspace.active_workspace_id = Some(workspace_id.clone());
            }
            let sidebar_workspace_id = state
                .workspace
                .active_workspace_id
                .clone()
                .unwrap_or(workspace_id.clone());
            apply_authoritative_badges(state);
            state.sidebar.sections = build_sidebar_sections(
                &state.workspace.channels,
                &state.workspace.direct_messages,
                &sidebar_workspace_id,
                &state.backend.user_profiles,
            );
            let route = route_for_summary(&conversation, &workspace_id);
            state.navigation.current_route = Some(route.clone());
            state.sidebar.highlighted_route = Some(route);
            state.timeline.conversation_id = Some(conversation.id.clone());
            state.timeline.messages.clear();
            state.timeline.typing_text = None;
            state.timeline.highlighted_message_id = None;
            state.timeline.unread_marker = None;
            state.timeline.older_cursor = None;
            state.timeline.newer_cursor = None;
            state.timeline.loading_older = false;
            state.navigation.active_thread_root = None;
            state.thread.open = false;
            state.thread.root_message_id = None;
            state.thread.replies.clear();
            state.thread.reply_draft.clear();
            state.thread.loading = false;
            state.new_chat = super::state::UiNewChatState::default();
            return ReducerOutput {
                effects: vec![Effect::Backend(BackendCommand::LoadConversation {
                    conversation_id: conversation.id,
                })],
            };
        }
        BackendEvent::BootStatus(status) => {
            state.app.boot_status = status.clone();
        }
        BackendEvent::NotifySoundLoaded { enabled } => {
            state.notifications.notify_sound = enabled;
        }
        BackendEvent::KeybaseNotifyStub { .. }
        | BackendEvent::BenchmarkPulse(_)
        | BackendEvent::DesktopNotification(_) => {}
    }

    ReducerOutput::default()
}

fn remember_read_position(
    state: &mut UiState,
    conversation: &ConversationId,
    read: Option<&MessageId>,
) {
    if let Some(read) = read.and_then(|id| id.0.parse::<u64>().ok()) {
        // Another client can intentionally mark a conversation unread.
        state
            .backend
            .confirmed_read_positions
            .entry(conversation.clone())
            .or_default()
            .read = read;
    }
}

fn apply_authoritative_badges(state: &mut UiState) {
    for summary in state
        .workspace
        .channels
        .iter_mut()
        .chain(state.workspace.direct_messages.iter_mut())
    {
        if let Some(binding) = state.backend.conversation_bindings.get(&summary.id) {
            if let Some(badges) = state.backend.conversation_badges.get(&binding.backend_id) {
                badges.apply(summary);
            }
            // Counts alone cannot make already-read visible content unread.
            if binding.backend_id.0 == "keybase"
                && let Some(position) = state.backend.confirmed_read_positions.get_mut(&summary.id)
            {
                if let Some(latest) = summary
                    .last_message
                    .as_ref()
                    .and_then(|preview| preview.message_id.as_ref())
                    .and_then(|id| id.0.parse::<u64>().ok())
                {
                    position.latest_visible = position.latest_visible.max(latest);
                }
                if position.latest_visible > 0 && position.read >= position.latest_visible {
                    summary.unread_count = 0;
                    summary.mention_count = 0;
                }
            }
        }
    }
}

fn reduce_command_failure(
    state: &mut UiState,
    command: BackendCommand,
    error: String,
) -> ReducerOutput {
    if let BackendCommand::SearchUsers { query_id, .. } = &command
        && state.new_chat.active_query_id.as_ref() != Some(query_id)
    {
        return ReducerOutput::default();
    }
    let mut effects = Vec::new();
    match command {
        BackendCommand::LeaveChannel {
            conversation_id, ..
        } => {
            state
                .workspace
                .leave_channel_errors
                .insert(conversation_id.clone(), error.clone());
            state
                .workspace
                .leaving_conversation_ids
                .remove(&conversation_id);
        }
        BackendCommand::SetNotifySound { previous, .. } => {
            state.notifications.notify_sound = previous;
        }
        BackendCommand::SetConversationNotificationSettings {
            conversation_id,
            previous,
            ..
        } => {
            // The server rejected the change: put the conversation back the way
            // it was instead of leaving the optimistic choice on screen.
            apply_conversation_notification_settings(state, &conversation_id, previous);
        }
        BackendCommand::SendMessage {
            client_message_id, ..
        }
        | BackendCommand::SendAttachment {
            client_message_id, ..
        } => {
            mark_message_failed(state, &pending_message_id_for_client(&client_message_id));
        }
        BackendCommand::Search { query_id, .. }
            if state.search.active_query_id.as_ref() == Some(&query_id) =>
        {
            state.search.is_loading = false;
        }
        BackendCommand::SearchUsers { query_id, .. }
            if state.new_chat.active_query_id.as_ref() == Some(&query_id) =>
        {
            state.new_chat.is_searching = false;
            state.new_chat.error = Some(error.clone());
        }
        BackendCommand::CreateConversation { .. } => {
            state.new_chat.creating = false;
            state.new_chat.error = Some(error.clone());
        }
        BackendCommand::LoadOlderMessages { key, .. } => match key {
            super::state::TimelineKey::Conversation(id)
                if state.timeline.conversation_id.as_ref() == Some(&id) =>
            {
                state.timeline.loading_older = false
            }
            super::state::TimelineKey::Thread {
                conversation_id,
                requested_root_id,
                ..
            } if state.timeline.conversation_id.as_ref() == Some(&conversation_id)
                && state.navigation.active_thread_root.as_ref() == Some(&requested_root_id) =>
            {
                state.thread.loading = false
            }
            _ => {}
        },
        BackendCommand::JumpToMessage {
            conversation_id, ..
        } if state.timeline.conversation_id.as_ref() == Some(&conversation_id) => {
            state.timeline.loading_older = false
        }
        BackendCommand::LoadThread {
            conversation_id,
            root_id,
        } if state.timeline.conversation_id.as_ref() == Some(&conversation_id)
            && state.navigation.active_thread_root.as_ref() == Some(&root_id) =>
        {
            state.thread.loading = false
        }
        BackendCommand::LoadUserProfile { user_id } => {
            state.backend.profile_panel.loading.remove(&user_id);
        }
        BackendCommand::LoadSocialGraphList { user_id, list_type } => {
            state
                .backend
                .profile_panel
                .social_lists
                .insert((user_id, list_type), SocialListLoadState::Failed);
        }
        BackendCommand::FollowUser { user_id } | BackendCommand::UnfollowUser { user_id } => {
            // Reconcile with the backend instead of leaving an optimistic follow stuck.
            effects.push(Effect::Backend(BackendCommand::LoadUserProfile { user_id }));
        }
        BackendCommand::ReactToMessage { op_id, .. } => {
            return reduce_backend_event(state, BackendEvent::ReactionFailed { op_id, error });
        }
        BackendCommand::LoadBootstrap { .. } | BackendCommand::ConnectAccount { .. } => {
            state.app.boot_phase = BootPhase::Degraded;
            state.app.boot_status = error.clone();
        }
        _ => {}
    }
    state.notifications.toasts.push(error);
    ReducerOutput { effects }
}

/// Typing indicator copy with display names, falling back to the username when
/// no profile is known yet.
pub(crate) fn typing_label_for_users(
    users: &[UserId],
    profiles: &HashMap<UserId, super::state::UserProfileState>,
) -> Option<String> {
    let names = users
        .iter()
        .map(|user_id| {
            let full_name = profiles
                .get(user_id)
                .or_else(|| profiles.get(&UserId::new(user_id.0.to_ascii_lowercase())))
                .map_or("", |profile| profile.display_name.as_str());
            crate::domain::user::shown_name(&user_id.0, full_name).to_string()
        })
        .collect::<Vec<_>>();
    crate::util::formatting::typing_indicator_label(&names)
}

fn current_user_id(state: &UiState) -> UserId {
    state
        .backend
        .accounts
        .values()
        .find(|account| matches!(account.connection_state, ConnectionState::Connected))
        .or_else(|| state.backend.accounts.values().next())
        .map(|account| UserId::new(account.display_name.clone()))
        .unwrap_or_else(|| UserId::new("user_me"))
}

fn presence_for_user(
    user_presences: &HashMap<UserId, Presence>,
    user_id: &UserId,
) -> Option<Presence> {
    user_presences.get(user_id).cloned().or_else(|| {
        let lower = user_id.0.to_ascii_lowercase();
        if lower == user_id.0 {
            None
        } else {
            user_presences.get(&UserId::new(lower)).cloned()
        }
    })
}

fn update_profile_presence(
    profiles: &mut HashMap<UserId, UserProfile>,
    user_id: &UserId,
    presence: Presence,
) {
    if let Some(profile) = profiles.get_mut(user_id) {
        profile.presence = presence.clone();
        return;
    }
    let lower = user_id.0.to_ascii_lowercase();
    if lower != user_id.0
        && let Some(profile) = profiles.get_mut(&UserId::new(lower))
    {
        profile.presence = presence;
    }
}

fn update_profile_affinity(
    profiles: &mut HashMap<UserId, UserProfile>,
    user_id: &UserId,
    affinity: Affinity,
) {
    if let Some(profile) = profiles.get_mut(user_id) {
        profile.affinity = affinity;
    }
    for profile in profiles.values_mut() {
        for section in &mut profile.sections {
            let ProfileSection::SocialGraph(graph) = section else {
                continue;
            };
            if let Some(followers) = graph.followers.as_mut() {
                for entry in followers
                    .iter_mut()
                    .filter(|entry| entry.user_id == *user_id)
                {
                    entry.affinity = affinity;
                }
            }
            if let Some(following) = graph.following.as_mut() {
                for entry in following
                    .iter_mut()
                    .filter(|entry| entry.user_id == *user_id)
                {
                    entry.affinity = affinity;
                }
            }
        }
    }
}

fn update_follow_status(
    profiles: &mut HashMap<UserId, UserProfile>,
    user_id: &UserId,
    you_are_following: bool,
) {
    if let Some(profile) = profiles.get_mut(user_id) {
        for section in &mut profile.sections {
            if let ProfileSection::SocialGraph(graph) = section {
                graph.you_are_following = you_are_following;
            }
        }
        return;
    }
    let lower = user_id.0.to_ascii_lowercase();
    if lower != user_id.0
        && let Some(profile) = profiles.get_mut(&UserId::new(lower))
    {
        for section in &mut profile.sections {
            if let ProfileSection::SocialGraph(graph) = section {
                graph.you_are_following = you_are_following;
            }
        }
    }
}

fn request_social_graph_refresh(
    state: &mut UiState,
    user_id: UserId,
    list_type: SocialGraphListType,
) -> ReducerOutput {
    let key = (user_id.clone(), list_type);
    if state.backend.profile_panel.social_lists.get(&key) == Some(&SocialListLoadState::Loading) {
        return ReducerOutput::default();
    }
    state
        .backend
        .profile_panel
        .social_lists
        .insert(key, SocialListLoadState::Loading);
    ReducerOutput {
        effects: vec![Effect::Backend(BackendCommand::LoadSocialGraphList {
            user_id,
            list_type,
        })],
    }
}

fn preserve_social_lists(profile: &mut UserProfile, previous: &UserProfile) {
    if profile.user_id != previous.user_id {
        return;
    }
    let Some(old) = previous.sections.iter().find_map(|section| match section {
        ProfileSection::SocialGraph(graph) => Some(graph),
        _ => None,
    }) else {
        return;
    };
    for section in &mut profile.sections {
        if let ProfileSection::SocialGraph(graph) = section {
            if graph.followers.is_none() {
                graph.followers = old.followers.clone();
            }
            if graph.following.is_none() {
                graph.following = old.following.clone();
            }
            return;
        }
    }
    profile
        .sections
        .push(ProfileSection::SocialGraph(old.clone()));
}

fn merge_social_graph_entries(
    profile: &mut UserProfile,
    list_type: SocialGraphListType,
    entries: Vec<SocialGraphEntry>,
) {
    for section in &mut profile.sections {
        let ProfileSection::SocialGraph(graph) = section else {
            continue;
        };
        match list_type {
            SocialGraphListType::Followers => {
                graph.followers_count = Some(entries.len() as u32);
                graph.followers = Some(entries);
            }
            SocialGraphListType::Following => {
                graph.following_count = Some(entries.len() as u32);
                graph.following = Some(entries);
            }
        }
        return;
    }

    let mut graph = SocialGraph {
        followers_count: None,
        following_count: None,
        is_following_you: false,
        you_are_following: false,
        followers: None,
        following: None,
    };
    match list_type {
        SocialGraphListType::Followers => {
            graph.followers_count = Some(entries.len() as u32);
            graph.followers = Some(entries);
        }
        SocialGraphListType::Following => {
            graph.following_count = Some(entries.len() as u32);
            graph.following = Some(entries);
        }
    }
    profile.sections.push(ProfileSection::SocialGraph(graph));
}

fn pending_message_id_for_client(client_message_id: &ClientMessageId) -> MessageId {
    MessageId::new(format!("local:{}", client_message_id.0))
}

fn add_pending_message_to_state(
    state: &mut UiState,
    conversation_id: &ConversationId,
    text: &str,
    attachments: Vec<AttachmentSummary>,
    reply_to: Option<MessageId>,
    client_message_id: &ClientMessageId,
) {
    if state.timeline.conversation_id.as_ref() != Some(conversation_id) {
        return;
    }
    let pending_id = pending_message_id_for_client(client_message_id);
    if state
        .timeline
        .messages
        .iter()
        .any(|message| message.id == pending_id)
    {
        return;
    }
    let text = text.trim();
    let (fragments, source_text) = if text.is_empty() {
        (Vec::new(), None)
    } else {
        (
            crate::domain::message_formatting::parse_message_text(text),
            Some(text.to_string()),
        )
    };
    let thread_root_id = reply_to.clone();
    let pending_message = MessageRecord {
        id: pending_id,
        conversation_id: conversation_id.clone(),
        author_id: current_user_id(state),
        reply_to: reply_to.clone(),
        thread_root_id: thread_root_id.clone(),
        timestamp_ms: Some(now_unix_ms()),
        event: None,
        link_previews: Vec::new(),
        permalink: format!("zbase://pending/{}", client_message_id.0),
        fragments,
        source_text,
        attachments,
        reactions: Vec::new(),
        thread_reply_count: 0,
        send_state: MessageSendState::Pending,
        edited: None,
    };
    note_conversation_message(state, &pending_message);
    state.timeline.messages.push(pending_message.clone());
    if let Some(root_id) = thread_root_id {
        refresh_thread_reply_count(&mut state.timeline.messages, &root_id);
        if state.thread.root_message_id.as_ref() == Some(&root_id) {
            state.thread.replies.push(pending_message);
            normalize_messages_by_id(&mut state.thread.replies);
        }
    }
    normalize_messages_by_id(&mut state.timeline.messages);
    trim_timeline_for_append(&mut state.timeline.messages);
    state.timeline.newer_cursor = state
        .timeline
        .messages
        .last()
        .map(|message| message.id.0.clone());
    if state.timeline.older_cursor.is_none() {
        state.timeline.older_cursor = state
            .timeline
            .messages
            .first()
            .map(|message| message.id.0.clone());
    }
}

fn message_body_text_for_matching(msg: &MessageRecord) -> Option<String> {
    msg.source_text
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            let t: String = msg
                .fragments
                .iter()
                .filter_map(|f| match f {
                    MessageFragment::Text(s) => Some(s.as_str()),
                    _ => None,
                })
                .collect();
            let t = t.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        })
}

/// A message arrived in its conversation (live, sent or confirmed): it bumps the
/// conversation's activity and becomes its preview when it is the newest.
fn note_conversation_message(state: &mut UiState, message: &MessageRecord) {
    if MessagePreview::from_message(message).is_some()
        && let Some(latest) = message.id.0.parse::<u64>().ok()
        && let Some(position) = state
            .backend
            .confirmed_read_positions
            .get_mut(&message.conversation_id)
    {
        position.latest_visible = position.latest_visible.max(latest);
    }
    let preview = MessagePreview::from_message(message);
    let mut activity_changed = false;
    for summary in conversation_summaries_mut(state, &message.conversation_id) {
        if let Some(activity) = message.timestamp_ms
            && activity > summary.last_activity_ms
        {
            summary.last_activity_ms = activity;
            activity_changed = true;
        }
        summary.last_message = MessagePreview::newest(summary.last_message.take(), preview.clone());
    }
    if activity_changed {
        refresh_sidebar(state);
    }
}

/// After a page of the open conversation loads: its newest message is a preview
/// fallback for conversations the inbox gave none, or an older one.
fn note_newest_timeline_message(state: &mut UiState, conversation_id: &ConversationId) {
    let newest = state
        .timeline
        .messages
        .iter()
        .rev()
        .filter(|message| &message.conversation_id == conversation_id)
        .find_map(MessagePreview::from_message);
    if newest.is_none() {
        return;
    }
    for summary in conversation_summaries_mut(state, conversation_id) {
        summary.last_message = MessagePreview::newest(summary.last_message.take(), newest.clone());
    }
}

/// A send was confirmed: its preview now names the server's id for the message.
fn rename_previewed_message(state: &mut UiState, pending_id: &MessageId, server_id: &MessageId) {
    for summary in state
        .workspace
        .channels
        .iter_mut()
        .chain(&mut state.workspace.direct_messages)
    {
        if let Some(preview) = summary.last_message.as_mut()
            && preview.message_id.as_ref() == Some(pending_id)
        {
            preview.message_id = Some(server_id.clone());
        }
    }
}

/// The previewed message was deleted: fall back to the newest message still in
/// the open timeline, or say it was deleted when nothing older is loaded.
fn forget_deleted_preview(
    state: &mut UiState,
    conversation_id: &ConversationId,
    message_id: &MessageId,
) {
    let fallback = (state.timeline.conversation_id.as_ref() == Some(conversation_id))
        .then(|| {
            state
                .timeline
                .messages
                .iter()
                .rev()
                .filter(|message| {
                    &message.conversation_id == conversation_id && &message.id != message_id
                })
                .find_map(MessagePreview::from_message)
        })
        .flatten();
    for summary in conversation_summaries_mut(state, conversation_id) {
        let Some(current) = summary.last_message.as_ref() else {
            continue;
        };
        if current.message_id.as_ref() != Some(message_id) {
            continue;
        }
        summary.last_message = Some(fallback.clone().unwrap_or_else(|| MessagePreview {
            body: PreviewBody::Deleted,
            ..current.clone()
        }));
    }
}

fn conversation_summaries_mut<'a>(
    state: &'a mut UiState,
    conversation_id: &'a ConversationId,
) -> impl Iterator<Item = &'a mut ConversationSummary> + 'a {
    state
        .workspace
        .channels
        .iter_mut()
        .chain(&mut state.workspace.direct_messages)
        .filter(move |summary| &summary.id == conversation_id)
}

fn upsert_message_in_state(state: &mut UiState, mut message: MessageRecord) {
    if state
        .backend
        .deleted_messages
        .contains(&(message.conversation_id.clone(), message.id.clone()))
    {
        return;
    }
    note_conversation_message(state, &message);
    if let Some(existing) = state
        .timeline
        .messages
        .iter()
        .find(|item| item.id == message.id && item.conversation_id == message.conversation_id)
    {
        message = crate::domain::message::merge_message_version(existing, &message);
    }
    if state.timeline.conversation_id.as_ref() != Some(&message.conversation_id) {
        // Keep the active timeline stable; incoming events for other conversations
        // should not force navigation.
        return;
    }

    // If we're upserting our own message (e.g. from MessageUpserted when outbox wasn't
    // matched), remove a matching pending so we don't show the same message twice.
    let current_user = current_user_id(state);
    if message.author_id == current_user
        && let Some(incoming_body) = message_body_text_for_matching(&message)
    {
        let pending_id_to_remove = state
            .timeline
            .messages
            .iter()
            .rev()
            .find(|m| {
                m.id.0.starts_with("local:")
                    && message_body_text_for_matching(m).as_ref() == Some(&incoming_body)
            })
            .map(|m| m.id.clone());
        if let Some(pending_id) = pending_id_to_remove {
            replace_pending_message_id(state, &pending_id, &message.id);
        }
    }

    apply_message_reactions(state, &message);
    if let Some(existing) = state
        .timeline
        .messages
        .iter_mut()
        .find(|existing| existing.id == message.id)
    {
        *existing = message.clone();
    } else {
        state.timeline.messages.push(message.clone());
    }
    if let Some(root_id) = message
        .thread_root_id
        .clone()
        .or_else(|| message.reply_to.clone())
    {
        refresh_thread_reply_count(&mut state.timeline.messages, &root_id);
        if state.thread.root_message_id.as_ref() == Some(&root_id) {
            if let Some(existing) = state
                .thread
                .replies
                .iter_mut()
                .find(|existing| existing.id == message.id)
            {
                *existing = crate::domain::message::merge_message_version(existing, &message);
            } else {
                state.thread.replies.push(message.clone());
            }
            normalize_messages_by_id(&mut state.thread.replies);
        }
    }
    normalize_messages_by_id(&mut state.timeline.messages);
    trim_timeline_for_append(&mut state.timeline.messages);
    state.timeline.newer_cursor = state
        .timeline
        .messages
        .last()
        .map(|message| message.id.0.clone());
    if state.timeline.older_cursor.is_none() && state.timeline.messages.len() == 1 {
        state.timeline.older_cursor = state
            .timeline
            .messages
            .first()
            .map(|message| message.id.0.clone());
    }
}

fn replace_pending_message_id(state: &mut UiState, pending_id: &MessageId, _server_id: &MessageId) {
    state.timeline.upload_progress.remove(pending_id);
    state
        .timeline
        .messages
        .retain(|message| message.id != *pending_id);
    state
        .thread
        .replies
        .retain(|message| message.id != *pending_id);
}

// This joins a known outbox upload with its server confirmation, whose metadata
// may still be incomplete. Updates to an existing canonical message instead use
// merge_message_version and never infer attachment identity from array position.
fn merge_pending_message_attachment_sources(
    server_message: &mut MessageRecord,
    pending_message: &MessageRecord,
) {
    if pending_message.attachments.is_empty() {
        return;
    }
    if server_message.attachments.is_empty() {
        server_message.attachments = pending_message.attachments.clone();
        return;
    }
    for (index, server_attachment) in server_message.attachments.iter_mut().enumerate() {
        let pending_attachment = pending_message
            .attachments
            .iter()
            .find(|candidate| {
                !candidate.name.is_empty()
                    && !server_attachment.name.is_empty()
                    && candidate.name.eq_ignore_ascii_case(&server_attachment.name)
            })
            .or_else(|| pending_message.attachments.get(index));
        let Some(pending_attachment) = pending_attachment else {
            continue;
        };
        let pending_has_local_source = matches!(
            &pending_attachment.source,
            Some(AttachmentSource::LocalPath(p)) if !p.is_empty()
        );
        if server_attachment.source.is_none() || pending_has_local_source {
            server_attachment.source = pending_attachment.source.clone();
        }
        let pending_has_local_preview = matches!(
            &pending_attachment.preview,
            Some(preview) if matches!(&preview.source, AttachmentSource::LocalPath(p) if !p.is_empty())
        );
        if server_attachment.preview.is_none() || pending_has_local_preview {
            server_attachment.preview = pending_attachment.preview.clone();
        }
        if server_attachment.width.is_none() {
            server_attachment.width = pending_attachment.width;
        }
        if server_attachment.height.is_none() {
            server_attachment.height = pending_attachment.height;
        }
        if server_attachment.mime_type.is_none() {
            server_attachment.mime_type = pending_attachment.mime_type.clone();
        }
        if server_attachment.duration_ms.is_none() {
            server_attachment.duration_ms = pending_attachment.duration_ms;
        }
        if server_attachment.waveform.is_none() {
            server_attachment.waveform = pending_attachment.waveform.clone();
        }
    }
}

fn discard_failed_message(state: &mut UiState, message_id: &MessageId) {
    let is_failed = |message: &MessageRecord| {
        message.id == *message_id && message.send_state == MessageSendState::Failed
    };
    let Some(failed) = state
        .timeline
        .messages
        .iter()
        .chain(&state.thread.replies)
        .find(|message| is_failed(message))
        .cloned()
    else {
        return;
    };
    state
        .timeline
        .messages
        .retain(|message| !is_failed(message));
    state.thread.replies.retain(|message| !is_failed(message));
    state.timeline.upload_progress.remove(message_id);
    // If the failed message was the conversation's newest, preview the one
    // before it instead.
    let replacement = state
        .timeline
        .messages
        .iter()
        .rev()
        .filter(|message| message.conversation_id == failed.conversation_id)
        .find_map(MessagePreview::from_message);
    let mut previews_changed = false;
    for summary in conversation_summaries_mut(state, &failed.conversation_id) {
        let previews_failed = summary
            .last_message
            .as_ref()
            .is_some_and(|preview| preview.message_id.as_ref() == Some(message_id));
        if previews_failed {
            summary.last_message = replacement.clone();
            previews_changed = true;
        }
    }
    if previews_changed {
        refresh_sidebar(state);
    }
}

fn mark_message_failed(state: &mut UiState, pending_id: &MessageId) {
    state.timeline.upload_progress.remove(pending_id);
    if let Some(message) = state
        .timeline
        .messages
        .iter_mut()
        .find(|message| message.id == *pending_id)
    {
        message.send_state = MessageSendState::Failed;
    }
    if let Some(message) = state
        .thread
        .replies
        .iter_mut()
        .find(|message| message.id == *pending_id)
    {
        message.send_state = MessageSendState::Failed;
    }
}

fn add_runtime_reaction(
    state: &mut UiState,
    conversation_id: ConversationId,
    message_id: MessageId,
    emoji: String,
    source_ref: Option<crate::domain::message::EmojiSourceRef>,
    actor_id: UserId,
    updated_ms: i64,
) {
    let message_entry = state
        .backend
        .message_reactions
        .entry(conversation_id)
        .or_default()
        .entry(message_id)
        .or_default();
    if let Some(existing) = message_entry
        .iter_mut()
        .find(|reaction| reaction.emoji.eq_ignore_ascii_case(&emoji))
    {
        if existing.source_ref.is_none() && source_ref.is_some() {
            existing.source_ref = source_ref;
        }
        if !existing.actor_ids.iter().any(|id| id == &actor_id) {
            existing.actor_ids.push(actor_id);
            existing
                .actor_ids
                .sort_by(|left, right| left.0.cmp(&right.0));
        }
        existing.updated_ms = updated_ms;
    } else {
        message_entry.push(super::state::MessageReactionState {
            emoji,
            source_ref,
            actor_ids: vec![actor_id],
            updated_ms,
        });
        message_entry.sort_by(|left, right| left.emoji.cmp(&right.emoji));
    }
}

fn toggle_runtime_reaction(
    state: &mut UiState,
    conversation_id: ConversationId,
    message_id: MessageId,
    emoji: String,
    actor_id: UserId,
    updated_ms: i64,
) {
    let reactions_by_message = state
        .backend
        .message_reactions
        .entry(conversation_id)
        .or_default();
    let remove_message;
    {
        let message_entry = reactions_by_message.entry(message_id.clone()).or_default();
        if let Some(existing) = message_entry
            .iter_mut()
            .find(|reaction| reaction.emoji.eq_ignore_ascii_case(&emoji))
        {
            if existing.actor_ids.iter().any(|id| id == &actor_id) {
                existing.actor_ids.retain(|id| id != &actor_id);
            } else {
                existing.actor_ids.push(actor_id);
                existing
                    .actor_ids
                    .sort_by(|left, right| left.0.cmp(&right.0));
            }
            existing.updated_ms = updated_ms;
        } else {
            message_entry.push(super::state::MessageReactionState {
                emoji,
                source_ref: None,
                actor_ids: vec![actor_id],
                updated_ms,
            });
            message_entry.sort_by(|left, right| left.emoji.cmp(&right.emoji));
        }
        message_entry.retain(|entry| !entry.actor_ids.is_empty());
        remove_message = message_entry.is_empty();
    }
    if remove_message {
        reactions_by_message.remove(&message_id);
    }
}

fn next_op_id(state: &mut UiState, prefix: &str) -> OpId {
    let value = format!("{prefix}-{}", state.next_op_seq);
    state.next_op_seq += 1;
    OpId::new(value)
}

fn next_query_id(state: &mut UiState) -> QueryId {
    let value = format!("query-{}", state.next_op_seq);
    state.next_op_seq += 1;
    QueryId::new(value)
}

fn next_client_message_id(state: &mut UiState) -> ClientMessageId {
    let value = format!("client-message-{}", state.next_op_seq);
    state.next_op_seq += 1;
    ClientMessageId::new(value)
}

const TIMELINE_WINDOW_MAX: usize = 5000;

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

fn normalize_messages_by_id(messages: &mut Vec<crate::domain::message::MessageRecord>) {
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
                if should_replace_duplicate(entry.get(), &message) {
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

fn should_replace_duplicate(
    existing: &crate::domain::message::MessageRecord,
    incoming: &crate::domain::message::MessageRecord,
) -> bool {
    const NON_TEXT_PLACEHOLDER_BODY: &str = "<non-text message>";
    let existing_is_placeholder = existing.event.is_none()
        && existing.attachments.is_empty()
        && existing.link_previews.is_empty()
        && matches!(
            existing.fragments.as_slice(),
            [crate::domain::message::MessageFragment::Text(text)] if text.trim() == NON_TEXT_PLACEHOLDER_BODY
        );
    let incoming_is_placeholder = incoming.event.is_none()
        && incoming.attachments.is_empty()
        && incoming.link_previews.is_empty()
        && matches!(
            incoming.fragments.as_slice(),
            [crate::domain::message::MessageFragment::Text(text)] if text.trim() == NON_TEXT_PLACEHOLDER_BODY
        );
    if existing_is_placeholder != incoming_is_placeholder {
        return existing_is_placeholder;
    }
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

fn trim_timeline_for_append(messages: &mut Vec<crate::domain::message::MessageRecord>) {
    if messages.len() <= TIMELINE_WINDOW_MAX {
        return;
    }
    let overflow = messages.len() - TIMELINE_WINDOW_MAX;
    messages.drain(0..overflow);
}

fn trim_timeline_for_prepend(messages: &mut Vec<crate::domain::message::MessageRecord>) {
    if messages.len() <= TIMELINE_WINDOW_MAX {
        return;
    }
    messages.truncate(TIMELINE_WINDOW_MAX);
}

fn refresh_thread_reply_count(
    messages: &mut [crate::domain::message::MessageRecord],
    root_id: &crate::domain::ids::MessageId,
) {
    let reply_count = messages
        .iter()
        .filter(|message| {
            message.id != *root_id && message.thread_root_id.as_ref() == Some(root_id)
        })
        .map(|message| message.id.clone())
        .collect::<HashSet<_>>()
        .len() as u32;
    if let Some(root) = messages.iter_mut().find(|message| message.id == *root_id) {
        root.thread_reply_count = root.thread_reply_count.max(reply_count);
    }
}

fn recompute_thread_reply_counts(messages: &mut [crate::domain::message::MessageRecord]) {
    let mut reply_count_by_root = HashMap::new();
    for message in messages.iter() {
        let Some(root_id) = message.thread_root_id.as_ref() else {
            continue;
        };
        if message.id == *root_id {
            continue;
        }
        *reply_count_by_root.entry(root_id.clone()).or_insert(0u32) += 1;
    }
    for message in messages.iter_mut() {
        if let Some(reply_count) = reply_count_by_root.get(&message.id) {
            message.thread_reply_count = message.thread_reply_count.max(*reply_count);
        }
    }
}

fn apply_message_reactions(state: &mut UiState, message: &crate::domain::message::MessageRecord) {
    let entry = state
        .backend
        .message_reactions
        .entry(message.conversation_id.clone())
        .or_default();
    if message.reactions.is_empty() {
        entry.remove(&message.id);
        return;
    }
    let mut reactions = message
        .reactions
        .iter()
        .map(|reaction| super::state::MessageReactionState {
            emoji: reaction.emoji.clone(),
            source_ref: reaction.source_ref.clone(),
            actor_ids: reaction.actor_ids.clone(),
            updated_ms: 0,
        })
        .collect::<Vec<_>>();
    reactions.sort_by(|left, right| left.emoji.cmp(&right.emoji));
    entry.insert(message.id.clone(), reactions);
}

// A conversation has one identity even when refreshed metadata changes its kind.
// Merge both buckets together, repairing duplicates left by earlier updates, then
// partition by the latest summary rather than by the event's input bucket.
fn merge_workspace_conversations(
    state: &mut UiState,
    channels: Vec<ConversationSummary>,
    direct_messages: Vec<ConversationSummary>,
) {
    let self_username = state
        .backend
        .accounts
        .values()
        .find(|account| matches!(account.connection_state, ConnectionState::Connected))
        .or_else(|| state.backend.accounts.values().next())
        .map(|account| account.display_name.as_str());
    let workspace = &mut state.workspace;
    let mut summaries: Vec<ConversationSummary> = Vec::new();
    let mut index_by_id = HashMap::new();
    for mut summary in std::mem::take(&mut workspace.channels)
        .into_iter()
        .chain(std::mem::take(&mut workspace.direct_messages))
        .chain(channels)
        .chain(direct_messages)
    {
        if workspace.excluded_conversation_ids.contains(&summary.id)
            || workspace.left_conversation_ids.contains(&summary.id)
        {
            continue;
        }
        // Cached and notification summaries can still contain the signed-in user.
        // Normalize once here so labels, search, and avatar projections agree.
        if summary.kind != ConversationKind::Channel {
            summary.title =
                crate::domain::conversation::dm_peer_title(&summary.title, self_username);
        }
        if let Some(index) = index_by_id.get(&summary.id).copied() {
            summary.preserve_known_metadata(&summaries[index]);
            summaries[index] = summary;
        } else {
            index_by_id.insert(summary.id.clone(), summaries.len());
            summaries.push(summary);
        }
    }
    (workspace.channels, workspace.direct_messages) = summaries
        .into_iter()
        .partition(|summary| matches!(summary.kind, ConversationKind::Channel));
}

/// Records a conversation's notification choices locally and mirrors the muted
/// flag onto its summary so badges react without waiting for the next inbox load.
fn apply_conversation_notification_settings(
    state: &mut UiState,
    conversation_id: &ConversationId,
    settings: ConversationNotificationSettings,
) {
    state
        .backend
        .conversation_notifications
        .insert(conversation_id.clone(), settings);
    for summary in state
        .workspace
        .channels
        .iter_mut()
        .chain(state.workspace.direct_messages.iter_mut())
    {
        if summary.id == *conversation_id {
            summary.muted = settings.muted;
            break;
        }
    }
    refresh_sidebar(state);
}

fn refresh_sidebar(state: &mut UiState) {
    apply_authoritative_badges(state);
    let workspace_id = state
        .workspace
        .active_workspace_id
        .clone()
        .unwrap_or_else(|| WorkspaceId::new("ws_primary"));
    state.sidebar.sections = build_sidebar_sections(
        &state.workspace.channels,
        &state.workspace.direct_messages,
        &workspace_id,
        &state.backend.user_profiles,
    );
}

fn build_sidebar_sections(
    channels: &[ConversationSummary],
    direct_messages: &[ConversationSummary],
    workspace_id: &WorkspaceId,
    user_profiles: &HashMap<UserId, UserProfileState>,
) -> Vec<UiSidebarSectionState> {
    let mut sorted_channels = channels.to_vec();
    sorted_channels.sort_by(|left, right| {
        left.title
            .to_ascii_lowercase()
            .cmp(&right.title.to_ascii_lowercase())
            .then_with(|| left.id.0.cmp(&right.id.0))
    });

    let mut group_order: Vec<(String, String)> = Vec::new();
    let mut group_rows: std::collections::HashMap<String, Vec<UiSidebarRowState>> =
        std::collections::HashMap::new();
    let mut ungrouped_rows = Vec::new();
    let mut flat_conversations = direct_messages.iter().collect::<Vec<_>>();

    for summary in &sorted_channels {
        if summary.is_simple_team() {
            flat_conversations.push(summary);
            continue;
        }
        let row = UiSidebarRowState {
            label: summary.title.clone(),
            unread_count: summary.unread_count,
            mention_count: summary.mention_count,
            route: Some(route_for_summary(summary, workspace_id)),
        };

        if let Some(group) = &summary.group {
            if !group_rows.contains_key(&group.id) {
                group_order.push((group.id.clone(), group.display_name.clone()));
            }
            group_rows.entry(group.id.clone()).or_default().push(row);
        } else {
            ungrouped_rows.push(row);
        }
    }

    // Simple teams share the recent-conversation list with DMs, but retain
    // Channel routes and team metadata for permissions, navigation and identity.
    flat_conversations.sort_by(|left, right| left.compare_recency(right));
    let conversation_rows = flat_conversations
        .iter()
        .map(|summary| UiSidebarRowState {
            label: summary
                .simple_team_label()
                .unwrap_or_else(|| dm_display_label(&summary.title, user_profiles)),
            unread_count: summary.unread_count,
            mention_count: summary.mention_count,
            route: Some(route_for_summary(summary, workspace_id)),
        })
        .collect::<Vec<_>>();
    let mut sections = Vec::new();
    if !conversation_rows.is_empty() {
        sections.push(UiSidebarSectionState {
            // Keep the section identity so existing saved order/collapse settings work.
            id: Some(SidebarSectionId::new("dms")),
            title: "Conversations".to_string(),
            rows: conversation_rows,
            collapsed: false,
        });
    }

    group_order.sort_by(|left, right| {
        left.1
            .to_ascii_lowercase()
            .cmp(&right.1.to_ascii_lowercase())
            .then_with(|| left.0.cmp(&right.0))
    });

    for (group_id, display_name) in group_order {
        if let Some(rows) = group_rows.remove(&group_id) {
            let has_unread = rows
                .iter()
                .any(|r| r.unread_count > 0 || r.mention_count > 0);
            sections.push(UiSidebarSectionState {
                id: Some(SidebarSectionId::new(format!("group_{group_id}"))),
                title: display_name,
                rows,
                collapsed: !has_unread,
            });
        }
    }

    if !ungrouped_rows.is_empty() {
        let has_unread = ungrouped_rows
            .iter()
            .any(|r| r.unread_count > 0 || r.mention_count > 0);
        sections.push(UiSidebarSectionState {
            id: Some(SidebarSectionId::new("channels")),
            title: "Channels".to_string(),
            rows: ungrouped_rows,
            collapsed: !has_unread,
        });
    }

    sections
}

fn dm_display_label(title: &str, user_profiles: &HashMap<UserId, UserProfileState>) -> String {
    let mut participants = Vec::new();
    for username in title
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let full_name = user_profiles
            .get(&UserId::new(username.to_ascii_lowercase()))
            .map_or("", |profile| profile.display_name.as_str());
        participants.push(crate::domain::user::shown_name(username, full_name).to_string());
    }
    if participants.is_empty() {
        title.to_string()
    } else {
        participants.join(", ")
    }
}

fn find_existing_direct_message_with_user(
    direct_messages: &[ConversationSummary],
    user_id: &UserId,
) -> Option<ConversationId> {
    direct_messages
        .iter()
        .find(|summary| {
            matches!(summary.kind, ConversationKind::DirectMessage)
                && dm_title_mentions_user(&summary.title, &user_id.0)
        })
        .map(|summary| summary.id.clone())
}

fn dm_title_mentions_user(title: &str, user_id: &str) -> bool {
    let target = user_id.trim().trim_start_matches('@');
    if target.is_empty() {
        return false;
    }
    title
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .any(|participant| participant.eq_ignore_ascii_case(target))
}

fn route_for_summary(summary: &ConversationSummary, workspace_id: &WorkspaceId) -> Route {
    match summary.kind {
        ConversationKind::Channel => Route::Channel {
            workspace_id: workspace_id.clone(),
            channel_id: ChannelId::new(summary.id.0.clone()),
        },
        ConversationKind::DirectMessage | ConversationKind::GroupDirectMessage => {
            Route::DirectMessage {
                workspace_id: workspace_id.clone(),
                dm_id: DmId::new(summary.id.0.clone()),
            }
        }
    }
}

fn route_for_conversation_id(
    conversation_id: &ConversationId,
    workspace_id: &WorkspaceId,
    channels: &[ConversationSummary],
    direct_messages: &[ConversationSummary],
) -> Option<Route> {
    if conversation_id.0.is_empty() {
        return None;
    }
    if direct_messages
        .iter()
        .any(|summary| summary.id == *conversation_id)
    {
        return Some(Route::DirectMessage {
            workspace_id: workspace_id.clone(),
            dm_id: DmId::new(conversation_id.0.clone()),
        });
    }
    if channels
        .iter()
        .any(|summary| summary.id == *conversation_id)
    {
        return Some(Route::Channel {
            workspace_id: workspace_id.clone(),
            channel_id: ChannelId::new(conversation_id.0.clone()),
        });
    }
    Some(Route::Channel {
        workspace_id: workspace_id.clone(),
        channel_id: ChannelId::new(conversation_id.0.clone()),
    })
}

fn conversation_id_from_route(route: &Route) -> Option<ConversationId> {
    match route {
        Route::Channel { channel_id, .. } => Some(ConversationId::new(channel_id.0.clone())),
        Route::DirectMessage { dm_id, .. } => Some(ConversationId::new(dm_id.0.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cached_and_refreshed_group_dms_exclude_self_from_shared_title() {
        use crate::domain::backend::BackendId;
        use crate::state::{event::BootstrapPayload, state::AccountState};
        let mut state = UiState::default();
        let account_id = AccountId::new("keybase");
        state.backend.accounts.insert(
            account_id.clone(),
            AccountState {
                account_id: account_id.clone(),
                backend_id: BackendId::new("keybase"),
                display_name: String::new(),
                avatar: None,
                connection_state: ConnectionState::Connected,
                capabilities: Default::default(),
            },
        );
        let mut summary = channel_summary("bot-group", "charlieexample,helperbot,aliceexample");
        summary.kind = ConversationKind::GroupDirectMessage;
        reduce_backend_event(
            &mut state,
            BackendEvent::BootstrapLoaded {
                source: BootstrapSource::Cache,
                account_id,
                payload: BootstrapPayload {
                    account_display_name: Some("AliceExample".into()),
                    direct_messages: vec![summary.clone()],
                    ..BootstrapPayload::default()
                },
            },
        );
        assert_eq!(
            state.workspace.direct_messages[0].title,
            "charlieexample, helperbot"
        );
        let workspace = WorkspaceId::new("ws");
        // A notification containing all members must not reintroduce self.
        reduce_backend_event(
            &mut state,
            BackendEvent::WorkspaceConversationsExtended {
                workspace_id: workspace,
                channels: vec![channel_summary("team", "aliceexample")],
                direct_messages: vec![summary],
                conversation_bindings: vec![],
            },
        );
        assert_eq!(
            state.workspace.direct_messages[0].title,
            "charlieexample, helperbot"
        );
        assert_eq!(state.workspace.channels[0].title, "aliceexample");
        let row = state.sidebar.sections.iter().flat_map(|section| &section.rows)
            .find(|row| matches!(&row.route, Some(Route::DirectMessage { dm_id, .. }) if dm_id.0 == "bot-group"))
            .unwrap();
        assert_eq!(row.label, "charlieexample, helperbot");
    }

    #[test]
    fn pending_json_has_its_final_code_structure_before_server_echo() {
        let id = crate::domain::ids::ConversationId::new("conv");
        let mut state = super::UiState::default();
        state.timeline.conversation_id = Some(id.clone());
        let source = "```json\n{\"ok\": true}\n```";
        super::add_pending_message_to_state(
            &mut state,
            &id,
            source,
            vec![],
            None,
            &super::ClientMessageId::new("local-json"),
        );
        let pending = &state.timeline.messages[0];
        assert_eq!(pending.source_text.as_deref(), Some(source));
        assert!(
            matches!(&pending.fragments[..], [crate::domain::message::MessageFragment::Code { text, lang }]
            if text == "{\"ok\": true}" && lang.as_deref() == Some("json"))
        );
    }

    use super::*;
    use crate::{
        domain::ids::UserId,
        domain::message::MessageSendState,
        state::event::{TeamRoleEntry, TeamRoleKind},
        state::state::TimelineKey,
    };

    fn message(
        conversation_id: &ConversationId,
        id: &str,
        timestamp_ms: i64,
    ) -> crate::domain::message::MessageRecord {
        crate::domain::message::MessageRecord {
            id: MessageId::new(id.to_string()),
            conversation_id: conversation_id.clone(),
            author_id: UserId::new("alice"),
            reply_to: None,
            thread_root_id: None,
            timestamp_ms: Some(timestamp_ms),
            event: None,
            link_previews: Vec::new(),
            permalink: format!("kb://{id}"),
            fragments: vec![crate::domain::message::MessageFragment::Text(format!(
                "m{id}"
            ))],
            source_text: None,
            attachments: Vec::new(),
            reactions: Vec::new(),
            thread_reply_count: 0,
            send_state: MessageSendState::Sent,
            edited: None,
        }
    }

    #[test]
    fn discarding_a_failed_send_removes_it_and_restores_the_preview() {
        let id = ConversationId::new("conv");
        let mut state = UiState::default();
        state.timeline.conversation_id = Some(id.clone());
        let mut summary = channel_summary(&id.0, "general");
        let earlier = message(&id, "5", 1_000);
        summary.last_message = MessagePreview::from_message(&earlier);
        state.workspace.channels.push(summary);
        state.timeline.messages.push(earlier.clone());
        let client = ClientMessageId::new("failed");
        add_pending_message_to_state(&mut state, &id, "lost words", vec![], None, &client);
        let failed_id = pending_message_id_for_client(&client);
        assert_eq!(
            state.workspace.channels[0]
                .last_message
                .as_ref()
                .and_then(|preview| preview.message_id.clone()),
            Some(failed_id.clone())
        );

        // Still pending: discarding is refused.
        reduce_ui_action(
            &mut state,
            UiAction::DiscardFailedMessage {
                message_id: failed_id.clone(),
            },
        );
        assert_eq!(state.timeline.messages.len(), 2);

        reduce_backend_event(
            &mut state,
            BackendEvent::MessageSendFailed {
                op_id: OpId::new("op"),
                client_message_id: client,
                error: "offline".into(),
            },
        );
        let output = reduce_ui_action(
            &mut state,
            UiAction::DiscardFailedMessage {
                message_id: failed_id.clone(),
            },
        );
        assert!(
            output.effects.is_empty(),
            "local only: nothing goes to Keybase"
        );
        assert!(
            state
                .timeline
                .messages
                .iter()
                .all(|message| message.id != failed_id)
        );
        assert_eq!(
            state.workspace.channels[0]
                .last_message
                .as_ref()
                .and_then(|preview| preview.message_id.clone()),
            Some(earlier.id.clone())
        );

        // Sent messages can't be discarded this way.
        reduce_ui_action(
            &mut state,
            UiAction::DiscardFailedMessage {
                message_id: earlier.id.clone(),
            },
        );
        assert_eq!(state.timeline.messages.len(), 1);
    }

    #[test]
    fn upload_progress_tracks_pending_attachments_until_they_settle() {
        let id = ConversationId::new("conv");
        let mut state = UiState::default();
        state.timeline.conversation_id = Some(id.clone());
        let client = ClientMessageId::new("upload");
        add_pending_message_to_state(&mut state, &id, "notes", vec![], None, &client);
        let pending_id = pending_message_id_for_client(&client);
        let progress = |sent| BackendEvent::AttachmentUploadProgress {
            client_message_id: client.clone(),
            sent_bytes: sent,
            total_bytes: 400,
        };
        reduce_backend_event(&mut state, progress(100));
        let tracked = state.timeline.upload_progress[&pending_id];
        assert_eq!(tracked.fraction(), 0.25);
        reduce_backend_event(&mut state, progress(300));
        assert_eq!(state.timeline.upload_progress[&pending_id].sent_bytes, 300);

        // Progress for a send this window doesn't know about is dropped.
        reduce_backend_event(
            &mut state,
            BackendEvent::AttachmentUploadProgress {
                client_message_id: ClientMessageId::new("elsewhere"),
                sent_bytes: 1,
                total_bytes: 2,
            },
        );
        assert_eq!(state.timeline.upload_progress.len(), 1);

        reduce_backend_event(
            &mut state,
            BackendEvent::MessageSendFailed {
                op_id: OpId::new("op"),
                client_message_id: client.clone(),
                error: "offline".into(),
            },
        );
        assert!(state.timeline.upload_progress.is_empty());
        // A late progress event can't revive a failed send's bar.
        reduce_backend_event(&mut state, progress(400));
        assert!(state.timeline.upload_progress.is_empty());
        assert_eq!(
            crate::state::state::UploadProgress {
                sent_bytes: 0,
                total_bytes: 0
            }
            .fraction(),
            0.
        );
    }

    #[test]
    fn confirmed_read_positions_prevent_old_badges_from_reviving_unread() {
        use crate::domain::{
            backend::{BackendId, ProviderConversationRef},
            conversation::ConversationBadgeCounts,
        };
        use crate::state::bindings::ConversationBinding;
        let mut state = UiState::default();
        let id = ConversationId::new("kb_conv:abcd");
        let mut summary = channel_summary(&id.0, "random");
        summary.last_message = MessagePreview::from_message(&message(&id, "43", 100));
        state.workspace.channels.push(summary.clone());
        state.timeline.conversation_id = Some(id.clone());
        state.backend.conversation_bindings.insert(
            id.clone(),
            ConversationBinding {
                conversation_id: id.clone(),
                backend_id: BackendId::new("keybase"),
                account_id: AccountId::new("account"),
                provider_conversation_ref: ProviderConversationRef::new(&id.0),
            },
        );
        let badge = || BackendEvent::ConversationBadgesUpdated {
            backend_id: BackendId::new("keybase"),
            complete: true,
            counts: HashMap::from([(
                id.clone(),
                ConversationBadgeCounts {
                    unread_count: 12,
                    badge_count: 0,
                },
            )]),
        };
        reduce_backend_event(&mut state, badge());
        assert_eq!(state.workspace.channels[0].unread_count, 12);
        reduce_backend_event(
            &mut state,
            BackendEvent::ReadMarkerUpdated {
                conversation_id: id.clone(),
                read_upto: Some(MessageId::new("43")),
            },
        );
        assert_eq!(
            state.workspace.channels[0].unread_count, 0,
            "confirmed read must clear the sidebar too"
        );
        let other = ConversationId::new("kb_conv:other");
        reduce_ui_action(
            &mut state,
            UiAction::Navigate(Route::DirectMessage {
                workspace_id: WorkspaceId::new("workspace"),
                dm_id: crate::domain::ids::DmId::new(&other.0),
            }),
        );
        assert_eq!(state.timeline.conversation_id.as_ref(), Some(&other));
        // A background inbox refresh and badge snapshot can arrive much later,
        // while this channel is no longer selected. Neither is a new message.
        summary.unread_count = 12;
        reduce_backend_event(
            &mut state,
            BackendEvent::WorkspaceConversationsExtended {
                workspace_id: WorkspaceId::new("workspace"),
                channels: vec![summary.clone()],
                direct_messages: vec![],
                conversation_bindings: vec![],
            },
        );
        assert_eq!(state.workspace.channels[0].unread_count, 0);
        reduce_backend_event(&mut state, badge());
        assert_eq!(
            state.workspace.channels[0].unread_count, 0,
            "a count cannot undo a confirmed read position"
        );
        // An intentional mark-unread from another client still takes effect.
        reduce_backend_event(
            &mut state,
            BackendEvent::ReadMarkerUpdated {
                conversation_id: id.clone(),
                read_upto: Some(MessageId::new("40")),
            },
        );
        assert_eq!(state.workspace.channels[0].unread_count, 12);
        reduce_backend_event(
            &mut state,
            BackendEvent::ReadMarkerUpdated {
                conversation_id: id.clone(),
                read_upto: Some(MessageId::new("43")),
            },
        );
        assert_eq!(state.workspace.channels[0].unread_count, 0);
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&id, "44", 200)),
        );
        assert!(
            state.workspace.channels[0].unread_count > 0,
            "a message beyond the read position must remain unread"
        );
        assert!(sidebar_shows_unread(&state));
        // A subsequent older inbox page cannot conceal the newer unread message.
        reduce_backend_event(
            &mut state,
            BackendEvent::WorkspaceConversationsExtended {
                workspace_id: WorkspaceId::new("workspace"),
                channels: vec![summary],
                direct_messages: vec![],
                conversation_bindings: vec![],
            },
        );
        assert!(state.workspace.channels[0].unread_count > 0);
    }

    #[test]
    fn badge_snapshot_survives_bootstrap_and_inferred_unread_updates() {
        use crate::domain::backend::{BackendId, ProviderConversationRef};
        use crate::domain::conversation::ConversationBadgeCounts;
        use crate::state::{bindings::ConversationBinding, event::BootstrapPayload};
        let mut state = UiState::default();
        let backend_id = BackendId::new("keybase");
        let id = ConversationId::new("kb_conv:abcd");
        let other = ConversationId::new("kb_conv:1234");
        let mut summary = channel_summary(&id.0, "general");
        summary.unread_count = 9;
        summary.mention_count = 3;
        state.timeline.conversation_id = Some(id.clone());
        state.timeline.unread_marker = Some(MessageId::new("42"));
        // Subscription can finish before bootstrap. Remember the snapshot.
        reduce_backend_event(
            &mut state,
            BackendEvent::ConversationBadgesUpdated {
                backend_id: backend_id.clone(),
                counts: HashMap::new(),
                complete: true,
            },
        );
        let payload = BootstrapPayload {
            channels: vec![summary.clone()],
            conversation_bindings: vec![ConversationBinding {
                conversation_id: id.clone(),
                backend_id: backend_id.clone(),
                account_id: AccountId::new("account"),
                provider_conversation_ref: ProviderConversationRef::new(&id.0),
            }],
            ..BootstrapPayload::default()
        };
        reduce_backend_event(
            &mut state,
            BackendEvent::BootstrapLoaded {
                source: BootstrapSource::Server,
                account_id: AccountId::new("account"),
                payload: payload.clone(),
            },
        );
        assert_eq!(state.workspace.channels[0].unread_count, 0);
        assert_eq!(state.workspace.channels[0].mention_count, 0);
        assert_eq!(state.timeline.unread_marker, Some(MessageId::new("42")));
        reduce_backend_event(
            &mut state,
            BackendEvent::ConversationUnreadChanged {
                conversation_id: id.clone(),
                unread_count: 9,
                mention_count: 3,
                read_upto: Some(MessageId::new("43")),
            },
        );
        assert_eq!(state.workspace.channels[0].unread_count, 0);
        assert_eq!(state.timeline.unread_marker, Some(MessageId::new("43")));
        // A later real badge update can make the conversation unread again.
        reduce_backend_event(
            &mut state,
            BackendEvent::ConversationBadgesUpdated {
                backend_id: backend_id.clone(),
                counts: HashMap::from([(
                    id.clone(),
                    ConversationBadgeCounts {
                        unread_count: 2,
                        badge_count: 1,
                    },
                )]),
                complete: true,
            },
        );
        assert_eq!(state.workspace.channels[0].unread_count, 2);
        assert!(sidebar_shows_unread(&state));
        // Older inbox metadata must not overwrite the badge snapshot.
        reduce_backend_event(
            &mut state,
            BackendEvent::WorkspaceConversationsExtended {
                workspace_id: WorkspaceId::new("workspace"),
                channels: vec![summary],
                direct_messages: vec![],
                conversation_bindings: vec![],
            },
        );
        assert_eq!(state.workspace.channels[0].unread_count, 2);
        // A confirmed local read is a delta, not an empty full snapshot.
        reduce_backend_event(
            &mut state,
            BackendEvent::ConversationBadgesUpdated {
                backend_id: backend_id.clone(),
                counts: HashMap::from([(
                    other,
                    ConversationBadgeCounts {
                        unread_count: 5,
                        badge_count: 0,
                    },
                )]),
                complete: false,
            },
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::ConversationBadgesUpdated {
                backend_id,
                counts: HashMap::from([(id, ConversationBadgeCounts::default())]),
                complete: false,
            },
        );
        assert_eq!(state.workspace.channels[0].unread_count, 0);
        assert!(!sidebar_shows_unread(&state));
        assert_eq!(state.timeline.unread_marker, Some(MessageId::new("43")));
    }

    #[test]
    fn server_refresh_preserves_navigation_thread_drafts_and_pending_messages() {
        let mut state = UiState::default();
        let account_id = AccountId::new("account");
        let original = ConversationId::new("original");
        let current = ConversationId::new("current");
        let payload = super::super::event::BootstrapPayload {
            active_workspace_id: Some(WorkspaceId::new("workspace")),
            selected_conversation_id: Some(original.clone()),
            selected_messages: vec![message(&original, "1", 1)],
            channels: vec![channel_summary("original", "original")],
            ..Default::default()
        };
        reduce_backend_event(
            &mut state,
            BackendEvent::BootstrapLoaded {
                source: BootstrapSource::Cache,
                account_id: account_id.clone(),
                payload: payload.clone(),
            },
        );
        let route = Route::Channel {
            workspace_id: WorkspaceId::new("workspace"),
            channel_id: ChannelId::new("current"),
        };
        reduce_ui_action(&mut state, UiAction::Navigate(route.clone()));
        reduce_ui_action(
            &mut state,
            UiAction::UpdateDraft {
                key: DraftKey::Conversation(current.clone()),
                text: "pending text".into(),
            },
        );
        reduce_ui_action(
            &mut state,
            UiAction::SendMessage {
                key: DraftKey::Conversation(current.clone()),
            },
        );
        reduce_ui_action(
            &mut state,
            UiAction::OpenThread {
                root_id: MessageId::new("10"),
            },
        );
        reduce_ui_action(
            &mut state,
            UiAction::UpdateDraft {
                key: DraftKey::Thread(MessageId::new("10")),
                text: "unsent reply".into(),
            },
        );
        let result = reduce_backend_event(
            &mut state,
            BackendEvent::BootstrapLoaded {
                source: BootstrapSource::Server,
                account_id,
                payload,
            },
        );
        assert_eq!(state.navigation.current_route, Some(route));
        assert_eq!(state.timeline.conversation_id, Some(current.clone()));
        assert!(
            state
                .timeline
                .messages
                .iter()
                .any(|message| message.send_state == MessageSendState::Pending)
        );
        assert!(state.thread.open);
        assert_eq!(state.thread.reply_draft, "unsent reply");
        assert!(result.effects.iter().any(|effect| matches!(effect, Effect::Backend(BackendCommand::LoadConversation { conversation_id }) if conversation_id == &current)));
    }

    #[test]
    fn late_cache_hydration_cannot_overwrite_server_state() {
        let mut state = UiState::default();
        let account_id = AccountId::new("account");
        reduce_backend_event(
            &mut state,
            BackendEvent::BootstrapLoaded {
                source: BootstrapSource::Server,
                account_id: account_id.clone(),
                payload: super::super::event::BootstrapPayload {
                    workspace_name: "server".into(),
                    ..Default::default()
                },
            },
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::BootstrapLoaded {
                source: BootstrapSource::Cache,
                account_id,
                payload: super::super::event::BootstrapPayload {
                    workspace_name: "stale cache".into(),
                    ..Default::default()
                },
            },
        );
        assert_eq!(state.workspace.workspace_name, "server");
    }

    #[test]
    fn replacement_attachments_do_not_inherit_timeline_or_thread_paths() {
        let conversation = ConversationId::new("conversation");
        let mut original = message(&conversation, "1", 1);
        original.thread_root_id = Some(MessageId::new("root"));
        original.attachments.push(AttachmentSummary {
            name: "old.txt".into(),
            kind: AttachmentKind::File,
            size_bytes: 1,
            source: Some(AttachmentSource::LocalPath("/cache/old.txt".into())),
            ..AttachmentSummary::default()
        });
        let mut state = UiState::default();
        state.timeline.conversation_id = Some(conversation.clone());
        state.timeline.messages = vec![original.clone()];
        state.thread.root_message_id = Some(MessageId::new("root"));
        state.thread.replies = vec![original.clone()];
        let mut replacement = original;
        replacement.attachments[0].name = "replacement.txt".into();
        replacement.attachments[0].source = None;
        upsert_message_in_state(&mut state, replacement);
        assert!(state.timeline.messages[0].attachments[0].source.is_none());
        assert!(state.thread.replies[0].attachments[0].source.is_none());
    }

    #[test]
    fn caption_edit_keeps_the_attachment_in_either_arrival_order() {
        let conversation_id = ConversationId::new("conversation");
        let mut original = message(&conversation_id, "1", 1);
        original.attachments.push(AttachmentSummary {
            name: "Screenshot.png".into(),
            kind: AttachmentKind::Image,
            size_bytes: 182_750,
            ..AttachmentSummary::default()
        });
        // A Keybase edit of an attachment carries only the new caption.
        let mut edit = message(&conversation_id, "1", 2);
        edit.fragments = vec![MessageFragment::Text("new caption".into())];
        edit.edited = Some(crate::domain::message::EditMeta {
            edit_id: MessageId::new("2"),
            edited_at_ms: Some(2),
        });

        for events in [
            [original.clone(), edit.clone()],
            [edit.clone(), original.clone()],
        ] {
            let mut state = UiState::default();
            state.timeline.conversation_id = Some(conversation_id.clone());
            for event in events {
                reduce_backend_event(&mut state, BackendEvent::MessageUpserted(event));
            }
            let merged = &state.timeline.messages[0];
            assert_eq!(merged.attachments, original.attachments);
            assert!(
                matches!(&merged.fragments[..], [MessageFragment::Text(text)] if text == "new caption")
            );
        }
    }

    #[test]
    fn stale_message_and_attachment_completion_cannot_undo_edit_or_delete() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("conversation");
        state.timeline.conversation_id = Some(conversation_id.clone());
        let original = message(&conversation_id, "1", 1);
        let mut edited = original.clone();
        edited.fragments = vec![MessageFragment::Text("edited".into())];
        edited.edited = Some(crate::domain::message::EditMeta {
            edit_id: MessageId::new("2"),
            edited_at_ms: Some(2),
        });
        reduce_backend_event(&mut state, BackendEvent::MessageUpserted(edited));
        reduce_backend_event(&mut state, BackendEvent::MessageUpserted(original.clone()));
        assert_eq!(
            state.timeline.messages[0].edited.as_ref().unwrap().edit_id,
            MessageId::new("2")
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageDeleted {
                conversation_id: conversation_id.clone(),
                message_id: original.id.clone(),
            },
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageAttachmentsUpdated {
                conversation_id: conversation_id.clone(),
                message_id: original.id.clone(),
                attachments: Vec::new(),
            },
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::TimelineReplaced {
                conversation_id,
                messages: vec![original],
                older_cursor: None,
                newer_cursor: None,
            },
        );
        assert!(state.timeline.messages.is_empty());
    }

    #[test]
    fn paging_failure_keeps_cursor_available_for_retry() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("conversation");
        state.timeline.conversation_id = Some(conversation_id.clone());
        state.timeline.older_cursor = Some("10".into());
        state.timeline.loading_older = true;
        reduce_backend_event(
            &mut state,
            BackendEvent::CommandFailed {
                command: Box::new(BackendCommand::LoadOlderMessages {
                    key: TimelineKey::Conversation(conversation_id),
                    cursor: "10".into(),
                }),
                error: "offline".into(),
            },
        );
        assert!(!state.timeline.loading_older);
        assert_eq!(state.timeline.older_cursor.as_deref(), Some("10"));
        assert_eq!(state.notifications.toasts, vec!["offline"]);
    }

    #[test]
    fn creation_failure_settles_loading_and_keeps_dialog_open() {
        let mut state = UiState::default();
        state.new_chat.creating = true;
        state.new_chat.open = true;
        reduce_backend_event(
            &mut state,
            BackendEvent::CommandFailed {
                command: Box::new(BackendCommand::CreateConversation {
                    op_id: OpId::new("create-1"),
                    participants: Vec::new(),
                    kind: ConversationKind::GroupDirectMessage,
                }),
                error: "offline".into(),
            },
        );
        assert!(!state.new_chat.creating);
        assert!(state.new_chat.open);
        assert_eq!(state.new_chat.error.as_deref(), Some("offline"));
    }

    #[test]
    fn late_reaction_failure_does_not_undo_a_newer_confirmed_operation() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("conversation");
        let message_id = MessageId::new("1");
        let action = UiAction::ReactToMessage {
            conversation_id: conversation_id.clone(),
            message_id: message_id.clone(),
            emoji: "thumbsup".into(),
        };
        let first = reduce_ui_action(&mut state, action.clone());
        let Effect::Backend(BackendCommand::ReactToMessage { op_id: old, .. }) = &first.effects[0]
        else {
            panic!("reaction command");
        };
        let second = reduce_ui_action(&mut state, action);
        let Effect::Backend(BackendCommand::ReactToMessage { op_id: new, .. }) = &second.effects[0]
        else {
            panic!("reaction command");
        };
        reduce_backend_event(
            &mut state,
            BackendEvent::ReactionConfirmed { op_id: new.clone() },
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::ReactionFailed {
                op_id: old.clone(),
                error: "offline".into(),
            },
        );
        assert!(
            state
                .backend
                .message_reactions
                .get(&conversation_id)
                .and_then(|messages| messages.get(&message_id))
                .is_none_or(|reactions| reactions.is_empty())
        );
    }

    #[test]
    fn search_ignores_completion_from_a_superseded_query() {
        let mut state = UiState::default();
        reduce_ui_action(&mut state, UiAction::SubmitSearch);
        let first_query = state.search.active_query_id.clone().unwrap();
        reduce_ui_action(&mut state, UiAction::SetSearchQuery("new query".into()));
        reduce_ui_action(&mut state, UiAction::SubmitSearch);
        let current_query = state.search.active_query_id.clone().unwrap();
        reduce_backend_event(
            &mut state,
            BackendEvent::SearchResults {
                query_id: first_query,
                results: Vec::new(),
                is_complete: true,
            },
        );
        assert!(state.search.is_loading);
        reduce_backend_event(
            &mut state,
            BackendEvent::SearchResults {
                query_id: current_query,
                results: Vec::new(),
                is_complete: true,
            },
        );
        assert!(!state.search.is_loading);
    }

    #[test]
    fn new_chat_search_is_local_until_current_debounce_fires() {
        let mut state = UiState::default();
        state.backend.user_affinities.insert(
            UserId::new("alice"),
            crate::domain::affinity::Affinity::Positive,
        );
        reduce_ui_action(&mut state, UiAction::OpenNewChat);
        let output = reduce_ui_action(
            &mut state,
            UiAction::NewChatSearchUsers {
                query: "ali".into(),
            },
        );
        assert!(output.effects.is_empty());
        assert_eq!(state.new_chat.search_results[0].id.0, "alice");
        assert!(state.new_chat.is_searching);
        let old = state.new_chat.active_query_id.clone().unwrap();
        reduce_ui_action(
            &mut state,
            UiAction::NewChatSearchUsers {
                query: "alice".into(),
            },
        );
        assert!(
            reduce_ui_action(
                &mut state,
                UiAction::NewChatSearchRemote {
                    query_id: old.clone()
                }
            )
            .effects
            .is_empty()
        );
        let current = state.new_chat.active_query_id.clone().unwrap();
        let output = reduce_ui_action(
            &mut state,
            UiAction::NewChatSearchRemote {
                query_id: current.clone(),
            },
        );
        assert!(
            matches!(&output.effects[0], Effect::Backend(BackendCommand::SearchUsers { query_id, query }) if query_id == &current && query == "alice")
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::UserSearchResults {
                query_id: old,
                results: Vec::new(),
            },
        );
        assert!(state.new_chat.is_searching);
        assert_eq!(state.new_chat.search_results.len(), 1);
        reduce_backend_event(
            &mut state,
            BackendEvent::UserSearchResults {
                query_id: current,
                results: Vec::new(),
            },
        );
        assert!(!state.new_chat.is_searching);
        assert_eq!(state.new_chat.search_results[0].id.0, "alice");
    }

    #[test]
    fn new_chat_remote_failure_preserves_local_people_and_close_invalidates_timer() {
        let mut state = UiState::default();
        state.backend.user_affinities.insert(
            UserId::new("alice"),
            crate::domain::affinity::Affinity::Positive,
        );
        reduce_ui_action(&mut state, UiAction::OpenNewChat);
        reduce_ui_action(
            &mut state,
            UiAction::NewChatSearchUsers {
                query: "ali".into(),
            },
        );
        let query_id = state.new_chat.active_query_id.clone().unwrap();
        reduce_backend_event(
            &mut state,
            BackendEvent::CommandFailed {
                command: Box::new(BackendCommand::SearchUsers {
                    query_id: query_id.clone(),
                    query: "ali".into(),
                }),
                error: "offline".into(),
            },
        );
        assert!(!state.new_chat.is_searching);
        assert_eq!(state.new_chat.search_results[0].id.0, "alice");
        assert_eq!(state.new_chat.error.as_deref(), Some("offline"));
        reduce_ui_action(&mut state, UiAction::CloseNewChat);
        reduce_ui_action(&mut state, UiAction::OpenNewChat);
        assert!(
            reduce_ui_action(&mut state, UiAction::NewChatSearchRemote { query_id })
                .effects
                .is_empty()
        );
        assert!(state.new_chat.search_results.is_empty());
    }

    #[test]
    fn new_chat_picks_up_profiles_loaded_while_open_on_next_input() {
        let mut state = UiState::default();
        reduce_ui_action(&mut state, UiAction::OpenNewChat);
        reduce_ui_action(
            &mut state,
            UiAction::NewChatSearchUsers {
                query: "river".into(),
            },
        );
        assert!(state.new_chat.search_results.is_empty());
        reduce_backend_event(
            &mut state,
            BackendEvent::UserProfileUpserted {
                user_id: UserId::new("sam"),
                display_name: "Sam Rivera".into(),
                avatar_asset: None,
                updated_ms: 1,
            },
        );
        assert!(state.new_chat.search_results.is_empty());
        reduce_ui_action(
            &mut state,
            UiAction::NewChatSearchUsers {
                query: "rivera".into(),
            },
        );
        assert_eq!(state.new_chat.search_results[0].id.0, "sam");
    }

    #[test]
    fn clearing_user_search_invalidates_pending_response() {
        let mut state = UiState::default();
        reduce_ui_action(&mut state, UiAction::OpenNewChat);
        reduce_ui_action(
            &mut state,
            UiAction::NewChatSearchUsers {
                query: "alice".into(),
            },
        );
        let query_id = state.new_chat.active_query_id.clone().unwrap();
        reduce_ui_action(
            &mut state,
            UiAction::NewChatSearchUsers {
                query: String::new(),
            },
        );
        // A search response must not change an independent create operation either.
        state.new_chat.creating = true;
        reduce_backend_event(
            &mut state,
            BackendEvent::UserSearchResults {
                query_id,
                results: Vec::new(),
            },
        );
        assert!(state.new_chat.active_query_id.is_none());
        assert!(state.new_chat.creating);
    }

    #[test]
    fn late_thread_response_cannot_reopen_or_switch_threads() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("conversation");
        state.timeline.conversation_id = Some(conversation_id.clone());
        let old_root = MessageId::new("1");
        let new_root = MessageId::new("2");
        reduce_ui_action(
            &mut state,
            UiAction::OpenThread {
                root_id: old_root.clone(),
            },
        );
        reduce_ui_action(
            &mut state,
            UiAction::OpenThread {
                root_id: new_root.clone(),
            },
        );
        let response = BackendEvent::MessagesPrepended {
            key: TimelineKey::Thread {
                conversation_id: conversation_id.clone(),
                root_id: old_root.clone(),
                requested_root_id: old_root.clone(),
            },
            messages: vec![message(&conversation_id, "1", 1)],
            cursor: None,
        };
        reduce_backend_event(&mut state, response.clone());
        assert_eq!(state.thread.root_message_id, Some(new_root));
        assert!(state.thread.loading);
        reduce_ui_action(&mut state, UiAction::CloseRightPane);
        reduce_backend_event(&mut state, response);
        assert!(!state.thread.open);
        assert!(state.thread.replies.is_empty());
    }

    #[test]
    fn thread_response_can_resolve_requested_reply_to_its_canonical_root() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("conversation");
        state.timeline.conversation_id = Some(conversation_id.clone());
        let requested_root_id = MessageId::new("2");
        reduce_ui_action(
            &mut state,
            UiAction::OpenThread {
                root_id: requested_root_id.clone(),
            },
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::MessagesPrepended {
                key: TimelineKey::Thread {
                    conversation_id: conversation_id.clone(),
                    root_id: MessageId::new("1"),
                    requested_root_id,
                },
                messages: vec![message(&conversation_id, "1", 1)],
                cursor: None,
            },
        );
        assert_eq!(state.thread.root_message_id, Some(MessageId::new("1")));
        assert!(!state.thread.loading);
        assert_eq!(state.thread.replies.len(), 1);
    }

    fn sidebar_shows_unread(state: &UiState) -> bool {
        state
            .sidebar
            .sections
            .iter()
            .flat_map(|section| &section.rows)
            .any(|row| row.unread_count > 0 || row.mention_count > 0)
    }

    fn dm_summary(id: &str, title: &str) -> ConversationSummary {
        ConversationSummary {
            id: ConversationId::new(id),
            title: title.to_string(),
            kind: ConversationKind::DirectMessage,
            topic: String::new(),
            group: None,
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 0,
            last_message: None,
        }
    }

    fn preview_text(state: &UiState, id: &str) -> Option<String> {
        state
            .workspace
            .channels
            .iter()
            .chain(&state.workspace.direct_messages)
            .find(|summary| summary.id.0 == id)
            .and_then(|summary| summary.last_message.as_ref())
            .map(|preview| match &preview.body {
                PreviewBody::Text(text) => {
                    let author = preview.author_id.as_ref().map_or("?", |id| id.0.as_str());
                    format!("{author}: {text}")
                }
                other => format!("{other:?}"),
            })
    }

    #[test]
    fn live_messages_keep_every_conversation_preview_current() {
        let open = ConversationId::new("open");
        let other = ConversationId::new("other");
        let mut state = UiState::default();
        state.workspace.channels = vec![channel_summary("open", "general")];
        state.workspace.direct_messages = vec![dm_summary("other", "bob")];
        state.timeline.conversation_id = Some(open.clone());

        // Another conversation previews a message that arrives while it is closed.
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&other, "10", 1_000)),
        );
        assert_eq!(preview_text(&state, "other").as_deref(), Some("alice: m10"));
        assert_eq!(state.workspace.direct_messages[0].last_activity_ms, 1_000);

        // A late copy of an older message never replaces the newer preview.
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&other, "9", 2_000)),
        );
        assert_eq!(preview_text(&state, "other").as_deref(), Some("alice: m10"));

        // An edit of the previewed message updates its text.
        let mut edit = message(&other, "10", 3_000);
        edit.fragments = vec![MessageFragment::Text("fixed".into())];
        edit.edited = Some(crate::domain::message::EditMeta {
            edit_id: MessageId::new("11"),
            edited_at_ms: Some(3_000),
        });
        reduce_backend_event(&mut state, BackendEvent::MessageUpserted(edit));
        assert_eq!(
            preview_text(&state, "other").as_deref(),
            Some("alice: fixed")
        );

        // Deleting it in a closed conversation leaves a "deleted" line.
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageDeleted {
                conversation_id: other.clone(),
                message_id: MessageId::new("10"),
            },
        );
        assert_eq!(preview_text(&state, "other").as_deref(), Some("Deleted"));

        // In the open conversation the preview falls back to the message before.
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&open, "20", 4_000)),
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&open, "21", 5_000)),
        );
        assert_eq!(preview_text(&state, "open").as_deref(), Some("alice: m21"));
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageDeleted {
                conversation_id: open.clone(),
                message_id: MessageId::new("21"),
            },
        );
        assert_eq!(preview_text(&state, "open").as_deref(), Some("alice: m20"));
    }

    #[test]
    fn your_send_previews_at_once_and_keeps_its_preview_when_confirmed() {
        let id = ConversationId::new("conv");
        let mut state = UiState::default();
        state.workspace.channels = vec![channel_summary("conv", "general")];
        state.timeline.conversation_id = Some(id.clone());
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&id, "30", now_unix_ms() - 60_000)),
        );
        let client = ClientMessageId::new("send-1");
        add_pending_message_to_state(&mut state, &id, "on my way", vec![], None, &client);
        assert_eq!(
            preview_text(&state, "conv").as_deref(),
            Some("user_me: on my way")
        );
        // The server's clock may stamp the message a little earlier than ours.
        let mut confirmed = message(&id, "31", now_unix_ms() - 30_000);
        confirmed.author_id = UserId::new("user_me");
        confirmed.source_text = Some("on my way".into());
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageSendConfirmed {
                op_id: OpId::new("op"),
                client_message_id: client,
                server_message: confirmed,
            },
        );
        let preview = state.workspace.channels[0].last_message.clone().unwrap();
        assert_eq!(preview.message_id, Some(MessageId::new("31")));
        assert_eq!(
            preview_text(&state, "conv").as_deref(),
            Some("user_me: on my way")
        );
    }

    #[test]
    fn inbox_updates_merge_previews_instead_of_dropping_or_regressing_them() {
        let id = ConversationId::new("conv");
        let mut state = UiState::default();
        state.workspace.channels = vec![channel_summary("conv", "general")];
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&id, "40", 1_000)),
        );
        let extend = |summary: ConversationSummary| BackendEvent::WorkspaceConversationsExtended {
            workspace_id: WorkspaceId::new("ws"),
            channels: vec![summary],
            direct_messages: vec![],
            conversation_bindings: vec![],
        };
        // Metadata without a preview, and Keybase's snippet of the same message.
        reduce_backend_event(&mut state, extend(channel_summary("conv", "general")));
        let mut snippet = channel_summary("conv", "general");
        snippet.last_message = Some(MessagePreview {
            message_id: Some(MessageId::new("40")),
            author_id: None,
            timestamp_ms: 1_000,
            body: PreviewBody::Snippet("alice: m40".into()),
        });
        reduce_backend_event(&mut state, extend(snippet.clone()));
        assert_eq!(preview_text(&state, "conv").as_deref(), Some("alice: m40"));
        assert!(matches!(
            state.workspace.channels[0]
                .last_message
                .as_ref()
                .unwrap()
                .body,
            PreviewBody::Text(_)
        ));
        // A snippet of a newer message does replace it.
        snippet.last_message.as_mut().unwrap().message_id = Some(MessageId::new("41"));
        reduce_backend_event(&mut state, extend(snippet));
        assert!(matches!(
            state.workspace.channels[0]
                .last_message
                .as_ref()
                .unwrap()
                .body,
            PreviewBody::Snippet(_)
        ));
    }

    fn channel_summary(id: &str, title: &str) -> ConversationSummary {
        ConversationSummary {
            id: ConversationId::new(id),
            title: title.to_string(),
            kind: ConversationKind::Channel,
            topic: String::new(),
            group: None,
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 0,
            last_message: None,
        }
    }

    fn profile_with_follow_status(user_id: &str, you_are_following: bool) -> UserProfile {
        UserProfile {
            user_id: UserId::new(user_id),
            username: user_id.to_string(),
            display_name: user_id.to_string(),
            avatar_asset: None,
            presence: Presence {
                availability: crate::domain::presence::Availability::Unknown,
                status_text: None,
            },
            affinity: if you_are_following {
                Affinity::Positive
            } else {
                Affinity::None
            },
            bio: None,
            location: None,
            title: None,
            sections: vec![ProfileSection::SocialGraph(SocialGraph {
                followers_count: Some(0),
                following_count: Some(0),
                is_following_you: false,
                you_are_following,
                followers: Some(Vec::new()),
                following: Some(Vec::new()),
            })],
        }
    }

    fn profile_following_flag(profile: &UserProfile) -> bool {
        profile
            .sections
            .iter()
            .find_map(|section| match section {
                ProfileSection::SocialGraph(graph) => Some(graph.you_are_following),
                _ => None,
            })
            .unwrap_or(false)
    }

    #[test]
    fn workspace_merge_repairs_identity_and_excludes_internal_conversations() {
        let mut state = UiState::default();
        state.workspace.channels = vec![
            channel_summary("same", "stale"),
            channel_summary("same", "stale"),
        ];
        state.workspace.direct_messages = vec![dm_summary("internal", "helperbot")];
        let event = BackendEvent::WorkspaceConversationsExtended {
            workspace_id: WorkspaceId::new("ws"),
            channels: vec![],
            direct_messages: vec![
                dm_summary("same", "helperbot"),
                dm_summary("other", "helperbot"),
            ],
            conversation_bindings: vec![],
        };
        reduce_backend_event(&mut state, event.clone());
        reduce_backend_event(
            &mut state,
            BackendEvent::WorkspaceConversationsExcluded {
                workspace_id: WorkspaceId::new("ws"),
                conversation_ids: vec![ConversationId::new("internal")],
            },
        );
        // A cache-extension snapshot captured before the repair must not re-add it.
        let mut stale = event;
        if let BackendEvent::WorkspaceConversationsExtended {
            direct_messages, ..
        } = &mut stale
        {
            direct_messages.push(dm_summary("internal", "helperbot"));
        }
        reduce_backend_event(&mut state, stale);
        assert!(state.workspace.channels.is_empty());
        assert_eq!(state.workspace.direct_messages.len(), 2);
        assert!(
            state
                .workspace
                .direct_messages
                .iter()
                .all(|s| s.id.0 != "internal")
        );
        let flat = state
            .sidebar
            .sections
            .iter()
            .find(|s| s.title == "Conversations")
            .unwrap();
        assert_eq!(
            flat.rows.len(),
            2,
            "distinct real IDs with the same name must survive"
        );
        // Classification changes also remove the old DM entry.
        merge_workspace_conversations(&mut state, vec![channel_summary("same", "general")], vec![]);
        assert_eq!(state.workspace.channels.len(), 1);
        assert_eq!(state.workspace.direct_messages.len(), 1);
    }

    #[test]
    fn conversations_lead_and_a_team_with_unread_opens_expanded() {
        use crate::domain::conversation::{ConversationGroup, ConversationTeamType};
        let team = |id: &str, name: &str| {
            let mut summary = channel_summary(id, "general");
            summary.group = Some(ConversationGroup {
                id: name.into(),
                display_name: name.into(),
                team_type: ConversationTeamType::Complex,
            });
            summary
        };
        let quiet = team("quiet", "Quiet");
        let mut busy = team("busy", "Busy");
        busy.unread_count = 1;
        let sections = build_sidebar_sections(
            &[quiet, busy],
            &[dm_summary("dm", "alice")],
            &WorkspaceId::new("ws"),
            &HashMap::new(),
        );
        let ids = sections
            .iter()
            .map(|section| (section.id.as_ref().unwrap().0.as_str(), section.collapsed))
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            [("dms", false), ("group_Busy", false), ("group_Quiet", true)]
        );
    }

    #[test]
    fn reopening_conversation_after_find_loads_latest_and_clears_match() {
        let mut state = UiState::default();
        let conversation = ConversationId::new("chat");
        let route = Route::DirectMessage {
            workspace_id: WorkspaceId::new("ws"),
            dm_id: DmId::new("chat"),
        };
        reduce_ui_action(&mut state, UiAction::Navigate(route.clone()));
        reduce_ui_action(
            &mut state,
            UiAction::JumpToMessage {
                conversation_id: conversation.clone(),
                message_id: MessageId::new("10"),
            },
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::TimelineReplaced {
                conversation_id: conversation.clone(),
                messages: vec![message(&conversation, "10", 100)],
                older_cursor: Some("10".into()),
                newer_cursor: Some("10".into()),
            },
        );

        let output = reduce_ui_action(&mut state, UiAction::ReturnToLatestMessages);
        assert_eq!(state.navigation.current_route, Some(route));
        assert!(state.timeline.highlighted_message_id.is_none());
        assert!(output.effects.iter().any(|effect| matches!(
            effect,
            Effect::Backend(BackendCommand::LoadConversation { conversation_id })
                if conversation_id == &conversation
        )));

        reduce_backend_event(
            &mut state,
            BackendEvent::TimelineReplaced {
                conversation_id: conversation.clone(),
                messages: vec![message(&conversation, "100", 1_000)],
                older_cursor: Some("100".into()),
                newer_cursor: Some("100".into()),
            },
        );
        assert_eq!(
            state.timeline.messages.last().unwrap().id,
            MessageId::new("100")
        );
    }

    #[test]
    fn recent_activity_reorders_inactive_chats_and_late_metadata_cannot_undo_it() {
        let mut state = UiState::default();
        state.workspace.active_workspace_id = Some(WorkspaceId::new("ws"));
        let mut first = dm_summary("first", "alice");
        first.last_activity_ms = 200;
        let mut second = dm_summary("second", "bob");
        second.last_activity_ms = 100;
        state.workspace.direct_messages = vec![first, second.clone()];
        refresh_sidebar(&mut state);
        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&ConversationId::new("second"), "10", 300)),
        );
        assert_eq!(
            state.timeline.conversation_id, None,
            "activity must not force navigation"
        );
        let flat = state
            .sidebar
            .sections
            .iter()
            .find(|s| s.title == "Conversations")
            .unwrap();
        assert_eq!(flat.rows[0].label, "bob");
        second.last_activity_ms = 50;
        reduce_backend_event(
            &mut state,
            BackendEvent::WorkspaceConversationsExtended {
                workspace_id: WorkspaceId::new("ws"),
                channels: vec![],
                direct_messages: vec![second],
                conversation_bindings: vec![],
            },
        );
        let flat = state
            .sidebar
            .sections
            .iter()
            .find(|s| s.title == "Conversations")
            .unwrap();
        assert_eq!(flat.rows[0].label, "bob");
        assert_eq!(state.workspace.direct_messages[1].last_activity_ms, 300);
    }

    #[test]
    fn incomplete_or_stale_profile_updates_preserve_known_avatars() {
        let mut state = UiState::default();
        let user = UserId::new("bot");
        for (avatar, updated_ms) in [
            (Some("/cache/bot.png".to_string()), 100),
            (None, 200),
            (Some("/cache/obsolete.png".to_string()), 50),
        ] {
            reduce_backend_event(
                &mut state,
                BackendEvent::UserProfileUpserted {
                    user_id: user.clone(),
                    display_name: "bot".into(),
                    avatar_asset: avatar,
                    updated_ms,
                },
            );
        }
        assert_eq!(
            state.backend.user_profiles[&user].avatar_asset.as_deref(),
            Some("/cache/bot.png")
        );
        // A delayed cached avatar may enrich a newer response that had no image.
        let other = UserId::new("other");
        for (avatar, updated_ms) in [(None, 200), (Some("/cache/other.png".to_string()), 100)] {
            reduce_backend_event(
                &mut state,
                BackendEvent::UserProfileUpserted {
                    user_id: other.clone(),
                    display_name: "other".into(),
                    avatar_asset: avatar,
                    updated_ms,
                },
            );
        }
        assert_eq!(
            state.backend.user_profiles[&other].avatar_asset.as_deref(),
            Some("/cache/other.png")
        );
    }

    #[test]
    fn profile_refresh_retains_loaded_rosters_until_authoritative_list_response() {
        let mut state = UiState::default();
        let user = UserId::new("alice");
        let mut cached = profile_with_follow_status("alice", false);
        let entry = SocialGraphEntry {
            user_id: UserId::new("bob"),
            display_name: "Bob".into(),
            avatar_asset: None,
            affinity: Affinity::None,
        };
        merge_social_graph_entries(
            &mut cached,
            SocialGraphListType::Followers,
            vec![entry.clone()],
        );
        merge_social_graph_entries(
            &mut cached,
            SocialGraphListType::Following,
            vec![entry.clone()],
        );
        state
            .backend
            .profile_panel
            .profiles
            .insert(user.clone(), cached);
        reduce_ui_action(
            &mut state,
            UiAction::ShowUserProfilePanel {
                user_id: user.clone(),
            },
        );
        let mut fresh = profile_with_follow_status("alice", true);
        if let ProfileSection::SocialGraph(graph) = &mut fresh.sections[0] {
            graph.followers = None;
            graph.following = None;
            graph.followers_count = Some(2);
        }
        reduce_backend_event(
            &mut state,
            BackendEvent::UserProfileLoaded {
                account_id: AccountId::new("account"),
                profile: fresh,
            },
        );
        let graph = match &state.backend.profile_panel.profiles[&user].sections[0] {
            ProfileSection::SocialGraph(graph) => graph,
            _ => panic!("missing social graph"),
        };
        assert_eq!(graph.followers.as_deref(), Some([entry.clone()].as_slice()));
        assert_eq!(graph.following.as_deref(), Some([entry].as_slice()));
        assert_eq!(
            graph.followers_count,
            Some(2),
            "metadata counts still refresh"
        );
        assert!(graph.you_are_following);
        assert_eq!(
            state.backend.profile_panel.social_lists
                [&(user.clone(), SocialGraphListType::Followers)],
            SocialListLoadState::Loading
        );
        let duplicate = reduce_ui_action(
            &mut state,
            UiAction::LoadSocialGraphList {
                user_id: user.clone(),
                list_type: SocialGraphListType::Followers,
            },
        );
        assert!(
            duplicate.effects.is_empty(),
            "coalesce an in-flight refresh"
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::SocialGraphListLoaded {
                user_id: user.clone(),
                list_type: SocialGraphListType::Followers,
                entries: vec![],
            },
        );
        let graph = match &state.backend.profile_panel.profiles[&user].sections[0] {
            ProfileSection::SocialGraph(graph) => graph,
            _ => panic!("missing social graph"),
        };
        assert_eq!(
            graph.followers.as_deref(),
            Some([].as_slice()),
            "an authoritative empty list must replace cached entries"
        );
        assert_eq!(graph.following.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn reopening_cached_following_refreshes_only_the_canonical_active_tab() {
        let mut state = UiState::default();
        let user = UserId::new("alice");
        let metadata = profile_with_follow_status("alice", false);
        state
            .backend
            .profile_panel
            .profiles
            .insert(user.clone(), metadata.clone());
        state.backend.profile_panel.active_social_tab = SocialGraphListType::Following;
        for _ in 0..2 {
            let opened = reduce_ui_action(
                &mut state,
                UiAction::ShowUserProfilePanel {
                    user_id: user.clone(),
                },
            );
            assert!(
                matches!(
                    opened.effects.as_slice(),
                    [
                        Effect::Backend(BackendCommand::LoadUserProfile { .. }),
                        Effect::Backend(BackendCommand::LoadSocialGraphList {
                            list_type: SocialGraphListType::Following,
                            ..
                        })
                    ]
                ),
                "reopening refreshes the loaded active list independently of metadata"
            );
            let output = reduce_backend_event(
                &mut state,
                BackendEvent::UserProfileLoaded {
                    account_id: AccountId::new("account"),
                    profile: metadata.clone(),
                },
            );
            assert!(
                output.effects.is_empty(),
                "metadata must not duplicate the active-tab refresh"
            );
            reduce_backend_event(
                &mut state,
                BackendEvent::SocialGraphListLoaded {
                    user_id: user.clone(),
                    list_type: SocialGraphListType::Following,
                    entries: vec![],
                },
            );
            // Even a completed roster refresh is not restarted by later metadata.
            let output = reduce_backend_event(
                &mut state,
                BackendEvent::UserProfileLoaded {
                    account_id: AccountId::new("account"),
                    profile: metadata.clone(),
                },
            );
            assert!(output.effects.is_empty());
            reduce_ui_action(&mut state, UiAction::CloseRightPane);
        }
        let output = reduce_backend_event(
            &mut state,
            BackendEvent::UserProfileLoaded {
                account_id: AccountId::new("account"),
                profile: metadata,
            },
        );
        assert!(
            output.effects.is_empty(),
            "closed profiles do not start hidden roster loads"
        );
    }

    #[test]
    fn social_list_failures_are_per_tab_and_retryable() {
        let mut state = UiState::default();
        let user = UserId::new("alice");
        for list_type in [
            SocialGraphListType::Followers,
            SocialGraphListType::Following,
        ] {
            reduce_ui_action(
                &mut state,
                UiAction::LoadSocialGraphList {
                    user_id: user.clone(),
                    list_type,
                },
            );
        }
        reduce_backend_event(
            &mut state,
            BackendEvent::CommandFailed {
                command: Box::new(BackendCommand::LoadSocialGraphList {
                    user_id: user.clone(),
                    list_type: SocialGraphListType::Followers,
                }),
                error: "Keybase unavailable".into(),
            },
        );
        assert_eq!(
            state.backend.profile_panel.social_lists
                [&(user.clone(), SocialGraphListType::Followers)],
            SocialListLoadState::Failed
        );
        assert_eq!(
            state.backend.profile_panel.social_lists
                [&(user.clone(), SocialGraphListType::Following)],
            SocialListLoadState::Loading
        );
        reduce_backend_event(
            &mut state,
            BackendEvent::SocialGraphListLoaded {
                user_id: user.clone(),
                list_type: SocialGraphListType::Following,
                entries: vec![],
            },
        );
        assert_eq!(
            state.backend.profile_panel.social_lists
                [&(user.clone(), SocialGraphListType::Followers)],
            SocialListLoadState::Failed
        );
        reduce_ui_action(
            &mut state,
            UiAction::LoadSocialGraphList {
                user_id: user.clone(),
                list_type: SocialGraphListType::Followers,
            },
        );
        assert_eq!(
            state.backend.profile_panel.social_lists[&(user, SocialGraphListType::Followers)],
            SocialListLoadState::Loading
        );
    }

    #[test]
    fn simple_teams_share_the_conversation_list_and_promote_to_groups() {
        use crate::domain::conversation::{ConversationGroup, ConversationTeamType};
        let workspace_id = WorkspaceId::new("ws");
        let mut simple = channel_summary("simple-general", "general");
        simple.group = Some(ConversationGroup {
            id: "finance".into(),
            display_name: "Finance".into(),
            team_type: ConversationTeamType::Simple,
        });
        simple.last_activity_ms = 20;
        simple.unread_count = 1;
        let mut complex = channel_summary("complex-general", "general");
        complex.group = Some(ConversationGroup {
            id: "engineering".into(),
            display_name: "Engineering".into(),
            team_type: ConversationTeamType::Complex,
        });
        let mut dm = dm_summary("dm", "alice");
        dm.last_activity_ms = 10;
        let sections = build_sidebar_sections(
            &[simple.clone(), complex.clone()],
            &[dm.clone()],
            &workspace_id,
            &HashMap::new(),
        );
        let flat = sections
            .iter()
            .find(|s| s.title == "Conversations")
            .unwrap();
        assert_eq!(
            flat.rows
                .iter()
                .map(|r| r.label.as_str())
                .collect::<Vec<_>>(),
            vec!["Finance", "alice"]
        );
        assert!(
            matches!(&flat.rows[0].route, Some(Route::Channel { channel_id, .. }) if channel_id.0 == "simple-general")
        );
        assert!(sections.iter().any(|s| s.title == "Engineering"));
        assert!(!sections.iter().any(|s| s.title == "Finance"));
        let mut state = UiState::default();
        state.workspace.channels = vec![simple.clone(), complex];
        state.workspace.direct_messages = vec![dm];
        simple.group.as_mut().unwrap().team_type = ConversationTeamType::Complex;
        reduce_backend_event(
            &mut state,
            BackendEvent::WorkspaceConversationsExtended {
                workspace_id: workspace_id.clone(),
                channels: vec![simple.clone()],
                direct_messages: vec![],
                conversation_bindings: vec![],
            },
        );
        assert!(
            state
                .sidebar
                .sections
                .iter()
                .any(|s| s.title == "Finance" && s.rows[0].label == "general")
        );
        assert_eq!(
            state
                .sidebar
                .sections
                .iter()
                .find(|s| s.title == "Conversations")
                .unwrap()
                .rows
                .len(),
            1
        );
        // Older caches with no team type must not undo the known classification.
        simple.group.as_mut().unwrap().team_type = ConversationTeamType::Unknown;
        reduce_backend_event(
            &mut state,
            BackendEvent::WorkspaceConversationsExtended {
                workspace_id,
                channels: vec![simple],
                direct_messages: vec![],
                conversation_bindings: vec![],
            },
        );
        assert_eq!(
            state.workspace.channels[0]
                .group
                .as_ref()
                .unwrap()
                .team_type,
            ConversationTeamType::Complex
        );
    }

    #[test]
    fn follow_user_ui_action_updates_follow_status_optimistically() {
        let mut state = UiState::default();
        let user_id = UserId::new("peer_user");
        state.backend.profile_panel.profiles.insert(
            user_id.clone(),
            profile_with_follow_status(&user_id.0, false),
        );

        let output = reduce_ui_action(
            &mut state,
            UiAction::FollowUser {
                user_id: user_id.clone(),
            },
        );

        assert_eq!(output.effects.len(), 1);
        assert!(matches!(
            &output.effects[0],
            Effect::Backend(BackendCommand::FollowUser { user_id: effected })
                if effected == &user_id
        ));
        assert_eq!(
            state.backend.user_affinities.get(&user_id),
            Some(&Affinity::Positive)
        );
        assert!(profile_following_flag(
            state
                .backend
                .profile_panel
                .profiles
                .get(&user_id)
                .expect("profile should exist")
        ));
    }

    #[test]
    fn notify_sound_applies_at_once_and_reverts_when_keybase_rejects_it() {
        let mut state = UiState::default();
        reduce_backend_event(
            &mut state,
            BackendEvent::NotifySoundLoaded { enabled: true },
        );
        assert!(state.notifications.notify_sound);

        let output = reduce_ui_action(&mut state, UiAction::SetNotifySound { enabled: false });
        assert!(!state.notifications.notify_sound);
        let [Effect::Backend(command)] = output.effects.as_slice() else {
            panic!("expected one backend command");
        };
        assert!(matches!(
            command,
            BackendCommand::SetNotifySound {
                enabled: false,
                previous: true
            }
        ));

        reduce_backend_event(
            &mut state,
            BackendEvent::CommandFailed {
                command: Box::new(command.clone()),
                error: "Keybase did not answer".to_string(),
            },
        );
        assert!(state.notifications.notify_sound);
        assert_eq!(state.notifications.toasts, vec!["Keybase did not answer"]);
    }

    #[test]
    fn follow_status_change_failed_reverts_optimistic_follow_toggle() {
        let mut state = UiState::default();
        let user_id = UserId::new("peer_user");
        state.backend.profile_panel.profiles.insert(
            user_id.clone(),
            profile_with_follow_status(&user_id.0, false),
        );
        let _ = reduce_ui_action(
            &mut state,
            UiAction::FollowUser {
                user_id: user_id.clone(),
            },
        );

        reduce_backend_event(
            &mut state,
            BackendEvent::FollowStatusChangeFailed {
                user_id: user_id.clone(),
                attempted_follow: true,
                error: "rpc failed".to_string(),
            },
        );

        assert_eq!(
            state.backend.user_affinities.get(&user_id),
            Some(&Affinity::None)
        );
        assert!(!profile_following_flag(
            state
                .backend
                .profile_panel
                .profiles
                .get(&user_id)
                .expect("profile should exist")
        ));
    }

    #[test]
    fn follow_status_changed_updates_lowercase_profile_key_without_forced_refresh() {
        let mut state = UiState::default();
        let lower = UserId::new("peer_user");
        state
            .backend
            .profile_panel
            .profiles
            .insert(lower.clone(), profile_with_follow_status(&lower.0, false));

        let output = reduce_backend_event(
            &mut state,
            BackendEvent::FollowStatusChanged {
                user_id: UserId::new("PEER_USER"),
                you_are_following: true,
            },
        );

        assert!(output.effects.is_empty());
        assert!(profile_following_flag(
            state
                .backend
                .profile_panel
                .profiles
                .get(&lower)
                .expect("profile should exist")
        ));
    }

    #[test]
    fn open_or_create_direct_message_navigates_existing_direct_message() {
        let mut state = UiState::default();
        state.workspace.active_workspace_id = Some(WorkspaceId::new("ws_primary"));
        state
            .workspace
            .direct_messages
            .push(dm_summary("kb_conv:dm-existing", "aliceexample,peer_user"));

        let output = reduce_ui_action(
            &mut state,
            UiAction::OpenOrCreateDirectMessage {
                user_id: UserId::new("peer_user"),
            },
        );

        assert_eq!(
            state.timeline.conversation_id,
            Some(ConversationId::new("kb_conv:dm-existing"))
        );
        assert!(matches!(
            state.navigation.current_route,
            Some(Route::DirectMessage { dm_id, .. }) if dm_id.0 == "kb_conv:dm-existing"
        ));
        assert_eq!(output.effects.len(), 1);
        assert!(matches!(
            &output.effects[0],
            Effect::Backend(BackendCommand::LoadConversation { conversation_id })
                if conversation_id.0 == "kb_conv:dm-existing"
        ));
    }

    #[test]
    fn open_or_create_direct_message_creates_when_missing() {
        let mut state = UiState::default();

        let output = reduce_ui_action(
            &mut state,
            UiAction::OpenOrCreateDirectMessage {
                user_id: UserId::new("peer_user"),
            },
        );

        assert_eq!(output.effects.len(), 1);
        assert!(matches!(
            &output.effects[0],
            Effect::Backend(BackendCommand::CreateConversation {
                participants,
                kind: ConversationKind::DirectMessage,
                ..
            }) if participants.len() == 1 && participants[0].0 == "peer_user"
        ));
    }

    #[test]
    fn refresh_thread_reply_count_counts_all_descendants() {
        let conversation_id = ConversationId::new("kb_conv:test");
        let root_id = MessageId::new("100");

        let mut root = message(&conversation_id, "100", 1_000);
        root.thread_reply_count = 1;

        let mut direct = message(&conversation_id, "101", 1_001);
        direct.reply_to = Some(root_id.clone());
        direct.thread_root_id = Some(root_id.clone());

        let mut nested = message(&conversation_id, "102", 1_002);
        nested.reply_to = Some(MessageId::new("101"));
        nested.thread_root_id = Some(root_id.clone());

        let mut messages = vec![root, direct, nested];
        refresh_thread_reply_count(&mut messages, &root_id);

        let root = messages
            .iter()
            .find(|message| message.id == root_id)
            .expect("root should exist");
        assert_eq!(
            root.thread_reply_count, 2,
            "all descendants should contribute to thread reply count"
        );
    }

    #[test]
    fn refresh_thread_reply_count_dedupes_descendants_by_message_id() {
        let conversation_id = ConversationId::new("kb_conv:test");
        let root_id = MessageId::new("200");

        let mut root = message(&conversation_id, "200", 2_000);
        root.thread_reply_count = 0;

        let mut first = message(&conversation_id, "201", 2_001);
        first.reply_to = Some(root_id.clone());
        first.thread_root_id = Some(root_id.clone());

        let mut second = message(&conversation_id, "202", 2_002);
        second.reply_to = Some(root_id.clone());
        second.thread_root_id = Some(root_id.clone());

        let mut third = message(&conversation_id, "203", 2_003);
        third.reply_to = Some(root_id.clone());
        third.thread_root_id = Some(root_id.clone());

        let mut duplicate_second = second.clone();
        duplicate_second.timestamp_ms = Some(2_100);

        let mut messages = vec![root, first, second, third, duplicate_second];
        refresh_thread_reply_count(&mut messages, &root_id);

        let root = messages
            .iter()
            .find(|message| message.id == root_id)
            .expect("root should exist");
        assert_eq!(root.thread_reply_count, 3);
    }

    #[test]
    fn team_roles_updated_populates_conversation_and_team_role_indexes() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("kb_conv:team");
        let team_id = "kb_team:abcd".to_string();

        reduce_backend_event(
            &mut state,
            BackendEvent::TeamRolesUpdated {
                conversation_id: conversation_id.clone(),
                team_id: team_id.clone(),
                roles: vec![
                    TeamRoleEntry {
                        user_id: UserId::new("alice"),
                        role: TeamRoleKind::Owner,
                    },
                    TeamRoleEntry {
                        user_id: UserId::new("bob"),
                        role: TeamRoleKind::Admin,
                    },
                ],
                updated_ms: 123,
            },
        );

        assert_eq!(
            state.backend.conversation_team_ids.get(&conversation_id),
            Some(&team_id)
        );
        let role_index = state
            .backend
            .team_roles
            .get(&team_id)
            .expect("team role index should be present");
        assert_eq!(
            role_index.get(&UserId::new("alice")),
            Some(&TeamRoleKind::Owner)
        );
        assert_eq!(
            role_index.get(&UserId::new("bob")),
            Some(&TeamRoleKind::Admin)
        );
    }

    #[test]
    fn messages_prepended_prefers_existing_duplicate_and_keeps_id_order() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("kb_conv:test");
        state.timeline.conversation_id = Some(conversation_id.clone());
        state.timeline.messages = vec![
            message(&conversation_id, "200", 2_000),
            message(&conversation_id, "300", 3_000),
        ];

        reduce_backend_event(
            &mut state,
            BackendEvent::MessagesPrepended {
                key: TimelineKey::Conversation(conversation_id.clone()),
                messages: vec![
                    message(&conversation_id, "100", 1_000),
                    message(&conversation_id, "200", 1_500),
                ],
                cursor: Some("100".to_string()),
            },
        );

        let ids = state
            .timeline
            .messages
            .iter()
            .map(|message| message.id.0.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["100", "200", "300"]);
        let middle = state
            .timeline
            .messages
            .iter()
            .find(|message| message.id.0 == "200")
            .expect("id 200 should exist");
        assert_eq!(middle.timestamp_ms, Some(2_000));
    }

    #[test]
    fn message_upserted_resorts_timeline_by_message_id() {
        let mut state = UiState::default();
        let conversation_id = ConversationId::new("kb_conv:test");
        state.timeline.conversation_id = Some(conversation_id.clone());
        state.timeline.messages = vec![
            message(&conversation_id, "100", 1_000),
            message(&conversation_id, "300", 3_000),
        ];

        reduce_backend_event(
            &mut state,
            BackendEvent::MessageUpserted(message(&conversation_id, "200", 2_000)),
        );

        let ids = state
            .timeline
            .messages
            .iter()
            .map(|message| message.id.0.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["100", "200", "300"]);
    }
}
