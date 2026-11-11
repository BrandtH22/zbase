use crate::domain::{
    affinity::Affinity,
    call::{CallStatus, ParticipantSummary},
    channel_details::ConversationNotificationSettings,
    conversation::{ConversationBadgeCounts, ConversationSummary},
    ids::{CallId, ConversationId, MessageId, UserId, WorkspaceId},
    message::{EmojiSourceRef, MessageRecord},
    pins::PinnedState,
    presence::Presence,
    profile::{SocialGraphEntry, SocialGraphListType, UserProfile},
    search::SearchResult,
    user::UserSummary,
};
use std::collections::HashMap;

use super::{
    action::DraftKey,
    bindings::{ConversationBinding, MessageBinding, WorkspaceBinding},
    effect::UploadedAttachment,
    ids::{ClientMessageId, DebounceKey, LocalAttachmentId, OpId, QueryId},
    state::TimelineKey,
};

#[derive(Clone, Debug)]
pub enum TickEvent {
    Heartbeat,
}

#[derive(Clone, Debug, Default)]
pub struct BootstrapPayload {
    pub workspace_ids: Vec<WorkspaceId>,
    pub active_workspace_id: Option<WorkspaceId>,
    pub workspace_name: String,
    pub channels: Vec<ConversationSummary>,
    pub direct_messages: Vec<ConversationSummary>,
    pub workspace_bindings: Vec<WorkspaceBinding>,
    pub conversation_bindings: Vec<ConversationBinding>,
    pub message_bindings: Vec<MessageBinding>,
    pub selected_conversation_id: Option<ConversationId>,
    pub selected_messages: Vec<MessageRecord>,
    pub unread_marker: Option<MessageId>,
    pub account_display_name: Option<String>,
    /// Notification choices Keybase returned for conversations in this inbox.
    pub conversation_notifications: Vec<(ConversationId, ConversationNotificationSettings)>,
}

#[derive(Clone, Debug, Default)]
pub struct ConversationPayload {
    pub conversation_id: Option<ConversationId>,
    pub message_ids: Vec<MessageId>,
}

#[derive(Clone, Debug)]
pub struct PresencePatch {
    pub user_id: UserId,
    pub presence: Presence,
}

#[derive(Clone, Debug)]
pub struct ConversationEmojiEntry {
    pub alias: String,
    pub unicode: Option<String>,
    pub asset_path: Option<String>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug)]
pub struct MessageReactionEntry {
    pub emoji: String,
    pub source_ref: Option<EmojiSourceRef>,
    pub actor_ids: Vec<UserId>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug)]
pub struct MessageReactionsForMessage {
    pub message_id: MessageId,
    pub reactions: Vec<MessageReactionEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TeamRoleKind {
    Member,
    Admin,
    Owner,
}

#[derive(Clone, Debug)]
pub struct TeamRoleEntry {
    pub user_id: UserId,
    pub role: TeamRoleKind,
}

#[derive(Clone, Debug)]
pub struct CallPatch {
    pub call_id: Option<CallId>,
    pub status: CallStatus,
    pub participants: Vec<ParticipantSummary>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapSource {
    Cache,
    Server,
}

#[derive(Clone, Debug)]
pub enum BackendEvent {
    BenchmarkPulse(std::time::Instant),
    AccountConnected {
        account_id: crate::domain::backend::AccountId,
    },
    AccountDisconnected {
        account_id: crate::domain::backend::AccountId,
        reason: String,
    },
    BootstrapLoaded {
        source: BootstrapSource,
        account_id: crate::domain::backend::AccountId,
        payload: BootstrapPayload,
    },
    WorkspaceConversationsExtended {
        workspace_id: WorkspaceId,
        channels: Vec<ConversationSummary>,
        direct_messages: Vec<ConversationSummary>,
        conversation_bindings: Vec<ConversationBinding>,
    },
    WorkspaceConversationsExcluded {
        workspace_id: WorkspaceId,
        conversation_ids: Vec<ConversationId>,
    },
    HistorySynced {
        conversation_id: ConversationId,
    },
    WorkspaceSynced {
        workspace_id: WorkspaceId,
    },
    ConversationLoaded {
        conversation_id: ConversationId,
        payload: ConversationPayload,
    },
    /// The server confirmed a conversation's notification choices.
    ConversationNotificationSettingsSynced {
        conversation_id: ConversationId,
        settings: ConversationNotificationSettings,
    },
    MessagesPrepended {
        key: TimelineKey,
        messages: Vec<MessageRecord>,
        cursor: Option<String>,
    },
    TimelineReplaced {
        conversation_id: ConversationId,
        messages: Vec<MessageRecord>,
        older_cursor: Option<String>,
        newer_cursor: Option<String>,
    },
    MessageUpserted(MessageRecord),
    /// Keybase's "Play a sound for new messages" setting, read on connect.
    NotifySoundLoaded {
        enabled: bool,
    },
    /// Keybase asked for a desktop banner for this message.
    DesktopNotification(crate::domain::desktop_notification::DesktopNotification),
    MessageAttachmentsUpdated {
        conversation_id: ConversationId,
        message_id: MessageId,
        attachments: Vec<crate::domain::attachment::AttachmentSummary>,
    },
    CommandFailed {
        command: Box<super::effect::BackendCommand>,
        error: String,
    },
    MessageSendConfirmed {
        op_id: OpId,
        client_message_id: ClientMessageId,
        server_message: MessageRecord,
    },
    MessageSendFailed {
        op_id: OpId,
        client_message_id: ClientMessageId,
        error: String,
    },
    /// Bytes of a pending attachment message uploaded so far.
    AttachmentUploadProgress {
        client_message_id: ClientMessageId,
        sent_bytes: u64,
        total_bytes: u64,
    },
    MessageDeleted {
        conversation_id: ConversationId,
        message_id: MessageId,
    },
    TypingUpdated {
        conversation_id: ConversationId,
        users: Vec<UserId>,
    },
    PresenceUpdated {
        account_id: crate::domain::backend::AccountId,
        users: Vec<PresencePatch>,
    },
    UserProfileUpserted {
        user_id: UserId,
        display_name: String,
        avatar_asset: Option<String>,
        updated_ms: i64,
    },
    UserProfileLoaded {
        account_id: crate::domain::backend::AccountId,
        profile: UserProfile,
    },
    SocialGraphListLoaded {
        user_id: UserId,
        list_type: SocialGraphListType,
        entries: Vec<SocialGraphEntry>,
    },
    FollowStatusChanged {
        user_id: UserId,
        you_are_following: bool,
    },
    FollowStatusChangeFailed {
        user_id: UserId,
        attempted_follow: bool,
        error: String,
    },
    AffinityChanged {
        user_id: UserId,
        affinity: Affinity,
    },
    AffinitySynced {
        affinities: HashMap<UserId, Affinity>,
    },
    ConversationEmojisSynced {
        conversation_id: ConversationId,
        emojis: Vec<ConversationEmojiEntry>,
    },
    EmojiSourceSynced {
        source_ref: EmojiSourceRef,
        alias: String,
        unicode: Option<String>,
        asset_path: Option<String>,
        updated_ms: i64,
    },
    MessageReactionsSynced {
        conversation_id: ConversationId,
        reactions_by_message: Vec<MessageReactionsForMessage>,
    },
    PinnedStateUpdated {
        conversation_id: ConversationId,
        pinned: PinnedState,
    },
    MessageReactionApplied {
        conversation_id: ConversationId,
        message_id: MessageId,
        emoji: String,
        source_ref: Option<EmojiSourceRef>,
        actor_id: UserId,
        updated_ms: i64,
    },
    MessageReactionRemoved {
        conversation_id: ConversationId,
        message_id: MessageId,
        emoji: String,
        actor_id: UserId,
    },
    ReactionConfirmed {
        op_id: OpId,
    },
    ReactionFailed {
        op_id: OpId,
        error: String,
    },
    TeamRolesUpdated {
        conversation_id: ConversationId,
        team_id: String,
        roles: Vec<TeamRoleEntry>,
        updated_ms: i64,
    },
    ConversationMembersUpdated {
        conversation_id: ConversationId,
        members: Vec<UserId>,
        updated_ms: i64,
        is_complete: bool,
    },
    ConversationUnreadChanged {
        conversation_id: ConversationId,
        unread_count: u32,
        mention_count: u32,
        read_upto: Option<MessageId>,
    },
    ConversationBadgesUpdated {
        backend_id: crate::domain::backend::BackendId,
        counts: HashMap<ConversationId, ConversationBadgeCounts>,
        complete: bool,
    },
    ReadMarkerUpdated {
        conversation_id: ConversationId,
        read_upto: Option<MessageId>,
    },
    SearchResults {
        query_id: QueryId,
        results: Vec<SearchResult>,
        is_complete: bool,
    },
    UserSearchResults {
        query_id: QueryId,
        results: Vec<UserSummary>,
    },
    ConversationCreated {
        op_id: OpId,
        workspace_id: WorkspaceId,
        conversation: ConversationSummary,
        conversation_binding: ConversationBinding,
    },
    ChannelResolved {
        workspace_id: WorkspaceId,
        conversation: ConversationSummary,
        conversation_binding: ConversationBinding,
        can_post: bool,
    },
    ChannelResolveFailed {
        workspace_id: WorkspaceId,
        channel_name: String,
        error: String,
    },
    ChannelJoined {
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    },
    ChannelLeft {
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
    },
    CallUpdated(CallPatch),
    BootStatus(String),
    KeybaseNotifyStub {
        method: String,
        payload_preview: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub enum EffectResult {
    DraftSaved {
        key: DraftKey,
    },
    DraftSaveFailed {
        key: DraftKey,
        error: String,
    },
    SettingsSaved,
    SettingsSaveFailed {
        error: String,
    },
    UploadProgress {
        op_id: OpId,
        attachment_id: LocalAttachmentId,
        sent_bytes: u64,
        total_bytes: u64,
    },
    UploadFinished {
        op_id: OpId,
        attachment: UploadedAttachment,
    },
    UploadFailed {
        op_id: OpId,
        attachment_id: LocalAttachmentId,
        error: String,
    },
    DebounceElapsed {
        key: DebounceKey,
    },
}

#[derive(Clone, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "Keep backend events inline when dispatching them through the reducer."
)]
pub enum AppEvent {
    Ui(super::action::UiAction),
    Backend(BackendEvent),
    EffectResult(EffectResult),
    Tick(TickEvent),
}
