use std::collections::{HashMap, HashSet};

use crate::domain::{
    affinity::Affinity,
    backend::{AccountId, BackendCapabilities, BackendId},
    call::CallStatus,
    channel_details::ConversationNotificationSettings,
    conversation::{ConversationBadgeState, ConversationSummary},
    ids::{CallId, ConversationId, MessageId, SidebarSectionId, UserId, WorkspaceId},
    message::{EmojiSourceRef, MessageRecord},
    pins::PinnedState,
    presence::Presence,
    profile::{SocialGraphListType, SocialListLoadState, UserProfile},
    route::Route,
    search::{SearchFilter, SearchResult},
    user::UserSummary,
};

use super::{
    action::DraftKey,
    bindings::{ConversationBinding, MessageBinding, WorkspaceBinding},
    event::TeamRoleKind,
    ids::{OpId, QueryId},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootPhase {
    Launching,
    HydratingLocalState,
    ConnectingBackend,
    Ready,
    Degraded,
    FatalError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Disconnected,
}

#[derive(Clone, Debug)]
pub enum TimelineKey {
    Conversation(ConversationId),
    Thread {
        conversation_id: ConversationId,
        root_id: MessageId,
        // Correlates the response even when the backend resolves a different canonical root.
        requested_root_id: MessageId,
    },
}

#[derive(Clone, Debug)]
pub struct AppState {
    pub boot_phase: BootPhase,
    pub boot_status: String,
}

#[derive(Clone, Debug, Default)]
pub struct NavigationState {
    pub current_route: Option<Route>,
    pub active_thread_root: Option<MessageId>,
}

#[derive(Clone, Debug, Default)]
pub struct DraftState {
    pub text: String,
}

#[derive(Clone, Debug, Default)]
pub struct UiWorkspaceState {
    pub left_conversation_ids: HashSet<ConversationId>,
    pub leaving_conversation_ids: HashSet<ConversationId>,
    pub leave_channel_errors: HashMap<ConversationId, String>,
    pub excluded_conversation_ids: HashSet<ConversationId>,
    pub active_workspace_id: Option<WorkspaceId>,
    pub workspace_name: String,
    pub channels: Vec<ConversationSummary>,
    pub direct_messages: Vec<ConversationSummary>,
}

#[derive(Clone, Debug, Default)]
pub struct UiSidebarRowState {
    pub label: String,
    pub unread_count: u32,
    pub mention_count: u32,
    pub route: Option<Route>,
}

#[derive(Clone, Debug, Default)]
pub struct UiSidebarSectionState {
    pub id: Option<SidebarSectionId>,
    pub title: String,
    pub rows: Vec<UiSidebarRowState>,
    pub collapsed: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UiSidebarState {
    pub sections: Vec<UiSidebarSectionState>,
    pub filter: String,
    pub highlighted_route: Option<Route>,
}

#[derive(Clone, Debug, Default)]
pub struct UiTimelineState {
    pub conversation_id: Option<ConversationId>,
    pub messages: Vec<MessageRecord>,
    /// Upload progress of pending attachment messages, by pending message id.
    pub upload_progress: HashMap<MessageId, UploadProgress>,
    pub typing_text: Option<String>,
    pub highlighted_message_id: Option<MessageId>,
    pub unread_marker: Option<MessageId>,
    pub older_cursor: Option<String>,
    pub newer_cursor: Option<String>,
    pub loading_older: bool,
}

/// How much of a pending attachment has reached the server.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UploadProgress {
    pub sent_bytes: u64,
    pub total_bytes: u64,
}

impl UploadProgress {
    /// Completed share in 0..=1; 0 while the total is still unknown.
    pub fn fraction(self) -> f32 {
        if self.total_bytes == 0 {
            return 0.;
        }
        (self.sent_bytes.min(self.total_bytes) as f64 / self.total_bytes as f64) as f32
    }
}

#[derive(Clone, Debug, Default)]
pub struct UiThreadState {
    pub open: bool,
    pub root_message_id: Option<MessageId>,
    pub replies: Vec<MessageRecord>,
    pub reply_draft: String,
    pub loading: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UiSearchState {
    pub active_query_id: Option<QueryId>,
    pub query: String,
    /// The query last sent with Return; results and "no results" belong to it.
    pub submitted_query: Option<String>,
    pub filters: Vec<SearchFilter>,
    pub results: Vec<SearchResult>,
    pub highlighted_index: Option<usize>,
    pub is_loading: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UiNewChatState {
    pub people: super::new_chat_search::PeopleCorpus,
    pub people_dirty: bool,
    pub is_searching: bool,
    pub active_query_id: Option<QueryId>,
    pub open: bool,
    pub search_query: String,
    pub search_results: Vec<UserSummary>,
    pub selected_participants: Vec<UserSummary>,
    pub creating: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct UiOverlayState {
    pub quick_switcher_open: bool,
    pub command_palette_open: bool,
    pub emoji_picker_open: bool,
    pub active_modal: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct UiCallState {
    pub active_call_id: Option<CallId>,
    pub call_status: Option<CallStatus>,
    pub participants: Vec<UserSummary>,
    pub is_muted: bool,
    pub is_sharing_screen: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UiNotificationsState {
    pub toasts: Vec<String>,
    pub notification_center_count: u32,
    /// Keybase's "Play a sound for new messages" setting, off until loaded.
    pub notify_sound: bool,
}

#[derive(Clone, Debug)]
pub struct AccountState {
    pub account_id: AccountId,
    pub backend_id: BackendId,
    pub display_name: String,
    pub avatar: Option<String>,
    pub connection_state: ConnectionState,
    pub capabilities: BackendCapabilities,
}

/// Keybase uses increasing numeric message IDs. Keep the last visible position
/// separately so older inbox summaries cannot rewind it.
#[derive(Clone, Debug, Default)]
pub struct ConfirmedReadPosition {
    pub read: u64,
    pub latest_visible: u64,
}

#[derive(Clone, Debug, Default)]
pub struct BackendRuntimeState {
    pub conversation_badges: HashMap<BackendId, ConversationBadgeState>,
    pub confirmed_read_positions: HashMap<ConversationId, ConfirmedReadPosition>,
    pub hydrated_accounts: HashSet<AccountId>,
    pub deleted_messages: HashSet<(ConversationId, MessageId)>,
    pub pending_reactions: HashMap<OpId, PendingReaction>,
    pub latest_reaction_ops: HashMap<(ConversationId, MessageId), OpId>,
    pub accounts: HashMap<AccountId, AccountState>,
    pub user_profiles: HashMap<UserId, UserProfileState>,
    pub user_affinities: HashMap<UserId, Affinity>,
    pub user_presences: HashMap<UserId, Presence>,
    pub profile_panel: UserProfilePanelState,
    pub conversation_emojis: HashMap<ConversationId, HashMap<String, EmojiRenderState>>,
    pub emoji_sources: HashMap<String, EmojiRenderState>,
    pub message_reactions: HashMap<ConversationId, HashMap<MessageId, Vec<MessageReactionState>>>,
    pub conversation_pins: HashMap<ConversationId, PinnedState>,
    /// Last notification choices loaded from (or sent to) Keybase, by conversation.
    pub conversation_notifications: HashMap<ConversationId, ConversationNotificationSettings>,
    pub conversation_team_ids: HashMap<ConversationId, String>,
    pub team_roles: HashMap<String, HashMap<UserId, TeamRoleKind>>,
    /// Authoritative per-conversation member roster (backend-provided when supported).
    pub conversation_members: HashMap<ConversationId, ConversationMembersState>,
    pub workspace_bindings: HashMap<crate::domain::ids::WorkspaceId, WorkspaceBinding>,
    pub conversation_bindings: HashMap<ConversationId, ConversationBinding>,
    pub message_bindings: HashMap<MessageId, MessageBinding>,
}

#[derive(Clone, Debug, Default)]
pub struct ConversationMembersState {
    pub members: Vec<UserId>,
    pub updated_ms: i64,
    pub is_complete: bool,
}

#[derive(Clone, Debug)]
pub struct UserProfileState {
    pub display_name: String,
    pub avatar_asset: Option<String>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug, Default)]
pub struct UserProfilePanelState {
    pub selected_user: Option<UserId>,
    pub active_social_tab: SocialGraphListType,
    pub profiles: HashMap<UserId, UserProfile>,
    pub loading: HashSet<UserId>,
    pub social_lists: HashMap<(UserId, SocialGraphListType), SocialListLoadState>,
}

#[derive(Clone, Debug)]
pub struct EmojiRenderState {
    pub alias: String,
    pub unicode: Option<String>,
    pub asset_path: Option<String>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug)]
pub struct PendingReaction {
    pub conversation_id: ConversationId,
    pub message_id: MessageId,
    pub previous: Vec<MessageReactionState>,
}

#[derive(Clone, Debug)]
pub struct MessageReactionState {
    pub emoji: String,
    pub source_ref: Option<EmojiSourceRef>,
    pub actor_ids: Vec<UserId>,
    pub updated_ms: i64,
}

#[derive(Clone, Debug)]
pub struct UiState {
    pub app: AppState,
    pub navigation: NavigationState,
    pub drafts: HashMap<DraftKey, DraftState>,
    pub backend: BackendRuntimeState,
    pub workspace: UiWorkspaceState,
    pub sidebar: UiSidebarState,
    pub timeline: UiTimelineState,
    pub thread: UiThreadState,
    pub search: UiSearchState,
    pub new_chat: UiNewChatState,
    pub overlay: UiOverlayState,
    pub call: UiCallState,
    pub notifications: UiNotificationsState,
    pub search_query: String,
    pub sidebar_filter: String,
    pub next_op_seq: u64,
}

impl UiState {
    pub fn with_route(route: Route) -> Self {
        let mut state = Self::default();
        state.navigation.current_route = Some(route);
        state
    }
}

impl Default for UiState {
    fn default() -> Self {
        Self {
            app: AppState {
                boot_phase: BootPhase::Launching,
                boot_status: String::new(),
            },
            navigation: NavigationState::default(),
            drafts: HashMap::new(),
            backend: BackendRuntimeState::default(),
            workspace: UiWorkspaceState::default(),
            sidebar: UiSidebarState::default(),
            timeline: UiTimelineState::default(),
            thread: UiThreadState::default(),
            search: UiSearchState::default(),
            new_chat: UiNewChatState::default(),
            overlay: UiOverlayState::default(),
            call: UiCallState::default(),
            notifications: UiNotificationsState::default(),
            search_query: String::new(),
            sidebar_filter: String::new(),
            next_op_seq: 1,
        }
    }
}
