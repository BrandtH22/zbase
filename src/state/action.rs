use crate::domain::{
    channel_details::ConversationNotificationSettings,
    ids::{ConversationId, MessageId, UserId},
    profile::SocialGraphListType,
    route::Route,
    search::SearchFilter,
    user::UserSummary,
};

use super::state::TimelineKey;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DraftKey {
    Conversation(ConversationId),
    Thread(MessageId),
}

#[derive(Clone, Debug)]
pub enum UiAction {
    StartApp,
    LeaveChannel {
        conversation_id: ConversationId,
    },
    /// Saves a conversation's notification choices on the Keybase server.
    SetConversationNotificationSettings {
        conversation_id: ConversationId,
        settings: ConversationNotificationSettings,
        previous: ConversationNotificationSettings,
    },
    /// Saves Keybase's "Play a sound for new messages" setting.
    SetNotifySound {
        enabled: bool,
    },
    Navigate(Route),
    /// Like Navigate but without triggering LoadConversation.
    /// Used by navigate_to_message to avoid a background load race with JumpToMessage.
    NavigateQuiet(Route),
    NavigateBack,
    OpenThread {
        root_id: MessageId,
    },
    CloseRightPane,
    SetSidebarFilter(String),
    /// The name style changed: rebuild labels that name people.
    RefreshPeopleNames,
    SetSearchQuery(String),
    SubmitSearch,
    OpenNewChat,
    CloseNewChat,
    NewChatSearchUsers {
        query: String,
    },
    NewChatSearchRemote {
        query_id: super::ids::QueryId,
    },
    NewChatAddParticipant {
        user: UserSummary,
    },
    NewChatRemoveParticipant {
        user_id: UserId,
    },
    NewChatCreate,
    QuickSwitcherSearch {
        seq: u64,
        query: String,
    },
    FindInChatSearch {
        seq: u64,
        conversation_id: ConversationId,
        query: String,
    },
    /// Leave a historical message window and reload the active chat's latest page.
    ReturnToLatestMessages,
    ToggleSearchFilter(SearchFilter),
    UpdateDraft {
        key: DraftKey,
        text: String,
    },
    SendMessage {
        key: DraftKey,
    },
    /// Removes a message that failed to send. It never reached Keybase, so
    /// this is local only; messages in any other state are left alone.
    DiscardFailedMessage {
        message_id: MessageId,
    },
    EditMessage {
        conversation_id: ConversationId,
        message_id: MessageId,
        text: String,
    },
    DeleteMessage {
        conversation_id: ConversationId,
        message_id: MessageId,
    },
    SendAttachment {
        key: DraftKey,
        local_path: String,
        filename: String,
        caption: String,
    },
    ReactToMessage {
        conversation_id: ConversationId,
        message_id: MessageId,
        emoji: String,
    },
    StartCall {
        conversation_id: ConversationId,
    },
    MarkConversationRead {
        conversation_id: ConversationId,
        message_id: Option<MessageId>,
    },
    JumpToMessage {
        conversation_id: ConversationId,
        message_id: MessageId,
    },
    LoadOlderMessages {
        key: TimelineKey,
        cursor: String,
    },
    ShowUserProfileCard {
        user_id: UserId,
    },
    ShowUserProfilePanel {
        user_id: UserId,
    },
    OpenOrCreateDirectMessage {
        user_id: UserId,
    },
    RefreshProfilePresence {
        user_id: UserId,
        conversation_id: Option<ConversationId>,
    },
    LoadSocialGraphList {
        user_id: UserId,
        list_type: SocialGraphListType,
    },
    FollowUser {
        user_id: UserId,
    },
    UnfollowUser {
        user_id: UserId,
    },
}
