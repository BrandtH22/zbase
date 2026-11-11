//! Desktop banners for incoming messages, following the Keybase desktop app.
//!
//! The Keybase service decides which messages notify: it applies each
//! conversation's desktop setting (all, mentions, never), `@here`/`@channel`,
//! reactions to your own messages, and skips your own messages. It marks those
//! with `displayDesktopNotification` and a ready-made snippet. The client then
//! skips muted conversations and the thread the user is actively reading.

use crate::{
    domain::{
        conversation::{ConversationSummary, ConversationTeamType},
        ids::{ConversationId, MessageId},
    },
    util::emoji_aliases::standard_emoji_for_alias,
};

/// Keybase stops treating a focused app as watched after five minutes without
/// input.
pub const USER_IDLE_AFTER_SECS: f64 = 300.0;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesktopNotification {
    pub conversation_id: ConversationId,
    /// The message to open: the new message, or for a reaction the message reacted to.
    pub message_id: MessageId,
    pub author: String,
    /// Keybase's `desktopNotificationSnippet`.
    pub snippet: String,
    /// The conversation as Keybase sent it with the message, for conversations
    /// not yet in the local inbox.
    pub conversation: Option<ConversationSummary>,
}

/// Where the user is looking, for Keybase's "actively looking at this thread" check.
pub struct Attention<'a> {
    pub window_focused: bool,
    pub seconds_since_input: f64,
    pub open_conversation: Option<&'a str>,
}

impl Attention<'_> {
    fn is_reading(&self, conversation_id: &ConversationId) -> bool {
        self.window_focused
            && self.seconds_since_input < USER_IDLE_AFTER_SECS
            && self.open_conversation == Some(conversation_id.0.as_str())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Banner {
    pub title: String,
    pub body: String,
}

impl DesktopNotification {
    /// The banner to show, or `None` when Keybase would stay quiet.
    pub fn banner(
        &self,
        known: Option<&ConversationSummary>,
        attention: &Attention<'_>,
    ) -> Option<Banner> {
        let summary = known.or(self.conversation.as_ref());
        if attention.is_reading(&self.conversation_id) || summary.is_some_and(|s| s.muted) {
            return None;
        }
        Some(Banner {
            title: summary.map_or_else(|| self.author.clone(), |s| self.title(s)),
            body: clean_snippet(&self.snippet),
        })
    }

    /// Team name for a small team, `team#channel` for a big team, else the sender.
    fn title(&self, summary: &ConversationSummary) -> String {
        let Some(group) = &summary.group else {
            return self.author.clone();
        };
        let team = if group.display_name.is_empty() {
            &self.author
        } else {
            &group.display_name
        };
        match group.team_type {
            ConversationTeamType::Simple => team.clone(),
            ConversationTeamType::Complex => format!("{team}#{}", summary.title),
            // Before Keybase reports the team type, a channel other than
            // #general means a big team.
            ConversationTeamType::Unknown if summary.title.eq_ignore_ascii_case("general") => {
                team.clone()
            }
            ConversationTeamType::Unknown => format!("{team}#{}", summary.title),
        }
    }
}

/// Hides spoilers (`!>text<!`) and turns `:shortcode:` emoji into characters.
fn clean_snippet(snippet: &str) -> String {
    let mut hidden = String::with_capacity(snippet.len());
    let mut rest = snippet;
    while let Some(start) = rest.find("!>") {
        let Some(len) = rest[start + 2..].find("<!") else {
            break;
        };
        hidden.push_str(&rest[..start]);
        hidden.push_str("•••");
        rest = &rest[start + 2 + len + 2..];
    }
    hidden.push_str(rest);
    replace_shortcodes(&hidden)
}

fn replace_shortcodes(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(':') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let emoji = after
            .find(':')
            .filter(|&end| {
                end > 0
                    && after[..end]
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '+'))
            })
            .and_then(|end| Some((end, standard_emoji_for_alias(&after[..end])?)));
        match emoji {
            Some((end, emoji)) => {
                out.push_str(emoji);
                rest = &after[end + 1..];
            }
            None => {
                out.push(':');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::conversation::{ConversationGroup, ConversationKind};

    fn notification() -> DesktopNotification {
        DesktopNotification {
            conversation_id: ConversationId::new("kb_conv:aa"),
            message_id: MessageId::new("12"),
            author: "alice".to_string(),
            snippet: "alice: hi :wave:".to_string(),
            conversation: None,
        }
    }

    fn summary(group: Option<(&str, ConversationTeamType)>, title: &str) -> ConversationSummary {
        ConversationSummary {
            id: ConversationId::new("kb_conv:aa"),
            title: title.to_string(),
            kind: if group.is_some() {
                ConversationKind::Channel
            } else {
                ConversationKind::DirectMessage
            },
            topic: String::new(),
            group: group.map(|(name, team_type)| ConversationGroup {
                id: name.to_string(),
                display_name: name.to_string(),
                team_type,
            }),
            unread_count: 0,
            mention_count: 0,
            muted: false,
            last_activity_ms: 0,
            last_message: None,
        }
    }

    const AWAY: Attention<'static> = Attention {
        window_focused: false,
        seconds_since_input: 0.0,
        open_conversation: Some("kb_conv:aa"),
    };

    #[test]
    fn titles_follow_keybase() {
        let n = notification();
        let title = |s: &ConversationSummary| n.banner(Some(s), &AWAY).unwrap().title;
        assert_eq!(title(&summary(None, "alice")), "alice");
        let small = summary(Some(("acme", ConversationTeamType::Simple)), "general");
        assert_eq!(title(&small), "acme");
        let big = summary(Some(("acme", ConversationTeamType::Complex)), "design");
        assert_eq!(title(&big), "acme#design");
        let unknown = summary(Some(("acme", ConversationTeamType::Unknown)), "design");
        assert_eq!(title(&unknown), "acme#design");
        assert_eq!(n.banner(None, &AWAY).unwrap().title, "alice");
    }

    #[test]
    fn falls_back_to_the_conversation_sent_with_the_message() {
        let mut n = notification();
        n.conversation = Some(summary(
            Some(("acme", ConversationTeamType::Complex)),
            "ops",
        ));
        assert_eq!(n.banner(None, &AWAY).unwrap().title, "acme#ops");
    }

    #[test]
    fn skips_muted_conversations() {
        let mut muted = summary(None, "alice");
        muted.muted = true;
        assert_eq!(notification().banner(Some(&muted), &AWAY), None);
    }

    #[test]
    fn skips_the_thread_the_user_is_reading() {
        let n = notification();
        let reading = Attention {
            window_focused: true,
            seconds_since_input: 10.0,
            open_conversation: Some("kb_conv:aa"),
        };
        assert_eq!(n.banner(None, &reading), None);
        let idle = Attention {
            seconds_since_input: USER_IDLE_AFTER_SECS + 1.0,
            ..reading
        };
        assert!(n.banner(None, &idle).is_some());
        let elsewhere = Attention {
            open_conversation: Some("kb_conv:bb"),
            ..reading
        };
        assert!(n.banner(None, &elsewhere).is_some());
    }

    #[test]
    fn cleans_spoilers_and_shortcodes() {
        assert_eq!(clean_snippet("alice: hi :wave:"), "alice: hi 👋");
        assert_eq!(clean_snippet("the end: !>he dies<! ok"), "the end: ••• ok");
        assert_eq!(
            clean_snippet("at 10:30 :nope_not_real:"),
            "at 10:30 :nope_not_real:"
        );
        assert_eq!(clean_snippet("unclosed !>spoiler"), "unclosed !>spoiler");
    }
}
