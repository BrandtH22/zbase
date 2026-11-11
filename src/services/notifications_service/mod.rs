//! Posts desktop banners and reports which one the user clicked.
//! macOS app bundles and Windows post; other builds do nothing.

use crate::domain::ids::{ConversationId, MessageId};
use std::{
    path::Path,
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// The message a clicked banner points at.
#[derive(Clone, Debug)]
pub struct ClickedNotification {
    pub conversation_id: ConversationId,
    pub message_id: MessageId,
}

static CLICKS: OnceLock<UnboundedSender<ClickedNotification>> = OnceLock::new();

/// Keybase's "Play a sound for new messages" setting, shared with the Keybase
/// app. The backend loads it on connect; Settings changes it.
static SOUND: AtomicBool = AtomicBool::new(false);

pub fn sound_enabled() -> bool {
    SOUND.load(Ordering::Relaxed)
}

pub fn set_sound_enabled(enabled: bool) {
    SOUND.store(enabled, Ordering::Relaxed);
}

/// Names the app to the OS. Call before the first window opens: Windows
/// groups the taskbar button and lists notifications under this identity.
pub fn register_app() {
    #[cfg(target_os = "windows")]
    windows::register_app();
}

/// Starts the bridge once and returns the stream of clicked banners, or `None`
/// when this build cannot post notifications.
pub fn start() -> Option<UnboundedReceiver<ClickedNotification>> {
    let (sender, receiver) = unbounded_channel();
    CLICKS.set(sender).ok()?;
    platform::start().then_some(receiver)
}

/// `image` is a local picture shown beside the text, such as the sender's avatar.
pub fn post(
    title: &str,
    body: &str,
    image: Option<&Path>,
    target: &ClickedNotification,
    sound: bool,
) {
    platform::post(title, body, image, target, sound);
}

/// Whether the OS shows zBase's banners.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NotificationPermission {
    /// This build cannot post: not macOS or Windows, or a macOS build not
    /// running as an app bundle.
    #[default]
    Unavailable,
    NotAsked,
    Denied,
    Allowed,
    /// Allowed, but the alert style in System Settings is None.
    Silenced,
}

/// The last answer from the OS. Call `refresh_permission` to ask again.
pub fn permission() -> NotificationPermission {
    match platform::permission() {
        1 => NotificationPermission::NotAsked,
        2 => NotificationPermission::Denied,
        3 => NotificationPermission::Allowed,
        4 => NotificationPermission::Silenced,
        _ => NotificationPermission::Unavailable,
    }
}

/// Asks the OS again in the background; `permission` reports the answer later.
pub fn refresh_permission() {
    platform::refresh_permission();
}

/// Shows the macOS prompt, if zBase has not asked yet. Windows has none.
pub fn request_permission() {
    platform::request_permission();
}

/// Opens zBase's notification settings: System Settings on macOS, Settings →
/// System → Notifications on Windows.
pub fn open_settings() {
    platform::open_settings();
}

/// Seconds since the last keyboard or mouse input anywhere on the system.
pub fn seconds_since_user_input() -> f64 {
    platform::seconds_since_user_input()
}

#[cfg_attr(not(any(target_os = "macos", target_os = "windows")), allow(dead_code))]
fn clicked(target: ClickedNotification) {
    if let Some(sender) = CLICKS.get() {
        let _ = sender.send(target);
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{ClickedNotification, clicked};
    use crate::domain::ids::{ConversationId, MessageId};
    use std::ffi::{CStr, CString, c_char};
    use std::path::Path;

    unsafe extern "C" {
        fn zbase_notifications_start(on_click: extern "C" fn(*const c_char, *const c_char))
        -> bool;
        fn zbase_notifications_post(
            title: *const c_char,
            body: *const c_char,
            conversation_id: *const c_char,
            message_id: *const c_char,
            image_path: *const c_char,
            sound: bool,
        );
        fn zbase_seconds_since_user_input() -> f64;
        fn zbase_notifications_permission() -> i32;
        fn zbase_notifications_refresh_permission();
        fn zbase_notifications_request_permission();
        fn zbase_notifications_open_settings();
    }

    extern "C" fn on_click(conversation_id: *const c_char, message_id: *const c_char) {
        // SAFETY: the bridge passes NUL-terminated strings that live for the call.
        let (conversation_id, message_id) = unsafe {
            (
                CStr::from_ptr(conversation_id)
                    .to_string_lossy()
                    .into_owned(),
                CStr::from_ptr(message_id).to_string_lossy().into_owned(),
            )
        };
        clicked(ClickedNotification {
            conversation_id: ConversationId::new(conversation_id),
            message_id: MessageId::new(message_id),
        });
    }

    pub fn start() -> bool {
        // SAFETY: `on_click` matches the bridge's callback type.
        unsafe { zbase_notifications_start(on_click) }
    }

    pub fn post(
        title: &str,
        body: &str,
        image: Option<&Path>,
        target: &ClickedNotification,
        sound: bool,
    ) {
        let c = |text: &str| CString::new(text.replace('\0', "")).unwrap_or_default();
        let (title, body) = (c(title), c(body));
        let (conversation_id, message_id) = (c(&target.conversation_id.0), c(&target.message_id.0));
        let image_path = c(&image.map(|path| path.to_string_lossy()).unwrap_or_default());
        // SAFETY: every pointer is a live NUL-terminated string; the bridge copies them.
        unsafe {
            zbase_notifications_post(
                title.as_ptr(),
                body.as_ptr(),
                conversation_id.as_ptr(),
                message_id.as_ptr(),
                image_path.as_ptr(),
                sound,
            );
        }
    }

    pub fn seconds_since_user_input() -> f64 {
        // SAFETY: a plain CoreGraphics query.
        unsafe { zbase_seconds_since_user_input() }
    }

    pub fn permission() -> i32 {
        // SAFETY: reads an atomic in the bridge.
        unsafe { zbase_notifications_permission() }
    }

    pub fn refresh_permission() {
        // SAFETY: no arguments; a no-op before `start` succeeds.
        unsafe { zbase_notifications_refresh_permission() }
    }

    pub fn request_permission() {
        // SAFETY: no arguments; a no-op before `start` succeeds.
        unsafe { zbase_notifications_request_permission() }
    }

    pub fn open_settings() {
        // SAFETY: no arguments; a no-op before `start` succeeds.
        unsafe { zbase_notifications_open_settings() }
    }
}

#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
use windows as platform;

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
mod platform {
    use super::ClickedNotification;
    use std::path::Path;

    pub fn start() -> bool {
        false
    }

    pub fn post(_: &str, _: &str, _: Option<&Path>, _: &ClickedNotification, _: bool) {}

    pub fn seconds_since_user_input() -> f64 {
        0.0
    }

    pub fn permission() -> i32 {
        0
    }

    pub fn refresh_permission() {}

    pub fn request_permission() {}

    pub fn open_settings() {}
}
