//! Windows toasts. An unpackaged app needs a registered app user model ID for
//! toasts to show; registering it also lists zBase under Settings → System →
//! Notifications with its Banners, Sounds and Badges switches.
use super::{ClickedNotification, clicked};
use std::{
    collections::VecDeque,
    fs,
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicI32, Ordering},
    },
};
use windows::{
    Data::Xml::Dom::XmlDocument,
    Foundation::TypedEventHandler,
    UI::Notifications::{
        NotificationSetting, ToastNotification, ToastNotificationManager, ToastNotifier,
    },
    Win32::{
        System::SystemInformation::GetTickCount,
        UI::{
            Input::KeyboardAndMouse::{GetLastInputInfo, LASTINPUTINFO},
            Shell::{SetCurrentProcessExplicitAppUserModelID, ShellExecuteW},
            WindowsAndMessaging::SW_SHOWNORMAL,
        },
    },
    core::{HSTRING, Result, w},
};

/// The macOS bundle identifier, also set on the installer's Start Menu shortcut.
const APP_ID: &str = "com.zbase.app";
const APP_NAME: &str = "zBase";
const ICON: &[u8] = include_bytes!("../../../assets/windows/zBase.ico");
/// Windows drops a toast's click handler with the toast's last reference, and
/// Action Center keeps toasts for a while, so the recent ones stay alive.
const KEPT_TOASTS: usize = 50;
static TOASTS: Mutex<VecDeque<ToastNotification>> = Mutex::new(VecDeque::new());
/// The codes `super::permission` maps; 0 until `start`.
static PERMISSION: AtomicI32 = AtomicI32::new(0);
const PERMISSION_DENIED: i32 = 2;
const PERMISSION_ALLOWED: i32 = 3;

pub fn register_app() {
    // SAFETY: a plain call with a live string; it only affects this process.
    if let Err(error) = unsafe { SetCurrentProcessExplicitAppUserModelID(&HSTRING::from(APP_ID)) } {
        tracing::warn!("could not set the app user model ID: {error}");
    }
    if let Err(error) = register_app_id() {
        tracing::warn!("could not register zBase for notifications: {error}");
    }
}

/// Covers builds without the installer's shortcut and names the app, with
/// its icon, in Settings and on each toast.
fn register_app_id() -> windows_registry::Result<()> {
    let key = windows_registry::CURRENT_USER
        .create(format!(r"Software\Classes\AppUserModelId\{APP_ID}"))?;
    key.set_string("DisplayName", APP_NAME)?;
    let icon = crate::services::local_store::paths::data_root().join("notification-icon.ico");
    let written = fs::read(&icon).ok().as_deref() == Some(ICON)
        || icon
            .parent()
            .is_some_and(|dir| fs::create_dir_all(dir).is_ok())
            && fs::write(&icon, ICON).is_ok();
    if written {
        key.set_string("IconUri", icon.to_string_lossy().as_ref())?;
    }
    Ok(())
}

fn notifier() -> Result<ToastNotifier> {
    ToastNotificationManager::CreateToastNotifierWithId(&HSTRING::from(APP_ID))
}

pub fn start() -> bool {
    refresh_permission();
    true
}

pub fn post(
    title: &str,
    body: &str,
    image: Option<&Path>,
    target: &ClickedNotification,
    sound: bool,
) {
    if let Err(error) = try_post(title, body, image, target, sound) {
        tracing::warn!("could not show a notification: {error}");
    }
}

fn try_post(
    title: &str,
    body: &str,
    image: Option<&Path>,
    target: &ClickedNotification,
    sound: bool,
) -> Result<()> {
    let xml = XmlDocument::new()?;
    xml.LoadXml(&HSTRING::from(toast_xml(title, body, image, sound)))?;
    let toast = ToastNotification::CreateToastNotification(&xml)?;
    let target = target.clone();
    toast.Activated(&TypedEventHandler::new(move |_, _| {
        clicked(target.clone());
        Ok(())
    }))?;
    notifier()?.Show(&toast)?;
    if let Ok(mut toasts) = TOASTS.lock() {
        toasts.push_back(toast);
        while toasts.len() > KEPT_TOASTS {
            toasts.pop_front();
        }
    }
    Ok(())
}

/// Without an `<audio>` element Windows plays its default notification sound,
/// subject to the Sounds switch in Settings.
fn toast_xml(title: &str, body: &str, image: Option<&Path>, sound: bool) -> String {
    // Toasts show PNG, JPEG and GIF; anything else would show no image.
    let image = image
        .filter(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| {
                    ["png", "jpg", "jpeg", "gif"].contains(&extension.to_ascii_lowercase().as_str())
                })
        })
        .map(|path| {
            format!(
                "<image placement=\"appLogoOverride\" hint-crop=\"circle\" src=\"{}\"/>",
                escape_xml(&path.to_string_lossy())
            )
        })
        .unwrap_or_default();
    let audio = if sound {
        ""
    } else {
        "<audio silent=\"true\"/>"
    };
    format!(
        "<toast><visual><binding template=\"ToastGeneric\"><text>{}</text><text>{}</text>{image}\
         </binding></visual>{audio}</toast>",
        escape_xml(title),
        escape_xml(body),
    )
}

fn escape_xml(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            // XML 1.0 forbids most control characters.
            ch if ch.is_control() && !matches!(ch, '\n' | '\t') => {}
            ch => escaped.push(ch),
        }
    }
    escaped
}

pub fn seconds_since_user_input() -> f64 {
    let mut info = LASTINPUTINFO {
        cbSize: size_of::<LASTINPUTINFO>() as u32,
        dwTime: 0,
    };
    // SAFETY: `info` is a live, correctly sized LASTINPUTINFO.
    if !unsafe { GetLastInputInfo(&mut info) }.as_bool() {
        return 0.0;
    }
    // Both tick counts wrap after 49.7 days; the wrapping difference stays right.
    let idle_ms = unsafe { GetTickCount() }.wrapping_sub(info.dwTime);
    f64::from(idle_ms) / 1000.0
}

pub fn permission() -> i32 {
    PERMISSION.load(Ordering::Relaxed)
}

/// Windows answers at once, so this reads the switch in Settings directly.
pub fn refresh_permission() {
    let allowed = notifier()
        .and_then(|notifier| notifier.Setting())
        .is_ok_and(|setting| setting == NotificationSetting::Enabled);
    PERMISSION.store(
        if allowed {
            PERMISSION_ALLOWED
        } else {
            PERMISSION_DENIED
        },
        Ordering::Relaxed,
    );
}

/// Windows shows toasts without asking first.
pub fn request_permission() {}

pub fn open_settings() {
    // SAFETY: plain call with static strings; the shell opens the Settings app.
    unsafe {
        ShellExecuteW(
            None,
            w!("open"),
            w!("ms-settings:notifications"),
            None,
            None,
            SW_SHOWNORMAL,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toast_text_is_escaped() {
        let xml = toast_xml("#dev · <team>", "a & b \"c\"\u{0007}", None, true);
        assert!(xml.contains("<text>#dev · &lt;team&gt;</text>"));
        assert!(xml.contains("<text>a &amp; b &quot;c&quot;</text>"));
    }

    #[test]
    fn toasts_are_silent_only_when_sound_is_off() {
        assert!(!toast_xml("t", "b", None, true).contains("<audio"));
        assert!(toast_xml("t", "b", None, false).contains("<audio silent=\"true\"/>"));
    }

    #[test]
    fn toasts_show_only_images_windows_can_draw() {
        let png = toast_xml("t", "b", Some(Path::new(r"C:\cache\alice.PNG")), true);
        assert!(png.contains(r#"src="C:\cache\alice.PNG""#));
        let webp = toast_xml("t", "b", Some(Path::new(r"C:\cache\alice.webp")), true);
        assert!(!webp.contains("<image"));
    }
}
