//! Settings content: the persisted preferences in `SettingsModel`, grouped like
//! macOS System Settings. Only settings that change the app are listed. Shown in
//! its own window (see `settings_window`); every control writes back through the
//! main window, which owns and persists the settings.
use crate::services::notifications_service::{self, NotificationPermission};
use crate::views::typography;
use crate::{
    domain::user::NameStyle,
    models::{
        app_model::Connectivity,
        settings_model::{Density, SettingsModel, SidebarMode, SidebarStyle, TextSize, ThemeMode},
    },
    views::{
        accent, accent_strong, app_window::AppWindow, avatar::Avatar, field_border, is_dark_theme,
        secondary_button, section_label, shell_border, success, text_primary, text_secondary, tint,
        warning,
    },
};
use gpui_kit::prelude::FluentBuilder;
use gpui_kit::{
    AnyElement, App, BoxShadow, CursorStyle, FontWeight, InteractiveElement, IntoElement,
    ParentElement, SharedString, StatefulInteractiveElement, Styled, WeakEntity, div, point, px,
    rgb,
};

const CONTENT_MAX_WIDTH_PX: f32 = 640.;
const THUMBNAIL_WIDTH_PX: f32 = 88.;
const THUMBNAIL_HEIGHT_PX: f32 = 56.;

#[derive(Default)]
pub struct PreferencesView;

/// What Settings shows besides the saved preferences: the sidebar order of the
/// signed-in account and who that account is.
#[derive(Clone, Debug, Default)]
pub struct PreferencesContext {
    pub sidebar_mode: SidebarMode,
    pub updates: crate::services::updates::UpdateSnapshot,
    pub notification_permission: NotificationPermission,
    /// Keybase's "Play a sound for new messages" setting.
    pub notification_sound: bool,
    pub account: Option<AccountSummary>,
}

#[derive(Clone, Debug)]
pub struct AccountSummary {
    pub display_name: String,
    pub username: String,
    pub avatar_asset: Option<String>,
    pub connectivity: Connectivity,
}

impl PreferencesView {
    /// The scrollable settings sections. `app` is the main window that owns the
    /// settings; controls do nothing once it is gone.
    pub fn render(
        &self,
        settings: &SettingsModel,
        context: &PreferencesContext,
        app: &WeakEntity<AppWindow>,
    ) -> AnyElement {
        let theme_options = [
            (ThemeMode::System, "System"),
            (ThemeMode::Light, "Light"),
            (ThemeMode::Dark, "Dark"),
        ];
        let theme_row = div()
            .px(px(16.))
            .py(px(14.))
            .flex()
            .flex_col()
            .gap(px(12.))
            .child(row_text(
                "Theme",
                "Match your Mac, or keep zBase light or dark.",
            ))
            .child(
                div()
                    .flex()
                    .gap(px(16.))
                    .children(theme_options.into_iter().map(|(mode, label)| {
                        let selected = settings.theme_mode == mode;
                        theme_tile(mode, label, selected, app)
                    })),
            );

        let density_row = setting_row(
            "Message density",
            "Compact fits more messages on screen.",
            segmented(
                "preferences-density",
                vec![
                    (
                        "Comfortable",
                        settings.density == Density::Comfortable,
                        Density::Comfortable,
                    ),
                    (
                        "Compact",
                        settings.density == Density::Compact,
                        Density::Compact,
                    ),
                ],
                app,
                |this, density, cx| this.set_density(density, cx),
            ),
        );

        let order_row = setting_row(
            "Sidebar order",
            "Newest activity first, or channels under their teams.",
            segmented(
                "preferences-sidebar-order",
                vec![
                    (
                        "Recent",
                        context.sidebar_mode == SidebarMode::Recent,
                        SidebarMode::Recent,
                    ),
                    (
                        "By team",
                        context.sidebar_mode == SidebarMode::Grouped,
                        SidebarMode::Grouped,
                    ),
                ],
                app,
                |this, mode, cx| this.set_sidebar_mode(mode, cx),
            ),
        );

        let sidebar_row = setting_row(
            "Sidebar rows",
            "Previews add the latest message under each name.",
            segmented(
                "preferences-sidebar-style",
                vec![
                    (
                        "Names only",
                        settings.sidebar_style == SidebarStyle::Compact,
                        SidebarStyle::Compact,
                    ),
                    (
                        "With previews",
                        settings.sidebar_style == SidebarStyle::Previews,
                        SidebarStyle::Previews,
                    ),
                ],
                app,
                |this, style, cx| this.set_sidebar_style(style, cx),
            ),
        );

        let names_row = setting_row(
            "Show people as",
            "Their profile name, or their Keybase username.",
            segmented(
                "preferences-name-style",
                vec![
                    (
                        "Full names",
                        settings.name_style == NameStyle::FullName,
                        NameStyle::FullName,
                    ),
                    (
                        "Usernames",
                        settings.name_style == NameStyle::Username,
                        NameStyle::Username,
                    ),
                ],
                app,
                |this, style, cx| this.set_name_style(style, cx),
            ),
        );

        let side_panel = settings.show_right_pane;
        let side_panel_row = setting_row(
            "Side panel",
            "Open threads, members and profiles beside the conversation.",
            toggle(
                "preferences-side-panel",
                side_panel,
                app,
                move |this, cx| {
                    this.set_show_right_pane(!side_panel, cx);
                },
            ),
        );

        let reduced_motion = settings.reduced_motion;
        let motion_row = setting_row(
            "Reduce motion",
            "Hold the typing indicator still and skip animations.",
            toggle(
                "preferences-reduce-motion",
                reduced_motion,
                app,
                move |this, cx| {
                    this.set_reduced_motion(!reduced_motion, cx);
                },
            ),
        );

        div()
            .flex_1()
            .min_h(px(0.))
            .id("preferences-scroll")
            .overflow_y_scroll()
            .scrollbar_width(px(8.))
            .child(
                div()
                    .w_full()
                    .max_w(px(CONTENT_MAX_WIDTH_PX))
                    .mx_auto()
                    .px(px(24.))
                    .pt(px(8.))
                    .pb(px(20.))
                    .flex()
                    .flex_col()
                    .gap(px(20.))
                    .when_some(context.account.as_ref(), |content, account| {
                        content.child(section("Account", vec![account_row(account)]))
                    })
                    .child(section(
                        "Appearance",
                        vec![
                            theme_row.into_any_element(),
                            text_size_row(settings.text_size, app),
                            density_row,
                            names_row,
                        ],
                    ))
                    .child(section(
                        "Layout",
                        vec![order_row, sidebar_row, side_panel_row],
                    ))
                    .when_some(
                        notifications_row(context.notification_permission),
                        |content, row| {
                            content.child(section(
                                "Notifications",
                                vec![row, sound_row(context.notification_sound, app)],
                            ))
                        },
                    )
                    .child(section("Accessibility", vec![motion_row]))
                    .child(section(
                        "Updates",
                        update_settings_rows(&context.updates, app),
                    ))
                    .child(
                        div()
                            .px(px(4.))
                            .text_size(typography::font_px(12.))
                            .text_color(rgb(text_secondary()))
                            .child(format!(
                                "zBase {} · Settings are saved on this Mac.",
                                env!("CARGO_PKG_VERSION")
                            )),
                    ),
            )
            .into_any_element()
    }
}

/// Whether the OS shows zBase's banners, with the way to change it. `None` when
/// this build cannot post notifications.
fn notifications_row(permission: NotificationPermission) -> Option<AnyElement> {
    let (description, label, act): (&'static str, &'static str, fn()) = match permission {
        NotificationPermission::Unavailable => return None,
        NotificationPermission::Allowed => (
            "On. Each conversation's Keybase notification level decides which messages notify.",
            OPEN_OS_SETTINGS,
            notifications_service::open_settings,
        ),
        NotificationPermission::NotAsked => (
            "Off. zBase hasn't asked macOS for permission yet.",
            "Turn On…",
            notifications_service::request_permission,
        ),
        NotificationPermission::Denied => (
            if cfg!(target_os = "windows") {
                "Off. Windows isn't allowing notifications from zBase."
            } else {
                "Off. macOS isn't allowing notifications from zBase."
            },
            OPEN_OS_SETTINGS,
            notifications_service::open_settings,
        ),
        NotificationPermission::Silenced => (
            "Off. zBase's alert style in System Settings is None.",
            OPEN_OS_SETTINGS,
            notifications_service::open_settings,
        ),
    };
    let button = secondary_button("settings-notifications-action", label)
        .on_click(move |_, _, _| act())
        .into_any_element();
    Some(setting_row("Desktop notifications", description, button))
}

const OPEN_OS_SETTINGS: &str = if cfg!(target_os = "windows") {
    "Open Windows Settings…"
} else {
    "Open System Settings…"
};

/// Keybase's own setting, so it matches the Keybase app.
fn sound_row(on: bool, app: &WeakEntity<AppWindow>) -> AnyElement {
    setting_row(
        "Play a sound",
        "For new message notifications. Shared with the Keybase app.",
        toggle(
            "preferences-notification-sound",
            on,
            app,
            move |this, cx| {
                this.set_notification_sound(!on, cx);
            },
        ),
    )
}

/// The sample uses the same inherited scale as the rest of Settings.
fn text_size_row(size: TextSize, app: &WeakEntity<AppWindow>) -> AnyElement {
    div()
        .px(px(16.))
        .py(px(14.))
        .flex()
        .flex_col()
        .gap(px(12.))
        .child(row_text(
            "Text size",
            "Adjust text throughout zBase. Changes apply immediately.",
        ))
        .child(
            div()
                .flex()
                .items_center()
                .gap(px(12.))
                .child(text_size_step(
                    "preferences-text-size-decrease",
                    "−",
                    size.smaller(),
                    size != size.smaller(),
                    app,
                ))
                .child(
                    div()
                        .min_w(typography::font_px(52.))
                        .text_center()
                        .text_size(typography::body())
                        .child(format!("{}%", size.percent())),
                )
                .child(text_size_step(
                    "preferences-text-size-increase",
                    "+",
                    size.larger(),
                    size != size.larger(),
                    app,
                )),
        )
        .child(
            div()
                .text_size(typography::body())
                .child("The quick brown fox jumps over the lazy dog."),
        )
        .child(
            div()
                .text_size(typography::caption())
                .text_color(rgb(text_secondary()))
                .child("Labels, timestamps, and code scale with your messages."),
        )
        .child(
            div()
                .id("preferences-text-size-reset")
                .text_size(typography::caption())
                .text_color(rgb(accent()))
                .cursor(CursorStyle::PointingHand)
                .on_click(update_app(app, |this, cx| {
                    this.set_text_size(TextSize::default(), cx)
                }))
                .child("Reset to default"),
        )
        .into_any_element()
}

fn text_size_step(
    id: &'static str,
    label: &'static str,
    target: TextSize,
    enabled: bool,
    app: &WeakEntity<AppWindow>,
) -> AnyElement {
    div()
        .id(id)
        .size(typography::font_px(28.))
        .flex()
        .items_center()
        .justify_center()
        .rounded(px(6.))
        .border_1()
        .border_color(shell_border())
        .text_size(typography::body())
        .opacity(if enabled { 1. } else { 0.35 })
        .when(enabled, |button| {
            button
                .cursor(CursorStyle::PointingHand)
                .hover(|style| style.bg(segment_track()))
                .on_click(update_app(app, move |this, cx| {
                    this.set_text_size(target, cx)
                }))
        })
        .child(label)
        .into_any_element()
}

/// Click handler that applies `change` to the main window, if it still exists.
fn update_app(
    app: &WeakEntity<AppWindow>,
    change: impl Fn(&mut AppWindow, &mut gpui_kit::Context<AppWindow>) + 'static,
) -> impl Fn(&gpui_kit::ClickEvent, &mut gpui_kit::Window, &mut App) + 'static {
    let app = app.clone();
    move |_, _, cx| {
        let _ = app.update(cx, |this, cx| change(this, cx));
    }
}

/// A titled group of setting rows in one rounded container, rows split by hairlines.
fn section(title: &'static str, rows: Vec<AnyElement>) -> AnyElement {
    let count = rows.len();
    div()
        .flex()
        .flex_col()
        .gap(px(8.))
        .child(section_label(title).px(px(4.)))
        .child(
            div()
                .rounded(px(8.))
                .border_1()
                .border_color(shell_border())
                .bg(rgb(group_bg()))
                .overflow_hidden()
                .flex()
                .flex_col()
                .children(rows.into_iter().enumerate().map(|(index, row)| {
                    div()
                        .when(index + 1 < count, |row| {
                            row.border_b_1().border_color(shell_border())
                        })
                        .child(row)
                        .into_any_element()
                })),
        )
        .into_any_element()
}

fn row_text(title: &'static str, description: &'static str) -> gpui_kit::Div {
    div()
        .flex_1()
        .min_w(px(0.))
        .flex()
        .flex_col()
        .gap(px(2.))
        .child(
            div()
                .text_size(typography::label())
                .line_height(typography::font_px(18.))
                .font_weight(FontWeight::MEDIUM)
                .text_color(rgb(text_primary()))
                .child(title),
        )
        .child(
            div()
                .text_size(typography::font_px(12.))
                .line_height(typography::font_px(16.))
                .text_color(rgb(text_secondary()))
                .child(description),
        )
}

/// Label and description on the left, the control on the right.
fn setting_row(title: &'static str, description: &'static str, control: AnyElement) -> AnyElement {
    div()
        .min_h(px(56.))
        .px(px(16.))
        .py(px(10.))
        .flex()
        .items_center()
        .gap(px(16.))
        .child(row_text(title, description))
        .child(div().flex_shrink_0().child(control))
        .into_any_element()
}

/// macOS-style segmented control: a recessed track with the chosen segment raised.
/// Clicking another segment hands its value to `select`.
fn segmented<T: Clone + 'static>(
    id: &'static str,
    options: Vec<(&'static str, bool, T)>,
    app: &WeakEntity<AppWindow>,
    select: impl Fn(&mut AppWindow, T, &mut gpui_kit::Context<AppWindow>) + Clone + 'static,
) -> AnyElement {
    div()
        .h(typography::font_px(28.))
        .p(px(2.))
        .flex()
        .items_center()
        .gap(px(2.))
        .rounded(px(7.))
        .bg(segment_track())
        .children(options.into_iter().map(|(label, selected, value)| {
            let select = select.clone();
            div()
                .id(SharedString::from(format!("{id}-{label}")))
                .h_full()
                .px(px(12.))
                .flex()
                .items_center()
                .rounded(px(5.))
                .text_size(typography::label())
                .font_weight(FontWeight::MEDIUM)
                .cursor(CursorStyle::PointingHand)
                .when(selected, |segment| {
                    segment
                        .bg(raised_surface())
                        .shadow(raised_shadow())
                        .text_color(rgb(text_primary()))
                        // AppKit's dark selected segment: a lighter fill with a hairline edge.
                        .when(is_dark_theme(), |segment| {
                            segment.border_1().border_color(rgb(0x606060))
                        })
                })
                .when(!selected, |segment| {
                    segment
                        .text_color(rgb(text_secondary()))
                        .hover(|style| style.text_color(rgb(text_primary())))
                        .on_click(update_app(app, move |this, cx| {
                            select(this, value.clone(), cx);
                        }))
                })
                .child(label)
                .into_any_element()
        }))
        .into_any_element()
}

/// On/off switch: accent track with the knob right when on.
fn toggle(
    id: &'static str,
    on: bool,
    app: &WeakEntity<AppWindow>,
    flip: impl Fn(&mut AppWindow, &mut gpui_kit::Context<AppWindow>) + 'static,
) -> AnyElement {
    div()
        .id(id)
        .w(px(32.))
        .h(px(18.))
        .p(px(2.))
        .rounded_full()
        .flex()
        .items_center()
        .when(on, |track| track.justify_end().bg(rgb(accent_strong())))
        .when(!on, |track| {
            track.bg(tint(
                text_primary(),
                if is_dark_theme() { 0.24 } else { 0.18 },
            ))
        })
        .cursor(CursorStyle::PointingHand)
        .on_click(update_app(app, flip))
        .child(
            div()
                .size(px(14.))
                .rounded_full()
                .bg(rgb(0xffffff))
                .shadow(raised_shadow()),
        )
        .into_any_element()
}

/// One theme choice: a miniature window in that theme, its name below, and an
/// accent ring when chosen.
fn theme_tile(
    mode: ThemeMode,
    label: &'static str,
    selected: bool,
    app: &WeakEntity<AppWindow>,
) -> AnyElement {
    let preview = match mode {
        ThemeMode::Light => mini_window(false),
        ThemeMode::Dark => mini_window(true),
        // Half light, half dark.
        ThemeMode::System => div()
            .relative()
            .size_full()
            .child(mini_window(false))
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left(px(THUMBNAIL_WIDTH_PX / 2.))
                    .w(px(THUMBNAIL_WIDTH_PX / 2.))
                    .h_full()
                    .overflow_hidden()
                    .child(
                        div()
                            .absolute()
                            .top_0()
                            .left(px(-THUMBNAIL_WIDTH_PX / 2.))
                            .w(px(THUMBNAIL_WIDTH_PX))
                            .h_full()
                            .child(mini_window(true)),
                    ),
            )
            .into_any_element(),
    };
    div()
        .id(SharedString::from(format!("preferences-theme-{label}")))
        .flex()
        .flex_col()
        .items_center()
        .gap(px(6.))
        .cursor(CursorStyle::PointingHand)
        .on_click(update_app(app, move |this, cx| {
            this.set_theme_mode(mode.clone(), cx);
        }))
        .child(
            // Ring sits outside the thumbnail so selecting never shifts layout.
            div()
                .p(px(2.))
                .rounded(px(9.))
                .border_2()
                .border_color(if selected {
                    rgb(accent()).into()
                } else {
                    gpui_kit::transparent_black()
                })
                .child(
                    div()
                        .w(px(THUMBNAIL_WIDTH_PX))
                        .h(px(THUMBNAIL_HEIGHT_PX))
                        .rounded(px(5.))
                        .border_1()
                        .border_color(rgb(field_border()))
                        .overflow_hidden()
                        .child(preview),
                ),
        )
        .child(
            div()
                .text_size(typography::font_px(12.))
                .font_weight(if selected {
                    FontWeight::SEMIBOLD
                } else {
                    FontWeight::NORMAL
                })
                .text_color(rgb(if selected {
                    text_primary()
                } else {
                    text_secondary()
                }))
                .child(label),
        )
        .into_any_element()
}

/// A tiny sketch of the app window: sidebar with a selected row, a few message
/// lines and a composer, in fixed light or dark colors.
fn mini_window(dark: bool) -> AnyElement {
    let (window, sidebar, line, field, field_line) = if dark {
        (0x191919, 0x202020, 0x4a4a4a, 0x202020, 0x3b3b3b)
    } else {
        (0xffffff, 0xf2f4f7, 0xd9dce1, 0xf5f6f8, 0xd6d9de)
    };
    let selected = if dark { 0x2b4a85 } else { 0x3663ea };
    let bar = |width: f32, color: u32| div().h(px(3.)).w(px(width)).rounded_full().bg(rgb(color));
    div()
        .size_full()
        .flex()
        .bg(rgb(window))
        .child(
            div()
                .w(px(24.))
                .h_full()
                .bg(rgb(sidebar))
                .pt(px(10.))
                .px(px(3.))
                .flex()
                .flex_col()
                .gap(px(3.))
                .child(div().h(px(5.)).w_full().rounded(px(1.5)).bg(rgb(selected)))
                .child(bar(14., line))
                .child(bar(12., line))
                .child(bar(15., line)),
        )
        .child(
            div()
                .flex_1()
                .h_full()
                .px(px(6.))
                .pt(px(9.))
                .pb(px(5.))
                .flex()
                .flex_col()
                .gap(px(4.))
                .child(bar(30., line))
                .child(bar(44., line))
                .child(bar(22., line))
                .child(div().flex_1())
                .child(
                    div()
                        .h(px(9.))
                        .w_full()
                        .rounded(px(2.5))
                        .border_1()
                        .border_color(rgb(field_line))
                        .bg(rgb(field)),
                ),
        )
        .into_any_element()
}

/// Fill of a group of rows, raised off the Settings window background.
fn group_bg() -> u32 {
    if is_dark_theme() { 0x2a2a2a } else { 0xffffff }
}

fn raised_surface() -> gpui_kit::Hsla {
    if is_dark_theme() {
        rgb(0x5a5a5a).into()
    } else {
        rgb(0xffffff).into()
    }
}

/// The recessed track behind segments.
fn segment_track() -> gpui_kit::Hsla {
    if is_dark_theme() {
        rgb(0x333333).into()
    } else {
        tint(text_primary(), 0.07)
    }
}

/// Who is signed in and whether Keybase is reachable.
fn account_row(account: &AccountSummary) -> AnyElement {
    let (status, dot) = match account.connectivity {
        Connectivity::Online => ("Connected to Keybase", success()),
        Connectivity::Reconnecting => ("Connecting to Keybase…", warning()),
        Connectivity::Offline => ("Keybase can't be reached", text_secondary()),
    };
    div()
        .min_h(px(64.))
        .px(px(16.))
        .py(px(12.))
        .flex()
        .items_center()
        .gap(px(12.))
        .child(Avatar::render(
            &account.display_name,
            account.avatar_asset.as_deref(),
            36.,
            crate::views::avatar::default_avatar_background(&account.display_name),
            text_primary(),
        ))
        .child(
            div()
                .flex_1()
                .min_w(px(0.))
                .flex()
                .flex_col()
                .gap(px(2.))
                .child(
                    div()
                        .text_size(typography::label())
                        .line_height(typography::font_px(18.))
                        .font_weight(FontWeight::MEDIUM)
                        .text_color(rgb(text_primary()))
                        .child(account.display_name.clone()),
                )
                .child(
                    div()
                        .text_size(typography::font_px(12.))
                        .line_height(typography::font_px(16.))
                        .text_color(rgb(text_secondary()))
                        .child(format!("Signed in as {}", account.username)),
                ),
        )
        .child(
            div()
                .flex_shrink_0()
                .flex()
                .items_center()
                .gap(px(6.))
                .text_size(typography::font_px(12.))
                .text_color(rgb(text_secondary()))
                .child(
                    div()
                        .size(px(8.))
                        .rounded_full()
                        .when(account.connectivity == Connectivity::Offline, |dot_box| {
                            dot_box.border(px(1.5)).border_color(rgb(dot))
                        })
                        .when(account.connectivity != Connectivity::Offline, |dot_box| {
                            dot_box.bg(rgb(dot))
                        }),
                )
                .child(status),
        )
        .into_any_element()
}

fn raised_shadow() -> Vec<BoxShadow> {
    vec![BoxShadow {
        inset: false,
        color: tint(0x000000, if is_dark_theme() { 0.40 } else { 0.14 }),
        offset: point(px(0.), px(1.)),
        blur_radius: px(2.),
        spread_radius: px(0.),
    }]
}

fn update_settings_rows(
    snapshot: &crate::services::updates::UpdateSnapshot,
    app: &WeakEntity<AppWindow>,
) -> Vec<AnyElement> {
    use crate::services::updates::UpdateAction;
    let mut rows = vec![setting_row(
        "Software updates",
        "Check for the latest version of zBase.",
        secondary_button("settings-check-updates", "Check Now")
            .on_click(move |_, window, cx| {
                window.dispatch_action(Box::new(crate::app::commands::CheckForUpdates), cx)
            })
            .into_any_element(),
    )];
    if snapshot.state != "disabled" {
        for (id, title, description, checked, action) in [
            (
                "settings-update-checks",
                "Automatically check",
                "Look for stable updates in the background.",
                snapshot.automatic_checks,
                UpdateAction::ToggleChecks,
            ),
            (
                "settings-update-downloads",
                "Download in background",
                "Install on quit, or restart when ready.",
                snapshot.automatic_downloads,
                UpdateAction::ToggleDownloads,
            ),
        ] {
            rows.push(setting_row(
                title,
                description,
                toggle(id, checked, app, move |this, cx| {
                    this.update_command(action, cx)
                }),
            ));
        }
        let last = chrono::DateTime::from_timestamp(snapshot.last_check as i64, 0)
            .filter(|_| snapshot.last_check > 0.)
            .map(|date| {
                date.with_timezone(&chrono::Local)
                    .format("%b %-d at %-I:%M %p")
                    .to_string()
            })
            .unwrap_or_else(|| "Not checked yet".into());
        rows.push(
            div()
                .px_4()
                .py_2()
                .text_size(typography::caption())
                .text_color(rgb(text_secondary()))
                .child(format!("Last checked: {last}"))
                .into_any_element(),
        );
    } else {
        rows.push(
            div()
                .px_4()
                .py_2()
                .text_size(typography::caption())
                .text_color(rgb(text_secondary()))
                .child(snapshot.message.clone())
                .into_any_element(),
        );
    }
    rows
}
