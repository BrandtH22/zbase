use crate::app::assets::KbuiAssets;
use crate::app::commands;
use crate::app::window::{open_clicked_notification, open_main_window};
use gpui_kit::{Menu, MenuItem, SystemMenuType};

pub fn run() {
    let application = gpui_kit::application().with_assets(KbuiAssets {});

    // On macOS, clicking the Dock icon to re-open an already-running app should restore a window.
    // If we have no windows, create one.
    application.on_reopen(|cx| {
        if cfg!(target_os = "macos") && cx.windows().is_empty() {
            open_main_window(cx);
        }
    });

    application.run(|cx| {
        gpui_kit::init(cx);
        // On platforms without a Dock/Menu affordance, quit when the last window closes.
        if !cfg!(target_os = "macos") {
            cx.on_window_closed(|cx, _| {
                if cx.windows().is_empty() {
                    cx.quit();
                }
            })
            .detach();
        }

        super::key_bindings::install(cx);
        cx.set_menus(app_menus());
        crate::services::notifications_service::register_app();
        open_main_window(cx);
        if let Some(mut clicks) = crate::services::notifications_service::start() {
            cx.spawn(async move |cx| {
                while let Some(click) = clicks.recv().await {
                    cx.update(|cx| open_clicked_notification(click, cx));
                }
            })
            .detach();
        }
    });
}

/// The macOS menu bar: the app menu carries Settings… (⌘,) and Quit, so both are
/// discoverable outside the keyboard. Shortcuts shown come from the key bindings.
fn app_menus() -> Vec<Menu> {
    vec![
        Menu {
            disabled: false,
            name: "zBase".into(),
            items: vec![
                MenuItem::action("Check for Updates…", commands::CheckForUpdates),
                MenuItem::action("Settings…", commands::OpenPreferences),
                MenuItem::separator(),
                MenuItem::os_submenu("Services", SystemMenuType::Services),
                MenuItem::separator(),
                MenuItem::action("Quit zBase", commands::QuitApp),
            ],
        },
        // The sidebar order; the main window keeps the check mark on the current one.
        Menu {
            disabled: false,
            name: VIEW_MENU.into(),
            items: vec![
                MenuItem::action(SORT_BY_RECENT_ITEM, commands::SortSidebarByRecent),
                MenuItem::action(GROUP_BY_TEAM_ITEM, commands::GroupSidebarByTeam),
            ],
        },
    ]
}

pub const VIEW_MENU: &str = "View";
pub const SORT_BY_RECENT_ITEM: &str = "Sort by Recent";
pub const GROUP_BY_TEAM_ITEM: &str = "Group by Team";
