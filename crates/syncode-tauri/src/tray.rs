//! System tray integration
//!
//! System tray menu builder and event handlers for the Tauri app.
//! Provides quick actions: show/hide window, new thread, quit.

use serde::{Deserialize, Serialize};

/// Tray menu action dispatched from the frontend
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TrayAction {
    /// Show the main window
    ShowWindow,
    /// Hide the main window
    HideWindow,
    /// Toggle window visibility
    ToggleWindow,
    /// Create a new thread
    NewThread,
    /// Open settings
    OpenSettings,
    /// Quit the application
    Quit,
}

impl std::fmt::Display for TrayAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrayAction::ShowWindow => write!(f, "show_window"),
            TrayAction::HideWindow => write!(f, "hide_window"),
            TrayAction::ToggleWindow => write!(f, "toggle_window"),
            TrayAction::NewThread => write!(f, "new_thread"),
            TrayAction::OpenSettings => write!(f, "open_settings"),
            TrayAction::Quit => write!(f, "quit"),
        }
    }
}

/// Tray icon state for the frontend
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrayState {
    pub visible: bool,
    pub tooltip: String,
    pub active_sessions: u32,
}

impl Default for TrayState {
    fn default() -> Self {
        Self {
            visible: true,
            tooltip: "Syncode".to_string(),
            active_sessions: 0,
        }
    }
}

/// Build a tray menu description for the frontend
pub fn build_tray_menu(active_sessions: u32, has_update: bool) -> Vec<TrayMenuItem> {
    let mut items = vec![
        TrayMenuItem {
            id: "show".to_string(),
            label: "Show Syncode".to_string(),
            action: Some(TrayAction::ShowWindow),
            enabled: true,
            separator_after: false,
        },
        TrayMenuItem {
            id: "new_thread".to_string(),
            label: format!("New Thread{}", if active_sessions > 0 { format!(" ({} active)", active_sessions) } else { String::new() }),
            action: Some(TrayAction::NewThread),
            enabled: true,
            separator_after: true,
        },
    ];

    if has_update {
        items.push(TrayMenuItem {
            id: "update".to_string(),
            label: "Update Available".to_string(),
            action: Some(TrayAction::OpenSettings),
            enabled: true,
            separator_after: true,
        });
    }

    items.push(TrayMenuItem {
        id: "settings".to_string(),
        label: "Settings".to_string(),
        action: Some(TrayAction::OpenSettings),
        enabled: true,
        separator_after: true,
    });

    items.push(TrayMenuItem {
        id: "quit".to_string(),
        label: "Quit".to_string(),
        action: Some(TrayAction::Quit),
        enabled: true,
        separator_after: false,
    });

    items
}

/// A single tray menu item
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrayMenuItem {
    pub id: String,
    pub label: String,
    pub action: Option<TrayAction>,
    pub enabled: bool,
    pub separator_after: bool,
}

/// Handle a tray action — returns the command the frontend should execute
pub fn handle_tray_action(action: &TrayAction) -> TrayActionResponse {
    match action {
        TrayAction::ShowWindow => TrayActionResponse {
            action: action.clone(),
            window_command: Some("show".to_string()),
        },
        TrayAction::HideWindow => TrayActionResponse {
            action: action.clone(),
            window_command: Some("hide".to_string()),
        },
        TrayAction::ToggleWindow => TrayActionResponse {
            action: action.clone(),
            window_command: Some("toggle".to_string()),
        },
        TrayAction::NewThread => TrayActionResponse {
            action: action.clone(),
            window_command: Some("show".to_string()),
        },
        TrayAction::OpenSettings => TrayActionResponse {
            action: action.clone(),
            window_command: Some("show".to_string()),
        },
        TrayAction::Quit => TrayActionResponse {
            action: action.clone(),
            window_command: None,
        },
    }
}

/// Response from handling a tray action
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrayActionResponse {
    pub action: TrayAction,
    pub window_command: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tray_action_serialization() {
        let actions = vec![
            TrayAction::ShowWindow,
            TrayAction::HideWindow,
            TrayAction::ToggleWindow,
            TrayAction::NewThread,
            TrayAction::OpenSettings,
            TrayAction::Quit,
        ];
        for action in actions {
            let json = serde_json::to_string(&action).unwrap();
            let back: TrayAction = serde_json::from_str(&json).unwrap();
            assert_eq!(action, back);
        }
    }

    #[test]
    fn tray_action_display() {
        assert_eq!(TrayAction::ShowWindow.to_string(), "show_window");
        assert_eq!(TrayAction::Quit.to_string(), "quit");
    }

    #[test]
    fn build_tray_menu_basic() {
        let items = build_tray_menu(0, false);
        assert_eq!(items.len(), 4); // show, new_thread, settings, quit
        assert_eq!(items[0].label, "Show Syncode");
        assert_eq!(items[3].label, "Quit");
    }

    #[test]
    fn build_tray_menu_with_sessions() {
        let items = build_tray_menu(3, false);
        let new_thread = &items[1];
        assert!(new_thread.label.contains("3 active"));
    }

    #[test]
    fn build_tray_menu_with_update() {
        let items = build_tray_menu(0, true);
        assert_eq!(items.len(), 5);
        let update = items.iter().find(|i| i.id == "update");
        assert!(update.is_some());
    }

    #[test]
    fn handle_tray_action_show() {
        let resp = handle_tray_action(&TrayAction::ShowWindow);
        assert_eq!(resp.window_command.as_deref(), Some("show"));
    }

    #[test]
    fn handle_tray_action_quit() {
        let resp = handle_tray_action(&TrayAction::Quit);
        assert!(resp.window_command.is_none());
    }

    #[test]
    fn tray_state_default() {
        let state = TrayState::default();
        assert!(state.visible);
        assert_eq!(state.active_sessions, 0);
    }

    #[test]
    fn tray_menu_item_serialization() {
        let item = TrayMenuItem {
            id: "test".to_string(),
            label: "Test Item".to_string(),
            action: Some(TrayAction::NewThread),
            enabled: true,
            separator_after: false,
        };
        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("test"));
        assert!(json.contains("separatorAfter"));
    }
}
