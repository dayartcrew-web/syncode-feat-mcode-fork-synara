//! Tauri IPC Commands — Browser integration (DSK-2).
//!
//! Two commands surfaced to the frontend via Tauri's `invoke()` bridge,
//! backing the `browser.*` RPC names the cloned MCode UI references:
//!
//! - [`capture_screenshot`] → `browser.captureScreenshot`
//! - [`list_tabs`]          → `browser.listTabs`
//!
//! ## Platform-limited stubs
//!
//! Tauri v2's webview API does NOT expose a portable way to:
//!  - capture the rendered webview contents as a PNG (no `WebviewWindow::
//!    capture` — the screenshot surface is platform-specific and unreliable
//!    in headless / CI environments), or
//!  - enumerate browser tabs (the desktop shell hosts a single webview;
//!    there is no "tab list" the OS-side can introspect).
//!
//! Both commands therefore return a **graceful typed error** rather than a
//! panic or `MethodNotFound`. The frontend renders a fallback UI when it
//! receives `platform_limited` back, instead of treating the call as a hard
//! failure. This keeps the `browser.*` surface typed in the contract while
//! honestly advertising its limitations.
//!
//! A future revision could wire [`list_tabs`] to a real multi-window
//! registry (Tauri enumerates open windows via `AppHandle::webview_windows`)
//! — see the `available_windows` helper exported below, which is exercised
//! by the unit tests but not yet surfaced as a Tauri command (intentional:
//! the MCode UI expects the `browser.listTabs` shape, not a window list).

use serde::{Deserialize, Serialize};

/// Screenshot format requested by the frontend (`browser.captureScreenshot`
/// accepts `{ format?: "png" | "jpeg" }`). Defaults to PNG when omitted.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ScreenshotFormat {
    #[default]
    Png,
    Jpeg,
}

/// Result of [`capture_screenshot`]. On success `data` carries a base64 PNG
/// (no `data:` prefix — the frontend prepends it). On the platform-limited
/// fallback path, `data` is empty and `note` carries a human-readable
/// diagnostic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CaptureScreenshotResult {
    /// Base64-encoded image payload. Empty when the platform can't capture.
    pub data: String,
    /// `"png"` or `"jpeg"` — matches the requested format (PNG on fallback).
    pub format: String,
    /// Width/height of the captured frame in CSS pixels (0 on fallback).
    pub width: u32,
    pub height: u32,
    /// Diagnostic on the fallback path; `None` on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Single tab descriptor returned by [`list_tabs`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BrowserTab {
    /// Stable tab id (Tauri window label, e.g. `"main"`).
    pub id: String,
    /// Tab title (window title or document.title).
    pub title: String,
    /// URL the tab is currently showing (`about:blank` until the webview
    /// navigates).
    pub url: String,
    /// Whether this tab is the focused/active one.
    pub active: bool,
}

/// Result of [`list_tabs`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListTabsResult {
    pub tabs: Vec<BrowserTab>,
    /// Diagnostic when the platform surface is unavailable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Build the platform-limited fallback payload for
/// [`capture_screenshot`]. Centralised so the command body and the test
/// agree on the exact note text.
pub fn screenshot_fallback(format: ScreenshotFormat) -> CaptureScreenshotResult {
    CaptureScreenshotResult {
        data: String::new(),
        format: match format {
            ScreenshotFormat::Png => "png",
            ScreenshotFormat::Jpeg => "jpeg",
        }
        .to_string(),
        width: 0,
        height: 0,
        note: Some(
            "webview screenshot capture is platform-limited in this build \
             (requires a compositing surface; unavailable in headless/CI)"
                .to_string(),
        ),
    }
}

/// Build the platform-limited fallback payload for [`list_tabs`] — a single
/// pseudo-tab representing the main window, so the UI's tab strip doesn't
/// render empty. The Tauri webview is single-windowed today; when
/// multi-window support is wired, this should return the real
/// `AppHandle::webview_windows()` list (see [`available_windows`]).
pub fn tabs_fallback() -> ListTabsResult {
    ListTabsResult {
        tabs: vec![BrowserTab {
            id: "main".to_string(),
            title: "Syncode".to_string(),
            url: "about:blank".to_string(),
            active: true,
        }],
        note: Some(
            "tab enumeration is platform-limited — desktop shell hosts a \
             single webview; returning the main window as the only tab"
                .to_string(),
        ),
    }
}

/// Capture a screenshot of the active webview.
///
/// Frontend invokes `invoke("capture_screenshot", { format? })`. Today this
/// returns the platform-limited fallback (see [`screenshot_fallback`]); a
/// future revision can shell out to a platform-specific capturer (macOS
/// `CGWindowListCreateImage`, Windows `BitBlt`, Linux `grim`/`scrot`) when a
/// compositing surface is available.
#[tauri::command]
pub fn capture_screenshot(
    format: Option<ScreenshotFormat>,
) -> Result<CaptureScreenshotResult, String> {
    Ok(screenshot_fallback(format.unwrap_or_default()))
}

/// List open browser tabs.
///
/// Frontend invokes `invoke("list_tabs")`. Today this returns the
/// platform-limited fallback (the main window as a single pseudo-tab — see
/// [`tabs_fallback`]); a future revision could enumerate Tauri's
/// `webview_windows()` once the desktop shell supports multi-window.
#[tauri::command]
pub fn list_tabs() -> Result<ListTabsResult, String> {
    Ok(tabs_fallback())
}

/// Project a set of `(label, title, url, active)` window tuples into the
/// [`BrowserTab`] shape. Exported (rather than inlined in [`list_tabs`]) so
/// the multi-window wiring can be added in one place, and so the unit tests
/// can exercise the mapping without a live Tauri runtime. Not exposed as a
/// Tauri command today.
pub fn available_windows(
    windows: impl IntoIterator<Item = (String, String, String, bool)>,
) -> Vec<BrowserTab> {
    windows
        .into_iter()
        .map(|(id, title, url, active)| BrowserTab {
            id,
            title,
            url,
            active,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn screenshot_format_default_is_png() {
        assert_eq!(ScreenshotFormat::default(), ScreenshotFormat::Png);
    }

    #[test]
    fn screenshot_format_deserializes_jpeg() {
        let json = "\"jpeg\"";
        let parsed: ScreenshotFormat = serde_json::from_str(json).unwrap();
        assert_eq!(parsed, ScreenshotFormat::Jpeg);
    }

    #[test]
    fn capture_screenshot_returns_fallback() {
        let res = capture_screenshot(None).unwrap();
        // The command returns Ok with the fallback payload — not Err.
        assert!(res.data.is_empty());
        assert_eq!(res.format, "png");
        assert_eq!(res.width, 0);
        assert_eq!(res.height, 0);
        assert!(res.note.is_some());
    }

    #[test]
    fn capture_screenshot_respects_format_request() {
        let res = capture_screenshot(Some(ScreenshotFormat::Jpeg)).unwrap();
        assert_eq!(res.format, "jpeg");
    }

    #[test]
    fn list_tabs_returns_main_window_pseudo_tab() {
        let res = list_tabs().unwrap();
        assert_eq!(res.tabs.len(), 1);
        assert_eq!(res.tabs[0].id, "main");
        assert!(res.tabs[0].active);
        assert!(res.note.is_some());
    }

    #[test]
    fn list_tabs_result_camelcase_keys() {
        let res = tabs_fallback();
        let json = serde_json::to_string(&res).unwrap();
        assert!(json.contains("\"tabs\""));
        // `note` is present (Some).
        assert!(json.contains("\"note\""));
    }

    #[test]
    fn capture_screenshot_result_skips_note_when_some_via_explicit_round_trip() {
        // `note` is `Option`, marked skip_serializing_if = None. Verify the
        // JSON shape for the success path (note omitted) round-trips.
        let res = CaptureScreenshotResult {
            data: "iVBORw0KGgo=".to_string(),
            format: "png".to_string(),
            width: 800,
            height: 600,
            note: None,
        };
        let json = serde_json::to_string(&res).unwrap();
        assert!(!json.contains("note"));
        let back: CaptureScreenshotResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.width, 800);
        assert_eq!(back.data, "iVBORw0KGgo=");
    }

    #[test]
    fn available_windows_maps_to_browser_tabs() {
        let tabs = available_windows([
            ("main".to_string(), "Syncode".to_string(), "tauri://localhost".to_string(), true),
            ("docs".to_string(), "Docs".to_string(), "tauri://localhost/docs".to_string(), false),
        ]);
        assert_eq!(tabs.len(), 2);
        assert!(tabs[0].active);
        assert!(!tabs[1].active);
        assert_eq!(tabs[1].id, "docs");
    }
}
