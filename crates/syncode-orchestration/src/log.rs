//! Tracing wrappers that ensure `Arguments<'_>` is never held across `.await` points.
//!
//! The `tracing::info!` / `tracing::warn!` macros expand to code that creates a
//! local `Arguments<'_>` value. When this value is alive across an `.await` point
//! in an async function, the compiler sees the future as non-`Send`, which breaks
//! axum's `WebSocketUpgrade::on_upgrade` (requires `Future + Send + 'static`).
//!
//! These wrapper functions are **synchronous** — the tracing macro is invoked
//! entirely within the sync function body, so `Arguments<'_>` is created, consumed,
//! and dropped before the function returns. The async caller never sees it.

pub fn info(msg: &str) {
    tracing::info!("{msg}");
}

pub fn warn(msg: &str) {
    tracing::warn!("{msg}");
}

pub fn warn_err(error: &dyn std::fmt::Display, context: &str) {
    tracing::warn!(error = %error, "{context}");
}

pub fn error(msg: &str) {
    tracing::error!("{msg}");
}
