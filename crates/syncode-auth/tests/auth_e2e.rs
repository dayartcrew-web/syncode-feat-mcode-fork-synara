//! End-to-end test — auth lifecycle with real clocks.
//!
//! Gating: `SYNICODE_AUTH_E2E=1`.

use chrono::{Duration, Utc};
use std::sync::Arc;
use syncode_auth::authenticator::{Authenticator, SharedSecretAuthenticator};
use syncode_auth::secret_store::{InMemorySecretStore, SecretStore};
use syncode_auth::session::SessionRegistry;

fn e2e_enabled() -> bool {
    std::env::var("SYNICODE_AUTH_E2E").ok().as_deref() == Some("1")
}

fn build_auth(ttl: Duration) -> (SharedSecretAuthenticator, Arc<SessionRegistry>) {
    let mut store = InMemorySecretStore::new();
    store.store(SharedSecretAuthenticator::TOKEN_KEY, "my-secret-key");
    let store: Arc<std::sync::Mutex<dyn SecretStore>> = Arc::new(std::sync::Mutex::new(store));
    let sessions = Arc::new(SessionRegistry::new());
    let auth = SharedSecretAuthenticator::new(store, Arc::clone(&sessions)).with_ttl(ttl);
    (auth, sessions)
}

#[tokio::test]
async fn auth_real_clock_session_expiry() {
    if !e2e_enabled() {
        eprintln!("[skip] auth e2e: set SYNICODE_AUTH_E2E=1");
        return;
    }
    let (auth, _sessions) = build_auth(Duration::milliseconds(200));

    let session = auth
        .authenticate("my-secret-key", Utc::now())
        .await
        .expect("authenticate");
    assert!(
        auth.validate_session(&session.token, Utc::now())
            .await
            .is_ok()
    );

    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    assert!(
        auth.validate_session(&session.token, Utc::now())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn auth_real_clock_wrong_secret_fails() {
    if !e2e_enabled() {
        eprintln!("[skip] auth e2e");
        return;
    }
    let (auth, _) = build_auth(Duration::hours(24));
    assert!(auth.authenticate("wrong-key", Utc::now()).await.is_err());
}

#[tokio::test]
async fn auth_real_clock_session_revocation() {
    if !e2e_enabled() {
        eprintln!("[skip] auth e2e");
        return;
    }
    let (auth, sessions) = build_auth(Duration::hours(24));

    let s1 = auth
        .authenticate("my-secret-key", Utc::now())
        .await
        .unwrap();
    let s2 = auth
        .authenticate("my-secret-key", Utc::now())
        .await
        .unwrap();

    assert!(sessions.revoke(&s1.token));
    assert!(auth.validate_session(&s1.token, Utc::now()).await.is_err());
    assert!(auth.validate_session(&s2.token, Utc::now()).await.is_ok());
}

#[tokio::test]
async fn auth_real_clock_purge_expired() {
    if !e2e_enabled() {
        eprintln!("[skip] auth e2e");
        return;
    }
    let (auth, sessions) = build_auth(Duration::milliseconds(100));

    let session = auth
        .authenticate("my-secret-key", Utc::now())
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let purged = sessions.purge_expired(Utc::now());
    assert!(purged >= 1);
    assert!(
        auth.validate_session(&session.token, Utc::now())
            .await
            .is_err()
    );
}
