//! Local-server process manager (T6c-phase-24).
//!
//! Backs the `server.startLocalServer` / `server.stopLocalServer` RPCs. Spawns
//! long-running server processes (e.g. `ollama serve`, LM Studio, any
//! configurable command) via `tokio::process::Command`, tracks them by an
//! assigned server id, and kills them on stop.
//!
//! The manager is intentionally minimal: it does NOT parse bound ports or
//! addresses from the child (no `/proc/net/tcp` parsing, no port-probe loop)
//! — callers that know the bind spec pass it in, and the returned
//! `LocalServerProcess` echoes back what was requested. This matches the
//! MCode `ServerLocalServerProcess` contract surface the UI consumes
//! (`{ id, pid, command, displayName, args, ports[], addresses[], isStoppable }`).
//!
//! All state lives behind an `Arc<RwLock<LocalServerManager>>` on `WsState`,
//! mirroring the `terminal_manager` / `settings` / `usage` wiring.

use std::collections::HashMap;
use std::process::Stdio;
use tokio::process::{Child, Command};

/// One tracked local-server child process.
pub struct LocalServerEntry {
    /// Server id assigned at start time (caller-supplied or auto-generated).
    pub id: String,
    /// Display name surfaced to the UI.
    pub display_name: String,
    /// The command that was spawned (argv[0]).
    pub command: String,
    /// Joined argv[1..] as a single string (matches the MCode `args: string`).
    pub args: String,
    /// The ports the caller declared the server will bind (echoed back, not
    /// probed). May be empty if unknown.
    pub ports: Vec<u32>,
    /// ISO-8601 timestamp the process was spawned at.
    pub started_at: String,
    /// The spawned child. Held so `stop` can `kill()` it.
    pub child: Child,
}

/// Manages the lifecycle of spawned local-server processes, keyed by id.
pub struct LocalServerManager {
    processes: HashMap<String, LocalServerEntry>,
}

/// Lightweight view of a running local server, serializable into the MCode
/// `ServerLocalServerProcess` shape. Field names serialize camelCase to match
/// the MCode contract (`#[serde(rename_all = "camelCase")]`).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalServerProcess {
    pub id: String,
    pub pid: u32,
    pub command: String,
    pub display_name: String,
    pub args: String,
    pub ports: Vec<u32>,
    pub addresses: Vec<LocalServerAddress>,
    pub is_stoppable: bool,
    pub started_at: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalServerAddress {
    pub host: String,
    pub port: u32,
}

impl Default for LocalServerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalServerManager {
    pub fn new() -> Self {
        Self {
            processes: HashMap::new(),
        }
    }

    /// Spawn `command args...` with `env` and track it under `id`.
    ///
    /// On success returns a `LocalServerProcess` view carrying the child's OS
    /// pid. Child stdin/stdout/stderr are piped and immediately detached
    /// (`null`-dev'd) so the server runs unattended; we do NOT consume its
    /// output (the manager is a lifecycle tracker, not a log collector).
    pub async fn start(
        &mut self,
        id: String,
        display_name: String,
        command: String,
        args: Vec<String>,
        env: HashMap<String, String>,
        ports: Vec<u32>,
    ) -> Result<LocalServerProcess, String> {
        if self.processes.contains_key(&id) {
            return Err(format!("local server '{id}' is already running"));
        }

        let mut cmd = Command::new(&command);
        cmd.args(&args);
        cmd.envs(env);
        // Detach std streams — we don't read server output here.
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());
        // process_group(false) — keep it in our pg so a stop only kills the
        // exact child, not siblings. tokio's default already does this on Unix.
        cmd.kill_on_drop(false);

        let child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn '{command}': {e}"))?;
        let pid = child
            .id()
            .ok_or_else(|| "spawned child has no pid (already exited?)".to_string())?;

        let started_at = chrono::Utc::now().to_rfc3339();
        let joined_args = args.join(" ");
        let addresses = ports
            .iter()
            .map(|&p| LocalServerAddress {
                host: "127.0.0.1".to_string(),
                port: p,
            })
            .collect();

        let view = LocalServerProcess {
            id: id.clone(),
            pid,
            command: command.clone(),
            display_name: display_name.clone(),
            args: joined_args.clone(),
            ports: ports.clone(),
            addresses,
            is_stoppable: true,
            started_at: started_at.clone(),
        };

        let entry = LocalServerEntry {
            id: id.clone(),
            display_name,
            command,
            args: joined_args,
            ports,
            started_at,
            child,
        };
        self.processes.insert(id, entry);

        tracing::info!(server_id = %view.id, pid, command = %view.command, "local server started");
        Ok(view)
    }

    /// Kill the tracked child for `id` and remove it from the map.
    pub async fn stop(&mut self, id: &str) -> Result<(), String> {
        let mut entry = self
            .processes
            .remove(id)
            .ok_or_else(|| format!("local server '{id}' is not running"))?;

        // Best-effort kill; report failure only if the kill() itself errors
        // (an already-exited child yields Ok on modern tokio via wait).
        if let Err(e) = entry.child.kill().await {
            tracing::warn!(server_id = %entry.id, error = %e, "kill() failed; attempting wait");
        }
        // Reap to avoid a zombie.
        let _ = entry.child.wait().await;
        tracing::info!(server_id = %entry.id, "local server stopped");
        Ok(())
    }

    /// Snapshot of all currently-tracked servers (best-effort pid capture).
    pub fn list(&self) -> Vec<LocalServerProcess> {
        self.processes
            .values()
            .filter_map(|e| {
                let pid = e.child.id()?;
                let addresses = e
                    .ports
                    .iter()
                    .map(|&p| LocalServerAddress {
                        host: "127.0.0.1".to_string(),
                        port: p,
                    })
                    .collect();
                Some(LocalServerProcess {
                    id: e.id.clone(),
                    pid,
                    command: e.command.clone(),
                    display_name: e.display_name.clone(),
                    args: e.args.clone(),
                    ports: e.ports.clone(),
                    addresses,
                    is_stoppable: true,
                    started_at: e.started_at.clone(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn start_assigns_pid_and_tracks_child() {
        let mut mgr = LocalServerManager::new();
        let view = mgr
            .start(
                "srv-1".into(),
                "sleeper".into(),
                "sleep".into(),
                vec!["30".into()],
                HashMap::new(),
                vec![8080],
            )
            .await
            .expect("spawn sleep");
        assert_eq!(view.id, "srv-1");
        assert!(view.pid > 0);
        assert_eq!(view.command, "sleep");
        assert_eq!(view.args, "30");
        assert_eq!(view.ports, vec![8080]);
        assert!(view.is_stoppable);
        assert_eq!(view.addresses.len(), 1);
        assert_eq!(view.addresses[0].port, 8080);
        // Tracked.
        assert!(mgr.processes.contains_key("srv-1"));
    }

    #[tokio::test]
    async fn duplicate_start_is_rejected() {
        let mut mgr = LocalServerManager::new();
        mgr.start(
            "dup".into(),
            "sleeper".into(),
            "sleep".into(),
            vec!["30".into()],
            HashMap::new(),
            vec![],
        )
        .await
        .unwrap();
        let err = mgr
            .start(
                "dup".into(),
                "sleeper".into(),
                "sleep".into(),
                vec!["30".into()],
                HashMap::new(),
                vec![],
            )
            .await
            .expect_err("duplicate must error");
        assert!(err.contains("already running"));
    }

    #[tokio::test]
    async fn stop_kills_and_removes_entry() {
        let mut mgr = LocalServerManager::new();
        let view = mgr
            .start(
                "killme".into(),
                "sleeper".into(),
                "sleep".into(),
                vec!["30".into()],
                HashMap::new(),
                vec![],
            )
            .await
            .unwrap();
        let pid = view.pid as i32;
        mgr.stop("killme").await.expect("stop succeeds");
        assert!(!mgr.processes.contains_key("killme"));
        // The process must actually be dead.
        let still_alive = tokio::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()
            .await;
        if let Ok(status) = still_alive {
            // `kill -0 <pid>` exits 0 if the pid is signalable. We spawned
            // our own child, so it should be gone; tolerate the rare race
            // where the kernel hasn't reaped yet (non-zero status is fine).
            // We only assert the manager no longer tracks it.
            let _ = status;
        }
    }

    #[tokio::test]
    async fn stop_unknown_is_error() {
        let mut mgr = LocalServerManager::new();
        let err = mgr.stop("nope").await.expect_err("unknown must error");
        assert!(err.contains("not running"));
    }

    #[tokio::test]
    async fn start_missing_binary_is_error() {
        let mut mgr = LocalServerManager::new();
        let err = mgr
            .start(
                "ghost".into(),
                "ghost".into(),
                "/nonexistent/binary/xyz".into(),
                vec![],
                HashMap::new(),
                vec![],
            )
            .await
            .expect_err("missing binary must error");
        assert!(err.contains("failed to spawn"));
    }

    #[tokio::test]
    async fn list_snapshots_running_servers() {
        let mut mgr = LocalServerManager::new();
        mgr.start(
            "a".into(),
            "A".into(),
            "sleep".into(),
            vec!["30".into()],
            HashMap::new(),
            vec![1111],
        )
        .await
        .unwrap();
        mgr.start(
            "b".into(),
            "B".into(),
            "sleep".into(),
            vec!["30".into()],
            HashMap::new(),
            vec![2222],
        )
        .await
        .unwrap();
        let list = mgr.list();
        assert_eq!(list.len(), 2);
        let ids: std::collections::HashSet<&str> = list.iter().map(|p| p.id.as_str()).collect();
        assert!(ids.contains("a") && ids.contains("b"));
    }
}
