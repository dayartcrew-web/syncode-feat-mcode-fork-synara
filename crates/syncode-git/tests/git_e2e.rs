//! End-to-end test — real `git` binary + `libgit2` on a temporary repository.
//!
//! Exercises the full `Git2Service` surface against an actual `.git` directory
//! created via `git init` in a temp dir. Complements the inline unit and
//! integration tests in `service.rs`.
//!
//! Gating: `SYNICODE_GIT_E2E=1` + `git` on PATH.

use std::process::Command;
use syncode_git::service::GitService;
use tempfile::TempDir;

fn e2e_enabled() -> bool {
    std::env::var("SYNICODE_GIT_E2E").ok().as_deref() == Some("1")
        && Command::new("git")
            .arg("--version")
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
}

fn init_repo() -> (TempDir, Box<dyn GitService>) {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path();

    Command::new("git").arg("init").current_dir(path).status().expect("git init");
    Command::new("git").args(["config", "user.email", "e2e@syncode.test"])
        .current_dir(path).status().expect("git config email");
    Command::new("git").args(["config", "user.name", "Syncode E2E"])
        .current_dir(path).status().expect("git config name");

    std::fs::write(path.join("hello.txt"), "hello world\n").expect("write");
    Command::new("git").args(["add", "."]).current_dir(path).status().expect("git add");
    Command::new("git").args(["commit", "-m", "initial"])
        .current_dir(path).status().expect("git commit");

    let svc: Box<dyn GitService> = Box::new(
        syncode_git::service::Git2Service::open(path).expect("open repo"),
    );
    (dir, svc)
}

#[test]
fn git_real_repo_status_after_init() {
    if !e2e_enabled() {
        eprintln!("[skip] git e2e: set SYNICODE_GIT_E2E=1 and ensure git is on PATH");
        return;
    }
    let (_dir, svc) = init_repo();
    let status = svc.status().expect("status");
    assert!(status.branch.is_some());
    assert_eq!(status.files.len(), 0);
}

#[test]
fn git_real_repo_current_branch() {
    if !e2e_enabled() { eprintln!("[skip] git e2e"); return; }
    let (_dir, svc) = init_repo();
    let branch = svc.current_branch().expect("current_branch");
    assert!(branch.is_some());
}

#[test]
fn git_real_repo_log_has_initial_commit() {
    if !e2e_enabled() { eprintln!("[skip] git e2e"); return; }
    let (_dir, svc) = init_repo();
    let log = svc.log(10).expect("log");
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].commit.message.trim(), "initial");
}

#[test]
fn git_real_repo_add_commit_diff() {
    if !e2e_enabled() { eprintln!("[skip] git e2e"); return; }
    let (dir, svc) = init_repo();
    let path = dir.path();

    std::fs::write(path.join("hello.txt"), "modified\n").expect("write");
    std::fs::write(path.join("new.txt"), "new file\n").expect("write");
    svc.add(&["."]).expect("add");
    svc.commit("second commit").expect("commit");

    let log = svc.log(10).expect("log");
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].commit.message.trim(), "second commit");

    let diff = svc.diff(None, None).expect("diff");
    assert_eq!(diff.len(), 0, "after commit, working tree should be clean");
}

#[test]
fn git_real_repo_branch_create_checkout_delete() {
    if !e2e_enabled() { eprintln!("[skip] git e2e"); return; }
    let (_dir, svc) = init_repo();

    svc.create_branch("feature/test-e2e", false).expect("create_branch");
    let branches = svc.branches().expect("branches");
    let names: Vec<&str> = branches.iter().map(|b| b.name.as_str()).collect();
    assert!(names.contains(&"feature/test-e2e"));

    svc.checkout("feature/test-e2e").expect("checkout");
    let current = svc.current_branch().expect("current_branch after checkout");
    assert_eq!(current.unwrap(), "feature/test-e2e");

    svc.checkout("main").expect("checkout main");
    svc.delete_branch("feature/test-e2e").expect("delete_branch");
    let branches = svc.branches().expect("branches after delete");
    let names: Vec<&str> = branches.iter().map(|b| b.name.as_str()).collect();
    assert!(!names.contains(&"feature/test-e2e"));
}

#[test]
fn git_real_repo_push_pull_with_local_remote() {
    if !e2e_enabled() { eprintln!("[skip] git e2e"); return; }
    let remote_dir = TempDir::new().expect("remote temp dir");

    Command::new("git").args(["init", "--bare"])
        .current_dir(remote_dir.path()).status().expect("git init --bare");

    let (dir, svc) = init_repo();
    let remote_url = format!("file://{}", remote_dir.path().display());
    Command::new("git").args(["remote", "add", "origin", &remote_url])
        .current_dir(dir.path()).status().expect("git remote add");

    let push_result = svc.push("origin", "main").expect("push");
    match push_result {
        syncode_git::service::PushResult::Pushed { .. } |
        syncode_git::service::PushResult::SkippedUpToDate { .. } => {}
    }

    std::fs::write(dir.path().join("push.txt"), "pushed\n").expect("write");
    svc.add(&["."]).expect("add");
    svc.commit("push test").expect("commit");

    let push_result2 = svc.push("origin", "main").expect("push after commit");
    assert!(
        matches!(push_result2, syncode_git::service::PushResult::Pushed { .. }),
        "second push should push new commit"
    );
}
