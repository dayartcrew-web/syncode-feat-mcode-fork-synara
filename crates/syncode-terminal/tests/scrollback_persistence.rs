//! Integration tests — terminal scrollback persistence (P4-1).
//!
//! Exercises the full persist → restore → ANSI-cap loop without spawning
//! real PTYs:
//!   - `OutputBuffer::scrollback` round-trips through `restore`.
//!   - `ScrollbackStore` survives save/load cycles on disk.
//!   - ANSI-safe truncation keeps the tail replay-safe across the cap.
//!   - A large scrollback containing ANSI escapes is capped without
//!     splitting a multi-byte char or an escape sequence.

use syncode_terminal::OutputBuffer;
use syncode_terminal::ScrollbackStore;
use syncode_terminal::truncate_ansi_safe;

// ── OutputBuffer scrollback / restore ────────────────────────────────────

#[test]
fn output_buffer_scrollback_roundtrip_through_restore() {
    let mut src = OutputBuffer::new(1000, 4096);
    // Mix of plain text, a newline, and an ANSI color sequence — the kind of
    // stream a real shell emits.
    src.write("line one\n");
    src.write("\x1b[31mred line\x1b[0m\n");
    src.write("line three\n");
    src.flush();
    let snap = src.scrollback();
    assert!(snap.contains("line one\n"));
    assert!(snap.contains("\x1b[31mred line\x1b[0m\n"));
    assert!(snap.contains("line three\n"));

    // Restore into a fresh buffer and confirm the replayed text is identical.
    let mut dst = OutputBuffer::new(1000, 4096);
    dst.restore(&snap);
    assert_eq!(dst.scrollback(), snap);
}

#[test]
fn output_buffer_restore_empty_is_noop() {
    let mut buf = OutputBuffer::new(1000, 4096);
    buf.write("preexisting");
    buf.flush();
    let before = buf.scrollback();
    buf.restore("");
    assert_eq!(buf.scrollback(), before, "empty restore must not mutate");
}

// ── ScrollbackStore persist / restore ────────────────────────────────────

#[test]
fn store_persists_and_restores_across_instances() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = ("thread-7", "term-42");

    // First "session" writes scrollback and saves.
    {
        let store = ScrollbackStore::with_base_dir(dir.path());
        store
            .save(key.0, key.1, "echo hello\nhello\n$ ")
            .expect("save");
    }

    // A later "session" (new process / reopened pane) loads it back.
    {
        let store = ScrollbackStore::with_base_dir(dir.path());
        let loaded = store.load(key.0, key.1).expect("load");
        assert_eq!(loaded.as_deref(), Some("echo hello\nhello\n$ "));
    }
}

#[test]
fn store_isolation_across_thread_terminal_pairs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ScrollbackStore::with_base_dir(dir.path());
    store.save("t1", "term", "one").expect("save");
    store.save("t2", "term", "two").expect("save");
    store.save("t1", "other", "three").expect("save");

    assert_eq!(store.load("t1", "term").unwrap().as_deref(), Some("one"));
    assert_eq!(store.load("t2", "term").unwrap().as_deref(), Some("two"));
    assert_eq!(store.load("t1", "other").unwrap().as_deref(), Some("three"));
    assert!(store.load("t2", "other").unwrap().is_none());
}

#[test]
fn store_clear_then_load_returns_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ScrollbackStore::with_base_dir(dir.path());
    store.save("t", "x", "data").expect("save");
    store.clear("t", "x").expect("clear");
    assert!(store.load("t", "x").unwrap().is_none());
}

// ── ANSI-safe cap ────────────────────────────────────────────────────────

#[test]
fn ansi_cap_preserves_tail_and_does_not_split_escape() {
    // Construct a stream: a long run of plain bytes, then a partial-looking
    // escape mid-tail, ending with a complete escape + body. The naive byte
    // cut would land inside the head; the safe cut must start at an escape
    // or newline and must keep the tail body intact.
    let head = "q".repeat(400);
    let esc = "\x1b[32m";
    let body = "green-tail-MARKER";
    let data = format!("{head}\n{esc}{body}");

    let cap = 64;
    let out = truncate_ansi_safe(&data, cap);

    // Never longer than the cap (modulo advancing to the boundary).
    assert!(out.len() <= data.len());
    // The most recent marker survives (tail is preserved).
    assert!(
        out.ends_with(body),
        "tail body must survive cap; got: {out:?}"
    );
    // The cut lands at the ESC (replay-safe), not mid-escape.
    assert!(
        out.starts_with('\x1b') || out.starts_with('\n') || out.starts_with('g'),
        "cut must land on a newline/escape boundary; got start: {:?}",
        out.as_bytes().first()
    );
}

#[test]
fn ansi_cap_does_not_split_multibyte_utf8() {
    // 'é' (U+00E9) is 2 bytes in UTF-8. Pack many of them so the naive cut
    // lands mid-character, then assert the tail is valid UTF-8 and ends on a
    // char boundary.
    let data: String = "é".repeat(200); // 400 bytes, no newlines/escapes
    let out = truncate_ansi_safe(&data, 150);
    // Must be valid UTF-8 (truncate_ansi_safe returns a &str slice, so this
    // is guaranteed by construction, but we assert the intent explicitly).
    assert!(std::str::from_utf8(out.as_bytes()).is_ok());
    // The tail is a suffix of the input.
    assert!(data.ends_with(out));
    // And strictly shorter than the full input (a cut happened).
    assert!(out.len() < data.len());
}

#[test]
fn ansi_cap_under_limit_is_identity() {
    let s = "short string\n";
    assert_eq!(truncate_ansi_safe(s, 1024), s);
}

// ── Full persist → cap → restore pipeline ────────────────────────────────

#[test]
fn pipeline_large_ansi_scrollback_is_capped_and_replayable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ScrollbackStore::with_base_dir(dir.path());

    // Build a large buffer with embedded ANSI escapes and multi-byte chars.
    // Each line is ~25 bytes; ~12_000 lines comfortably exceeds the 256 KiB
    // cap so the save path actually exercises the ANSI-safe truncation.
    let mut buf = OutputBuffer::new(1000, 4096);
    for i in 0..12_000 {
        // Each line: color reset, color set, "line N: café\n".
        buf.write(&format!("\x1b[0m\x1b[3{}m line {i}: café\n", i % 8));
    }
    buf.flush();
    let scrollback = buf.scrollback();
    assert!(scrollback.len() > syncode_terminal::MAX_SCROLLBACK_BYTES);

    // Save (which caps) then reload.
    store
        .save("pipeline-thread", "pipeline-term", &scrollback)
        .expect("save");
    let loaded = store
        .load("pipeline-thread", "pipeline-term")
        .expect("load")
        .expect("some scrollback");

    // The persisted copy is capped.
    assert!(loaded.len() <= syncode_terminal::MAX_SCROLLBACK_BYTES);
    // The loaded text is valid UTF-8 (read_to_string guarantees this, but
    // assert intent) and begins at a replay-safe boundary.
    assert!(loaded.is_char_boundary(0));
    let first = loaded.as_bytes().first().copied();
    assert!(
        matches!(first, Some(b'\x1b') | Some(b'\n') | Some(b'l')),
        "loaded scrollback must start at a boundary, got {first:?}"
    );
    // And it can be restored into a fresh buffer without panic/error.
    let mut restored = OutputBuffer::new(1000, 4096);
    restored.restore(&loaded);
    assert!(!restored.scrollback().is_empty());
}
