# Git hooks

Version-controlled git hooks for the syncode repo. Kept under `.githooks/`
and opt-in via git's `core.hooksPath` — no framework, no binary dependency.

## Enable (once per clone)

```sh
git config core.hooksPath .githooks
```

After this, every `git commit` runs the active hooks.

## Hooks

### `pre-commit`

Runs `cargo fmt --all -- --check` when Rust files (`.rs`) are staged and
rejects the commit if anything is unformatted. Mirrors the CI fmt gate in
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) so a formatting
violation is caught locally before it can redden CI.

- **Bypass once:** `git commit --no-verify` (CI still enforces it).
- **No Rust files staged?** The hook is a no-op.
- **`cargo` not on PATH?** The hook skips with a warning (does not block).
