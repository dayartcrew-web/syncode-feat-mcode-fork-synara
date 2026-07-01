#!/usr/bin/env bash
# scripts/flow.sh — full development verification + doc-refresh flow.
#
# Stages: build → clippy → fmt → test → docs
#   docs regenerates TEST_SUMMARY.md (deterministic, from real test counts)
#   and reports staleness of the agent-maintained semantic docs
#   (docs/ARCHITECTURE.md, docs/CRATES.md, .masday/intel/*), printing the exact
#   masday agent commands to refresh them.
#
# The syncode-tauri crate is excluded from build/clippy/test because it pulls
# glib/gtk/gobject-sys C libs that are not installed in this environment
# (environmental, not a code defect). Everything else runs workspace-wide.
#
# Usage:
#   scripts/flow.sh                # run all stages, stop on first failure
#   scripts/flow.sh --keep-going   # run all stages even if one fails (CI mode)
#   scripts/flow.sh --fix          # apply cargo fmt (instead of --check)
#   scripts/flow.sh --stage docs   # run a single stage: build|clippy|fmt|test|docs
#   scripts/flow.sh --help
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Crate excluded from workspace build/test (missing GTK C libs — environmental).
EXCLUDE="--exclude syncode-tauri"

KEEP_GOING=0
FMT_MODE="--check"
STAGE_FILTER=""

usage() {
  sed -n '2,22p' "${BASH_SOURCE[0]}"
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-going) KEEP_GOING=1; shift ;;
    --fix) FMT_MODE=""; shift ;;
    --stage) STAGE_FILTER="$2"; shift 2 ;;
    --help|-h) usage ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# --- output helpers -------------------------------------------------------
C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YLW=$'\033[33m'; C_BLU=$'\033[34m'; C_OFF=$'\033[0m'
PASS=0; FAIL=0

banner() { printf "\n${C_BLU}▶ %s${C_OFF}\n" "$*"; }
ok()      { printf "${C_GRN}  ✓ %s${C_OFF}\n" "$*"; PASS=$((PASS+1)); }
fail()    { printf "${C_RED}  ✗ %s${C_OFF}\n" "$*"; FAIL=$((FAIL+1)); }
note()    { printf "${C_YLW}  · %s${C_OFF}\n" "$*"; }

run_stage() { # name
  [[ -z "$STAGE_FILTER" || "$STAGE_FILTER" == "$1" ]] || return 255
  return 0
}

# --- stage 1: build -------------------------------------------------------
stage_build() {
  banner "build  (cargo build --workspace $EXCLUDE)"
  run_stage build || return 0
  if cargo build --workspace $EXCLUDE --all-targets >&2; then
    ok "build passed"
  else
    fail "build failed"; [[ $KEEP_GOING == 1 ]] || exit 1
  fi
}

# --- stage 2: clippy ------------------------------------------------------
stage_clippy() {
  banner "clippy  (cargo clippy --workspace $EXCLUDE -D warnings)"
  run_stage clippy || return 0
  if cargo clippy --workspace $EXCLUDE --all-targets -- -D warnings >&2; then
    ok "clippy clean"
  else
    fail "clippy reported warnings/errors"; [[ $KEEP_GOING == 1 ]] || exit 1
  fi
}

# --- stage 3: fmt ---------------------------------------------------------
stage_fmt() {
  banner "fmt  (cargo fmt --all ${FMT_MODE:---check})"
  run_stage fmt || return 0
  if cargo fmt --all -- $FMT_MODE >&2; then
    ok "fmt ${FMT_MODE:+ok|}${FMT_MODE:-applied}"
  else
    fail "fmt ${FMT_MODE:+check failed|} — run: scripts/flow.sh --fix"
    [[ $KEEP_GOING == 1 ]] || exit 1
  fi
}

# --- stage 4: test --------------------------------------------------------
stage_test() {
  banner "test  (cargo test --workspace $EXCLUDE)"
  run_stage test || return 0
  if cargo test --workspace $EXCLUDE >&2; then
    ok "tests passed"
  else
    fail "tests failed"; [[ $KEEP_GOING == 1 ]] || exit 1
  fi
}

# --- stage 5: docs --------------------------------------------------------
# TEST_SUMMARY.md is regenerated deterministically from per-crate test counts
# + Rust LOC. ARCHITECTURE/CRATES/intel are semantic and agent-maintained —
# we only check staleness and print the refresh commands.
stage_docs() {
  banner "docs  (TEST_SUMMARY.md + staleness of ARCHITECTURE/CRATES/intel)"
  run_stage docs || return 0

  # Parse ONLY the workspace `members = [ ... ]` array (won't bleed into
  # [workspace.dependencies] feature arrays), then add the integration package.
  local members=()
  while IFS= read -r m; do [[ -n "$m" ]] && members+=("$m"); done < <(
    sed -n '/^members *= *\[/,/^\]/p' Cargo.toml \
      | grep -oE 'crates/[a-z0-9_-]+|tests' \
      | sed 's|^|./|'
  )
  members+=("./tests")

  # Count tests per crate by parsing "test result: ok. N passed" lines.
  local summary_rows="" total=0 crate_count=0
  local crate
  for crate in "${members[@]}"; do
    local name; name="$(basename "$crate")"
    # package name: strip the syncode- prefix dir convention only for display
    local pkg; pkg="$name"
    [[ "$name" == "tests" ]] && pkg="syncode-integration-tests"
    local out; out="$(cargo test -p "$pkg" 2>/dev/null || true)"
    local n; n="$(printf '%s\n' "$out" | grep -oE '[0-9]+ passed' | awk '{s+=$1} END{print s+0}')"
    summary_rows+="| \`$pkg\` | $n |\n"
    total=$((total+n)); crate_count=$((crate_count+1))
  done

  # Rust LOC across crates (excluding target).
  local loc; loc="$(git ls-files 'crates/**/*.rs' 'tests/**/*.rs' 2>/dev/null | xargs wc -l 2>/dev/null | tail -1 | awk '{print $1}')"
  loc="${loc:-?}"

  local today; today="$(date +%Y-%m-%d)"
  {
    printf '# Syncode — Test Summary Report\n\n'
    printf '**Generated:** %s · regenerated by `scripts/flow.sh` from live `cargo test` counts.\n' "$today"
    printf '**Total Tests:** %s across %d crates/packages (syncode-tauri excluded — missing GTK C libs).\n' "$total" "$crate_count"
    printf '**Total Rust LOC:** %s (`crates/` + `tests/`, excluding `target/`)\n\n'
    printf '## Test Breakdown by Crate\n\n'
    printf '| Crate | Tests |\n|---|---|\n'
    printf '%b' "$summary_rows"
    printf '\n> Counts are captured live by `scripts/flow.sh docs`. Semantic docs below are agent-maintained.\n'
  } > TEST_SUMMARY.md
  ok "TEST_SUMMARY.md regenerated (total $total tests, $loc LOC)"

  # Staleness check for semantic docs (agent-maintained).
  printf "\n${C_BLU}  agent-maintained docs (refresh with masday agents):${C_OFF}\n"
  check_doc_fresh() { # docfile
    local doc="$1" newest_src newest_doc
    [[ -f "$doc" ]] || { note "$doc MISSING"; return; }
    newest_src="$(git ls-files 'crates/**/*.rs' 2>/dev/null | xargs stat -c '%Y %n' 2>/dev/null | sort -rn | head -1 | awk '{print $1}')"
    newest_doc="$(stat -c '%Y' "$doc" 2>/dev/null || echo 0)"
    if [[ "${newest_src:-0}" -gt "${newest_doc:-0}" ]]; then
      note "$doc STALE (source newer)"
    else
      ok "$doc up to date"
    fi
  }
  check_doc_fresh "docs/ARCHITECTURE.md"
  check_doc_fresh "docs/CRATES.md"
  check_doc_fresh ".masday/intel/00-overview.md"

  cat <<'EOF'

  Refresh commands (semantic docs — require an LLM agent):
    masday-intel-updater   # regenerates .masday/intel/* (file graph, API surface, deps, coverage)
    masday-doc-updater     # regenerates docs/ARCHITECTURE.md, docs/CRATES.md against live code
EOF
}

# --- run ------------------------------------------------------------------
stage_build
stage_clippy
stage_fmt
stage_test
stage_docs

banner "flow result: ${C_GRN}$PASS passed${C_OFF}, ${C_RED}$FAIL failed${C_OFF}"
[[ $FAIL -eq 0 ]]
