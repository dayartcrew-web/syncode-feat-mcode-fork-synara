#!/usr/bin/env bash
# Centralized node_modules for git worktrees.
#
# The canonical node_modules lives in the MAIN repo's frontend/ (gitignored,
# root .gitignore line 10). Each worktree symlinks its frontend/node_modules
# to it, so `npm install` runs ONCE (canonical) instead of per-worktree.
#
# Usage:
#   .masday/link-node-modules.sh <worktree-path>
#   .masday/link-node-modules.sh .masday/worktrees/<slug>
#
# The canonical store must be installed first (run once in the main repo):
#   npm install --prefix <repo>/frontend
#
# Worktree frontend/package.json must match the canonical's (same deps). In the
# clone+rewire flow package.json is stable across worktrees (set by T2), so the
# shared store is valid for all of them.
set -euo pipefail

REPO="/home/vibe-dev/syncode-feat-mcode-fork-synara"
CENTRAL="$REPO/frontend/node_modules"
WT="${1:-}"

if [ -z "$WT" ]; then
  echo "usage: $0 <worktree-path>" >&2
  exit 1
fi

# Resolve to absolute path.
WT="$(cd "$WT" && pwd)"
TARGET="$WT/frontend/node_modules"

if [ ! -d "$CENTRAL" ]; then
  echo "ERROR: canonical store missing at $CENTRAL" >&2
  echo "Run first: npm install --prefix $REPO/frontend" >&2
  exit 1
fi

mkdir -p "$(dirname "$TARGET")"

if [ -L "$TARGET" ]; then
  echo "already symlinked: $TARGET -> $(readlink "$TARGET")"
  exit 0
fi

if [ -d "$TARGET" ]; then
  echo "WARNING: real node_modules dir exists at $TARGET (not a symlink)." >&2
  echo "Replace it with the symlink? This deletes the local copy. [y/N]" >&2
  read -r ans
  if [ "$ans" = "y" ] || [ "$ans" = "Y" ]; then
    rm -rf "$TARGET"
  else
    echo "leaving as-is"; exit 0
  fi
fi

ln -s "$CENTRAL" "$TARGET"
echo "linked $TARGET -> $CENTRAL"
