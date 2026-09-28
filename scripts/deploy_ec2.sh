#!/usr/bin/env bash
set -euo pipefail

# Deploy helper for low-memory EC2 hosts.
# - Postgres always starts alongside the app (no profile needed)
# - Rebuilds app image only when Dockerfile or requirements.txt changed
# - Otherwise recreates containers without on-host build
#
# Usage:
#   ./scripts/deploy_ec2.sh --pull
#   ./scripts/deploy_ec2.sh
#   ./scripts/deploy_ec2.sh --rebuild
#
# Options:
#   --pull     Run git pull before deploy
#   --rebuild  Force image rebuild
#   --help     Show usage

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

DO_PULL=false
FORCE_REBUILD=false

for arg in "$@"; do
  case "$arg" in
    --pull)
      DO_PULL=true
      ;;
    --rebuild)
      FORCE_REBUILD=true
      ;;
    --help)
      sed -n '1,40p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      echo "Use --help for usage." >&2
      exit 1
      ;;
  esac
done

if [[ "$DO_PULL" == "true" ]]; then
  echo "[deploy] Pulling latest changes..."
  git pull --ff-only
fi

# Detect whether app image rebuild is required.
# We inspect the latest commit range where possible; if unavailable, fall back to no-rebuild.
REBUILD_REQUIRED=false
if [[ "$FORCE_REBUILD" == "true" ]]; then
  REBUILD_REQUIRED=true
else
  CHANGED_FILES="$(git diff --name-only HEAD~1..HEAD 2>/dev/null || true)"
  if echo "$CHANGED_FILES" | grep -Eq '^(Dockerfile|requirements\.txt)$'; then
    REBUILD_REQUIRED=true
  fi
fi

if [[ "$REBUILD_REQUIRED" == "true" ]]; then
  echo "[deploy] Rebuild required. Building app image..."
  docker compose build app
  echo "[deploy] Starting app + local postgres..."
  docker compose up -d app postgres
else
  echo "[deploy] No rebuild required. Recreating app + local postgres..."
  docker compose up -d --no-build --force-recreate app postgres
fi

echo "[deploy] Done."
