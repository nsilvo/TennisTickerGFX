#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/prepare_repo.sh <remote-url>
# Example: ./scripts/prepare_repo.sh git@github.com:you/TennisXML.git

REMOTE_URL=${1:-}
if [[ -z "$REMOTE_URL" ]]; then
  echo "Remote URL required. Example: git@github.com:you/TennisXML.git" >&2
  exit 1
fi

# Clean local virtual env if present
if [[ -d .venv ]]; then
  echo "Removing local .venv..."
  rm -rf .venv
fi

# Ensure .gitignore exists
if [[ ! -f .gitignore ]]; then
  cat > .gitignore <<'EOF'
# Python
__pycache__/
*.pyc
*.pyo
*.pyd

# Virtual environments
.venv/
venv/
ENV/

# VS Code
.vscode/

# Logs
*.log

# Byte-compiled / packages
*.egg-info/

# Docker
*.pid

# Local data (uncomment if you persist DB locally)
# data/
EOF
fi

# Initialize and push repository
if [[ ! -d .git ]]; then
  echo "Initializing git repository..."
  git init
fi

git add .
if ! git diff --cached --quiet; then
  git commit -m "Initial commit"
fi

git branch -M main
if ! git remote | grep -q '^origin$'; then
  git remote add origin "$REMOTE_URL"
else
  git remote set-url origin "$REMOTE_URL"
fi

echo "Pushing to origin main..."
 git push -u origin main

echo "Done. On the server, run:\n  git clone $REMOTE_URL TennisXML && cd TennisXML && docker compose up --build -d"