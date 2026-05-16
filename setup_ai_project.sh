#!/usr/bin/env bash

set -e

echo "🚀 Setting up AI + Python project structure..."

# ---------- helper functions ----------

create_dir() {
  if [ ! -d "$1" ]; then
    mkdir -p "$1"
    echo "📁 Created directory: $1"
  else
    echo "↪️  Directory exists: $1"
  fi
}

create_file() {
  if [ ! -f "$1" ]; then
    touch "$1"
    echo "📄 Created file: $1"
  else
    echo "↪️  File exists: $1"
  fi
}

# ---------- AI structure ----------

create_dir ".ai"
create_dir ".ai/roles"
create_dir ".ai/skills"

create_file ".ai/memory.md"
create_file ".ai/conventions.md"
create_file ".ai/architecture.md"

create_file ".ai/roles/coder.md"
create_file ".ai/roles/reviewer.md"
create_file ".ai/roles/planner.md"

create_file ".ai/skills/api_design.md"
create_file ".ai/skills/db_patterns.md"
create_file ".ai/skills/testing.md"

# ---------- Python project basics ----------

create_file "README.md"
create_file "requirements.txt"

create_dir ".github"
create_dir ".github/workflows"

create_file ".github/workflows/ci.yml"

# ---------- Optional nice-to-have ----------

create_file ".gitignore"

if [ ! -s ".gitignore" ]; then
  cat <<EOF >> .gitignore
__pycache__/
*.pyc
.venv/
env/
.DS_Store
.vscode/
EOF
  echo "🧹 Initialized .gitignore"
fi

echo "✅ Setup complete!"