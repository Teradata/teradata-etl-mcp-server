#!/bin/bash
# Install git hooks for Teradata ETL MCP Server test VMs.
# Run once after cloning: bash scripts/install-hooks.sh

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOK_DIR="$REPO_ROOT/.git/hooks"

if [ ! -d "$HOOK_DIR" ]; then
    echo "Error: $HOOK_DIR does not exist. Are you in a Git repository?" >&2
    exit 1
fi

if [ -f "$HOOK_DIR/post-merge" ]; then
    cp "$HOOK_DIR/post-merge" "$HOOK_DIR/post-merge.bak"
    echo "Backed up existing post-merge hook to post-merge.bak"
fi

cat > "$HOOK_DIR/post-merge" << 'HOOK'
#!/bin/bash
# Auto-check .env after git pull
export CHECK_ENV_VIA_HOOK=1
PYTHON_CMD="${PYTHON_CMD:-python3}"
command -v "$PYTHON_CMD" >/dev/null 2>&1 || PYTHON_CMD="python"
if ! command -v "$PYTHON_CMD" >/dev/null 2>&1; then
    echo "[check-env] Skipped: no Python interpreter found (tried python3, python)" >&2
    exit 0
fi
"$PYTHON_CMD" "$(git rev-parse --show-toplevel)/scripts/check_env.py" "$@"
HOOK

chmod +x "$HOOK_DIR/post-merge"
echo "Installed post-merge hook — .env will be checked after every git pull."
