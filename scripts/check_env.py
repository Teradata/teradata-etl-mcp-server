#!/usr/bin/env python3
"""Check .env against .env.example for missing, extra, or renamed keys.

Usage:
    python scripts/check_env.py                                    # defaults
    python scripts/check_env.py /path/to/.env                      # custom .env
    python scripts/check_env.py /path/to/.env /path/to/.env.example  # custom both

Exit codes:
    0 = in sync
    1 = deviations found

Intended to run as a git post-merge hook so testers see warnings after git pull.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

KNOWN_RENAMES = {
    "AIRFLOW_SSH_HOST": "AIRFLOW_REMOTE_HOST",
    "AIRFLOW_SSH_USER": "AIRFLOW_REMOTE_USER",
    "AIRFLOW_SSH_PORT": "AIRFLOW_REMOTE_PORT",
    "AIRFLOW_SSH_KEY_PATH": "AIRFLOW_REMOTE_SSH_KEY",
    "AIRFLOW_SSH_KEY_PASSPHRASE": "AIRFLOW_REMOTE_SSH_KEY_PASSPHRASE",
    "AIRFLOW_REMOTE_DAGS_DIR": "AIRFLOW_DAG_FOLDER",
    "TTU_VERSION": "TTU_TTU_VERSION",
    "PORT": "TERADATA_PORT",
    "SERVER_HOST": "(removed — MCP uses stdio, not HTTP)",
    "SERVER_PORT": "(removed — MCP uses stdio, not HTTP)",
    "SERVER_WORKERS": "(removed — MCP uses stdio, not HTTP)",
    "SERVER_LOG_LEVEL": "MCP_LOG_LEVEL",
    "LOG_LEVEL": "MCP_LOG_LEVEL",
    "LOG_FORMAT": "(removed)",
    "LOG_FILE": "MCP_LOG_FILE",
    "DBT_WORKER_SSH_HOST": "(removed — not used by any code)",
    "DBT_WORKER_SSH_PORT": "(removed — not used by any code)",
    "DBT_WORKER_SSH_USER": "(removed — not used by any code)",
    "DBT_WORKER_SSH_KEY_PATH": "(removed — not used by any code)",
    "SSH_ENABLED": "(removed — use MCP_CLIENT_SSH_* for runtime SSH credentials)",
    "SSH_HOST": "(removed — use MCP_CLIENT_SSH_HOST)",
    "SSH_PORT": "(removed — use MCP_CLIENT_SSH_PORT)",
    "SSH_USERNAME": "(removed — use MCP_CLIENT_SSH_USER)",
    "SSH_PASSWORD": "(removed — use MCP_CLIENT_SSH_PASSWORD)",
    "SSH_KEY_FILE": "(removed — use MCP_CLIENT_SSH_KEY_PATH)",
    "SSH_CONN_ID": "(removed — connection ID is auto-generated)",
    "SSH_TIMEOUT": "(removed)",
    "SSH_REMOTE_WORKING_DIR": "(removed)",
    "MCP_TRANSPORT": "(removed — MCP uses stdio only)",
}

_KEY_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=")


def extract_keys(path: Path) -> set[str]:
    """Extract all KEY= definitions from an env file (ignores commented lines)."""
    keys: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        m = _KEY_RE.match(stripped)
        if m:
            keys.add(m.group(1))
    return keys


def extract_keys_active_and_commented(path: Path) -> tuple[set[str], set[str]]:
    """Extract KEY= definitions from .env.example, split into active and commented.

    Returns:
        (active_keys, commented_keys) — active are uncommented, commented start with #.
    """
    active: set[str] = set()
    commented: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            inner = stripped.lstrip("#").strip()
            m = _KEY_RE.match(inner)
            if m:
                commented.add(m.group(1))
        else:
            m = _KEY_RE.match(stripped)
            if m:
                active.add(m.group(1))
    return active, commented


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent

    if len(sys.argv) > 2:
        env_path = Path(sys.argv[1]).resolve()
        example_path = Path(sys.argv[2]).resolve()
    elif len(sys.argv) > 1:
        env_path = Path(sys.argv[1]).resolve()
        example_path = repo_root / ".env.example"
    else:
        env_path = repo_root / ".env"
        example_path = repo_root / ".env.example"

    print()
    print(f"  [check-env] Comparing:")
    print(f"    .env         : {env_path}")
    print(f"    .env.example : {example_path}")
    print()

    if not example_path.exists():
        print(f"  [check-env] {example_path} not found — skipping")
        print()
        print(f"  Tip: bash .git/hooks/post-merge [.env path] [.env.example path]")
        print()
        return 0

    if not env_path.exists():
        print(f"  [check-env] {env_path} not found — skipping")
        print()
        print(f"  Tip: bash .git/hooks/post-merge [.env path] [.env.example path]")
        print()
        return 0

    active_example_keys, commented_example_keys = extract_keys_active_and_commented(example_path)
    all_example_keys = active_example_keys | commented_example_keys
    actual_keys = extract_keys(env_path)

    # Keys in .env.example (active) but not in .env — these are required
    missing_required = active_example_keys - actual_keys

    # Keys in .env.example (commented) but not in .env — these are optional
    missing_optional = commented_example_keys - actual_keys

    # Keys in .env but not in .env.example
    extra = actual_keys - all_example_keys

    renamed = {}
    unknown = set()
    for key in extra:
        if key in KNOWN_RENAMES:
            renamed[key] = KNOWN_RENAMES[key]
        else:
            unknown.add(key)

    has_issues = missing_required or renamed or unknown
    has_info = missing_optional

    if not has_issues and not has_info:
        print(f"  [check-env] In sync — no deviations found.")
        print()
        return 0

    if has_issues:
        print(f"  [check-env] Deviations found:")
        print(f"  {'=' * 60}")

    if missing_required:
        print()
        print(f"  MISSING — REQUIRED ({len(missing_required)} active keys not in your .env):")
        for key in sorted(missing_required):
            print(f"    + {key}")

    if renamed:
        print()
        print(f"  RENAMED ({len(renamed)} keys using old names):")
        for old, new in sorted(renamed.items()):
            print(f"    {old}  -->  {new}")

    if unknown:
        print()
        print(f"  EXTRA ({len(unknown)} keys not in .env.example — may be dead config):")
        for key in sorted(unknown):
            print(f"    ? {key}")

    if missing_optional:
        if has_issues:
            print()
            print(f"  {'- ' * 30}")
        print()
        print(f"  OPTIONAL ({len(missing_optional)} commented keys in .env.example, not in your .env):")
        print(f"  (These have sensible defaults — add only if you need to override)")
        for key in sorted(missing_optional):
            print(f"    # {key}")

    print()
    if has_issues:
        print(f"  Fix: compare your .env with .env.example and update accordingly.")

    import os as _os
    invoked_via_hook = _os.environ.get("CHECK_ENV_VIA_HOOK") == "1"

    if invoked_via_hook:
        print()
        print(f"  Usage:")
        print(f"    bash .git/hooks/post-merge                                          # repo .env vs .env.example")
        print(f"    bash .git/hooks/post-merge /c/sourcecode/run_server/.env            # custom .env (Git Bash path)")
        print(f'    bash .git/hooks/post-merge "C:/sourcecode/run_server/.env"          # custom .env (Windows path)')
        print(f'    bash .git/hooks/post-merge "C:/my/.env" "C:/my/.env.example"        # custom both')
        print()
        print(f"  Note: In Git Bash, use forward slashes or quote Windows paths.")
    else:
        print()
        print(f"  Usage:")
        print(f"    python scripts/check_env.py                                         # repo .env vs .env.example")
        print(f'    python scripts/check_env.py C:\\sourcecode\\run_server\\.env           # custom .env')
        print(f'    python scripts/check_env.py C:\\my\\.env C:\\my\\.env.example           # custom both')
    print()
    return 1 if has_issues else 0


if __name__ == "__main__":
    sys.exit(main())
