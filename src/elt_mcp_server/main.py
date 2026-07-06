"""Main entry point and CLI for ELT MCP Server.

This module provides the command-line interface and entry point for running
the ELT MCP Server via stdio transport.

Import discipline: keep this module cheap to import. ``python -m elt_mcp_server
--help`` is the health check the VS Code extension runs during startup under
a 15-second timeout, and cold-path loading of ``.server`` (which transitively
imports fastmcp, pandas, teradatasql, etc.) can exceed that window on Windows
with Defender scanning fresh ``.pyd`` files. Heavy imports therefore live
inside the subcommand handlers / ``run_stdio_async`` — only pydantic is
unavoidable at module level because the error formatters need its types.
"""

import argparse
import asyncio
import os
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

from pydantic import ValidationError
from pydantic_settings import SettingsError

if TYPE_CHECKING:
    # Pulled in only for type-checker lookups, never at runtime.
    from .config import Settings  # noqa: F401

_SETTINGS_ENV_PREFIX: dict[str, str] = {
    "TeradataSettings": "TERADATA_",
    "AirflowSettings": "AIRFLOW_",
    "AirbyteSettings": "AIRBYTE_",
    "DBTSettings": "DBT_",
    "TTUSettings": "TTU_",
    "PipelineSettings": "PIPELINE_",
    "MCPServerSettings": "MCP_",
    "OrchestratorSettings": "ORCHESTRATOR_",
    "SecuritySettings": "SECURITY_",
}


def _settings_classes() -> dict[str, type]:
    """Return the class-name → class mapping used by error formatters.

    Imports ``.config`` lazily so ``--help`` / argparse parse errors don't pay
    the ~4-second cold-import cost of the FastMCP + Teradata + pandas dep tree.
    """
    from .config import (
        AirbyteSettings,
        AirflowSettings,
        DBTSettings,
        MCPServerSettings,
        OrchestratorSettings,
        PipelineSettings,
        SecuritySettings,
        TeradataSettings,
        TTUSettings,
    )

    return {
        "TeradataSettings": TeradataSettings,
        "AirflowSettings": AirflowSettings,
        "AirbyteSettings": AirbyteSettings,
        "DBTSettings": DBTSettings,
        "TTUSettings": TTUSettings,
        "PipelineSettings": PipelineSettings,
        "MCPServerSettings": MCPServerSettings,
        "OrchestratorSettings": OrchestratorSettings,
        "SecuritySettings": SecuritySettings,
    }

_SECTION_ENV_PREFIX: dict[str, str] = {
    "teradata": "TERADATA_",
    "airflow": "AIRFLOW_",
    "airbyte": "AIRBYTE_",
    "dbt": "DBT_",
    "ttu": "TTU_",
    "pipeline": "PIPELINE_",
    "mcp": "MCP_",
    "orchestrator": "ORCHESTRATOR_",
    "security": "SECURITY_",
}


def _format_config_error(exc: ValidationError, env_file: Path | None) -> str:
    """Turn a Pydantic ValidationError into a human-readable startup message.

    Handles two shapes of ValidationError:
    - Leaf settings class raised directly (exc.title == "TeradataSettings"):
      loc is relative to that class, e.g. ('host',) → TERADATA_HOST
    - Parent Settings class fails (exc.title == "Settings"):
      loc includes the section attribute, e.g. ('teradata', 'host') → TERADATA_HOST

    Errors are split into two buckets:
    - missing: shown as env var names under "Missing:"
    - all other types (invalid format, value out of range, etc.): shown under "Invalid:"
      with the Pydantic error message so the user knows what's wrong, not just what's absent.
    """
    missing_vars: list[str] = []
    invalid_vars: list[str] = []

    for err in exc.errors():
        loc = err["loc"]
        if exc.title in _SETTINGS_ENV_PREFIX:
            prefix = _SETTINGS_ENV_PREFIX[exc.title]
            field = "_".join(str(p) for p in loc).upper()
        elif loc and str(loc[0]) in _SECTION_ENV_PREFIX:
            prefix = _SECTION_ENV_PREFIX[str(loc[0])]
            field = "_".join(str(p) for p in loc[1:]).upper()
        else:
            prefix = ""
            field = "_".join(str(p) for p in loc).upper()
        var_name = f"{prefix}{field}" if field else "_".join(str(p) for p in loc).upper()

        if err["type"] == "missing":
            if var_name:
                missing_vars.append(var_name)
        else:
            invalid_vars.append(f"{var_name}: {err['msg']}")

    if missing_vars and not invalid_vars:
        header = "[FAIL] Server startup failed — required environment variables are not set."
    elif invalid_vars and not missing_vars:
        header = "[FAIL] Server startup failed — one or more configuration values are invalid."
    else:
        header = "[FAIL] Server startup failed — configuration error."

    lines = ["", header, ""]

    if missing_vars:
        lines.append("  Missing:")
        for var in missing_vars:
            lines.append(f"    {var}")
        lines.append("")

    if invalid_vars:
        lines.append("  Invalid:")
        for var in invalid_vars:
            lines.append(f"    {var}")
        lines.append("")

    env_path = env_file or Path(".env")
    if not env_path.exists():
        lines.append(f"  .env file not found at: {env_path.resolve()}")
        lines.append("")

    lines += [
        "  How to fix:",
        "    1. Pass the .env file explicitly:",
        "         elt-mcp-server --env-file /absolute/path/to/.env",
        "    2. Or place a .env file in the working directory the server is launched from.",
        "    3. Or set the missing variables directly in your system environment.",
    ]
    return "\n".join(lines)


def _format_settings_error(exc: SettingsError, env_file: Path | None) -> str:
    """Turn a pydantic-settings SettingsError into a human-readable startup message.

    SettingsError is raised before validators run (e.g. when an env var value cannot
    be decoded into the expected type). The exception message already names the field
    and source, so we surface it directly alongside fix instructions.
    """
    import re

    msg = str(exc)
    # Extract field name from: 'error parsing value for field "foo" from source "..."'
    match = re.search(r'field "([^"]+)"', msg)
    field_hint = ""
    if match:
        field_name = match.group(1)
        # Find every settings class that owns this field and build env var name(s).
        settings_classes = _settings_classes()
        env_vars = [
            f"{_SETTINGS_ENV_PREFIX[cls_name]}{field_name.upper()}"
            for cls_name, cls in settings_classes.items()
            if field_name in cls.model_fields
        ]
        if env_vars:
            field_hint = f"\n  Field:   {field_name}\n  Env var: {', '.join(env_vars)}"
        else:
            field_hint = f"\n  Field:   {field_name}"

    lines = [
        "",
        "[FAIL] Server startup failed — could not parse a configuration value.",
        field_hint,
        f"  Detail:  {msg}",
        "",
        "  How to fix:",
        "    1. Check the value of the env var shown above in your .env file.",
        "    2. For list fields (e.g. NOTIFICATION_SMTP_TO_ADDRESSES) use:",
        "         comma-separated: alerts@company.com,team@company.com",
        "         or JSON array:   [\"alerts@company.com\",\"team@company.com\"]",
    ]
    return "\n".join(lines)


def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown.

    SIGINT (Ctrl+C): Python's default handler raises KeyboardInterrupt,
    which is caught by the except/finally blocks in run_stdio_async().
    No custom handler needed.

    SIGTERM: Convert to KeyboardInterrupt so the same cleanup path runs
    (finally blocks call ``await server.cleanup()``).
    """

    def sigterm_handler(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, sigterm_handler)


async def run_stdio_async():
    """Run the MCP server with stdio transport."""
    # Deferred: pulls fastmcp / pandas / teradatasql (~3s cold on Windows).
    from .config import load_settings
    from .server import create_app_with_lifespan, get_server_instance

    # Ensure stdout is unbuffered for MCP protocol
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    settings = load_settings()

    # Cwd-change is now defence-in-depth, NOT a load-bearing path-resolution
    # step. ``Settings.validate_settings`` resolves every artefact-directory
    # default under ``settings.workspace_dir`` and creates the directories,
    # so subsystems no longer rely on cwd matching the workspace. The chdir
    # here keeps a sensible cwd for any subprocess or library code that
    # still emits relative paths.
    os.chdir(settings.workspace_dir)
    print(f"[OK] Workspace directory: {settings.workspace_dir}", file=sys.stderr)

    app = create_app_with_lifespan(settings)
    server = get_server_instance()

    print("[OK] ELT MCP Server starting with stdio transport...", file=sys.stderr)
    print(f"  Environment: {settings.environment}", file=sys.stderr)
    print(f"  Teradata: {settings.teradata.host}", file=sys.stderr)
    print(f"  Airflow: {settings.airflow.base_url}", file=sys.stderr)

    print("[OK] Server ready for MCP connections\n", file=sys.stderr)

    try:
        # Use FastMCP's stdio async runner to avoid nested event loop errors
        await app.run_stdio_async()
    except KeyboardInterrupt:
        print("\n[INFO] Server stopped by user", file=sys.stderr)
    except Exception as e:
        print(f"\n[FAIL] Server error: {e}", file=sys.stderr)
        raise
    finally:
        if server:
            await server.cleanup()


def cmd_config(args: argparse.Namespace):
    """Show current configuration."""
    from .config import load_settings  # deferred — see module docstring

    try:
        settings = load_settings()
    except SettingsError as e:
        print(_format_settings_error(e, args.env_file), file=sys.stderr)
        sys.exit(1)
    except ValidationError as e:
        print(_format_config_error(e, args.env_file), file=sys.stderr)
        sys.exit(1)

    print("ELT MCP Server Configuration")
    print("=" * 60)
    print(f"\n  Environment: {settings.environment}")
    print("\n  Teradata:")
    print(f"   Host: {settings.teradata.host}")
    print(f"   Port: {settings.teradata.port}")
    print(f"   Database: {settings.teradata.database or '(default)'}")
    print(f"   Username: {settings.teradata.username}")

    print("\n  Airflow:")
    print(f"   Enabled: {settings.airflow.enabled}")
    if settings.airflow.enabled:
        print(f"   URL: {settings.airflow.base_url}")

    print("\n  Airbyte:")
    print(f"   Enabled: {settings.airbyte.enabled}")
    if settings.airbyte.enabled:
        print(f"   URL: {settings.airbyte.base_url}")

    print("\n[INFO] dbt:")
    print(f"   Project Dir: {settings.dbt.project_dir}")
    print(f"   Target: {settings.dbt.target}")
    print("\n[INFO] TTU (Teradata Tools & Utilities):")
    print(f"   Enabled: {settings.ttu.enabled}")
    if settings.ttu.enabled:
        print(f"   Version: {settings.ttu.ttu_version}")
        print(f"   TPT Binary: {settings.ttu.tpt_binary_path}")
        print(f"   BTEQ Binary: {settings.ttu.bteq_binary_path}")
        print(f"   tdload Binary: {settings.ttu.tdload_binary_path}")
        print(f"   Scripts Dir: {settings.ttu.scripts_dir}")

    print("\n  Pipeline:")
    print(f"   DAGs Output: {settings.pipeline.dags_output_dir}")
    print(f"   Generate dbt by default: {settings.pipeline.generate_dbt_by_default}")

    print("\n  MCP Server:")
    print(f"   Log Level: {settings.mcp.log_level}")
    print(f"   Log File: {settings.mcp.log_file}")
    print()

    if args.json:
        import json

        config_dict = settings.to_dict(include_secrets=False)
        print("\n" + json.dumps(config_dict, indent=2, default=str))


def cmd_validate(args: argparse.Namespace):
    """Validate configuration and connections."""
    from .config import load_settings  # deferred — see module docstring

    print("Validating ELT MCP Server configuration...")
    print("=" * 60)

    try:
        settings = load_settings()
        print("[OK] Configuration loaded successfully")

        # Validate Teradata connection
        print("\n  Testing Teradata connection...")
        try:
            from .client_factory import DefaultClientFactory

            factory = DefaultClientFactory(settings)
            td_client = factory.create_teradata_client()
            result = td_client.execute_query("SELECT CURRENT_DATE AS today")
            td_client.close()
            date_val = result[0].get('today') if result else 'unknown'
            print(f"[OK] Teradata connection successful (Date: {date_val})")
        except Exception as e:
            print(f"[FAIL] Teradata connection failed: {e}")

        # Validate Airflow connection (if enabled)
        if settings.airflow.enabled:
            print("\n  Testing Airflow connection...")
            try:
                from .clients.async_airflow_client import AsyncAirflowClient

                af_client = AsyncAirflowClient(
                    base_url=settings.airflow.base_url,
                    username=settings.airflow.username,
                    password=(
                        settings.airflow.password.get_secret_value()
                        if hasattr(settings.airflow.password, "get_secret_value")
                        else settings.airflow.password
                    ),
                    auth_manager=settings.airflow.auth_manager,
                    token_endpoint=settings.airflow.token_endpoint,
                )
                # Try to list DAGs (run async in sync context).
                # Both calls share one event loop to avoid cross-loop errors.
                async def _validate_airflow():
                    try:
                        return await af_client.list_dags()
                    finally:
                        await af_client.close()

                dags = asyncio.run(_validate_airflow())
                print(f"[OK] Airflow connection successful ({len(dags)} DAGs found)")
            except Exception as e:
                print(f"[FAIL] Airflow connection failed: {e}")
        else:
            print("\n  Airflow connection skipped (not enabled)")

        # Validate Airbyte connection (if enabled)
        if settings.airbyte.enabled:
            print("\n  Testing Airbyte connection...")
            try:
                ab_client = factory.create_airbyte_client()
                # Try to get health status (async client, run in sync context).
                # Both calls share one event loop to avoid cross-loop errors.
                async def _validate_airbyte():
                    try:
                        return await ab_client.get_health()
                    finally:
                        await ab_client.close()

                health = asyncio.run(_validate_airbyte())
                print(
                    f"[OK] Airbyte connection successful (Status: {health.get('status', 'unknown')})"
                )
            except Exception as e:
                print(f"[FAIL] Airbyte connection failed: {e}")
        else:
            print("\n  Airbyte connection skipped (not enabled)")

        # Validate dbt project
        print("\n[INFO] Validating dbt project...")
        if settings.dbt.project_dir.exists():
            dbt_yml = settings.dbt.project_dir / "dbt_project.yml"
            if dbt_yml.exists():
                print(f"[OK] dbt project found at {settings.dbt.project_dir}")
            else:
                print("[WARN]  dbt project directory exists but dbt_project.yml not found")
        else:
            print(f"[WARN]  dbt project directory does not exist: {settings.dbt.project_dir}")

        # Validate TTU binaries (if enabled)
        if settings.ttu.enabled:
            print("\n[INFO] Validating TTU binaries...")
            import shutil as _shutil

            for binary_name, binary_path in [
                ("tbuild", settings.ttu.tpt_binary_path),
                ("bteq", settings.ttu.bteq_binary_path),
                ("tdload", settings.ttu.tdload_binary_path),
            ]:
                found = _shutil.which(binary_path)
                if found:
                    print(f"[OK] {binary_name}: {found}")
                else:
                    print(f"[WARN]  {binary_name}: not found in PATH ({binary_path})")

        print("\n" + "=" * 60)
        print("[OK] Validation complete")

    except SettingsError as e:
        print(_format_settings_error(e, args.env_file), file=sys.stderr)
        sys.exit(1)
    except ValidationError as e:
        print(_format_config_error(e, args.env_file), file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n[FAIL] Validation failed: {e}")
        sys.exit(1)


def cmd_version(args: argparse.Namespace):
    """Show version information."""
    from . import __description__, __version__

    print(f"ELT MCP Server v{__version__}")
    print(__description__)
    print("\nComponents:")
    print("  - FastMCP: 3.2.0+")
    print("  - Python: " + sys.version.split()[0])
    print("\nSupported integrations:")
    print("  - Teradata Database")
    print("  - Apache Airflow")
    print("  - Airbyte (optional)")
    print("  - dbt Core/Cloud")
    print("  - Teradata TPT/BTEQ (optional)")


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="elt-mcp-server",
        description="Unified data pipeline orchestration MCP server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run server (stdio transport — works with any MCP client)
  elt-mcp-server

  # Show configuration
  elt-mcp-server config

  # Validate setup
  elt-mcp-server validate

  # Show version
  elt-mcp-server version

For VS Code integration, add to settings.json:
  {
    "github.copilot.chat.mcp.servers": {
      "elt-pipeline": {
        "command": "elt-mcp-server",
        "args": []
      }
    }
  }
        """,
    )

    # Global options
    parser.add_argument(
        "--env-file", type=Path, help="Path to .env file (default: .env in current directory)"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Override logging level",
    )

    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Config command
    config_parser = subparsers.add_parser("config", help="Show current configuration")
    config_parser.add_argument("--json", action="store_true", help="Output as JSON")

    # Validate command
    subparsers.add_parser("validate", help="Validate configuration and connections")

    # Version command
    subparsers.add_parser("version", help="Show version information")

    return parser


def main() -> NoReturn:
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Setup signal handlers
    setup_signal_handlers()

    # Handle env file
    # Load environment from provided .env path when specified
    if args.env_file:
        import os

        from dotenv import load_dotenv

        # Load variables from the specified env file into process environment
        load_dotenv(dotenv_path=str(args.env_file), override=True)
        os.environ["ENV_FILE"] = str(args.env_file)
    else:
        # Try loading a default .env in current working directory if present
        try:
            from dotenv import load_dotenv

            load_dotenv()
        except Exception:
            # dotenv is optional; ignore if not available
            pass

    # Handle log level override
    if args.log_level:
        import os

        os.environ["MCP_LOG_LEVEL"] = args.log_level

    # Handle subcommands
    if args.command == "config":
        cmd_config(args)
        sys.exit(0)
    elif args.command == "validate":
        cmd_validate(args)
        sys.exit(0)
    elif args.command == "version":
        cmd_version(args)
        sys.exit(0)

    # Run server with stdio transport
    try:
        asyncio.run(run_stdio_async())
    except KeyboardInterrupt:
        print("\nGoodbye!", file=sys.stderr)
        sys.exit(0)
    except SettingsError as e:
        print(_format_settings_error(e, args.env_file), file=sys.stderr)
        sys.exit(1)
    except ValidationError as e:
        print(_format_config_error(e, args.env_file), file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n[FAIL] Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

