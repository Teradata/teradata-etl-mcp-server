"""Console script entrypoint for Teradata ETL MCP Server.

This module allows running the server via the installed
CLI `teradata-etl-mcp-server` by exposing a `main()` function
that delegates to the CLI implementation in `main.py`.
"""


def main() -> None:
    from .main import main as cli_main

    cli_main()


if __name__ == "__main__":
    main()
