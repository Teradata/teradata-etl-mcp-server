# Contributing to devtools-elt-mcp-server

## Prerequisites

- Python 3.10+
- Git

## Setup

```bash
git clone https://github.com/Teradata-PE/devtools-elt-mcp-server.git
cd devtools-elt-mcp-server
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
pip install -e ".[dev,all]"
```

## Development Workflow

### Server-only changes (most common)

1. Make your changes in `src/elt_mcp_server/`
2. Run tests: `pytest`
3. Run linters: `ruff check src tests && ruff format --check src tests`
4. Type check: `mypy src`
5. Security scan: `bandit -c pyproject.toml -r src`

### Testing with the VS Code Extension

If you need to test your server changes with the extension:

1. Clone the extension repo: `git clone https://github.com/Teradata-PE/devtools-elt-mcp-vscode-extension.git`
2. In VS Code, set `eltMcpServer.devSourcePath` to the path of this server repo
3. Press F5 in the extension repo to launch the Extension Development Host
4. The extension will install your local server via `pip install -e <path>`

### Publishing

Use `build_wheel.py` for building and publishing:

```bash
python build_wheel.py --bump dev:auto --publish    # TestPyPI
python build_wheel.py --bump release:1.0.0 --publish pypi --yes  # Production
```

## Code Style

- Ruff handles linting and formatting
- Mypy for type checking (strict mode on `src/`)
- Bandit for security scanning
- Pre-commit hooks enforce all of the above

## Testing

```bash
pytest                           # all tests
pytest tests/unit/               # unit tests only
pytest -k "test_name"            # single test by name
pytest --tb=short -q             # quiet output
```
