# Contributing to devtools-etl-mcp-server

## Prerequisites

- Python 3.10+
- Git

## Setup

```bash
git clone https://github.com/Teradata-PE/devtools-etl-mcp-server.git
cd devtools-etl-mcp-server
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
pip install -e ".[dev,all]"
```

## Development Workflow

### Server-only changes (most common)

1. Make your changes in `src/teradata_etl_mcp_server/`
2. Run tests: `pytest`
3. Run linters: `ruff check src tests && ruff format --check src tests`
4. Type check: `mypy src`
5. Security scan: `bandit -c pyproject.toml -r src`


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
