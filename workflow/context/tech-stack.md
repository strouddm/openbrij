# Tech Stack

## Quick Reference

| Component | Choice |
|-----------|--------|
| Language | Python 3.10+ |
| CI matrix | 3.10, 3.11, 3.12, 3.13 |
| Data models | `dataclasses` (stdlib) |
| Database | SQLite via `sqlite3` (stdlib) |
| Logging | `logging` (stdlib) |
| Test runner | `pytest` |
| Linter | `ruff check` |
| Formatter | `ruff format` |
| Config | `pyproject.toml` |

## Commands

```bash
pip install -e ".[dev]"   # Install for development
ruff check brij/          # Lint
ruff format brij/         # Format
pytest tests/ -v          # Run tests
```

## Dependency Policy

- **stdlib first.** Only add a dependency if stdlib genuinely cannot do the job.
- `dataclasses` not Pydantic. `sqlite3` not SQLAlchemy. `logging` not structlog.
- New dependencies require justification in the PR.

## CI

- **On every PR:** ruff check + pytest on Python 3.10–3.13
- **On version tag (`v*`):** full test suite + build + publish to PyPI
