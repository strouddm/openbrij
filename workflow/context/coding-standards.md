# Coding Standards

## Style Rules

- Line length: 100 (ruff enforced)
- `snake_case` for functions/variables, `PascalCase` for classes, `UPPER_SNAKE` for constants
- Imports: stdlib, then third-party, then local (ruff enforces)
- Type hints on all public function signatures and class attributes
- Docstrings on all public classes and methods (Google style)

## File & Naming Patterns

```
brij/connectors/gmail.py        → class GmailConnector(BaseConnector)
tests/connectors/test_gmail.py  → def test_fetch_empty_inbox_returns_empty_list()
brij/core/storage.py            → class Storage
tests/core/test_storage.py      → def test_store_records_persists_to_sqlite()
```

Pattern: `test_<method>_<scenario>_<expected>`

## Logging

```python
import logging

logger = logging.getLogger(__name__)
```

| Level | When |
|-------|------|
| `DEBUG` | Data access, connector lifecycle, SQL queries |
| `INFO` | Connector registered, sync complete, connection state changes |
| `WARNING` | Deprecated usage, retryable failures, bad config |
| `ERROR` | Unrecoverable failures, data integrity issues |

Rules:
1. No `print()` — all output through `logging`
2. No PII, credentials, or tokens in log messages
3. Use `extra={"connector": name, "operation": op}` for structured context
4. Log at public method boundaries, not deep in internals
5. Use `logger.exception()` for error tracebacks

## Error Handling

- Raise specific exceptions — never bare `Exception`
- Define hierarchy in `brij/exceptions.py`: `BrijError` -> `ConnectorError` -> `AuthenticationError`
- Never swallow exceptions silently — log at ERROR and re-raise or wrap
- Use context managers for resource cleanup (DB connections, file handles)

## Testing

- `tests/` mirrors `brij/` structure 1:1
- In-memory SQLite (`:memory:`) for every test — no shared state
- Mock external APIs in unit tests — no network calls
- Prefer pytest fixtures over setup/teardown
- Every PR includes tests for new functionality

## Commit Messages

```
<type>: <short description>

<optional body — explain why, not what>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `ci`

## Quality Gates

```bash
ruff check brij/           # Lint
ruff format --check brij/  # Format check
pytest tests/ -v           # Tests
```

All must pass before merge.
