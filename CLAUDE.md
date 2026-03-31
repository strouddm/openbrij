# CLAUDE.md

## Project

Brij is an open-source personal data connectivity layer for AI agents. Python. Apache-2.0 license. The project connects personal data sources to AI agents via the Model Context Protocol (MCP).

## Project Structure

```
brij/
├── CLAUDE.md
├── README.md
├── LICENSE                    # Apache-2.0
├── CONTRIBUTING.md
├── CODE_OF_CONDUCT.md
├── SECURITY.md
├── CHANGELOG.md
├── pyproject.toml             # Single source of truth for packaging, deps, tooling
├── .gitignore
│
├── .github/
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.md
│   │   ├── feature_request.md
│   │   └── connector_request.md
│   ├── PULL_REQUEST_TEMPLATE.md
│   └── workflows/
│       ├── ci.yml             # Tests + lint on every PR
│       └── release.yml        # Publish to PyPI on tagged release
│
├── brij/                      # Core Python package
├── tests/                     # Mirrors brij/ structure
├── spec/                      # Design specs — architecture decisions live here
└── docs/                      # User-facing documentation
```

## Branch Strategy

- `main` is the stable branch. Always passes CI. All changes come through PRs.
- Feature branches: `feature/short-description`
- Bug fixes: `fix/short-description`
- No direct commits to main.

## CI Pipeline

GitHub Actions runs on every PR to main:

1. **Lint** — `ruff check brij/`
2. **Test** — `pytest tests/ -v` on Python 3.10, 3.11, 3.12, 3.13
3. **All checks must pass before merge.**

Release workflow triggers on version tags (`v0.1.0`, `v0.2.0`):
1. Runs full test suite
2. Builds package
3. Publishes to PyPI

## Contribution Workflow

1. Contributor opens an issue or picks an existing one
2. Fork → branch → code → PR
3. PR template asks: what does this change, how was it tested, does it follow the specs
4. CI runs automatically
5. Maintainer reviews — code quality, test coverage, spec alignment
6. Squash merge into main

## Issue Templates

Three types:
- **Bug report** — steps to reproduce, expected vs actual, environment
- **Feature request** — use case, proposed solution, alternatives considered
- **Connector request** — which data source, why it matters, API availability

Connector requests are signal collectors — they tell us what the community wants built next.

## Code Standards

- Python 3.10+ with type hints on all public APIs
- Dataclasses over Pydantic (minimal dependencies)
- Docstrings on all public classes and methods
- Line length 100 (ruff enforced)
- No print statements in library code — use logging
- Every module has tests. Tests are documentation.

## Testing

- `pytest` as the test runner
- Tests live in `tests/`, mirroring `brij/` structure
- Every PR must include tests for new functionality
- All tests must pass before merge
- Use in-memory SQLite (`:memory:`) for test isolation

## Releases

Follow semantic versioning:
- `0.x.y` — pre-1.0, breaking changes allowed between minor versions
- Tag format: `v0.1.0`
- Every release gets a CHANGELOG entry describing what changed

## Documentation

- `README.md` — what Brij is, install, quickstart, how it works, how to contribute
- `docs/` — user guides and technical references
- `spec/` — internal design specs that drive implementation decisions
- Code docstrings — the primary reference for contributors

## Security

- `SECURITY.md` explains how to report vulnerabilities responsibly
- No credentials in code or config files committed to the repo
- SQLite database files are in `.gitignore`

## Detailed Reference

- [workflow/context/prd.md](workflow/context/prd.md) — MVP scope, connector interface contract, data rules
- [workflow/context/tech-stack.md](workflow/context/tech-stack.md) — toolchain quick reference and commands
- [workflow/context/coding-standards.md](workflow/context/coding-standards.md) — how to write code for this project (logging, testing, error handling)
- [workflow/context/project-charter.md](workflow/context/project-charter.md) — mission, priorities, how to contribute

## Commands

```bash
pytest tests/ -v          # run tests
ruff check brij/          # lint
ruff format brij/         # format
pip install -e ".[dev]"   # install locally for development
```
