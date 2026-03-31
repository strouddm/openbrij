# Project Charter

## Mission

Build the standard open-source bridge between personal data and AI agents, using the Model Context Protocol.

## What We're Building First

1. Connector base class and interface (see [prd.md](prd.md) for the contract)
2. SQLite storage layer
3. Reference connector proving the pattern
4. MCP server exposing connectors to agents
5. Package and publish v0.1 to PyPI

## How to Contribute

- Fork, branch, PR. That's it.
- Pick an open issue or propose one.
- Follow [coding-standards.md](coding-standards.md).
- Tests must pass on all Python versions.
- Ask questions in issues — no question is too basic.
- First PR? Welcome. We'll help you land it.

## Versioning

- Semantic versioning: `0.x.y`
- Breaking changes are fine in `0.x` — document them in CHANGELOG
- Tag format: `v0.1.0`

## License

Apache-2.0
