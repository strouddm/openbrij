# Brij

An open-source personal data connectivity layer for AI agents.

![Status: Alpha](https://img.shields.io/badge/status-alpha-orange)
![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-green)

Brij connects your personal data sources (CSV files, Google Sheets, and more) to AI agents via the [Model Context Protocol](https://modelcontextprotocol.io/). It gives agents structured, searchable access to your data without requiring you to hand over raw files or copy-paste context.

## Project Status

Brij is in early alpha. The core data model and storage layer are functional, but there are no connectors or MCP server yet. This is a good time to explore the codebase and contribute.

## Development Setup

```bash
git clone https://github.com/strouddm/openbrij.git
cd openbrij
pip install -e ".[dev]"
```

## Running Tests

```bash
pytest tests/ -v
```

## Linting

```bash
ruff check brij/
ruff format brij/
```

## Architecture

Brij models all data as **Entities** and **Signals**. An Entity is a node in a data graph (a source, collection, record, or field). Each Entity carries a list of Signals — typed key-value pairs that describe it (name, email, field values, summaries). This two-layer model lets Brij represent data from any source in a uniform way, enabling cross-source search and AI-friendly retrieval without source-specific logic.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to get involved.

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
