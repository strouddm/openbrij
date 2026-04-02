# Brij

![Status: Alpha](https://img.shields.io/badge/status-alpha-orange)
![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-green)

Brij is an open-source personal data connectivity layer for AI agents. It connects your data sources — CSV files, Google Sheets, and more — to AI agents via the [Model Context Protocol](https://modelcontextprotocol.io/), giving them structured, searchable access without handing over raw files.

## Install

```bash
pip install brij
```

## Quick Start

### Connect a CSV and search it

```bash
brij connect csv_local --path contacts.csv
brij search "engineers in San Francisco"
```

### Connect Google Sheets and search it

```bash
brij connect google_sheets
# Opens browser for OAuth, then lets you select a spreadsheet
brij search "Q1 revenue"
```

### Start the MCP server

```bash
brij serve
```

Add Brij to Claude Desktop by editing `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "brij": {
      "command": "brij",
      "args": ["serve"]
    }
  }
}
```

Once connected, Claude can discover your data, search across sources, and write back to them.

## How It Works

**Entity/Signal model.** Brij models all data as Entities (source, collection, record, field) and Signals (typed key-value pairs like name, email, or field values). This uniform representation lets it work across any data source without source-specific logic.

**Three MCP tools.** The server exposes `brij_discover` (catalog of connected sources), `brij_search` (natural language search across sources), and `brij_write` (create, add, update, delete records).

**Hybrid search.** Queries run against both semantic embeddings and structured signals, so agents find relevant records whether they match keywords exactly or by meaning.

## CLI Reference

```
brij connect <connector>   Connect a data source (csv_local, google_sheets)
brij status                Show connected sources and entity counts
brij search <query>        Search across connected data
brij serve                 Start the MCP server
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
