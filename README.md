# Brij

The open-source bridge between your data and AI agents.

## The problem

- **Discoverability.** AI agents don't know what data you have or where it is. Brij indexes connected sources into a searchable catalog. Any agent can find what it needs without the user pointing it there.
- **Zero manual work.** No exporting, uploading, or copy-pasting. Connect a source once, every agent searches it instantly in conversation.
- **Cross-source search.** Data lives in silos. Brij bridges them. One query across all sources, one answer.

## How it works

Connect a data source. Brij maps its structure, indexes the contents, and serves it to AI agents over the Model Context Protocol (MCP).

```bash
pip install brij
brij connect google_sheets    # OAuth in browser, select a spreadsheet
brij search "Q1 revenue"      # search across all connected sources
brij serve                     # start the MCP server for agents
```

Currently supports Google Sheets and CSV. More connectors coming. You can build your own.

## Under the hood

Brij models all data as Entities and Signals. An Entity is a node in a data graph (source, collection, record, field). Each Entity carries Signals -- typed key-value pairs that describe it. This uniform representation lets Brij work across any source without source-specific retrieval logic.

Search combines semantic embeddings with keyword matching against a local SQLite store. No external services, no API calls, no dependencies beyond what ships with the package.

## Security

Data never leaves your machine. Local SQLite database, local OAuth tokens, local MCP server. There is no cloud component, no telemetry, no data collection. The code is open source -- read every line.

Source access uses standard OAuth. You grant permission in your browser, and you can revoke it anytime. Brij never sees your passwords.

Report vulnerabilities responsibly via [SECURITY.md](SECURITY.md).

## Build a connector

Connectors are Python classes that implement a standard interface: authenticate, discover, read, write. If you can call an API, you can build a connector. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Current status

Alpha. Google Sheets and CSV connectors are live. The MCP server connects to Claude. See [open issues](https://github.com/strouddm/openbrij/issues) for what's next.

## License

Apache-2.0 -- see [LICENSE](LICENSE).
