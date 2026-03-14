# Brownfield Cartographer

Brownfield Cartographer maps dbt-style repositories into queryable architecture and lineage artifacts.

## What It Produces
- Module graph with project, YAML, Python module, and dataset nodes
- Lineage graph with SQL + Python + config-derived transformation edges
- Git change velocity enrichment on discovered files
- Optional LLM semantic outputs (purpose statements, drift checks, domain map, onboarding brief)
- Archivist outputs for handoff and auditing

## Core Pipeline
- `Surveyor`: project/YAML discovery + Python analysis + git velocity
- `Hydrologist`: SQL lineage + config lineage + edge/source merging
- `Semanticist` (optional): local Ollama-powered semantic enrichment
- `Archivist`: writes `CODEBASE.md`, `audit_trace.log`, and normalized onboarding brief

## Install
```bash
git clone https://github.com/nebiyou27/brownfield-cartographer.git
cd brownfield-cartographer
uv sync
```

## CLI Usage
Analyze a local repository:
```bash
uv run python -m src.cli analyze <path-to-repository>
```

Analyze a GitHub repository URL (auto-cloned to a temp dir):
```bash
uv run python -m src.cli analyze https://github.com/<org>/<repo>
```

Fast run without Semanticist:
```bash
uv run python -m src.cli analyze <path-to-repository> --no-semanticist
```

Incremental run (re-analyzes files changed since saved commit state):
```bash
uv run python -m src.cli analyze <path-to-repository> --incremental
```

Query existing artifacts:
```bash
uv run python -m src.cli query <path-to-repository> --ask "What produces mart.orders?"
```

Interactive query mode:
```bash
uv run python -m src.cli query <path-to-repository>
```

## Streamlit UI
```bash
uv run streamlit run app.py
```

The app runs analysis, visualizes module/lineage graphs, and shows node tables.

## Artifacts
Outputs are written to `.cartography/` in the current working directory:
- `module_graph.json`
- `lineage_graph.json`
- `CODEBASE.md`
- `audit_trace.log`
- `onboarding_brief.md`
- `state.json` (when incremental mode is used)
- `purpose_statements.json`, `drift_flags.json`, `domain_map.json`, `budget_summary.json` (when Semanticist runs)

Additional root artifact:
- `lineage_final.txt`
