# Brownfield Cartographer

Codebase intelligence tool for brownfield dbt-style repositories. It builds:
- A module graph (project/module metadata + Python data-flow signals)
- A lineage graph (SQL and Python dataset dependencies)

## What It Does
- Parses `dbt_project.yml` to discover configured `model-paths`, `seed-paths`, and `macro-paths`
- Parses schema YAML files (`.yml` / `.yaml`) to extract models, sources, columns, and descriptions
- Resolves dbt `{{ doc("...") }}` references from `docs.md` blocks
- Computes per-file git change velocity for dataset nodes discovered via YAML
- Analyzes Python files with tree-sitter to capture:
  - Module nodes
  - File/database reads (`consumes`)
  - File/database writes (`produces`)
- Analyzes SQL lineage with `sqlglot`, including dbt/Jinja cleanup (`ref`, `source`, macros, comments/blocks)
- Emits JSON graph artifacts and a human-readable lineage text report

## Architecture
- `Surveyor`:
  - YAML + project config discovery
  - Git change velocity enrichment
  - Python tree-sitter analysis
- `Hydrologist`:
  - SQL lineage extraction across discovered model paths
  - Optional macro mapping for macro dependency edges
  - Merges precomputed non-SQL edges from Surveyor into lineage graph
- `Orchestrator`:
  - Runs Surveyor then Hydrologist
  - Marks orphaned lineage nodes
  - Saves artifacts to `.cartography/` and `lineage_final.txt`

## Installation
```bash
git clone https://github.com/nebiyou27/brownfield-cartographer.git
cd brownfield-cartographer
uv sync
```

## Usage
```bash
# Analyze any local repository path
uv run python -m src.cli analyze <path-to-repository>
```

Example:
```bash
uv run python -m src.cli analyze make-open-data
```

## Output Artifacts
Generated after each run:
- `.cartography/module_graph.json`
- `.cartography/lineage_graph.json`
- `lineage_final.txt` (project root)

## Project Structure
```text
brownfield-cartographer/
|-- src/
|   |-- cli.py
|   |-- orchestrator.py
|   |-- agents/
|   |   |-- surveyor.py
|   |   `-- hydrologist.py
|   |-- analyzers/
|   |   |-- dag_config_parser.py
|   |   |-- git_analyzer.py
|   |   |-- sql_lineage.py
|   |   `-- tree_sitter_analyzer.py
|   |-- graph/
|   |   `-- knowledge_graph.py
|   `-- models/
|       `-- schemas.py
|-- .cartography/
|-- pyproject.toml
`-- README.md
```

## Current Limitations
- SQL parsing still depends on heuristic Jinja cleanup; highly dynamic templates may be partial/inferred.
- Python analyzer currently targets `.py` files only.
- `lineage_final.txt` is a flat edge list (not grouped or deduplicated for presentation).
- Macro mapping uses discovered macro definitions from SQL macro files; unresolved macro calls are logged.
