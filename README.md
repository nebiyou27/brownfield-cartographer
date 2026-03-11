# Brownfield Cartographer ![Python](https://img.shields.io/badge/python-3.10+-blue)

## One Line Description
A codebase intelligence tool that maps modular dependencies and data lineage in existing repositories.

## What Has Been Built (Interim Submission)
1. **The Surveyor** — parses `dbt_project.yml` and `schema.yml` to extract model metadata, column descriptions, and git change velocity. Successfully resolves dbt `doc()` references into human-readable text.
2. **The Hydrologist** — parses SQL transformation models using `sqlglot`, handles dbt Jinja templating (`ref()`, `source()`, set blocks, comment blocks), filters CTE aliases, and builds a data lineage graph.
3. **Knowledge Graph** — NetworkX DiGraph storing all nodes and edges, serialized to JSON and human-readable text.
4. **Orchestrator & CLI** — command line interface to trigger the full analysis pipeline.

## Tech Stack
- Python 3.10+
- `sqlglot` (SQL parsing)
- `networkx` (graph construction)
- `pydantic` (data schemas)
- `tree-sitter` (AST parsing - coming in final)
- `pyyaml` (YAML parsing)
- `uv` (dependency management)

## Target Codebase Analyzed
- **make-open-data** — A production-grade French open data platform (https://github.com/make-open-data/make-open-data)

## How to Install
```bash
# Clone the repo
git clone <https://github.com/nebiyou27/brownfield-cartographer.git>
cd brownfield-cartographer

# Install dependencies using uv (recommended)
uv sync
```

## How to Run
```bash
# Run the full analysis pipeline on make-open-data
uv run python -m src.cli analyze make-open-data

# The results will be saved to .cartography/ and lineage_final.txt
```

## Output Files
Both files are generated in the `.cartography/` directory:
- `lineage_graph.json` — full table-to-table data flow graph
- `module_graph.json` — repository structure and file metadata
- `lineage_final.txt` — human-readable lineage report with source traceability (project root)

## Verified Lineage Output (Sample)
The tool correctly extracts dozens of edges from make-open-data. Sample output:
```
infos_communes      → infos_iris               (from .../geographie/infos_iris.sql)
cog_poste           → postes_communes          (from .../geographie/postes_communes.sql)
ventes_immobilieres → ventes_immobilieres_enrichies (from .../foncier/ventes_immobilieres_enrichies.sql)
```

## Project Structure
```
brownfield-cartographer/
├── src/
│   ├── cli.py                          # Entry point
│   ├── orchestrator.py                 # Pipeline manager
│   ├── agents/
│   │   ├── surveyor.py                 # YAML + git velocity analysis
│   │   └── hydrologist.py              # Data lineage graph builder
│   ├── analyzers/
│   │   ├── sql_lineage.py              # sqlglot SQL parser
│   │   ├── dag_config_parser.py        # dbt YAML parser
│   │   └── tree_sitter_analyzer.py     # AST parser (final submission)
│   ├── graph/
│   │   └── knowledge_graph.py          # NetworkX wrapper
│   └── models/
│       └── schemas.py                  # Pydantic data schemas
├── make-open-data/                     # Target codebase (cloned)
├── .cartography/                       # Generated output files
│   ├── lineage_graph.json
│   └── module_graph.json
├── pyproject.toml
└── README.md
```

## Known Limitations (Interim Submission)
- `module_graph.json` links are empty — file relationship edges coming in final submission via tree-sitter
- tree-sitter AST analysis not yet implemented
- Semanticist agent (LLM-powered purpose statements) not yet built
- Archivist agent (CODEBASE.md generation) not yet built
- Navigator agent (LangGraph query interface) not yet built

## Coming in Final Submission
- tree-sitter multi-language AST parsing (Python + SQL + YAML)
- LLM-powered semantic purpose statements per module
- Documentation drift detection
- `CODEBASE.md` living context file
- `onboarding_brief.md` answering the 5 FDE Day-One questions
- Navigator agent with 4 query tools
- Second target codebase (Apache Airflow examples)
