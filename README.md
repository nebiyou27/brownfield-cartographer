## Project Name
Brownfield Cartographer

## One Line Description
A codebase intelligence tool that maps modular dependencies and data lineage in existing repositories.

## What Has Been Built (Interim Submission)
1. **The Surveyor** — parses dbt_project.yml and schema.yml to extract model metadata, column descriptions, and git change velocity
2. **The Hydrologist** — parses SQL transformation models using sqlglot, handles dbt Jinja templating (ref(), source(), set blocks, comment blocks), filters CTE aliases, and builds a data lineage graph
3. **Knowledge Graph** — NetworkX DiGraph storing all nodes and edges, serialized to JSON
4. **Orchestrator & CLI** — command line interface to trigger the full analysis pipeline

## Tech Stack
- Python 3.10+
- sqlglot (SQL parsing)
- networkx (graph construction)
- pydantic (data schemas)
- tree-sitter (AST parsing - coming in final)
- pyyaml (YAML parsing)
- uv (dependency management)

## Target Codebase Analyzed
- jaffle_shop by dbt-labs (https://github.com/dbt-labs/jaffle_shop)

## How to Install
```bash
# Clone the repo
git clone <https://github.com/nebiyou27/brownfield-cartographer.git>
cd brownfield-cartographer

# Create and activate virtual environment (Windows)
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install sqlglot networkx pydantic tree-sitter pyyaml
```

## How to Run
```bash
# Run the full analysis pipeline
python -m src.cli analyze jaffle_shop

# Run just the SQL lineage analyzer directly
python -m src.analyzers.sql_lineage

# Run just the YAML config parser directly  
python -m src.analyzers.dag_config_parser
```

## Output Files
Both files are generated in the `.cartography/` directory:
- `lineage_graph.json` — full table-to-table data flow graph
- `module_graph.json` — repository structure and file metadata

## Verified Lineage Output
The tool correctly extracts these 8 edges from jaffle_shop:
```
raw_customers  → stg_customers
raw_orders     → stg_orders
raw_payments   → stg_payments
stg_customers  → customers
stg_orders     → orders
stg_payments   → orders
stg_orders     → customers
stg_payments   → customers
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
├── jaffle_shop/                        # Target codebase (cloned)
├── .cartography/                       # Generated output files
│   ├── lineage_graph.json
│   └── module_graph.json
├── pyproject.toml
└── README.md
```

## Known Limitations (Interim Submission)
- `module_graph.json` links are empty — file relationship edges coming in final submission via tree-sitter
- `orders_status` column description contains unresolved Jinja `doc()` reference — will be handled in final submission
- tree-sitter AST analysis not yet implemented
- Semanticist agent (LLM-powered purpose statements) not yet built
- Archivist agent (CODEBASE.md generation) not yet built
- Navigator agent (LangGraph query interface) not yet built

## Coming in Final Submission
- tree-sitter multi-language AST parsing (Python + SQL + YAML)
- LLM-powered semantic purpose statements per module
- Documentation drift detection
- CODEBASE.md living context file
- onboarding_brief.md answering the 5 FDE Day-One questions
- Navigator agent with 4 query tools
- Second target codebase (Apache Airflow examples)

## README Format Requirements
- Use clean markdown with headers and code blocks
- Keep language simple and clear
- Do not use excessive bullet points in prose sections
- Add a badge at the top for Python version: ![Python](https://img.shields.io/badge/python-3.10+-blue)
- Keep the tone professional but readable
- Total length: approximately 100-150 lines