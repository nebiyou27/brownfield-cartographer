# Brownfield Cartographer Architecture (Current)

This document reflects the architecture currently implemented in the codebase.

## End-to-End Flow

```mermaid
flowchart LR
    IN[Target Repository<br/>Local path or GitHub URL]
    CLI[CLI: src/cli.py<br/>analyze / query]
    ORCH[Orchestrator]

    S[Surveyor]
    H[Hydrologist]
    SE[Semanticist<br/>(optional: --no-semanticist)]
    A[Archivist]

    MG[(Module Graph)]
    LG[(Lineage Graph)]

    OUT1[.cartography/module_graph.json]
    OUT2[.cartography/lineage_graph.json]
    OUT3[.cartography/CODEBASE.md]
    OUT4[.cartography/audit_trace.log]
    OUT5[.cartography/onboarding_brief.md]
    OUT6[Semantic JSONs<br/>purpose_statements, drift_flags,<br/>domain_map, budget_summary]
    OUT7[lineage_final.txt]

    IN --> CLI --> ORCH
    ORCH --> S
    S --> MG
    S --> LG
    ORCH --> H
    H --> LG
    ORCH --> SE
    SE --> OUT6
    ORCH --> A
    MG --> A
    LG --> A
    A --> OUT3
    A --> OUT4
    A --> OUT5
    MG --> OUT1
    LG --> OUT2
    ORCH --> OUT7
```

## Agent Responsibilities

- `Surveyor`
  - Parses YAML/dbt config and schema metadata
  - Analyzes Python files (tree-sitter router)
  - Computes git change velocity
  - Seeds module graph and non-SQL lineage edges

- `Hydrologist`
  - Extracts SQL lineage from configured model paths
  - Includes config-derived lineage edges
  - Merges SQL/Python/config edge variants and source labels

- `Semanticist` (optional)
  - Uses local Ollama models for purpose statements, doc drift, domain clustering, and day-one synthesis
  - Persists semantic outputs under `.cartography/`

- `Archivist`
  - Produces `CODEBASE.md`, `audit_trace.log`, and normalized `onboarding_brief.md`
  - Highlights structural risks like cycles, orphans, and low-confidence edges

## Execution Modes

- Full analysis:
  - `uv run python -m src.cli analyze <repo>`

- Fast structural mode (skip Semanticist):
  - `uv run python -m src.cli analyze <repo> --no-semanticist`

- Incremental mode:
  - `uv run python -m src.cli analyze <repo> --incremental`
  - Reuses saved graphs and re-analyzes changed files using commit diff state

- Query mode (Navigator):
  - `uv run python -m src.cli query <repo> --ask "<question>"`
  - or interactive `query` without `--ask`

## UI Entry Point

- `app.py` provides a Streamlit interface to run analysis and visualize module/lineage graphs.
