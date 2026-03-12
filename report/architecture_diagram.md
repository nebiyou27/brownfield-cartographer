# Brownfield Cartographer Architecture

## Current Architecture (Implemented)

```mermaid
flowchart LR
    IN[Target Codebase\nGitHub repo or local path]

    O[Orchestrator]
    S[Surveyor]
    H[Hydrologist]

    KG[(Knowledge Graph)]
    OUT[Artifacts\nmodule_graph.json\nlineage_graph.json\nlineage_final.txt]

    IN --> O
    O --> S
    S -->|module metadata + python data-flow + velocity| KG
    O --> H
    H -->|SQL lineage + dependency edges| KG
    KG --> OUT
```

## Planned Architecture (Final Vision)

```mermaid
flowchart LR
    IN[Target Codebase\nGitHub repo or local path]

    subgraph P[Agent Pipeline]
        S[Surveyor]
        H[Hydrologist]
        M[Semanticist]
        A[Archivist]
    end

    KG[(Central Knowledge Graph)]
    OUT[Generated Artifacts\nCODEBASE.md\nonboarding brief\nlineage outputs\naudit trace]

    IN --> S
    IN --> H
    IN --> M

    S -->|module metadata, schema, change velocity| KG
    H -->|lineage edges, source-to-model dependencies| KG
    M -->|purpose statements, domain semantics, drift flags| KG

    KG -->|consolidated graph context| A
    A --> OUT

    S -. pipeline coordination .-> H
    H -. pipeline coordination .-> M
    M -. pipeline coordination .-> A
```

## Notes

- Implemented today: `Surveyor`, `Hydrologist`, and `Orchestrator`.
- Planned for final phase: `Semanticist` and `Archivist` (plus query/navigation layer).
- This split avoids mixing shipped architecture with roadmap architecture.
