# Brownfield Cartographer — Interim Submission Report

**Name:** Nebiyou Abebe  
**Date:** March 11, 2026  
**Submission Type:** Interim

---

## Section 1: RECONNAISSANCE.md — Manual Day-One Analysis of make-open-data

Before I wrote a single line of code for the Brownfield Cartographer, I spent thirty minutes manually exploring the make-open-data repository—a production-grade French open data platform. I opened every file, read the SQL models, and traced the macro-heavy dependencies by hand. The goal was to answer five questions that any FDE needs answered before they can safely make changes to an unfamiliar data pipeline.

**Question 1 — Primary data ingestion path:**

The data enters the system through two main channels. The first is the `4_seeds/` folder, which contains files like `logement_2020_valeurs.csv`—a raw CSV with French housing values that dbt loads directly. The second channel is defined in `1_data/sources/schema.yml`, where external data sources pulled from French government APIs and open data portals are specified. From these ingestion points, the data flows through a layered architecture: `1_data/intermediaires/` handles the initial cleaning, `1_data/prepare/` shapes that data into business-ready tables, and `2_analyses/` produces the analytical outputs.

**Question 2 — Critical output datasets:**

The most critical outputs are the prepared tables in `1_data/prepare/` upon which all downstream analyses rely. Specifically, `demographie_communes` and `demographie_iris` provide the foundational population statistics at the municipality and neighborhood levels. `revenu_commune` provides essential income data per municipality, while `infos_communes` serves as the geographic metadata backbone for almost all other datasets.

**Question 3 — Blast radius:**

The highest-risk single point of failure I identified is `1_data/intermediaires/geographie/postes_communes.sql`. This is the intermediate model responsible for mapping postal codes to municipalities. Geographic data is the bedrock of this entire project; demographic, income, and health data are all aggregated or reported based on these geographic units. If this mapping model breaks, the error would cascade across all five business domains, leading to incorrect or incomplete results in every single downstream prepared table.

**Question 4 — Business logic distribution:**

The business logic in this repository follows a hybrid pattern. The domain-specific transformation logic is distributed throughout the subfolders of `1_data/prepare/`. However, the complex, reusable computational logic is concentrated in `5_macros/`. This folder contains 15 SQL macro files, including a geographic k-nearest-neighbors algorithm and real estate aggregation logic. This means the full logic isn't visible just by reading model files; you must also trace the macro calls.

**Question 5 — Recent changes (90 days):**

By analyzing the git log, I observed that the highest change velocity is concentrated in the census and geography domain models, specifically the demography and habitat prepared models. This indicates that active development is driven by updates to the source government data itself—such as new census releases or updated geographic boundaries—rather than changes in the core infrastructure.

**Difficulty Reflection:**

The hardest part of doing this analysis manually was the macro system and the French language. SQL models frequently call complex macros like `{{ aggreger_ventes_immobiliers(...) }}`, forcing me to constantly jump between model files and macro definitions. Additionally, navigating a codebase filled with files like `ventes_immobilieres_renomee.sql` requires constant translation. This experience reinforced why an LLM-powered Semanticist agent is so critical; modern LLMs can handle French variable names natively, bridging a gap that would otherwise significantly slow down a new engineer's onboarding.

---

## Section 2: Architecture Diagram

```mermaid
flowchart TD
    A([GitHub Repo / Local Path])

    A --> B

    B["SURVEYOR AGENT\n─────────────────\nReads: dbt_project.yml, schema.yml\nProduces: ModuleNodes\nmetadata + git velocity"]

    B -->|ModuleNodes| C

    C["HYDROLOGIST AGENT\n─────────────────\nReads: .sql model files via sqlglot\nProduces: TransformationEdges\ndata lineage DAG"]

    C -->|TransformationEdges| D

    D["KNOWLEDGE GRAPH\n─────────────────\nNetworkX DiGraph\nStores all nodes and edges\nOutputs: module_graph.json\n+ lineage_graph.json"]

    D --> E

    E["SEMANTICIST AGENT\n─────────────────\nUses: Gemini Flash LLM\nProduces: Purpose statements\n+ doc drift flags\n\nFinal Submission"]

    E -->|Purpose Statements| F

    F["ARCHIVIST AGENT\n─────────────────\nProduces: CODEBASE.md\nonboarding_brief.md\n+ audit trace log\n\nFinal Submission"]

    F -->|Living Documents| G

    G["NAVIGATOR AGENT\n─────────────────\nInterface: LangGraph\n4 query tools\n\nFinal Submission"]

    G --> H([FDE Engineer — Day-One Ready])
```

The pipeline runs in this specific order because each agent depends on the output of the one before it. The Surveyor runs first because it reads the project configuration files, which tell us where the models live and what they are called — without this structural skeleton, the other agents would not know what to analyze. The Hydrologist runs second because it needs the model names from the Surveyor to correctly assign target tables when parsing SQL dependencies. The Knowledge Graph layer then stores everything the first two agents discovered into a unified NetworkX graph structure, serialized as JSON for downstream consumption. In the final submission, the Semanticist will run third because it needs the complete graph to understand each module in context before generating LLM-powered purpose statements. The Archivist runs fourth to package all of the accumulated intelligence into human-readable documents. Finally, the Navigator will provide a query interface so that an FDE can ask natural language questions against the assembled knowledge base.

---

## Section 3: Progress Summary

As of this interim submission, the core two-agent pipeline is fully operational and verified against the production-grade `make-open-data` repository. The Surveyor agent successfully parses `dbt_project.yml` to dynamically discover model paths, recursively finds and parses all `schema.yml` files (even in complex subfolder structures like those in `make-open-data`), and resolves dbt `doc()` references into human-readable text. The Hydrologist agent reads every `.sql` file, strips all dbt Jinja templating, and parses the cleaned SQL with multi-dialect fallback. The tool correctly handles the high-velocity domain folders (`1_data/prepare/`) and captures the foundational dependencies of the geography mapping models. The full pipeline is invoked through a single CLI command and produces two JSON graphs in `.cartography/`, along with a human-readable lineage export. I have verified that the tool successfully captures the core architecture of the French open-data pipeline, identifying dozens of model dependencies that would take an engineer hours to map manually.

The final submission will add several major capabilities that are not yet implemented. Tree-sitter AST parsing will be integrated to generate module graph links — the edges that connect files to each other based on imports and references, which is why the `module_graph.json` currently has an empty `links` array. The Semanticist agent will use the Gemini Flash LLM to read each module's actual code and generate plain English purpose statements, flagging any cases where documentation contradicts implementation. The Archivist agent will package all accumulated intelligence into a `CODEBASE.md` living context file and an `onboarding_brief.md` that directly answers the five FDE Day-One questions. The Navigator agent will provide a LangGraph-powered query interface with four specialized tools for exploring the knowledge graph. I also plan to analyze a second target codebase, likely the Apache Airflow example DAGs repository, to demonstrate that the tool generalizes beyond dbt projects.

---

## Section 4: Early Accuracy Observations

The performance on `make-open-data` has highlighted the robustness of the system. The lineage extraction successfully maps the flow from raw source definitions in `1_data/sources/` to the final prepared tables. For example, it correctly identifies how demographic data relies on the foundational geographic mapping in `postes_communes.sql`. There are zero false positives from SQL aliases, thanks to our CTE filtering logic.

The doc resolution system proved critical for this codebase, as many column descriptions in French government schemas rely on shared documentation blocks. By resolving `doc()` references, we've made the automated output significantly more readable than the raw YAML files. Additionally, the git change velocity measurements accurately identified the high-activity areas in census and geography models, providing exactly the kind of technical signal an FDE needs when triaging a new codebase.

---

## Section 5: Known Gaps and Plan for Final Submission

The most visible gap in the current output is the empty `links` array in `module_graph.json`. The module graph has nodes — it knows about every model and source in the project — but it does not yet have edges connecting files to each other based on their import relationships. This is because the file-to-file relationship edges require AST-level parsing that goes beyond what the YAML and SQL analyzers currently provide. I plan to close this gap in the final submission by integrating tree-sitter to parse SQL and YAML files at the syntax tree level, extracting cross-file references that can be represented as module graph edges.

The Semanticist agent is the most technically ambitious component still to be built. It will use the Gemini Flash LLM to read each module's source code and generate a one-sentence purpose statement in plain English. More importantly, it will compare that generated purpose against any existing documentation (model descriptions from `schema.yml`, column descriptions, inline comments) and flag cases where the documentation says one thing but the code does another. I expect this documentation drift detection to be the most valuable feature for FDE onboarding, since stale documentation is one of the most common sources of confusion on brownfield projects.

The Archivist and Navigator agents are planned as the final integration layer. The Archivist will take everything the other agents have discovered and produce two documents: a `CODEBASE.md` file that serves as a living context reference for the entire repository, and an `onboarding_brief.md` that directly answers the five FDE Day-One questions using data from the knowledge graph rather than manual analysis. The Navigator will wrap the knowledge graph in a LangGraph-powered query interface with four specialized tools, allowing an FDE to ask natural language questions like "what breaks if I change this table?" and receive answers grounded in the actual dependency data. I also intend to run the complete pipeline against a second target codebase — likely the Apache Airflow example DAGs — to demonstrate that the architecture generalizes beyond dbt projects.

---

## Conclusion

This interim submission demonstrates a working two-agent pipeline that accurately maps a complex, production-grade open data repository. The Surveyor and Hydrologist agents together parse multi-layered YAML configurations, strip Jinja templating, resolve documentation references, and produce verified dependency graphs—all in a multilingual French environment. The tool correctly captures the foundational geographic mapping dependencies that underpin the entire `make-open-data` project, providing an automated "ground truth" that aligns with my manual reconnaissance. What remains to be built—tree-sitter AST parsing, LLM-powered semantic analysis, and the interactive query interface—will transform this foundation into a complete codebase intelligence platform capable of handling the largest and most complex brownfield repositories.
