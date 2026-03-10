# Brownfield Cartographer — Interim Submission Report

**Name:** Nebiyou Abebe  
**Date:** March 11, 2026  
**Submission Type:** Interim

---

## Section 1: RECONNAISSANCE.md — Manual Day-One Analysis of jaffle_shop

Before I wrote a single line of code for the Brownfield Cartographer, I spent thirty minutes manually exploring the jaffle_shop repository the way a new Field Data Engineer would on their first day. I opened every file, read every SQL query, and traced every dependency by hand. The goal was to answer five questions that any FDE needs answered before they can safely make changes to an unfamiliar data pipeline.

**Question 1 — Where does data enter this system?**

The primary data ingestion path starts with three CSV seed files in the `seeds/` folder: `raw_customers.csv`, `raw_orders.csv`, and `raw_payments.csv`. These are the true entry points of the entire pipeline. Nothing feeds into them — they represent the raw source data that everything downstream depends on. The three staging models (`stg_customers.sql`, `stg_orders.sql`, `stg_payments.sql`) are the first SQL transformations that touch this raw data, each performing simple cleaning and renaming before passing data further down the chain.

**Question 2 — What are the most critical output datasets?**

The `customers` and `orders` tables, built by `customers.sql` and `orders.sql` respectively, are the two most critical outputs. Everything else in the pipeline exists to produce these two tables. They sit at the very bottom of the dependency graph, aggregating data from all three staging models. If a downstream dashboard or analytics query is consuming data from jaffle_shop, it is almost certainly reading from one of these two tables.

**Question 3 — What is the blast radius if a critical module fails?**

If `stg_payments.sql` fails, both `orders` and `customers` break downstream. This was genuinely surprising to discover. At first glance, it looks like a simple staging model — just a pass-through that renames columns from `raw_payments`. But because both final models join against payment data, a failure in this single staging file cascades to every output table in the system. You only see this by tracing the full dependency chain, which is exactly the kind of hidden risk that manual exploration can miss if you are in a hurry.

**Question 4 — Where does the business logic actually live?**

The business logic is concentrated almost entirely in `customers.sql` and `orders.sql`. These files contain multi-CTE queries that join staging tables, aggregate payment amounts by method, compute customer lifetime metrics like total order count and most recent order date, and pivot payment types into separate columns. By contrast, the three staging models are simple pass-through cleaning layers — they select columns, rename a few fields, and cast types, but they contain almost no business logic of their own.

**Question 5 — Which files change most frequently?**

Using git history, I found that `customers.sql` and `orders.sql` have two commits each, while the staging models have only one commit each. Active development is happening in the final transformation layer, not in the staging models. This makes sense — the staging models were written once as straightforward cleaning layers, while the final models are where new business requirements get implemented and iterated on.

**Difficulty Reflection:**

The hardest part of doing this analysis manually was the Jinja templating. Every SQL file in jaffle_shop uses dbt's `{{ ref('stg_customers') }}` syntax instead of plain table names. This means that as I traced the lineage from file to file, I had to mentally translate every `ref()` call into the actual table it pointed to, then find that table's SQL file, then read its `ref()` calls, and so on. Across five SQL files with multiple CTEs each, this mental translation process took about twenty minutes by hand. That is exactly the problem the Brownfield Cartographer is designed to solve — the tool strips the Jinja, parses the SQL, and builds the complete dependency graph in under two seconds.

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

As of this interim submission, the core two-agent pipeline is fully operational and verified against a real dbt project. The Surveyor agent successfully parses `dbt_project.yml` to dynamically discover model paths, recursively finds and parses all `schema.yml` files to extract model names, column descriptions, and dataset metadata, resolves dbt `doc()` references from `docs.md` into actual human-readable text, and computes git change velocity for every analyzed file. The Hydrologist agent reads every `.sql` file in the models directory, strips all four types of dbt Jinja templating (`ref()`, `source()`, logic blocks, and comment blocks), parses the cleaned SQL using sqlglot with multi-dialect fallback support, correctly identifies and filters out CTE aliases so that only real physical table dependencies appear in the graph, and produces verified TransformationEdge objects that are stored in a NetworkX directed graph. The full pipeline is invoked through a single CLI command and produces two JSON output files in the `.cartography/` directory. I have verified that the tool correctly extracts all eight lineage edges from jaffle_shop with zero false positives and zero missed dependencies.

The final submission will add several major capabilities that are not yet implemented. Tree-sitter AST parsing will be integrated to generate module graph links — the edges that connect files to each other based on imports and references, which is why the `module_graph.json` currently has an empty `links` array. The Semanticist agent will use the Gemini Flash LLM to read each module's actual code and generate plain English purpose statements, flagging any cases where documentation contradicts implementation. The Archivist agent will package all accumulated intelligence into a `CODEBASE.md` living context file and an `onboarding_brief.md` that directly answers the five FDE Day-One questions. The Navigator agent will provide a LangGraph-powered query interface with four specialized tools for exploring the knowledge graph. I also plan to analyze a second target codebase, likely the Apache Airflow example DAGs repository, to demonstrate that the tool generalizes beyond dbt projects.

---

## Section 4: Early Accuracy Observations

The SQL lineage extraction has proven to be highly accurate on jaffle_shop. All eight expected dependency edges are correctly identified: three edges from raw seed tables to their corresponding staging models, three edges from staging models to the `orders` output, and two edges from staging models to the `customers` output. There are zero false positives — no CTE aliases, no self-references, and no phantom tables appear in the output. This accuracy comes from the combination of thorough Jinja stripping (which handles all four dbt tag types) and explicit CTE filtering (which identifies every `WITH` clause alias and excludes it from the final edge list).

The YAML metadata extraction is also working correctly, with one notable edge case that I fixed during development. The `orders` table's `status` column had its description stored as `{{ doc("orders_status") }}` — a raw Jinja string rather than actual text. I implemented a doc resolution system that reads `docs.md`, extracts named documentation blocks, and substitutes them into column descriptions at parse time. After the fix, the status column correctly displays the full markdown table describing all five order statuses. This kind of mid-development gap discovery and fix is exactly the iterative process I expect to continue through the final submission.

The git change velocity measurements are producing meaningful signal even on a small repository like jaffle_shop. The `schema.yml` files in the root models directory show a velocity of 2 (two commits), while the staging `schema.yml` shows a velocity of 1 (one commit). This aligns with the manual observation that active development is concentrated in the final transformation layer. On a larger production repository, these velocity numbers would help an FDE quickly identify which parts of the codebase are actively changing and therefore most likely to need attention during onboarding.

---

## Section 5: Known Gaps and Plan for Final Submission

The most visible gap in the current output is the empty `links` array in `module_graph.json`. The module graph has nodes — it knows about every model and source in the project — but it does not yet have edges connecting files to each other based on their import relationships. This is because the file-to-file relationship edges require AST-level parsing that goes beyond what the YAML and SQL analyzers currently provide. I plan to close this gap in the final submission by integrating tree-sitter to parse SQL and YAML files at the syntax tree level, extracting cross-file references that can be represented as module graph edges.

The Semanticist agent is the most technically ambitious component still to be built. It will use the Gemini Flash LLM to read each module's source code and generate a one-sentence purpose statement in plain English. More importantly, it will compare that generated purpose against any existing documentation (model descriptions from `schema.yml`, column descriptions, inline comments) and flag cases where the documentation says one thing but the code does another. I expect this documentation drift detection to be the most valuable feature for FDE onboarding, since stale documentation is one of the most common sources of confusion on brownfield projects.

The Archivist and Navigator agents are planned as the final integration layer. The Archivist will take everything the other agents have discovered and produce two documents: a `CODEBASE.md` file that serves as a living context reference for the entire repository, and an `onboarding_brief.md` that directly answers the five FDE Day-One questions using data from the knowledge graph rather than manual analysis. The Navigator will wrap the knowledge graph in a LangGraph-powered query interface with four specialized tools, allowing an FDE to ask natural language questions like "what breaks if I change this table?" and receive answers grounded in the actual dependency data. I also intend to run the complete pipeline against a second target codebase — likely the Apache Airflow example DAGs — to demonstrate that the architecture generalizes beyond dbt projects.

---

## Conclusion

This interim submission demonstrates a working two-agent pipeline that accurately maps data lineage in a real dbt project. The Surveyor and Hydrologist agents together parse YAML configuration, strip Jinja templating, resolve documentation references, filter CTE aliases, and produce verified dependency graphs — all invoked through a single CLI command. The tool correctly extracts all eight lineage edges from jaffle_shop with zero errors, and a mid-submission gap fix for `doc()` reference resolution shows the kind of iterative quality improvement that will continue through the final submission. What remains to be built — tree-sitter AST parsing, LLM-powered semantic analysis, automated documentation generation, and an interactive query interface — will transform this foundation from a lineage extraction tool into a complete codebase intelligence platform for FDE onboarding.
