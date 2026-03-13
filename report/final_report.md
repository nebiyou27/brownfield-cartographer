# Brownfield Cartographer — Final Submission Report

**Date:** March 13, 2026  
**Phase:** P5 Final Gate  
**Subject:** Cross-Repo Generalization + Final QA

---

## 1. End-to-End Pipeline Validation

The Brownfield Cartographer pipeline was successfully run on a second, non-primary repository: `ol-data-platform` (MIT Open Learning Data Platform). This repository relies heavily on Python (Dagster) and DuckDB/Athena, contrasting with the primary `make-open-data` repo which was heavily dbt-centric and Jinja/SQL-dominated.

**Run Statistics (`ol-data-platform`):**
- **Modules Scanned:** 263
- **Lineage Nodes:** 73
- **Lineage Edges:** 66
- **Parse Failures:** 0
- **Purpose Statements Generated:** 219
- **Doc Drift Flags:** 155

This successful end-to-end execution confirms **Pipeline Transferability**. The Surveyor, Hydrologist, Semanticist, and Archivist agents functioned cohesively without modification to accommodate the new repository.

---

## 2. Documented Failure Modes and Limitations

Although the pipeline executed successfully, running the tool on a distinct architectural style (`ol-data-platform`) revealed critical failure modes that differ from the primary repository. 

### A. Python AST SQL String Extraction Pollution
**The Problem:** The `ol-data-platform` uses in-memory DuckDB extensively, meaning Python files (e.g., `dg_projects\openedx\openedx\ops\normalize_logs.py`) contain raw SQL execution strings like `INSTALL json;` or `DROP TABLE IF EXISTS tracking_logs;`.  
**The Failure:** The Cartographer's Tree-sitter analyzer blindly extracted these string literals and promoted them to first-class lineage nodes and modules. `CODEBASE.md` is populated with nodes literally named `INSTALL json;` or `DROP TABLE IF EXISTS tracking_logs`, attributing downstream relationships to them.  
**Impact:** It pollutes the module graph with fragmented, transient script commands rather than structural datasets, breaking the dbt-first assumption where every SQL snippet implies a persistent architectural node.

### B. Relative Path Resolution Defect in Archivist
**The Problem:** The paths generated in `CODEBASE.md` and `onboarding_brief.md` for the target repository contain incorrect prefixes, assuming the directory of execution rather than the repository root.  
**The Failure:** For `ol-data-platform`, files are indexed as `..\brownfield-cartographer\dg_deployments\reconcile_edxorg_partitions.py` instead of the accurate relative path within `ol-data-platform`.  
**Impact:** Harms usability and causes file-linking breaks within the Navigator and Markdown readouts.

### C. Missing Edge Context in Orchestration Pipelines
**The Problem:** Python files using Dagster orchestration (e.g., Ops, Assets, Sensors) rely on decorators (`@op`, `@asset`) and implicit dependency injections.  
**The Failure:** The graph failed to link many orchestration assets correctly because it lacks an AST parser specific to Dagster's implicit dependency resolution (instead, it relies only on basic static Python imports or explicit SQL `ref` statements).

### Minimal Adaptation Needed:
To generalize properly:
1. **AST Filtering Rule:** Python AST SQL extraction must distinguish between structural persistence commands (`CREATE TABLE`) and ephemeral environment setup (`INSTALL`, `LOAD`, `DROP`). Or it should attach queries *to* the Python module node rather than inventing floating nodes.
2. **Pathing Overhaul:** Replace `os.path.abspath` or `os.path.relpath()` logic in Surveyor/Archivist with reliable resolution anchored strictly to the `target_repo` path argument.

---

## 3. Final Accuracy Table: Generated Edges vs. Manual Checks

| Repository | Edge / Relationship | Manual Check (Ground Truth) | Cartographer Graph Output | Result |
| :--- | :--- | :--- | :--- | :--- |
| **make-open-data** | `ventes_immobilieres` → `ventes_immobilieres_enrichies` | Explicit dbt `ref()` call in `.sql` | `ventes_immobilieres` → `ventes_immobilieres_enrichies` | ✅ Accurate |
| **make-open-data** | `infos_communes` → `demographie_communes` | Explicit dbt `ref()` join in `.sql` | `infos_communes` → `demographie_communes` | ✅ Accurate |
| **make-open-data** | `logement_2020_valeurs` → `demographie_communes` | Comment `depends_on: {{ ref(...) }}` | Missed initially (no comment parser) | ❌ Missed |
| **ol-data-platform** | `bin/dbt-local-dev.py` downstream of `_glue_source_registry` | Script executes DuckDB load/selects | Python module listed as downstream of `CREATE TABLE _glue_source_registry` node. | ⚠️ Partial (Messy Node Names) |
| **ol-data-platform** | `normalize_logs.py` downstream of `INSTALL json;` | Script executes `INSTALL json;` | Script listed as downstream of `INSTALL json;` literal string node. | ❌ False Positive (Fragment) |
| **ol-data-platform** | `event_log.py` (Dagster Storage pooling) | Standalone Postgres storage resource | Detected as high PageRank sink with accurate Purpose Statement. | ✅ Accurate |

**Overall Verdict:** The Cartographer performs exceptionally well on standard dbt paradigms (direct `ref()` calls) and accurately synthesizes code intent (Semanticist purpose statements were highly accurate for complex Dagster/Python files). However, it degrades on implicit DAG orchestrations (Dagster) and imperative script-based SQL (DuckDB in-memory executions), proving that multi-paradigm lineage extraction requires syntax-aware semantic parsers beyond basic tree-sitter queries.

---

**P5 Milestone Completed.** The Brownfield Cartographer MVP is finalized.
