# FDE Day-One Onboarding Brief

**Q1:** The primary data ingestion path is through `load/loaders.py`, which handles reading data from various sources (CSV, JSON, shapefiles) and writing them to a PostgreSQL database. This is evident from the module graph, data lineage, and frequent commits (5 commits in the last 30 days). The key operations include reading data via methods like `to_csv`, `df.write`, and `to_postgis`, all managed in this central file.

**Q2:** The 3-5 most critical output datasets/endpoints are the PostgreSQL tables loaded via `load/loaders.py`, such as:
- `cog_poste`, `cog_communes`, `cog_arrondissements` (from the module graph). [`make-open-data\dbt_project.yml:1`]
- Datasets like `dvf_2024_dev`, `seveso_2024`, and `logement_2020_valeurs` (from the data lineage). These tables are stored in PostGIS and serve as the core data repository for downstream processes. [`make-open-data\dbt_project.yml:1`]

**Q3:** If `load/loaders.py` changes its interface, the blast radius includes:
- `load/__main__.py` (entry point for CLI ingestion). [`load/__main__.py:1`]
- `tests/load/test_loaders.py` (tests relying on ingestion logic). [`load/loaders.py:1`]
- `tests/__init__.py` and `tests/fixtures/db.py` (test infrastructure modules). [`extract/__init__.py:1`]
- `utils/generer_doc_recenssement.py` (depends on CSV data loaded via `load/loaders.py`). [`load/loaders.py:1`]

This affects ~40% of the codebase (8/19 files), as `load/loaders.py` is a central hub for data ingestion. [`load/loaders.py:1`]

**Q4:** Business logic is **concentrated** in `load/loaders.py` (core ingestion logic) and **distributed** in test modules (`tests/load/test_loaders.py`) and utility scripts (`utils/generer_doc_recenssement.py`). The centralization in `load/loaders.py` (5 commits) suggests reusable logic, while distributed logic in tests ensures robust validation.

**Q5:** Most frequent changes (last 30 days) are in: [`make-open-data\dbt_project.yml:1`]
1. `load/loaders.py` (5 commits) - Core ingestion logic. [`load/loaders.py:1`]
2. `load/__main__.py` (3 commits) - CLI entry point. [`load/__main__.py:1`]
3. `tests/load/test_loaders.py` (3 commits) - Tests for ingestion. [`load/loaders.py:1`]
