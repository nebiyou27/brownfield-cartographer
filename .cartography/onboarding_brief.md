# FDE Day-One Onboarding Brief

Q1: The primary data ingestion path starts from the raw DVF data (e.g., `dvf_default`, `dvf_20XX_dev`, `dvf_20XX`), which is loaded into the `ventes_immobilieres` table via the `load\loaders.py` module. This table is then enriched and becomes `ventes_immobilieres_enrichies`. Other datasets like `logement_2020` and `logement_2020_dev` are loaded into various tables, but `ventes_immobilieres` appears to be the central ingestion point.

Q2: The 3-5 most critical output datasets/endpoints are:
- `ventes_immobilieres_enrichies` (downstream from multiple sources, including `ventes_immobilieres`)
- `infos_communes` (used by multiple modules like `commune_centroid_poste`, `infos_departements`, etc.)
- `commune_centroid_poste` (derived from multiple sources, including `infos_communes` and `cog_departements`)
- `activite_communes` (used in the lineage to `activite_departements`)
- `demographie_communes` (used in the lineage to `demographie_departements`)

These datasets are critical because they serve as foundational inputs for many downstream processes and are frequently referenced in the data lineage.

Q3: If the most critical module (e.g., `dvf_default`) changes its interface, the blast radius would include all modules that depend on it, such as `dvf_20XX_dev`, `dvf_20XX`, `ventes_immobilieres`, and `ventes_immobilieres_enrichies`. This change would also propagate to any module that relies on `ventes_immobilieres_enrichies`, such as `postes_communes` and `commune_centroid_poste`. The blast radius would extend to the entire data pipeline, potentially affecting reporting and analysis that depends on these datasets.

Q4: Business logic is concentrated in the `load\loaders.py` module, which handles data ingestion from various sources (CSV, JSON, shapefiles) and transforms data into PostgreSQL tables. Distributed logic is found in the test modules, such as `tests\load\test_loaders.py`, which contains unit tests for data loading and validation. Additionally, the `utils\generer_doc_recenssement.py` module contains logic for generating documentation from CSV files, indicating distributed functionality in data processing and reporting.

Q5: The files with the most frequent changes in the last 30 days, based on Git Velocity, are:
1. `load\loaders.py` (5 commits) - Handles data ingestion and transformation.
2. `load\__main__.py` (3 commits) - Orchestrates data loading into PostgreSQL.
3. `tests\load\test_loaders.py` (3 commits) - Contains tests for data loading functionality.
4. `extract\__init__.py` (2 commits) - Manages manual data extraction.
5. Other files with 1 commit (e.g., `tests\conftest.py`, `tests\fixtures\db.py`) - Support test infrastructure and database fixtures.