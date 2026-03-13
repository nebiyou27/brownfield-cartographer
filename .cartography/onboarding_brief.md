# FDE Day-One Onboarding Brief

Q1: The primary data ingestion path begins with the `dvf_default` and `dvf_?[2014-2024]_dev` datasets, which are selected via SQL and ingested into the `ventes_immobilieres` table. This table is then enriched to produce `ventes_immobilieres_enrichies`. The ingestion process is handled by the `load` module, which uses configurations from a YAML file to orchestrate the loading of data into PostgreSQL.  
**Primary Ingestion Path**:  
`dvf_default`/`dvf_?[2014-2024]_dev` --> `ventes_immobilieres` (via SQL select) --> `ventes_immobilieres_enrichies` (via SQL select)  
[load\loaders.py orchestrates ingestion from DVF sources into PostgreSQL]

Q2: The 3-5 most critical output datasets/endpoints are:  
1. `ventes_immobilieres_enrichies` (central enrichment point for real estate data).  
2. `cog_communes`, `cog_departements`, `cog_regions` (geographic reference datasets).  
3. `activite_communes`, `demographie_communes`, `habitat_communes`, `mobilite_communes` (derived from `logement_2020` and `logement_2020_dev`).  
4. `revenu_commune` (derived from `filosofi_commune_2021`).  
5. `shape_commune_2024`, `shape_arrondissement_municipal_2024`, `shape_iris_2024` (geospatial datasets).  
[These datasets are central to downstream processes and are referenced in multiple lineage edges]

Q3: If the most critical module (`load\loaders.py`) changes its interface, the blast radius would include all data ingestion points (DVF, LOGEMENT, SCOT, etc.), as this module orchestrates the loading of multiple datasets into PostgreSQL. Changes here could disrupt ingestion for all sources, including `dvf_?[2014-2024]`, `logement_2020`, `logement_2020_dev`, and others.  
**Blast Radius**: All datasets ingested via `load\loaders.py`, including `ventes_immobilieres`, `logement_2020`, `logement_2020_dev`, and geospatial datasets.  
[load\loaders.py orchestrates ingestion for multiple sources]

Q4: Business logic is concentrated in the `load` module (orchestration) and distributed across the `prepare` modules (data transformation and schema definition).  
- **Concentration**: The `load` module handles ingestion orchestration, while the `prepare` modules (e.g., `1_data/prepare/recensement/`) contain schema definitions and transformations.  
- **Distribution**: The `prepare` modules are distributed across different subdirectories (e.g., `geographie`, `foncier`, `recensement`) for specific data domains.  
[1_data/prepare/* directories contain schema and transformation logic]

Q5: The most frequent changes in the last 30 days are in the `schema.yml` files under `1_data/prepare/`, particularly in the `recensement` subdirectories.  
**Ranked by Commit Count**:  
1. `1_data/prepare/geographie/schema.yml` (3 commits)  
2. `1_data/prepare/foncier/schema.yml` (3 commits)  
3. `1_data/prepare/revenu/schema.yml` (3 commits)  
4. `1_data/prepare/sante/schema.yml` (3 commits)  
5. `1_data/prepare/recensement/activite/schema.yml` (2 commits)  
[Git Velocity data: 1_data/sources/schema.yml has 26 commits, but the provided data lists `1_data/prepare/` files with commit counts]