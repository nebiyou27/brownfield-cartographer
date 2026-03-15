# FDE Day-One Onboarding Brief

Q1: [`load/loaders.py:1`]
The primary data ingestion path consists of two stages: [`load/loaders.py:1`]
1. **Stage 1 - extraction_config**: `extract/source_to_storage.yml` [`extract/source_to_storage.yml:1`]
2. **Stage 2 - loading_config**: `load/storage_to_pg.yml` [Ingestion Pipeline] [`load/storage_to_pg.yml:1`]

--- [`load/loaders.py:1`]

Q2: [`load/loaders.py:1`]
The 3-5 most critical output datasets/endpoints are: [`load/loaders.py:1`]
1. **`ventes_immobilieres_enrichies`** (CRITICAL_NODE) [`make-open-data/1_data/prepare/foncier/ventes_immobilieres_enrichies.sql:1`]
2. **`make-open-data\5_macros\foncier\preparer_dvf_biens_immobilier.sql`** (SQL model) [`make-open-data/5_macros/foncier/preparer_dvf_biens_immobilier.sql:1`]
3. **`make-open-data\5_macros\recensement\aggreger_colonnes_theme_geo.sql`** (SQL model) [`make-open-data/5_macros/recensement/aggreger_colonnes_theme_geo.sql:1`]
4. **`make-open-data\5_macros\recensement\aggreger_supra_commune.sql`** (SQL model) [`make-open-data/5_macros/recensement/aggreger_supra_commune.sql:1`]
5. **`make-open-data\5_macros\recensement\renommer_colonnes_valeurs_logement.sql`** (SQL model) [CRITICAL_NODES, BLAST_RADIUS_TOP5] [`make-open-data/5_macros/recensement/renommer_colonnes_valeurs_logement.sql:1`]

--- [`load/loaders.py:1`]

Q3: [`load/loaders.py:1`]
The most critical node is **`sources`** (not explicitly listed as a CRITICAL_NODE but identified via CROSS_DOMAIN_RISK). [`make-open-data/1_data/sources/schema.yml:1`]
- **Blast Radius**: `sources` affects 8 domains (datatourisme, emploi, foncier, geographie, recensement, revenu, risques, sante) and has 72 downstream nodes. This node is foundational across multiple business domains and cannot be changed without cascading impacts. [CROSS_DOMAIN_RISK] [`make-open-data/1_data/sources/schema.yml:1`]

--- [`load/loaders.py:1`]

Q4: [`load/loaders.py:1`]
The business logic is concentrated in the **`5_macros/`** folder, with 17 reusable SQL macros. Key examples include:
- `aggreger_colonnes_theme_geo.sql` [`5_macros/recensement/aggreger_colonnes_theme_geo.sql:1`]
- `aggreger_logement_par_colonne.sql` [`5_macros/recensement/aggreger_logement_par_colonne.sql:1`]
- `preparer_dvf_biens_immobilier.sql` [`5_macros/foncier/preparer_dvf_biens_immobilier.sql:1`]

This layer is distributed across SQL models and ingestion pipelines, with business rules embedded in both. [MACRO_SUMMARY] [`load/loaders.py:1`]

--- [`load/loaders.py:1`]

Q5: [`load/loaders.py:1`]
No files have changed most frequently in the last 90 days, as the **HIGH_VELOCITY_FILES** list is empty. [HIGH_VELOCITY_FILES] [`load/loaders.py:1`]
