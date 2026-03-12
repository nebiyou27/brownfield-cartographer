# FDE Day-One Onboarding Brief

**Q1:** The primary data ingestion path involves loading data from the `dvf_default` and `dvf_YYYY_dev` tables into the `ventes_immobilieres` table, which is then enriched to produce `ventes_immobilieres_enrichies`. This is supported by the data lineage showing direct `SELECT` operations from these tables and the `load` module's role in ingestion [loaders.py: purpose]. The `extract` module (e.g., `extract\__init__.py`) handles source extraction, but the `load` module manages the actual loading process.

**Q2:** The 3-5 most critical output datasets are:
1. `ventes_immobilieres_enrichies` (derived from real estate data) [data/prepare/foncier/schema.yml: enrichment].
2. `infos_communes` (central geodemographic dataset) [data/prepare/geographie/schema.yml: core usage].
3. `commune_centroid_poste` (spatial/geographic intermediate table) [data/prepare/geographie/schema.yml: derived from `cog_poste`].
4. `postes_communes` (job/position data) [data/prepare/geographie/schema.yml: direct output].
5. `cog_poste` (prepared data table) [data/prepare/foncier/schema.yml: foundational for multiple downstream uses].

**Q3:** If `cog_poste` (a core geospatial module) changes its interface, the blast radius includes `postes_communes`, `commune_centroid_poste`, `infos_communes`, and `infos_postes` (all directly dependent on it) [module graph: `cog_poste` → these modules]. Indirectly, changes could affect `ventes_immobilieres` (via `cog_poste`'s role in spatial analysis) and downstream enriched tables, but the immediate impact is limited to its direct consumers.

**Q4:** Business logic is concentrated in the `prepare` directories (e.g., `1_data/prepare/geographie/schema.yml` for geospatial processing) and distributed via the `cog` modules (e.g., `cog_poste`, `cog_communes`). The `load` and `extract` modules handle ingestion/transformation, while `utils` (e.g., `utils/generer_doc_recenssement.py`) focuses on documentation generation [module graph: `prepare`/`cog` modules].

**Q5:** The most frequently changed files (last 30 days) are ranked by commit count:
1. `1_data/sources/schema.yml` (26 commits) - Core schema definition.
2. `1_data/prepare/geographie/schema.yml` (7 commits) - Geospatial data preparation.
3. `1_data/prepare/foncier/schema.yml` (3 commits), `1_data/prepare/revenu/schema.yml` (3 commits), `1_data/prepare/sante/schema.yml` (3 commits) - Other preparation schemas.