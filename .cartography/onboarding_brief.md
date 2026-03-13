# FDE Day-One Onboarding Brief

**Q1: What is the primary data ingestion path?**  
The primary data ingestion path involves AWS Glue tables, managed via `brownfield-cartographer\bin\dbt-local-dev.py` to register and interact with them [brownfield-cartographer\bin\dbt-local-dev.py]. The system leverages `httpfs` and `aws` modules for Glue/external storage access [brownfield-cartographer\bin\dbt-local-dev.py]. The `edxorg_archive.py` processes tracking logs but is likely a transformation/sink, not the primary ingestion source [brownfield-cartographer\dg_projects\edxorg\edxorg\assets\edxorg_archive.py].

**Q2: What are the 3-5 most critical output datasets/endpoints?**  
1. **AWS Glue tables** (ingested and registered via `dbt-local-dev.py`) [brownfield-cartographer\bin\dbt-local-dev.py].  
2. **Tracking Logs (`tracking_logs`)** processed by `edxorg_archive.py` [brownfield-cartographer\dg_projects\edxorg\edxorg\assets\edxorg_archive.py].  
3. **Corrected S3 partitions** via `reconcile_edxorg_partitions.py` [brownfield-cartographer\dg_deployments\reconcile_edxorg_partitions.py].  
4. **DuckDB views** created by `dbt-local-dev.py` for local testing [brownfield-cartographer\bin\dbt-local-dev.py].  
5. **Exported CSVs** from `data_export.py` for organizational data [brownfield-cartographer\dg_projects\b2b_organization\b2b_organization\assets\data_export.py].

**Q3: What is the blast radius if the most critical module changes its interface?**  
The most critical module is `brownfield-cartographer\bin\dbt-local-dev.py`, which handles Glue table registration and dbt operations. A change here would affect:  
- **AWS Glue table interactions** (used by `dbt` models and Glue-dependent systems) [brownfield-cartographer\bin\dbt-local-dev.py].  
- **DuckDB view creation** (critical for local testing) [brownfield-cartographer\bin\dbt-local-dev.py].  
- **External storage access** (via `httpfs/aws` modules) [brownfield-cartographer\bin\dbt-local-dev.py].  
Blast radius includes all dbt models, Glue tables, and systems relying on DuckDB views for testing/transformation.

**Q4: Where is the business logic concentrated vs distributed?**  
- **Concentrated**: AWS Glue tables and `dbt` models (not fully visible in the provided code).  
- **Distributed**:  
  - `edxorg_archive.py` handles tracking log processing [brownfield-cartographer\dg_projects\edxorg\edxorg\assets\edxorg_archive.py].  
  - `reconcile_edxorg_partitions.py` manages S3 partition corrections [brownfield-cartographer\dg_deployments\reconcile_edxorg_partitions.py].  
  - `data_export.py` exports organizational CSVs [brownfield-cartographer\dg_projects\b2b_organization\b2b_organization\assets\data_export.py].  
The provided code lacks centralized business logic; instead, it’s modularized into specific-purpose files.

**Q5: What has changed most frequently in the last 30 days?**  
All listed files show **0 commits** in the last 30 days [brownfield-cartographer\bin\dbt-local-dev.py, `edxorg_archive.py`, `reconcile_edxorg_partitions.py`, etc.]. No changes detected in the provided files.