# FDE Day-One Onboarding Brief

Q1: The primary data ingestion path involves loading files from various sources (GCS, S3) into SQL tables or Athena databases, often triggered by Python scripts like `edxorg_course_content_webhook`, `normalize_logs`, and `download_gcs_data`. These scripts process and store data in destinations like `edxorg_raw_tracking_logs`, `course_content_metadata`, or directly into Superset for visualization. [`dg_projects/openedx/openedx/ops/normalize_logs.py:246`]

Q2: The 3-5 most critical output datasets/endpoints are: [`dg_projects/legacy_openedx/legacy_openedx/ops/open_edx.py:1`]
- `edxorg_raw_tracking_logs` (processed via `edxorg_archive.py` at [dg_projects/edxorg/edxorg/assets/edxorg_archive.py:460])
- `course_content_metadata` (stored via `edxorg_archive.py` at [dg_projects/edxorg/edxorg/assets/edxorg_archive.py:464])
- `student_risk_probability` (from `src/ol_superset/ol_superset/commands/promote.py:263`) [`src/ol_superset/ol_superset/commands/promote.py:263`]
- `student_submissions` (from `src/ol_superset/ol_superset/commands/promote.py:265`) [`src/ol_superset/ol_superset/commands/promote.py:265`]
- `course_enrollments` (from `bin/dbt-local-dev.py`) [`bin/dbt-local-dev.py:270`]

Q3: The most dangerous node is `edxorg_raw_tracking_logs` (domain count: 4, downstream nodes: 3). It is a critical SQL table used across multiple domains and has a blast radius of 3 downstream nodes, making it highly impactful if modified. The blast radius includes nodes like `edxorg_course_content_webhook`, `normalize_edxorg_tracking_log`, and `student_risk_probability`. [`bin/dbt-local-dev.py:1605`]

Q4: Business logic is distributed across Python scripts and SQL models. There is no macro layer detected in the provided context. The majority of business logic is concentrated in Python files like `bin/dbt-local-dev.py`, `edxorg_archive.py`, and `normalize_logs.py`, while SQL models (e.g., `_superset_dataset`) and CSV seeds handle data transformations and storage. [`bin/dbt-local-dev.py:1`]

Q5: The file that has changed most frequently in the last 90 days is `uv.lock`, with 51 commits. Other high-velocity files include `dg_projects/lakehouse/uv.lock` (37 commits), `dg_projects/legacy_openedx/uv.lock` (25 commits), and `src/ol_dbt/models/reporting/_reporting__models.yml` (21 commits). [`packages/ol-orchestrate-lib/src/ol_orchestrate/lib/dagster_types/files.py:1`]
