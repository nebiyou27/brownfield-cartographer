# FDE Day-One Onboarding Brief

often, I use this to guide my thinking. Let's break it down step by step. [`load/loaders.py:1`]

First, the user has given a detailed system context with various sections like CRITICAL_NODES, BLAST_RADIUS_TOP5, CROSS_DOMAIN_RISK, and others. I need to answer five specific questions based on this data. The questions are: [`load/loaders.py:1`]

1. What is the primary data ingestion path? [`load/loaders.py:1`]
2. What are the 3-5 most critical output datasets/endpoints? [`load/loaders.py:1`]
3. What is the blast radius if the most critical module changes its interface? [`load/loaders.py:1`]
4. Where is the business logic concentrated vs distributed? [`load/loaders.py:1`]
5. What has changed most frequently in the last 90 days? [`load/loaders.py:1`]

For each question, I must use clear headings and back claims with file path citations where possible. The instructions provide specific guidance for each question. [`load/loaders.py:1`]

Starting with Q1: Primary data ingestion path. From the context, there's an INGESTION_PIPELINE section mentioning two stages: extraction_config and loading_config. Specifically, it says "Stage 1 - extraction_config: extract/source_to_storage.yml" and "Stage 2 - loading_config: load/storage_to_pg.yml". So, the primary path involves extracting data to storage and then loading it into PostgreSQL. I should cite the relevant files. [`extract/source_to_storage.yml:1`]

Next, Q2: 3-5 most critical output datasets/endpoints. According to CRITICAL_NODES, the highest PageRank score is 0.97 for "ventes_immobilieres_enrichies". CRITICAL_NODES lists nodes with high scores, and BLAST_RADIUS_TOP5 shows that "ventes_immobilieres_enrichies" has strong downstream confidence. Also, TRUE_SINKS lists many datasets, but I need to focus on outputs. The instructions say to list only SQL dataset nodes and CSV seed files. From TRUE_SINKS, I see outputs like "activite_departements", "demographie_departements", etc. But CRITICAL_NODES has "ventes_immobilieres_enrichies" with score 0.97, which is high. I should include that and others with high downstream impact. [`make-open-data/1_data/prepare/recensement/activite/activite_departements.sql:1`]

Q3: Blast radius if the most critical module changes interface. The instructions specify to use CROSS_DOMAIN_RISK first. The node with the highest domain count is "sources" with 8 domains. CROSS_DOMAIN_RISK says "domains=datatourisme, emploi, foncier, geographie, recensement, revenu, risques, sante | domain_count=8". This node has the highest domain count, so it's the most dangerous. Blast radius includes downstream nodes from BLAST_RADIUS_TOP5 or CROSS_DOMAIN_RISK data. I need to describe its blast radius. [UNVERIFIED: this file was not found in the analyzed graph]

Q4: Business logic concentrated vs distributed. Check MACRO_SUMMARY. It shows macro_count=17 and key macros. So, logic is concentrated in these macros. I need to list the macro folder and specific macro files. [`load/loaders.py:1`]

Q5: What has changed most frequently. Use HIGH_VELOCITY_FILES. But the list is empty, so I must state that there are no high-velocity files. [`load/loaders.py:1`]

Now, for citations: I should use file paths where possible. For example, in Q4, cite the macro folder and specific files. [`load/loaders.py:1`]

Ensure not to list Python files as outputs for Q2. Only datasets. [`load/loaders.py:1`]

Finally, structure the response with clear headings for each question. [`load/loaders.py:1`]

**Q1:** [`load/loaders.py:1`]
The primary data ingestion path consists of two stages: [`load/loaders.py:1`]
1. **Extraction**: Data is extracted from source systems (e.g., CSV seeds or APIs) and stored in a temporary location using `extract/source_to_storage.yml`. [`extract/source_to_storage.yml:1`]
2. **Loading**: The stored data is then loaded into PostgreSQL using `load/storage_to_pg.yml`. [`load/storage_to_pg.yml:1`]

This path is inferred from the `INGESTION_PIPELINE` section, which explicitly defines the two stages [INGESTION_PIPELINE].

--- [`load/loaders.py:1`]

**Q2:** [`load/loaders.py:1`]
The 3-5 most critical output datasets/endpoints are: [`load/loaders.py:1`]
1. **ventes_immobilieres_enrichies**: A high-scorer in CRITICAL_NODES (PageRank 0.97) with strong downstream confidence (confidence=0.95). [UNVERIFIED: this file was not found in the analyzed graph]
2. **activite_departements**: Appears in BLAST_RADIUS_TOP5 for `infos_communes` (confidence=0.95). [`make-open-data/1_data/prepare/recensement/activite/activite_departements.sql:1`]
3. **demographie_departements**: Also appears in BLAST_RADIUS_TOP5 for `infos_communes` (confidence=0.95). [`make-open-data/1_data/prepare/recensement/demographie/demographie_departements.sql:1`]
4. **habitat_departements**: Listed in TRUE_SINKS and has high downstream confidence in BLAST_RADIUS_TOP5. [`make-open-data/1_data/prepare/recensement/habitat/habitat_departements.sql:1`]
5. **mobilite_departements**: Similarly, it appears in BLAST_RADIUS_TOP5 for `infos_communes` (confidence=0.95). [`make-open-data/1_data/prepare/recensement/mobilite/mobilite_departements.sql:1`]

These datasets are critical because they are high-scoring in CRITICAL_NODES and have significant downstream dependencies [CRITICAL_NODES, BLAST_RADIUS_TOP5, TRUE_SINKS]. [`load/loaders.py:1`]

--- [`load/loaders.py:1`]

**Q3:** [`load/loaders.py:1`]
The blast radius if the most critical module changes its interface is determined by the node with the highest domain count in CROSS_DOMAIN_RISK, which is **`sources`** (domain_count=8). This node is the most dangerous because it spans multiple domains (datatourisme, emploi, foncier, geographie, recensement, revenu, risques, sante). Its blast radius includes all downstream nodes listed in CROSS_DOMAIN_RISK and BLAST_RADIUS_TOP5, such as `ventes_immobilieres_enrichies`, `activite_departements`, `demographie_departements`, and others, totaling 72 downstream nodes [CROSS_DOMAIN_RISK, BLAST_RADIUS_TOP5]. [`make-open-data/1_data/prepare/recensement/activite/activite_departements.sql:1`]

--- [`load/loaders.py:1`]

**Q4:** [`load/loaders.py:1`]
Business logic is concentrated in the **`5_macros/`** folder, specifically in reusable SQL macros. Evidence:
- **Macro files**: `aggreger_colonnes_theme_geo.sql`, `aggreger_logement_par_colonne.sql`, `aggreger_supra_commune.sql`, `aggreger_ventes_immobiliers.sql`, and `calculate_geo_knn.sql` are identified in MACRO_SUMMARY as key macros [MACRO_SUMMARY]. [`5_macros/foncier/aggreger_ventes_immobiliers.sql:1`]

If no macros existed (e.g., if macro_count=0), the logic would be distributed across files in CRITICAL_NODES and TRUE_SINKS, such as `make-open-data/1_data/prepare/recensement/activite_communes.sql` and `load/loaders.py`, but no such case applies here [MACRO_SUMMARY]. [`load/loaders.py:1`]

--- [`load/loaders.py:1`]

**Q5:** [`load/loaders.py:1`]
There are no high-velocity files in the last 90 days, as the `HIGH_VELOCITY_FILES` list is empty. This means no files have changed frequently recently [HIGH_VELOCITY_FILES].

