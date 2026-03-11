# RECONNAISSANCE.md — Manual Day-One Analysis

Target Codebase: make-open-data
Repository: https://github.com/make-open-data/make-open-data
Time spent: ~30 minutes manual exploration
Date: March 12, 2026
Language: French open data platform (SQL + Python + YAML)

## Why This Codebase Was Chosen

I chose make-open-data as the primary target codebase for the Brownfield Cartographer because it perfectly meets all four Phase 0 criteria for an advanced agentic coding task. First, it is a substantial project with over 60 files across SQL models, Python scripts, YAML configurations, GitHub Actions workflows, and test files—well above the 50-file requirement. Second, it is a truly multi-language environment where SQL, Python, YAML, and shell scripts coexist and perform critical functions in the same data pipeline. Third, it features a clear split between SQL and Python, where SQL is used for dbt transformations and Python handles the extract and load layer through scripts like loaders.py and __main__.py. Finally, and most importantly, it is a real-world production system processing actual French government open data—including real estate transactions, census demographics, income statistics, and health professional data. With its real CI/CD pipelines, Docker setup, and data quality tests, it represents a genuine engineering challenge rather than a simplified educational example.

## The Five FDE Day-One Questions

**Question 1 — What is the primary data ingestion path?**

The data enters the system through two main channels. The first is the `4_seeds/` folder, which contains files like `logement_2020_valeurs.csv`—a raw CSV with French housing values that dbt loads directly. The second channel is defined in `1_data/sources/schema.yml`, where external data sources pulled from French government APIs and open data portals are specified. From these ingestion points, the data flows through a layered architecture: `1_data/intermediaires/` handles the initial cleaning and renaming of raw fields, `1_data/prepare/` shapes that data into business-ready tables across domains like geography, real estate, census, income, and health, and finally `2_analyses/` produces the analytical outputs. The numerical prefixing of the folders (1, 2, 3...) provides a structured execution order that makes this pipeline's architecture surprisingly intuitive to navigate.

**Question 2 — What are the 3-5 most critical output datasets?**

The most critical outputs are the prepared tables in `1_data/prepare/` upon which all downstream analyses rely. Specifically, `demographie_communes` and `demographie_iris` provide the foundational population statistics at the municipality and neighborhood levels. `revenu_commune` provides essential income data per municipality, while `infos_communes` serves as the geographic metadata backbone for almost all other datasets. Lastly, `ventes_immobilieres_enrichies` represents the final, enriched state of real estate transaction data. These tables are the core products of the pipeline and feed directly into the `2_analyses/` layer.

**Question 3 — What is the blast radius if the most critical module fails?**

The highest-risk single point of failure I identified is `1_data/intermediaires/geographie/postes_communes.sql`. This is the intermediate model responsible for mapping postal codes to municipalities. Geographic data is the bedrock of this entire project; real estate data is meaningless without geographic context, and demographic, income, and health data are all aggregated or reported based on these geographic units (communes, departments, etc.). If this mapping model breaks, the error would cascade across all five business domains, potentially leading to incorrect or incomplete results in every single downstream prepared table. This systemic dependency is subtle and only becomes clear when you trace the lineage across the different domain folders.

**Question 4 — Where is the business logic concentrated vs. distributed?**

The business logic in this repository follows an interesting architectural pattern of being both distributed and concentrated. The domain-specific transformation logic is distributed throughout the subfolders of `1_data/prepare/`, where each specific area like census or health has its own SQL models. However, the complex, reusable computational logic is concentrated in `5_macros/`. This folder contains 15 SQL macro files, including a geographic k-nearest-neighbors algorithm in `geo_knn.sql`, real estate aggregation logic in `aggreger_ventes_immobiliers.sql`, and census data pivoting functions in `pivoter_logement.sql`. This means the full logic isn't visible just by reading the individual model files; you must also trace the macro calls, adding a layer of hidden complexity that simple projects like jaffle_shop lack.

**Question 5 — What has changed most frequently in the last 90 days?**

By analyzing the git log with `git log --oneline --since="90 days ago" --name-only`, I observed that the highest change velocity is concentrated in the census and geography domain models, specifically the demography and habitat prepared models. This indicates that active development is driven by updates to the source government data itself—such as new census releases or updated geographic boundaries—rather than changes in the core infrastructure. The `5_macros/` folder shows relatively low velocity, suggesting that the foundational reusable logic is stable and mature, while the models that consume that logic are constantly evolving to accommodate new data.

## Difficulty Analysis

The macro system was undoubtedly the hardest aspect to wrap my head around during this manual exploration. In a simpler project like jaffle_shop, every table reference is a straightforward `ref()` call. In make-open-data, however, SQL models frequently call complex macros like `{{ aggreger_ventes_immobiliers(...) }}`. To truly understand what a model is doing, I was forced to constantly jump between the model file and the corresponding macro definition in `5_macros/`, which effectively doubled the reading effort for each piece of code. It became clear that any automated tool that doesn't account for these macro-driven dependencies would provide a very incomplete and potentially misleading picture of the pipeline.

The French language also presented a recurring obstacle for me. Navigating a codebase filled with files like `ventes_immobilieres_renomee.sql` or `aggreger_supra_commune.sql` requires constant translation or specialized knowledge just to identify the basic purpose of a model. This is a very realistic challenge in many professional brownfield environments where the primary language of the engineering team isn't English. This experience reinforced why an LLM-powered Semanticist agent is so critical; modern LLMs can handle French variable names and comments natively, bridging a gap that would otherwise significantly slow down a new engineer's onboarding.

## Ground Truth Lineage (Manual)

```
French Government APIs + CSV seeds
        ↓
1_data/sources/          (raw source definitions)
        ↓
1_data/intermediaires/   (cleaning + renaming per domain)
  ├── foncier/           (real estate)
  ├── geographie/        (geography — feeds all other domains)
  └── recensement/       (census data)
        ↓
1_data/prepare/          (business-ready tables per domain)
  ├── geographie/        (commune, department, iris metadata)
  ├── recensement/       (demographics, habitat, mobility, activity)
  ├── foncier/           (enriched property transactions)
  ├── revenu/            (income by commune)
  ├── sante/             (health professionals by department)
  └── risques/           (Seveso industrial risk sites)
        ↓
2_analyses/              (final analytical outputs)
  ├── geo/               (geographic analysis)
  └── revenu/            (income analysis)
```

This manually traced ground truth will serve as the baseline I use to verify the Brownfield Cartographer’s automated lineage output for the final submission.
