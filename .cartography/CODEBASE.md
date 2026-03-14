<!-- CARTOGRAPHER v1 | generated: 2026-03-14T14:49:50.716285 | nodes: 112 | edges: 121 -->

## SECTION:ARCHITECTURE_SUMMARY
target_dir=D:\TRP-1\Week-4\brownfield-cartographer\make-open-data
module_nodes=13
dataset_nodes=99
lineage_edges=121
domain_count=5

## SECTION:CRITICAL_PATH
1|node=ventes_immobilieres|pagerank=0.1001|why=Centrality driven by 23 upstream sources and 1 downstream dependents.|purpose=
2|node=ventes_immobilieres_enrichies|pagerank=0.0899|why=Centrality driven by 1 upstream sources and 0 downstream dependents.|purpose=
3|node=load/loaders.py|pagerank=0.0338|why=Centrality driven by 7 upstream sources and 4 downstream dependents.|purpose=The module enables the ingestion of structured data (CSV, JSON, shapefiles) from external storage into a PostgreSQL database, facilitating data integration and persistent storage for downstream applications. It handles file downloading, format conversion, and table creation with proper schema management, ensuring data is correctly structured and validated before insertion.
4|node=infos_communes|pagerank=0.0252|why=Centrality driven by 7 upstream sources and 9 downstream dependents.|purpose=
5|node=tests/load/test_loaders.py|pagerank=0.0203|why=Centrality driven by 3 upstream sources and 0 downstream dependents.|purpose=The module enables the ingestion of CSV data into a PostgreSQL database, ensuring that the data is correctly formatted and mapped to the target schema. It verifies the integrity of the loaded data through automated tests, allowing for reliable data entry and retrieval.

## SECTION:SOURCES
node=4_seeds/logement_2020_valeurs.csv
node=bmo_2024
node=cog_arrondissements
node=cog_communes
node=cog_departements
node=cog_poste
node=cog_regions
node=communes_to_scot
node=datatourisme_place
node=dvf_2014
node=dvf_2014_dev
node=dvf_2015
node=dvf_2015_dev
node=dvf_2016
node=dvf_2016_dev
node=dvf_2017
node=dvf_2017_dev
node=dvf_2018
node=dvf_2018_dev
node=dvf_2019

## SECTION:SINKS
node=activite_departements
node=activite_iris
node=activite_renomee
node=besoin_main_oeuvre_departement
node=commune_centroid_poste
node=demographie_departements
node=demographie_iris
node=demographie_renomee
node=habitat_departements
node=habitat_iris
node=infos_postes
node=infos_scot
node=load/__main__.py
node=mobilite_departements
node=mobilite_iris
node=mobilite_renomee
node=place
node=postes_communes
node=professionels_sante_departement
node=revenu_commune

## SECTION:KNOWN_DEBT
cycles=0
drift_flags=9
drift|module=4_seeds/logement_2020_valeurs.csv|verdict=DRIFT|explanation=The docstring claims the code generates documentation but actually prints data without creating any documentation. It also includes a warning about checking values, which is not part of the implementation.
drift|module=load/__init__.py|verdict=MISSING|explanation=No docstring found.
drift|module=load/loaders.py|verdict=MISSING|explanation=No docstring found.
drift|module=tests/__init__.py|verdict=MISSING|explanation=No docstring found.
drift|module=tests/conftest.py|verdict=MISSING|explanation=No docstring found.
drift|module=tests/fixtures/__init__.py|verdict=MISSING|explanation=No docstring found.
drift|module=tests/load/__init__.py|verdict=MISSING|explanation=No docstring found.
drift|module=tests/load/test_loaders.py|verdict=MISSING|explanation=No docstring found.
drift|module=utils/generer_doc_recenssement.py|verdict=DRIFT|explanation=The docstring mentions "generating documentation for schemas" but lacks specific details about the code's logic, data processing, or output format. The implementation, however, explicitly shows how the code reads a CSV, processes data, and prints formatted output. The docstring's vague description and missing context significantly misrepresent the code's purpose.
orphans=0
semantic_anomalies=3
anomaly|node=load/loaders.py|issue=Purpose claims it's a source, but graph shows 7 upstream dependencies.
anomaly|node=load/__main__.py|issue=Purpose claims it's a source, but graph shows 1 upstream dependencies.
anomaly|node=tests/load/test_loaders.py|issue=Purpose claims it's a source, but graph shows 3 upstream dependencies.

## SECTION:HIGH_VELOCITY_FILES
file=load/loaders.py|commits=5
file=load/__main__.py|commits=3
file=tests/load/test_loaders.py|commits=3
file=extract/__init__.py|commits=2
file=load/__init__.py|commits=1
file=tests/conftest.py|commits=1
file=tests/__init__.py|commits=1
file=tests/fixtures/db.py|commits=1
file=tests/fixtures/__init__.py|commits=1
file=tests/load/__init__.py|commits=1
file=utils/generer_doc_recenssement.py|commits=1

## SECTION:MODULE_PURPOSE_INDEX
module=4_seeds/logement_2020_valeurs.csv|purpose=This module generates a structured documentation of demographic values from a CSV file, enabling users to understand the relationship between different variables and their respective values in the dataset. It provides a clear overview of the data schema, including the number of households, variable codes, and descriptive labels, which is essential for data preparation and analysis workflows.
module=extract/__init__.py|purpose=The module enables the manual extraction of data from source systems to storage, leveraging the Lake House approach to manage data movement, despite the complexity and infrequency of the process. It serves as a critical component for transitioning data into storage formats required by downstream analytics or processing workflows.
module=load/__init__.py|purpose=This module provides functions to load data and configuration parameters from various sources, enabling applications to retrieve and initialize data efficiently, thereby supporting scalable and flexible data processing workflows.
module=load/__main__.py|purpose=This module extracts data from specified sources and loads it into a PostgreSQL database, handling different file formats (CSV, JSON, shapefiles) and skipping operations in non-production environments. It ensures data is properly processed and stored according to the configuration defined in the `storage_to_pg.yml` file.
module=load/loaders.py|purpose=The module enables the ingestion of structured data (CSV, JSON, shapefiles) from external storage into a PostgreSQL database, facilitating data integration and persistent storage for downstream applications. It handles file downloading, format conversion, and table creation with proper schema management, ensuring data is correctly structured and validated before insertion.
module=tests/__init__.py|purpose=This module serves as a central repository for test cases, facilitating organization and management of test suites across different modules. It ensures that tests are structured and accessible, enabling efficient maintenance and execution. By providing a standardized structure, it enhances collaboration and reduces duplication across the codebase.
module=tests/conftest.py|purpose=The module enables pytest tests to access a database for integration and unit testing, ensuring that the application's logic is validated against real-world data, which is critical for maintaining data integrity and functionality in production.
module=tests/fixtures/__init__.py|purpose=This module ensures that the test directory is properly configured by creating necessary directories and setting up test environments, enabling seamless execution of test cases and maintaining a structured, organized test framework. It facilitates the isolation of test data and configurations, ensuring consistency across different test runs.
module=tests/fixtures/db.py|purpose=This module provides a reliable, secure connection to a test PostgreSQL database by configuring environment variables and establishing a persistent connection, ensuring tests can seamlessly access the test database without manual setup. It ensures the database is available for use by waiting until the service is responsive, facilitating consistent and reliable test execution.
module=tests/load/__init__.py|purpose=This module manages test cases and provides the necessary configurations to run tests, ensuring that the test environment is set up correctly for the business requirements. It facilitates the execution of test scenarios by loading test data and initializing dependencies, enabling reliable and consistent test runs across different environments.
module=tests/load/test_loaders.py|purpose=The module enables the ingestion of CSV data into a PostgreSQL database, ensuring that the data is correctly formatted and mapped to the target schema. It verifies the integrity of the loaded data through automated tests, allowing for reliable data entry and retrieval.
module=utils/generer_doc_recenssement.py|purpose=The module generates structured documentation for demography-related variables in a commune, extracting and formatting data from a CSV file to provide clear insights into household counts and response patterns. It ensures consistent labeling and presentation of demographic data, aiding in data interpretation and system documentation.

## SECTION:LOW_CONFIDENCE_LINEAGE
edge=dvf_2024_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2024->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2023_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2023->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2022_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2022->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2021_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2021->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2020_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2020->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2019_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2019->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2018_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2018->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2017_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2017->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2016_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2016->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2015_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2015->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2014_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_default->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2014->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=logement_2020->activite_renomee|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/recensement/activite_renomee.sql
edge=logement_2020->demographie_renomee|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/recensement/demographie_renomee.sql
edge=logement_2020->habitat_renomee|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/recensement/habitat_renomee.sql
edge=logement_2020->mobilite_renomee|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/recensement/mobilite_renomee.sql
edge=logement_2020_dev->activite_renomee|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/recensement/activite_renomee.sql
edge=logement_2020_dev->demographie_renomee|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/recensement/demographie_renomee.sql
edge=logement_2020_dev->habitat_renomee|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/recensement/habitat_renomee.sql
edge=logement_2020_dev->mobilite_renomee|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/recensement/mobilite_renomee.sql
edge=infos_communes->revenu_commune|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/prepare/revenu/revenu_commune.sql
edge=filosofi_commune_2021->revenu_commune|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/prepare/revenu/revenu_commune.sql
edge=4_seeds/logement_2020_valeurs.csv->utils/generer_doc_recenssement.py|confidence=0.75|reason=inferred from literal argument in read_csv() call|source=utils/generer_doc_recenssement.py

