<!-- CARTOGRAPHER v1 | generated: 2026-03-15T04:02:25.338290 | nodes: 135 | edges: 164 -->

## SECTION:COMPLETENESS_SCORE
COMPLETENESS: MEDIUM
edges_total=164 high_confidence=62% medium=27% low=11% dynamic_refs_unresolved=0 macro_nodes=17

## SECTION:ARCHITECTURE_SUMMARY
target_dir=D:\TRP-1\Week-4\brownfield-cartographer\make-open-data
module_nodes=32
dataset_nodes=103
lineage_edges=164
domain_count=5

## SECTION:CRITICAL_PATH
1|node=ventes_immobilieres|pagerank=0.0986|why=Centrality driven by 23 upstream sources and 1 downstream dependents.|purpose=
2|node=ventes_immobilieres_enrichies|pagerank=0.0885|why=Centrality driven by 1 upstream sources and 0 downstream dependents.|purpose=
3|node=load/loaders.py|pagerank=0.0327|why=Centrality driven by 7 upstream sources and 5 downstream dependents.|purpose=The module enables the ingestion of structured data (CSV, JSON, shapefiles) from external storage into a PostgreSQL database, facilitating data integration and persistent storage for downstream applications. It handles file downloading, format conversion, and table creation with proper schema management, ensuring data is correctly structured and validated before insertion.
4|node=infos_communes|pagerank=0.0248|why=Centrality driven by 7 upstream sources and 9 downstream dependents.|purpose=
5|node=tests/load/test_loaders.py|pagerank=0.0183|why=Centrality driven by 3 upstream sources and 0 downstream dependents.|purpose=The module enables the ingestion of CSV data into a PostgreSQL database, ensuring that the data is correctly formatted and mapped to the target schema. It verifies the integrity of the loaded data through automated tests, allowing for reliable data entry and retrieval.

## SECTION:SOURCES
node=4_seeds/logement_2020_valeurs.csv
node=load/__init__.py
node=logement_2020_valeurs
node=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
node=make-open-data/1_data/intermediaires/recensement/activite_renomee.sql
node=make-open-data/1_data/intermediaires/recensement/demographie_renomee.sql
node=make-open-data/1_data/intermediaires/recensement/habitat_renomee.sql
node=make-open-data/1_data/intermediaires/recensement/mobilite_renomee.sql
node=make-open-data/1_data/prepare/recensement/activite/activite_communes.sql
node=make-open-data/1_data/prepare/recensement/activite/activite_departements.sql
node=make-open-data/1_data/prepare/recensement/activite/activite_iris.sql
node=make-open-data/1_data/prepare/recensement/demographie/demographie_communes.sql
node=make-open-data/1_data/prepare/recensement/demographie/demographie_departements.sql
node=make-open-data/1_data/prepare/recensement/demographie/demographie_iris.sql
node=make-open-data/1_data/prepare/recensement/habitat/habitat_communes.sql
node=make-open-data/1_data/prepare/recensement/habitat/habitat_departements.sql
node=make-open-data/1_data/prepare/recensement/habitat/habitat_iris.sql
node=make-open-data/1_data/prepare/recensement/mobilite/mobilite_communes.sql
node=make-open-data/1_data/prepare/recensement/mobilite/mobilite_departements.sql
node=make-open-data/1_data/prepare/recensement/mobilite/mobilite_iris.sql

## SECTION:SINKS
node=activite_departements
node=activite_iris
node=activite_renomee
node=besoin_main_oeuvre_departement
node=bpe_2023
node=bpe_metadata
node=commune_centroid_poste
node=demographie_departements
node=demographie_iris
node=demographie_renomee
node=filosofi_iris_2021
node=habitat_departements
node=habitat_iris
node=infos_postes
node=infos_scot
node=load/__main__.py
node=mobilite_departements
node=mobilite_iris
node=mobilite_renomee
node=place

## SECTION:KNOWN_DEBT
cycles=0
drift_flags=25
drift|module=4_seeds/logement_2020_valeurs.csv|verdict=DRIFT|explanation=The docstring claims the code generates documentation but actually prints data without creating any documentation. It also includes a warning about checking values, which is not part of the implementation.
drift|module=extract/source_to_storage.yml|verdict=MISSING|explanation=No docstring found.
drift|module=load/__init__.py|verdict=MISSING|explanation=No docstring found.
drift|module=load/loaders.py|verdict=MISSING|explanation=No docstring found.
drift|module=macro:aggreger_colonnes_theme_geo:5_macros/recensement/aggreger_colonnes_theme_geo.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:aggreger_logement_par_colonne:5_macros/recensement/aggreger_logement_par_colonne.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:aggreger_supra_commune:5_macros/recensement/aggreger_supra_commune.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:aggreger_ventes_immobiliers:5_macros/foncier/aggreger_ventes_immobiliers.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:filtrer_unpivot_logement:5_macros/recensement/filtrer_unpivot_logement.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:filtrer_ventes_immobilieres:5_macros/foncier/filtrer_ventes_immobilieres.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:lister_champs_valeurs_libelle:5_macros/recensement/lister_valeurs_un_champ.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:lister_colonnes_modaliter_libelles:5_macros/recensement/lister_colonnes_codes_libelle.sql:1|verdict=DRIFT|explanation=The docstring claims the macro returns "Colonnes uniques" (unique columns), but the implementation generates three lists: the first is the unique columns, while the other two are lists of modalities and their labels. The docstring does not mention the latter two lists, creating confusion about the macro's purpose.
drift|module=macro:lister_colonnes_par_theme:5_macros/recensement/lister_champs_par_theme.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:nettoyer_modalite_revenu:5_macros/revenu/nettoyer_modalite_revenu.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:pivoter_logement:5_macros/recensement/pivoter_logement.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:preparer_dvf_biens_immobilier:5_macros/foncier/preparer_dvf_biens_immobilier.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:renommer_colonnes_values_logement:5_macros/recensement/renommer_colonnes_valeurs_logement.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:renommer_logement:5_macros/recensement/renommer_logement.sql:1|verdict=MISSING|explanation=No docstring found.
drift|module=macro:selectionner_bien_principal:5_macros/foncier/selectionner_bien_principal.sql:1|verdict=MISSING|explanation=No docstring found.
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
none

## SECTION:MACRO_INDEX
macro=aggreger_ventes_immobiliers|source=5_macros/foncier/aggreger_ventes_immobiliers.sql|line=1|args=ventes_immobiliers_filtrees|purpose=This module aggregates real estate sales data by property mutations, summarizing total surface area and number of principal rooms across all listed properties, enabling business stakeholders to analyze property performance and calculate aggregate metrics for reporting or decision-making.
macro=filtrer_ventes_immobilieres|source=5_macros/foncier/filtrer_ventes_immobilieres.sql|line=1|args=source_dvf|purpose=This module filters real estate sales data to include only transactions of apartments and houses, ensuring only relevant property types are processed. It excludes non-sale mutations (e.g., rentals, renovations) and ensures data consistency by linking each sale to its corresponding property record. The result is a curated dataset of verified real estate sales transactions for analysis or reporting purposes.
macro=calculate_geo_knn|source=5_macros/foncier/geo_knn.sql|line=4|args=source_table,id_column,geopoint_column,value_column,k|purpose=This macro calculates the k nearest geographic points for each row in a dataset, computing the average of a specified value column for each neighbor, enabling spatial analysis to assess property values or other metrics in proximity-based contexts.
macro=preparer_dvf_biens_immobilier|source=5_macros/foncier/preparer_dvf_biens_immobilier.sql|line=1|args=millesime|purpose=This module extracts and aggregates real estate sales data from a source database, calculates per-square-meter property values, and joins with geographic information to provide a structured dataset for analyzing property valuations and spatial distribution. It ensures consistent data across production and development environments while maintaining accurate geographic and financial metrics for business insights.
macro=selectionner_bien_principal|source=5_macros/foncier/selectionner_bien_principal.sql|line=1|args=ventes_immobiliers_filtrees|purpose=This module identifies the principal property for each real estate mutation by ranking sales based on property type (Maison first, then Appartement) and square footage, returning the highest-ranked entry. It ensures consistency in selecting the most suitable property for each transaction, supporting accurate data for listing or reporting purposes.
macro=aggreger_colonnes_theme_geo|source=5_macros/recensement/aggreger_colonnes_theme_geo.sql|line=1|args=theme,table_renomee,champs_geo_arrivee|purpose=The macro aggregates geographic demographic data by theme, summarizing key metrics such as household counts, occupied logements, and occupancy statuses for different geographic areas. It combines structured data from a source table with pre-aggregated values, enabling business users to analyze population distribution, housing availability, and occupancy trends at the regional level.
macro=aggreger_logement_par_colonne|source=5_macros/recensement/aggreger_logement_par_colonne.sql|line=1|args=table_renomee,colonnes_a_aggreger_liste,colonne_a_aggreger,colonnes_cles_liste,champs_geo_arrivee|purpose=This macro aggregates housing data by specified columns to summarize the number of residences in each category, enabling business analysts to gain insights into demographic distribution and spatial patterns across different geographic areas.
macro=aggreger_supra_commune|source=5_macros/recensement/aggreger_supra_commune.sql|line=1|args=theme,nouveau_niveau_geo|purpose=This macro aggregates housing data (e.g., number of occupied units, vacancies, or total residences) at a specified geographic level, summarizing key metrics from the `logement_2020_valeurs` table. It ensures accurate counts by filtering out missing geographic data and presents these aggregated values for business insights, such as demographic analysis or housing trend reporting.
macro=filtrer_unpivot_logement|source=5_macros/recensement/filtrer_unpivot_logement.sql|line=1|args=unpivoted,libelle_liste,champs_geo,champs_geo_arrivee|purpose=The module filters the unpivoted logement data to include only rows where the `valeur` matches any of the specified `libelle_liste`, enabling analysis of housing properties and their geographic distribution. It extracts key metrics like `poids_du_logement` and `valeur` for further business insights, such as evaluating logement values and spatial patterns.
macro=lister_colonnes_par_theme|source=5_macros/recensement/lister_champs_par_theme.sql|line=1|args=theme|purpose=The module retrieves and aggregates distinct code values (COD_VAR) from the logement_2020_valeurs table for a specified theme, enabling data analysts to generate thematic summaries or reports by focusing on relevant field identifiers tied to specific categories or topics.
macro=lister_colonnes_modaliter_libelles|source=5_macros/recensement/lister_colonnes_codes_libelle.sql|line=1|args=theme|purpose=The module retrieves and organizes distinct variable modalities and their associated labels from a specified dataset, tailored to a given theme, to facilitate structured data aggregation or reporting. It generates unique column identifiers and corresponding label pairs, enabling downstream processes to categorize and analyze data based on predefined criteria.
macro=lister_champs_valeurs_libelle|source=5_macros/recensement/lister_valeurs_un_champ.sql|line=1|args=colonne_a_aggreger|purpose=The macro retrieves distinct aggregated values and their corresponding labels for a specified column from the logement_2020_valeurs table, enabling structured data preparation for downstream processing or display. It ensures accurate mapping of values to their respective labels, facilitating efficient data aggregation and visualization.
macro=pivoter_logement|source=5_macros/recensement/pivoter_logement.sql|line=1|args=unpivot_filtree,libelle_liste,champs_geo_arrivee|purpose=The macro aggregates the total weight of logements (e.g., housing units) by distinct categories (e.g., types of residences) from a flattened dataset, enabling business users to analyze spatial distribution and demographic characteristics of logements across different geographic areas.
macro=renommer_colonnes_values_logement|source=5_macros/recensement/renommer_colonnes_valeurs_logement.sql|line=1|args=logement,theme|purpose=This module standardizes geographic identifiers in the logement table by renaming columns like "ARM" to "ARM" or "COMMUNE" based on specific conditions, and mapping "IRIS" codes to combined abbreviations. It ensures consistent representation of administrative divisions, enabling accurate data interpretation for spatial analysis and reporting.
macro=renommer_logement|source=5_macros/recensement/renommer_logement.sql|line=1|args=unpivot_filtree,champs_valeurs_liste,libelle_liste,champs_geo_arrivee|purpose=The module renames logements (homes) based on their values in a list, aggregates population data, and is used to facilitate analysis or reporting by grouping and summarizing demographic information across different geographic locations.
macro=unpivot_logement|source=5_macros/recensement/unpivot_logement.sql|line=1|args=table_renomee,colonnes_a_aggreger_liste,colonnes_cles_liste,colonne_a_aggreger|purpose=This macro transforms a table's long-format data into a wide format by un-pivoting specified columns, excluding predefined keys and irrelevant columns, to prepare structured data for analysis or reporting purposes.
macro=nettoyer_modalite_revenu|source=5_macros/revenu/nettoyer_modalite_revenu.sql|line=1|args=nom_colonne|purpose=This macro formats a column name by replacing commas with dots and converting it to a numeric format, ensuring consistent data representation for financial calculations. It handles edge cases like special characters by using nullif to eliminate unwanted values, preparing the column name for proper data processing and validation.

## SECTION:MODULE_PURPOSE_INDEX
module=4_seeds/logement_2020_valeurs.csv|purpose=This module generates a structured documentation of demographic values from a CSV file, enabling users to understand the relationship between different variables and their respective values in the dataset. It provides a clear overview of the data schema, including the number of households, variable codes, and descriptive labels, which is essential for data preparation and analysis workflows.
module=extract/__init__.py|purpose=The module enables the manual extraction of data from source systems to storage, leveraging the Lake House approach to manage data movement, despite the complexity and infrequency of the process. It serves as a critical component for transitioning data into storage formats required by downstream analytics or processing workflows.
module=extract/source_to_storage.yml|purpose=The module aggregates diverse datasets from public sources such as postal codes, communes, and statistical records to provide structured geographical and demographic information for business analytics and decision-making. It combines data from various domains to support applications requiring detailed spatial and demographic data.
module=load/__init__.py|purpose=This module provides functions to load data and configuration parameters from various sources, enabling applications to retrieve and initialize data efficiently, thereby supporting scalable and flexible data processing workflows.
module=load/__main__.py|purpose=This module extracts data from specified sources and loads it into a PostgreSQL database, handling different file formats (CSV, JSON, shapefiles) and skipping operations in non-production environments. It ensures data is properly processed and stored according to the configuration defined in the `storage_to_pg.yml` file.
module=load/loaders.py|purpose=The module enables the ingestion of structured data (CSV, JSON, shapefiles) from external storage into a PostgreSQL database, facilitating data integration and persistent storage for downstream applications. It handles file downloading, format conversion, and table creation with proper schema management, ensuring data is correctly structured and validated before insertion.
module=load/storage_to_pg.yml|purpose=The module retrieves structured datasets from S3 storage, loads them into the "sources" database schema, and provides business-ready data for analysis, reporting, and integration with applications requiring geographic or demographic information. It ensures consistency across development and production environments by managing both full and dev versions of datasets.
module=macro:aggreger_colonnes_theme_geo:5_macros/recensement/aggreger_colonnes_theme_geo.sql:1|purpose=The macro aggregates geographic demographic data by theme, summarizing key metrics such as household counts, occupied logements, and occupancy statuses for different geographic areas. It combines structured data from a source table with pre-aggregated values, enabling business users to analyze population distribution, housing availability, and occupancy trends at the regional level.
module=macro:aggreger_logement_par_colonne:5_macros/recensement/aggreger_logement_par_colonne.sql:1|purpose=This macro aggregates housing data by specified columns to summarize the number of residences in each category, enabling business analysts to gain insights into demographic distribution and spatial patterns across different geographic areas.
module=macro:aggreger_supra_commune:5_macros/recensement/aggreger_supra_commune.sql:1|purpose=This macro aggregates housing data (e.g., number of occupied units, vacancies, or total residences) at a specified geographic level, summarizing key metrics from the `logement_2020_valeurs` table. It ensures accurate counts by filtering out missing geographic data and presents these aggregated values for business insights, such as demographic analysis or housing trend reporting.
module=macro:aggreger_ventes_immobiliers:5_macros/foncier/aggreger_ventes_immobiliers.sql:1|purpose=This module aggregates real estate sales data by property mutations, summarizing total surface area and number of principal rooms across all listed properties, enabling business stakeholders to analyze property performance and calculate aggregate metrics for reporting or decision-making.
module=macro:calculate_geo_knn:5_macros/foncier/geo_knn.sql:4|purpose=This macro calculates the k nearest geographic points for each row in a dataset, computing the average of a specified value column for each neighbor, enabling spatial analysis to assess property values or other metrics in proximity-based contexts.
module=macro:filtrer_unpivot_logement:5_macros/recensement/filtrer_unpivot_logement.sql:1|purpose=The module filters the unpivoted logement data to include only rows where the `valeur` matches any of the specified `libelle_liste`, enabling analysis of housing properties and their geographic distribution. It extracts key metrics like `poids_du_logement` and `valeur` for further business insights, such as evaluating logement values and spatial patterns.
module=macro:filtrer_ventes_immobilieres:5_macros/foncier/filtrer_ventes_immobilieres.sql:1|purpose=This module filters real estate sales data to include only transactions of apartments and houses, ensuring only relevant property types are processed. It excludes non-sale mutations (e.g., rentals, renovations) and ensures data consistency by linking each sale to its corresponding property record. The result is a curated dataset of verified real estate sales transactions for analysis or reporting purposes.
module=macro:lister_champs_valeurs_libelle:5_macros/recensement/lister_valeurs_un_champ.sql:1|purpose=The macro retrieves distinct aggregated values and their corresponding labels for a specified column from the logement_2020_valeurs table, enabling structured data preparation for downstream processing or display. It ensures accurate mapping of values to their respective labels, facilitating efficient data aggregation and visualization.
module=macro:lister_colonnes_modaliter_libelles:5_macros/recensement/lister_colonnes_codes_libelle.sql:1|purpose=The module retrieves and organizes distinct variable modalities and their associated labels from a specified dataset, tailored to a given theme, to facilitate structured data aggregation or reporting. It generates unique column identifiers and corresponding label pairs, enabling downstream processes to categorize and analyze data based on predefined criteria.
module=macro:lister_colonnes_par_theme:5_macros/recensement/lister_champs_par_theme.sql:1|purpose=The module retrieves and aggregates distinct code values (COD_VAR) from the logement_2020_valeurs table for a specified theme, enabling data analysts to generate thematic summaries or reports by focusing on relevant field identifiers tied to specific categories or topics.
module=macro:nettoyer_modalite_revenu:5_macros/revenu/nettoyer_modalite_revenu.sql:1|purpose=This macro formats a column name by replacing commas with dots and converting it to a numeric format, ensuring consistent data representation for financial calculations. It handles edge cases like special characters by using nullif to eliminate unwanted values, preparing the column name for proper data processing and validation.
module=macro:pivoter_logement:5_macros/recensement/pivoter_logement.sql:1|purpose=The macro aggregates the total weight of logements (e.g., housing units) by distinct categories (e.g., types of residences) from a flattened dataset, enabling business users to analyze spatial distribution and demographic characteristics of logements across different geographic areas.
module=macro:preparer_dvf_biens_immobilier:5_macros/foncier/preparer_dvf_biens_immobilier.sql:1|purpose=This module extracts and aggregates real estate sales data from a source database, calculates per-square-meter property values, and joins with geographic information to provide a structured dataset for analyzing property valuations and spatial distribution. It ensures consistent data across production and development environments while maintaining accurate geographic and financial metrics for business insights.
module=macro:renommer_colonnes_values_logement:5_macros/recensement/renommer_colonnes_valeurs_logement.sql:1|purpose=This module standardizes geographic identifiers in the logement table by renaming columns like "ARM" to "ARM" or "COMMUNE" based on specific conditions, and mapping "IRIS" codes to combined abbreviations. It ensures consistent representation of administrative divisions, enabling accurate data interpretation for spatial analysis and reporting.
module=macro:renommer_logement:5_macros/recensement/renommer_logement.sql:1|purpose=The module renames logements (homes) based on their values in a list, aggregates population data, and is used to facilitate analysis or reporting by grouping and summarizing demographic information across different geographic locations.
module=macro:selectionner_bien_principal:5_macros/foncier/selectionner_bien_principal.sql:1|purpose=This module identifies the principal property for each real estate mutation by ranking sales based on property type (Maison first, then Appartement) and square footage, returning the highest-ranked entry. It ensures consistency in selecting the most suitable property for each transaction, supporting accurate data for listing or reporting purposes.
module=macro:unpivot_logement:5_macros/recensement/unpivot_logement.sql:1|purpose=This macro transforms a table's long-format data into a wide format by un-pivoting specified columns, excluding predefined keys and irrelevant columns, to prepare structured data for analysis or reporting purposes.
module=tests/__init__.py|purpose=This module serves as a central repository for test cases, facilitating organization and management of test suites across different modules. It ensures that tests are structured and accessible, enabling efficient maintenance and execution. By providing a standardized structure, it enhances collaboration and reduces duplication across the codebase.
module=tests/conftest.py|purpose=The module enables pytest tests to access a database for integration and unit testing, ensuring that the application's logic is validated against real-world data, which is critical for maintaining data integrity and functionality in production.
module=tests/fixtures/__init__.py|purpose=This module ensures that the test directory is properly configured by creating necessary directories and setting up test environments, enabling seamless execution of test cases and maintaining a structured, organized test framework. It facilitates the isolation of test data and configurations, ensuring consistency across different test runs.
module=tests/fixtures/db.py|purpose=This module provides a reliable, secure connection to a test PostgreSQL database by configuring environment variables and establishing a persistent connection, ensuring tests can seamlessly access the test database without manual setup. It ensures the database is available for use by waiting until the service is responsive, facilitating consistent and reliable test execution.
module=tests/load/__init__.py|purpose=This module manages test cases and provides the necessary configurations to run tests, ensuring that the test environment is set up correctly for the business requirements. It facilitates the execution of test scenarios by loading test data and initializing dependencies, enabling reliable and consistent test runs across different environments.
module=tests/load/test_loaders.py|purpose=The module enables the ingestion of CSV data into a PostgreSQL database, ensuring that the data is correctly formatted and mapped to the target schema. It verifies the integrity of the loaded data through automated tests, allowing for reliable data entry and retrieval.
module=utils/generer_doc_recenssement.py|purpose=The module generates structured documentation for demography-related variables in a commune, extracting and formatting data from a CSV file to provide clear insights into household counts and response patterns. It ensures consistent labeling and presentation of demographic data, aiding in data interpretation and system documentation.

## SECTION:LOW_CONFIDENCE_LINEAGE
edge=logement_2020->activite_renomee|confidence=0.60|reason=conditional Jinja branch — one path active at runtime|source=make-open-data/1_data/intermediaires/recensement/activite_renomee.sql
edge=logement_2020->demographie_renomee|confidence=0.60|reason=conditional Jinja branch — one path active at runtime|source=make-open-data/1_data/intermediaires/recensement/demographie_renomee.sql
edge=logement_2020->habitat_renomee|confidence=0.60|reason=conditional Jinja branch — one path active at runtime|source=make-open-data/1_data/intermediaires/recensement/habitat_renomee.sql
edge=logement_2020->mobilite_renomee|confidence=0.60|reason=conditional Jinja branch — one path active at runtime|source=make-open-data/1_data/intermediaires/recensement/mobilite_renomee.sql
edge=logement_2020_dev->activite_renomee|confidence=0.60|reason=conditional Jinja branch — one path active at runtime|source=make-open-data/1_data/intermediaires/recensement/activite_renomee.sql
edge=logement_2020_dev->demographie_renomee|confidence=0.60|reason=conditional Jinja branch — one path active at runtime|source=make-open-data/1_data/intermediaires/recensement/demographie_renomee.sql
edge=logement_2020_dev->habitat_renomee|confidence=0.60|reason=conditional Jinja branch — one path active at runtime|source=make-open-data/1_data/intermediaires/recensement/habitat_renomee.sql
edge=logement_2020_dev->mobilite_renomee|confidence=0.60|reason=conditional Jinja branch — one path active at runtime|source=make-open-data/1_data/intermediaires/recensement/mobilite_renomee.sql
edge=filosofi_commune_2021->revenu_commune|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/prepare/revenu/revenu_commune.sql
edge=dvf_default->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2014_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2015_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2016_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2017_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2018_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2019_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2020_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2021_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2022_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2023_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2024_dev->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2014->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2015->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2016->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2017->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2018->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2019->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2020->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2021->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2022->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2023->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=dvf_2024->ventes_immobilieres|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/intermediaires/foncier/ventes_immobilieres.sql
edge=infos_communes->revenu_commune|confidence=0.70|reason=extracted via sqlglot but found Jinja placeholders, structural integrity uncertain|source=make-open-data/1_data/prepare/revenu/revenu_commune.sql
edge=4_seeds/logement_2020_valeurs.csv->utils/generer_doc_recenssement.py|confidence=0.75|reason=inferred from literal argument in read_csv() call|source=utils/generer_doc_recenssement.py

