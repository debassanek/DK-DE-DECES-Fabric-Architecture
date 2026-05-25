
> **Data Engineering (Cloud) end-to-end· Microsoft Fabric · Architecture Médaillon**  
> Pipeline médallion complet: de l'API data.gouv.fr jusqu'au rapport Power BI

![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=flat&logo=microsoft&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=flat&logo=delta&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![DAX](https://img.shields.io/badge/DAX-0078D4?style=flat&logo=microsoft&logoColor=white)

---

## Vue d'ensemble

Ce projet implémente un pipeline **Data Engineering end-to-end** sur **Microsoft Fabric**, analysant les données de mortalité françaises publiées par [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/).

Il couvre l'intégralité de la chaîne de valeur data :

- **Ingestion automatisée** via l'API data.gouv.fr (découverte dynamique, téléchargement résilient)
- **Architecture Médallion** : Bronze → Silver → Gold avec Delta Lake sur OneLake
- **Modèle décisionnel en étoile** : fact_deces + 4 dimensions + 4 tables d'agrégation
- **Rapport Power BI** : 7 pages analytiques avec 50+ mesures DAX

---

## Architecture

```

API data.gouv.fr
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE — DK_DE_ListeDeces_Bronze_Ingest (OneLake Files)        │
│  Discover → Download → Parse → Write                            │
│  Sortie : fichiers Parquet bruts partitionnés par année         │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER — DK_DE_ListeDeces_Silver_Clean (Delta Lake)            │
│  Clean → Normalize → Validate                                   │
│  Sortie : silver_deces — table Delta unifiée et qualifiée       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD — DK_DE_ListeDeces_Gold_Build (Delta Lake)                │
│  Fact + AggAge + AggTemporel + AggGeographie + AggGeneration    │
│  Sortie : modèle en étoile prêt pour la couche sémantique       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  DIMENSIONS — DK_DE_Source_Dim (Delta Lake)                     │
│  dim_lieu (API geo.api.gouv.fr + INSEE)                         │
│  dim_date · dim_age · dim_generation                            │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
              Power BI - Rapport d'analyse
              7 pages · 50+ mesures DAX
```

Pour le détail de l'architecture, cliquer => [docs/architecture.md](docs/architecture.md).

---

## Stack technique

| Composant | Technologie |
|---|---|
| Plateforme cloud | Microsoft Fabric (OneLake, Spark, Data Pipeline) |
| Traitement données | PySpark (Synapse Analytics) |
| Format de stockage | Delta Lake (format ouvert, ACID) |
| Orchestration | Data Pipelines Fabric (Master → Bronze → Silver → Gold) |
| Sources externes | API data.gouv.fr, API geo.api.gouv.fr, API INSEE Métadonnées |
| Visualisation | Power BI (DirectLake / Import) |
| Langage mesures | DAX |
| Monitoring | Notebook dédié + Lakehouse Monitoring |

---

## Structure du projet

![DK-DE-DECES-Fabric_Workspace de dev](https://raw.githubusercontent.com/debassanek/DK-DE-DECES-Fabric-Architecture/main/img/Fabric/DK-DE-DECES-Fabric_Workspace%20de%20dev.png)
```
DK-DE-DECES-Fabric-Architecture/
│
├── 01_Notebooks/
│   ├── 01_Bronze/          # Discover · Download · Parse · Write
│   ├── 02_Silver/          # Clean · Normalize · Validate
│   ├── 03_Gold/            # Fact · AggAge · AggTemporel · AggGeographie · AggGeneration · Marts
│   └── 04_Dimensions/      # Dim_Lieu · Dim_Date · Dim_Age · Dim_Generation
│
├── 02_Pipelines/
│   ├── MasterPipeline      # Orchestrateur principal (Bronze → Silver → Gold → Dims)
│   ├── BronzePipeline      # Ingestion brute
│   ├── SilverPipeline      # Nettoyage
│   ├── GoldPipeline        # Construction modèle
│   ├── PipelineDimensions  # Chargement dimensions (planifié)
│   ├── MonitoringPipeline  # Observabilité
│   └── CopyData_pl         # Utilitaire copie données test en local via SQL server
│
├── 03_Lakehouses/
│   ├── DK_DE_ListeDeces_Bronze_Ingest.Lakehouse
│   ├── DK_DE_ListeDeces_Silver_Clean.Lakehouse
│   ├── DK_DE_ListeDeces_Gold_Build.Lakehouse
│   ├── DK_DE_Source_Dim.Lakehouse
│   └── DK_DE_DECES_API_Monitoring.Lakehouse
│
├── 04_Reports&Dashboard/
│   ├── DK-BI-ListeDeces-Reports-PowerBI.pbix   # Rapport Power BI
│   ├── DK-BI-ListeDeces-DAX-Measures.md        # Toutes les mesures DAX documentées
│   ├── DK-BI-ListeDeces-Maquettes.html         # Maquettes des 7 pages
│   └── DK-BI-Guide-Navigation-PowerBI.html     # Guide visuel de navigation
│
├── 90_Monitoring/
│   ├── Monitoring-Metrics.Notebook             # KPIs de qualité des données
│   └── Monitoring-PipelineRuns.Notebook        # Suivi des exécutions pipeline
│
├── 99_Utils/
│   ├── Utils-DeltaUtils.Notebook               # Helpers Delta Lake (MERGE, VACUUM, OPTIMIZE)
│   ├── Utils-LoggingUtils.Notebook             # Logging structuré centralisé
│   ├── Utils-ParsingUtils.Notebook             # Parsing des fichiers sources INSEE
│   ├── Utils-SparkUtils.Notebook               # Configuration Spark / session
│   └── Utils-ValidationUtils.Notebook          # Contrôles qualité réutilisables
│
├── docs/
│   ├── architecture.md                         # Architecture détaillée + diagrammes
│   └── data_dictionary.md                      # Dictionnaire des données (toutes les tables)
│
├── Datamart_Deces.xlsx                         # Aperçu du datamart
├── .gitignore
└── README.md
```

---

## Pages du rapport Power BI

| # | Page | Description |
|---|---|---|
| 1 | Overview: Vue globale | Évolution temporelle, répartition géographique nationale |
| 2 | Démographie : Analyse par âge et sexe | Pyramide des décès, Part Femmes/Hommes, évolution, écarts régionaux |
| 3 | Temporelle : Analyse dans le temps | Evolution dans le temps, saisonnalité, facteurs évènementiels, anomalies |
| 4 | Analyse Géographique | Carte France, comparaison communes/départements/régions, densités |
| 5 | Analyse Générationnelle | Décès par cohorte de naissance |
| 6 | Centenaires | Focus 100 ans+, super-centenaires, records, espérance de vie |
| 7 | Profil communes | Focus sur chaque commune, tableau de bord comparatif commune vs France entière |


Pour visualiser le rapport PowerBI, cliquer => **[Aperçu des reports](docs/report_powerbi.md)**.

---

## Lancer le pipeline

> **Prérequis** : accès à un workspace Microsoft Fabric avec droits Contributor.

Le pipeline est conçu pour tourner entièrement dans Fabric. Il n'y a pas d'exécution locale.

1. Importer le workspace depuis le repo Git (Fabric ALM / Git Integration)
2. Vérifier les connexions Lakehouse dans chaque notebook (IDs auto-résolus via ALM)
3. Déclencher **MasterPipeline** — il enchaîne automatiquement Bronze → Silver → Gold → Dimensions
4. Le pipeline **PipelineDimensions** est planifié (schedule hebdomadaire) pour les mises à jour dim_lieu

### Environnements supportés

Le pipeline détecte l'environnement via le nom du workspace :

| Workspace contient | Environnement | Comportement |
|---|---|---|
| `Dev` | dev | Traite 3 année de données |
| `Test` | test | Traite 5 années de données |
| *(autre)* | prod | Traite l'intégralité des fichiers disponibles |

---

## Source des données

- **Fichier des personnes décédées** - INSEE / data.gouv.fr  
  Données publiques en open data, mise à jour mensuelle  
  Environ 600 000 décès par an, couvrant 1970 à aujourd'hui

- **Référentiel communes** - API geo.api.gouv.fr  
  Coordonnées GPS, département, région, superficie

- **Population communale** - API INSEE Métadonnées  
  Population légale par commune pour calcul de densité

---

## Voir aussi

- **[Architecture détaillée](docs/architecture.md)**: Organisation et transformation des données
- **[Dictionnaire des données](docs/data_dictionary.md)**: Référentiel des tables, champs et règles de gestion
- **[Aperçu des reports](docs/report_powerbi.md)**: Aperçu visuel des pages du rapport Power BI

---

## Auteur

**Debassane K.**  
Data & BI  
[debassanek@gmail.com](mailto:debassanek@gmail.com)
