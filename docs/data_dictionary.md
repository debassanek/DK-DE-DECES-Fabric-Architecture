# Dictionnaire des Données : DK-DE-DECES . Microsoft Fabric

> Projet : Analyse de la Mortalité en France · Microsoft Fabric  
> Format de stockage : Delta Lake (OneLake)  

---

## Table des matières

-[Couche Bronze](#1-couche-bronze)
-[Couche Silver](#2-couche-silver--silver_deces)
-[Couche Gold : Table de faits](#3-couche-gold--fact_deces)
-[Couche Gold : Agrégations](#4-couche-gold--tables-dagrégation)
-[Dimensions](#5-dimensions)
-[Monitoring](#6-monitoring)

---

## 1-Couche Bronze

### Fichiers bruts (`DK_DE_ListeDeces_Bronze_Ingest`)

Les fichiers bruts sont stockés dans OneLake Files au format texte/CSV (format original INSEE).  
Un manifeste JSON est produit par le notebook Discover et consommé par Download.

**Manifeste `_discovery.json`**

| Champ | Type | Description |
|---|---|---|
| `url` | string | URL de téléchargement sur data.gouv.fr |
| `filename` | string | Nom du fichier source |
| `annee` | integer | Année couverte par le fichier |
| `taille_bytes` | integer | Taille en octets du fichier source |
| `date_decouverte` | timestamp | Horodatage de la découverte |

**Manifeste `_download.json`**

| Champ | Type | Description |
|---|---|---|
| `fichiers_telecharges` | list | Liste des fichiers téléchargés avec statut |
| `nb_ok` | integer | Nombre de fichiers téléchargés avec succès |
| `nb_skip` | integer | Fichiers ignorés (déjà présents, skip_existing) |
| `nb_erreur` | integer | Fichiers en erreur |
| `date_telechargement` | timestamp | Horodatage du téléchargement |

---

## 2-Couche Silver : `silver_deces`

**Lakehouse** : `DK_DE_ListeDeces_Silver_Clean`  
**Table Delta** : `silver_deces`  
**Granularité** : 1 ligne = 1 décès individuel  
**Source** : fichiers bruts Bronze parsés et normalisés

| Colonne | Type | Description | Valeurs / Format |
|---|---|---|---|
| `nom` | string | Nom de famille de la personne décédée | Majuscules, nettoyé |
| `prenom` | string | Premier prénom | Majuscules |
| `sexe` | string | Genre | `M` = masculin, `F` = féminin |
| `date_naissance` | date | Date de naissance | `yyyy-MM-dd` (null si inconnue) |
| `date_deces` | date | Date de décès | `yyyy-MM-dd` |
| `age_au_deces` | integer | Âge au moment du décès (calculé) | 0-145 ; null si dates absentes |
| `code_lieu_deces` | string | Code INSEE de la commune de décès | 5 caractères (zfill) |
| `departement_deces` | string | Code département extrait du code INSEE | 2 car. (01-95) ou 3 car. (DOM-TOM) |
| `annee_deces` | integer | Année du décès (extraite de date_deces) | 1970–présent |
| `mois_deces` | integer | Mois du décès (1–12) | 1–12 |

**Règles de qualité appliquées à Silver :**
- Lignes sans `date_deces` exploitable → rejetées
- `age_au_deces` < 0 ou > 145 → mis à null
- `code_lieu_deces` : zero-fill à 5 caractères, null si vide
- Dédoublonnage sur (nom, prenom, date_naissance, date_deces, code_lieu_deces)

---

## 3-Couche Gold : `fact_deces`

**Lakehouse** : `DK_DE_ListeDeces_Gold_Build`  
**Table Delta** : `fact_deces`  
**Granularité** : 1 ligne = 1 décès individuel (même cardinalité que Silver)  
**Source** : `silver_deces` + enrichissement des clés de jointure

Toutes les colonnes de Silver sont conservées, plus les clés de liaison vers les tables d'agrégation Gold :

| Colonne | Type | Description |
|---|---|---|
| *(toutes les colonnes Silver)* | — | Voir section Silver |
| `key_aggAge` | string | Clé vers `agg_mortalite_age` : format `{age}_{AnMois}` |
| `key_aggTemporel` | string | Clé vers `agg_mortalite_mensuelle` : format `AnMois yyyyMM` |
| `key_aggGeographie` | string | Clé vers `agg_mortalite_commune` : code INSEE commune (5 car.) |
| `key_aggGeneration` | string | Clé vers `agg_mortalite_generation` : composite naissance+age+commune |

> Les clés sont `null` si les colonnes sources nécessaires sont absentes (ex : code_lieu_deces vide → key_aggGeographie = null).

---

## 4-Couche Gold : Tables d'agrégation

### `agg_mortalite_age`

**Granularité** : 1 ligne = 1 âge exact × 1 période (AnMois)

| Colonne | Type | Description |
|---|---|---|
| `key_age` | string | Clé primaire : `{age}_{AnMois}` |
| `age` | integer | Âge exact au décès |
| `id_date` | string | Période au format `yyyyMM` (jointure `dim_date`) |
| `nb_deces` | long | Nombre total de décès |
| `nb_hommes` | long | Décès masculins |
| `nb_femmes` | long | Décès féminins |
| `pct_hommes` | double | Part hommes (%) |
| `pct_femmes` | double | Part femmes (%) |
| `est_centenaire` | integer | Indicateur âge ≥ 100 |
| `est_mineur` | integer | Indicateur âge < 18 |

---

### `agg_mortalite_mensuelle`

**Granularité** : 1 ligne = 1 période mensuelle (AnMois)

| Colonne | Type | Description |
|---|---|---|
| `id_date` | string | Clé primaire : `yyyyMM` |
| `nb_deces` | long | Nombre total de décès du mois |
| `age_moyen_deces` | double | Âge moyen au décès |
| `age_median_deces` | double | Âge médian au décès |
| `nb_hommes` | long | Décès masculins |
| `nb_femmes` | long | Décès féminins |
| `nb_centenaires` | long | Décès de personnes ≥ 100 ans |

---

### `agg_mortalite_commune`

**Granularité** : 1 ligne = 1 commune (agrégation sur toute la période)

| Colonne | Type | Description |
|---|---|---|
| `id_commune` | string | Code INSEE commune (5 car.) — clé primaire |
| `nb_deces` | long | Nombre total de décès |
| `age_moyen_deces` | double | Âge moyen au décès dans la commune |
| `age_median_deces` | double | Âge médian au décès |
| `age_min` | integer | Âge le plus jeune enregistré |
| `age_max` | integer | Âge le plus élevé enregistré |
| `nb_hommes` | long | Décès masculins |
| `nb_femmes` | long | Décès féminins |
| `nb_centenaires` | long | Décès centenaires (≥ 100 ans) |
| `nb_mineurs` | long | Décès de mineurs (< 18 ans) |
| `taux_mortalite` | double | Taux pour 1 000 habitants (si population connue) |
| `anomalie_mortalite` | boolean | Indicateur z-score : commune statistiquement atypique |

---

### `agg_mortalite_generation`

**Granularité** : 1 ligne = 1 cohorte de naissance × commune × âge

| Colonne | Type | Description |
|---|---|---|
| `id_generation` | string | Clé composite : génération + commune + âge |
| `annee_naissance` | integer | Année de naissance de la cohorte |
| `id_commune` | string | Code INSEE commune |
| `age_au_deces` | integer | Âge au décès |
| `nb_deces` | long | Nombre de décès dans cette combinaison |
| `nb_hommes` | long | Décès masculins |
| `nb_femmes` | long | Décès féminins |

---

## 5-Dimensions

### `dim_lieu`

**Lakehouse** : `DK_DE_Source_Dim`  
**Sources** : API geo.api.gouv.fr (communes, départements, régions) + API INSEE Métadonnées (population)

| Colonne | Type | Description |
|---|---|---|
| `id_commune` | string | Code INSEE commune : clé primaire (5 car.) |
| `commune` | string | Libellé de la commune |
| `département` | string | Nom du département |
| `code_dept` | string | Code département (2 ou 3 caractères) |
| `région` | string | Nom de la région |
| `code_region` | string | Code région INSEE |
| `latitude` | double | Latitude GPS (centroïde commune) |
| `longitude` | double | Longitude GPS (centroïde commune) |
| `population_commune` | double | Population légale (INSEE ou géo API) |
| `superficie_commune` | double | Superficie en km² |
| `densite_commune` | double | Densité habitants/km² |

---

### `dim_date`

**Sources** : générée programmatiquement (calendrier)

| Colonne | Type | Description |
|---|---|---|
| `id_date` | string | Clé primaire — `yyyyMM` |
| `annee` | integer | Année |
| `mois` | integer | Mois (1–12) |
| `trimestre` | integer | Trimestre (1–4) |
| `semestre` | integer | Semestre (1–2) |
| `libelle_mois` | string | Ex. `Janvier`, `Février`... |
| `annee_mois` | string | Ex. `2023-01` |

---

### `dim_age`

| Colonne | Type | Description |
|---|---|---|
| `age` | integer | Âge exact : clé primaire |
| `tranche_age` | string | Ex. `0-4`, `5-9`, ..., `95-99`, `100+` |
| `groupe_age` | string | Ex. `Mineur`, `Adulte`, `Senior`, `Centenaire` |
| `est_centenaire` | boolean | Âge ≥ 100 |
| `est_mineur` | boolean | Âge < 18 |

---

### `dim_generation`

| Colonne | Type | Description |
|---|---|---|
| `annee_naissance` | integer | Année de naissance : clé primaire |
| `generation` | string | Libellé générationnel (ex. `Baby-Boomers`, `Génération X`) |
| `periode` | string | Décennie ou période (ex. `1946–1964`) |

---

## 6-Monitoring

**Lakehouse** : `DK_DE_DECES_API_Monitoring`

Les notebooks de monitoring produisent des tables de suivi des exécutions pipeline et de qualité des données. Ces tables ne font pas partie du modèle analytique mais servent à l'observabilité opérationnelle.

| Table | Description |
|---|---|
| `pipeline_runs` | Historique des exécutions (statut, durée, nb lignes traitées) |
| `data_quality_metrics` | KPIs qualité par couche (taux de null, taux de rejet, volumétrie) |

---

## Conventions globales

| Règle | Détail |
|---|---|
| **Nommage tables** | `snake_case` : préfixe par couche (`silver_`, `agg_`, `dim_`, `fact_`) |
| **Clés primaires** | Toujours nommées `id_<entité>` ou `key_<agrégation>` |
| **Dates** | Format ISO `yyyy-MM-dd` pour les dates, `yyyyMM` pour les clés temporelles |
| **Codes INSEE** | Toujours zfill à 5 caractères pour les communes, 2-3 pour les départements |
| **Valeurs manquantes** | `null` natif Delta (jamais chaîne vide `""` pour les codes) |
| **Environnements** | Dev (1 an) · Test (3 ans) · Prod (complet) : détection automatique |

---

## Retour au projet principal

- [Projet principal](../README.md)  
- [Aperçu rapport Power BI](../docs/report_powerbi.md)  
- [Architecture détaillée](../docs/architecture.md)

---

Auteur : Debassane K. · Data & BI · debassanek@gmail.com
