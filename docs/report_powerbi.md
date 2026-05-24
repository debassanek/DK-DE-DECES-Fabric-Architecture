# Rapport Power BI : Aperçus & Description

> **DK-DE-DECES · Microsoft Fabric · Architecture Médaillon**  
> Rapport d'analyse de la mortalité française : 7 pages analytiques · 50+ mesures DAX

---

## Table des matières

- [Modèle de données](#modèle-de-données)
- [Pages du rapport](#pages-du-rapport)
  - [1-Overview : Vue globale](#1--overview--vue-globale)
  - [2-Démographie : Analyse par âge et sexe](#2--démographie--analyse-par-âge-et-sexe)
  - [3-Géographie : Analyse territoriale](#3--géographie--analyse-territoriale)
  - [4-Temporelle : Analyse dans le temps](#4--temporelle--analyse-dans-le-temps)
  - [5-Générationnelle : Analyse par cohorte](#5--générationnelle--analyse-par-cohorte)
  - [6-Centenaires](#6--centenaires)
  - [7-Profil commune](#7--profil-commune)
- [Stack DAX & modélisation](#stack-dax--modélisation)

---

## Modèle de données

Le rapport repose sur un **modèle en étoile** construit sur la couche Gold du Lakehouse, connecté via **DirectQuery** à la place du **DirectLake** pour des raisons techniques.

![07_DK-DE-DECES-PBI_Modele etoile](https://raw.githubusercontent.com/debassanek/DK-DE-DECES-Fabric-Architecture/main/img/PowerBI/07_DK-DE-DECES-PBI_Modele%20etoile.png)


### Composition du modèle

| Rôle | Table | Description |
|---|---|---|
| **Fait** | `fact_deces` | Table de faits centrale — un enregistrement par décès |
| **Dimension** | `dim_lieu` | Référentiel géographique (commune, département, région) |
| **Dimension** | `dim_age` | Tranches d'âge (5 ans, 10 ans, large), catégories, centenaire |
| **Dimension** | `dim_date` | Calendrier complet (année, mois, saison, libellés) |
| **Dimension** | `dim_generation` | Cohortes de naissance (Silent Gen, Baby Boomers, Gen X…) |
| **Agrégat** | `agg_mortalite_commune` | Indicateurs pré-calculés par commune |
| **Agrégat** | `agg_mortalite_mensuelle` | Indicateurs pré-calculés par mois |
| **Agrégat** | `agg_mortalite_age` | Indicateurs pré-calculés par tranche d'âge |
| **Agrégat** | `agg_mortalite_generation` | Indicateurs pré-calculés par génération |
| **Paramètre** | `param_generations` | Seuils dynamiques des cohortes |
| **Paramètre** | `param_seuil_z-score` | Seuil de détection des anomalies statistiques |
| **Paramètre** | `param` | Couleurs et thèmes dynamiques |
| **KPI** | `table_KPI_Comparaison` | Indicateurs comparatifs commune vs national |
| **Tri** | `tri_tranche_age_5` | Table de tri personnalisée pour les tranches d'âge |
| **Mesures** | `mesures` | Table dédiée aux mesures DAX isolées |

---

## Pages du rapport

### 1-Overview : Vue globale

![01_DK-DE-DECES-PBI_Overview](https://raw.githubusercontent.com/debassanek/DK-DE-DECES-Fabric-Architecture/main/img/PowerBI/01_DK-DE-DECES-PBI_Overview.png)


**Objectif :** Donner une vision synthétique et nationale de la mortalité sur l'ensemble de la période couverte.

**KPIs en-tête :**
- Total décès · Âge moyen au décès · Ratio H/F · Nb centenaires · Nb de mineurs · Communes couvertes

**Visuels :**

| Visuel | Description |
|---|---|
| Évolution annuelle des décès | Courbe de tendance avec taux de variation YoY |
| Répartition H/F | Donut chart avec volumes absolus Hommes / Femmes |
| Pyramide des âges des décès | Pyramide bidirectionnelle par tranche d'âge (5 ans) |
| Répartition nb décès par génération | Treemap des générations (Silent Gen, Baby Boomers, Greatest Gen, Gen X…) |
| Anomalies détectées | Tableau des surmortalités par génération (détection Z-score) |
| Carte nationale des décès | Carte choroplèthe France par région |

**Filtres disponibles :** Année décès · Sexe · Région

---

### 2-Démographie : Analyse par âge et sexe

![02_DK-DE-DECES-PBI_Démographie](https://raw.githubusercontent.com/debassanek/DK-DE-DECES-Fabric-Architecture/main/img/PowerBI/02_DK-DE-DECES-PBI_D%C3%A9mographie.png)

**Objectif :** Explorer la structure démographique des décès : répartition par âge, sexe, et mortalité relative.

**KPIs en-tête :**
- Âge moyen au décès Femmes / Hommes · Âge médian global · Écart longévité F/H · Indice de survie · Nb décès mineurs

**Visuels :**

| Visuel | Description |
|---|---|
| Pyramide des âges des décès | Comparaison Femmes / Hommes par tranche de 5 ans |
| Courbe de mortalité par âge | Superposition Nb décès + Indice de survie sur l'axe secondaire |
| Ratio H/F par tranche d'âge | Barres empilées 100 % permettant de visualiser le basculement de surmortalité |
| Répartition par catégorie d'âge | Donut chart (Grand senior, Senior, Adulte, Adolescent, Enfant) |
| Tableau récapitulatif par tranche d'âge | Âge moyen F/H · Indice de survie · Écart longévité pour chaque décennie |

**Filtres disponibles :** Catégorie âge · Sexe · Année décès · Centenaire · Tranche 5 ans

---

### 3-Géographie : Analyse territoriale

![03_DK-DE-DECES-PBI_Géographie](https://raw.githubusercontent.com/debassanek/DK-DE-DECES-Fabric-Architecture/main/img/PowerBI/03_DK-DE-DECES-PBI_G%C3%A9ographie.png)

**Objectif :** Cartographier les disparités territoriales de mortalité à l'échelle commune, département et région.

**KPIs en-tête :**
- Taux mortalité moyen · Densité des décès · Indice vieillissement médian · Communes à anomalie · Nb décès mineurs

**Visuels :**

| Visuel | Description |
|---|---|
| Top 15 communes | Classement horizontal des communes les plus touchées en volume absolu |
| Scatter : densité vs mortalité | Nuage de points croisant densité de décès et taux de mortalité, avec taille = volume |
| Mortalité par région | Carte choroplèthe rouge : intensité proportionnelle au taux de mortalité |
| Répartition des décès par territoire | Treemap des régions avec volumes absolus |
| Vieillissement au décès par région | Carte choroplèthe rose/violet : indice de vieillissement médian par région |

**Filtres disponibles :** Région · Département · Commune · Année décès · Sexe · Est Métropole

---

### 4-Temporelle : Analyse dans le temps

![04_DK-DE-DECES-PBI_Temporelle](https://raw.githubusercontent.com/debassanek/DK-DE-DECES-Fabric-Architecture/main/img/PowerBI/04_DK-DE-DECES-PBI_Temporelle.png)

**Objectif :** Détecter les tendances, saisonnalités et anomalies statistiques (pics de surmortalité) sur la série temporelle.

**KPIs en-tête :**
- Variation vs N-1 · Indice de saisonnalité max · Moyenne mobile 12 mois · Z-score max détecté

**Visuels :**

| Visuel | Description |
|---|---|
| Série temporelle des décès | Histogramme mensuel + seuil Z-score superposé (détection anomalies) |
| Variation N-1 mensuelle | Barres de variation rouge/vert mois par mois vs année précédente |
| Évolution H/F dans le temps | Courbes distinctes Femmes / Hommes sur la période |
| Tableau saisonnalité | Matrice Année × Mois avec indices de saisonnalité colorés (heatmap) |
| Profil saisonnier moyen | Histogramme des indices moyens par mois, coloré par trimestre (T1–T4) |

**Filtres disponibles :** Année décès · Mois · Saison · Sexe · Anomalie · Année naissance (slider)

---

### 5-Générationnelle : Analyse par cohorte

![05_DK-DE-DECES-PBI_Générationnelle](https://raw.githubusercontent.com/debassanek/DK-DE-DECES-Fabric-Architecture/main/img/PowerBI/05_DK-DE-DECES-PBI_G%C3%A9n%C3%A9rationnelle.png)

**Objectif :** Comparer les profils de mortalité par cohorte de naissance (Greatest Generation, Silent Generation, Baby Boomers, Gen X).

**KPIs en-tête :**
- Génération dominante · Surmortalité max (Z-score) · Espérance de vie moyenne · Longévité max observée

**Visuels :**

| Visuel | Description |
|---|---|
| Distribution de mortalité par âge par génération | Courbes de mortalité normalisées par âge pour chaque cohorte |
| Durée de vie moyenne H/F (écart) | Courbes comparatives Hommes / Femmes par génération |
| Écart longévité H/F | Barres horizontales montrant l'écart en années par cohorte |
| Décès par génération par sexe | Barres groupées Femmes/Hommes avec volumes absolus |
| Répartition des décès par génération | Treemap des générations avec volumes |
| Surmortalité par génération | Barres du taux de surmortalité moyen par cohorte |

**Filtres disponibles :** Génération · Année naissance (slider) · Sexe · Année décès · Région · Saison

---

### 6-Centenaires

![06_DK-DE-DECES-PBI_Centenaire](https://raw.githubusercontent.com/debassanek/DK-DE-DECES-Fabric-Architecture/main/img/PowerBI/06_DK-DE-DECES-PBI_Centenaire.png)

**Objectif :** Focus sur la population des 100 ans et plus : répartition, tendances et géographie des centenaires.

**KPIs en-tête :**
- Total décès centenaires · Part des Femmes · Âge moyen au décès · Nb décès super-centenaires (+110 ans)

**Visuels :**

| Visuel | Description |
|---|---|
| Évolution annuelle des décès | Courbe YoY des décès centenaires (variation) |
| Répartition des décès par génération | Treemap distinguant les tranches 100-104, 105-109, 110+ |
| Top 10 départements | Classement des départements concentrant le plus de décès centenaires |
| Répartition par sexe | Donut chart (83,8 % Femmes vs 16,2 % Hommes) |

**Filtres disponibles :** Sexe · Année décès

---

### 7-Profil commune

![07_DK-DE-DECES-PBI_Profil commune](https://raw.githubusercontent.com/debassanek/DK-DE-DECES-Fabric-Architecture/main/img/PowerBI/07_DK-DE-DECES-PBI_Profil%20commune.png)

**Objectif :** Tableau de bord de drill-down sur une commune spécifique, avec comparaison systématique au niveau national.

**Bandeau commune :**  
Nom commune · Code INSEE · Total décès enregistrés · Rang national (sur 34 970 communes)

**Visuels :**

| Visuel | Description |
|---|---|
| Évolution annuelle des décès | Courbe temporelle lissée des décès de la commune |
| Répartition par sexe | Donut chart Femmes / Hommes pour la commune sélectionnée |
| Tableau comparatif commune vs national | Indicateurs clés avec écart coloré (vert/orange/rouge) : Décès/an · Centenaires · % Femmes · % Mineurs · Âge moyen · % 85 ans+ |
| Distribution de mortalité par âge par génération | Courbes superposées par génération pour la commune |

**Filtres disponibles :** Sexe · Région · Département · Commune (sélection individuelle)

---

## Stack DAX & modélisation

Le rapport mobilise **50+ mesures DAX** organisées en catégories fonctionnelles :

| Catégorie | Exemples de mesures |
|---|---|
| Volumes & ratios | Total décès, Ratio H/F, % Femmes, % Mineurs, % Centenaires |
| Statistiques d'âge | Âge moyen décès, Âge médian, Écart longévité F/H |
| Indices | Indice de survie, Indice vieillissement, Indice saisonnalité |
| Anomalies | Z-score, Seuil anomalie, % Communes anomalie |
| Comparaison | Écart commune vs national, Rang national |
| Temporel | Variation N-1, Moyenne mobile 12 mois, Taux variation YoY |
| Génération | Surmortalité par génération, Longévité max, Espérance de vie moy. |


---

## Retour au projet principal

- **[Projet principal](../README.md)** : Présentation générale du projet
- **[Architecture détaillée](../docs/architecture.md)** : Organisation et transformation des données
- **[Dictionnaire des données](../docs/data_dictionary.md)** : Référentiel des tables, champs et règles de gestion

---

**Auteur :** Debassane K. · Data & BI · <debassanek@gmail.com>
