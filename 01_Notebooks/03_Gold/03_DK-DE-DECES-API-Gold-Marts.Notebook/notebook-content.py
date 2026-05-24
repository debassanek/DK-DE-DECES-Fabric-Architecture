# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ef922306-2cca-4783-b2d3-96e5b020eeb5",
# META       "default_lakehouse_name": "DK_DE_DECES_API_Gold_Build",
# META       "default_lakehouse_workspace_id": "35193659-8177-497e-ae34-111479e85809",
# META       "known_lakehouses": [
# META         {
# META           "id": "ef922306-2cca-4783-b2d3-96e5b020eeb5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
#  Notebook : 03_DK-DE-DECES-API-Gold-Marts
#  Couche   : Gold
#  Domaine  : Sante - Deces
#  Objectif : Construction des data marts analytiques pour Power BI
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook responsable de la construction des data marts Gold, tables
# denormalisees et enrichies optimisees pour la consommation Power BI
# et les usages analytiques directs.
#
# Deux marts produits :
#
#   mart_deces_mensuel
#     Base : agg_mortalite_mensuelle
#     Enrichissement : labels temporels calcules (trimestre, semestre,
#     saison, libelle_mois) pour les slicers et la time intelligence BI.
#
#   mart_deces_geographique
#     Base : agg_mortalite_commune (metriques de mortalite par commune)
#     Enrichissement : jointure avec dim_lieu pour les labels geographiques
#     complets (commune, departement, region, type_commune, coordonnees,
#     superficie) necessaires aux cartes et aux hierarchies BI.
#     Ces labels sont absents de agg_mortalite_commune qui ne conserve
#     que id_commune comme cle geographique.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter mart_deces_age : agg_mortalite_age enrichi des labels
#     de tranche d'age et de generation (via dim_age / dim_generation)
# [ ] Ajouter mart_deces_generation : cohortes avec labels generationnels
# [ ] Activer le Change Data Feed sur les deux marts apres premiere
#     execution (operation one-shot) :
#
#     spark.sql("ALTER TABLE mart_deces_mensuel
#                SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
#     spark.sql("ALTER TABLE mart_deces_geographique
#                SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
#
# [ ] Ajouter partitionnement de mart_deces_mensuel par annee
# [ ] Etudier la fusion fact_deces + mart_deces_geographique en vue
#     unique pour simplifier le modele semantique Power BI
# ----------------------------------------------------------------------------

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 1 — IMPORTS & CONFIGURATION
# ============================================================================

# ─────────────────────────────────────────────────────────
# Imports Python
# ─────────────────────────────────────────────────────────
import logging
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

import notebookutils

def _detect_environment() -> str:
    try:
        workspace_name = notebookutils.runtime.context.get("currentWorkspaceName", "")
        if "Dev" in workspace_name:    return "dev"
        elif "Test" in workspace_name: return "test"
        else:                          return "prod"
    except Exception:
        return "dev"

ENVIRONMENT = _detect_environment()

# ─────────────────────────────────────────────────────────
# Configuration du logging
# ─────────────────────────────────────────────────────────
_LOG_LEVEL_PAR_ENV = {"dev": logging.INFO, "test": logging.INFO, "prod": logging.WARNING}
logging.basicConfig(level=_LOG_LEVEL_PAR_ENV[ENVIRONMENT],
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("gold_marts")

# ─────────────────────────────────────────────────────────
# Configuration Spark
# ─────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    # Sources
    "agg_mensuel_table": "agg_mortalite_mensuelle",  # produit par Gold-AggTemporel
    "agg_commune_table": "agg_mortalite_commune",    # produit par Gold-AggGeographie
    "dim_lieu_table":    "dim_lieu",                 # produit par Dim-Lieu

    # Sorties
    "mart_mensuel_table": "mart_deces_mensuel",
    "mart_mensuel_path":  "Tables/mart_deces_mensuel",
    "mart_geo_table":     "mart_deces_geographique",
    "mart_geo_path":      "Tables/mart_deces_geographique",

    "write_mode": "overwrite",
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Gold-Marts — demarrage | env : %s", ENVIRONMENT)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — LECTURE DES SOURCES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 2a — agg_mortalite_mensuelle
# ─────────────────────────────────────────────────────────
try:
    df_agg_mensuel = spark.read.table(CONFIG["agg_mensuel_table"])
    df_agg_mensuel.cache()
    nb_mensuel = df_agg_mensuel.count()
    log.info("agg_mortalite_mensuelle : %d lignes", nb_mensuel)
except Exception as e:
    log.error("Echec lecture '%s' : %s", CONFIG["agg_mensuel_table"], e)
    raise

# ─────────────────────────────────────────────────────────
# Sous-partie 2b — agg_mortalite_commune
# ─────────────────────────────────────────────────────────
try:
    df_agg_commune = spark.read.table(CONFIG["agg_commune_table"])
    df_agg_commune.cache()
    nb_commune = df_agg_commune.count()
    log.info("agg_mortalite_commune : %d communes", nb_commune)
except Exception as e:
    log.error("Echec lecture '%s' : %s", CONFIG["agg_commune_table"], e)
    raise

# ─────────────────────────────────────────────────────────
# Sous-partie 2c — dim_lieu
# ─────────────────────────────────────────────────────────
try:
    df_dim_lieu = (
        spark.read.table(CONFIG["dim_lieu_table"])
        .select(
            "id_commune",
            "commune",
            "département",
            "région",
            "code_region",
            "latitude",
            "longitude",
            "superficie_commune",
            "densite_commune",
            "type_commune",
            "epci",
        )
        .withColumn("id_commune",
            F.lpad(F.col("id_commune").cast(StringType()), 5, "0"))
    )
    df_dim_lieu.cache()
    nb_dim_lieu = df_dim_lieu.count()
    log.info("dim_lieu : %d communes", nb_dim_lieu)
except Exception as e:
    log.error("Echec lecture '%s' : %s", CONFIG["dim_lieu_table"], e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — CONSTRUCTION DE MART_DECES_MENSUEL
# ============================================================================
#
# Enrichissement de agg_mortalite_mensuelle avec les labels temporels
# calcules inline : aucune dependance a dim_date.
# Ces labels sont indispensables pour les slicers et la time intelligence
# Power BI (filtres par trimestre, saison, mois nominal, etc.).

# ─────────────────────────────────────────────────────────
# Sous-partie 3a — Labels temporels derives
# ─────────────────────────────────────────────────────────
trimestre = (
    F.when(F.col("mois").between(1, 3), F.lit(1))
     .when(F.col("mois").between(4, 6), F.lit(2))
     .when(F.col("mois").between(7, 9), F.lit(3))
     .otherwise(F.lit(4))
).cast(IntegerType())

semestre = F.when(F.col("mois") <= 6, F.lit(1)).otherwise(F.lit(2)).cast(IntegerType())

# Saison hemisphere nord (convention meteorologique)
saison = (
    F.when(F.col("mois").isin(12, 1, 2),  F.lit("Hiver"))
     .when(F.col("mois").isin(3, 4, 5),   F.lit("Printemps"))
     .when(F.col("mois").isin(6, 7, 8),   F.lit("Ete"))
     .otherwise(F.lit("Automne"))
)

libelle_mois = (
    F.when(F.col("mois") == 1,  F.lit("Janvier"))
     .when(F.col("mois") == 2,  F.lit("Fevrier"))
     .when(F.col("mois") == 3,  F.lit("Mars"))
     .when(F.col("mois") == 4,  F.lit("Avril"))
     .when(F.col("mois") == 5,  F.lit("Mai"))
     .when(F.col("mois") == 6,  F.lit("Juin"))
     .when(F.col("mois") == 7,  F.lit("Juillet"))
     .when(F.col("mois") == 8,  F.lit("Aout"))
     .when(F.col("mois") == 9,  F.lit("Septembre"))
     .when(F.col("mois") == 10, F.lit("Octobre"))
     .when(F.col("mois") == 11, F.lit("Novembre"))
     .otherwise(F.lit("Decembre"))
)

# Libelle trimestre pour les slicers (ex : "2023 T1")
libelle_trimestre = F.concat(
    F.col("annee").cast(StringType()), F.lit(" T"), trimestre.cast(StringType())
)

# ─────────────────────────────────────────────────────────
# Sous-partie 3b — Construction du DataFrame mart
# ─────────────────────────────────────────────────────────
COLONNES_MART_MENSUEL = [
    # Cles temporelles
    "annee", "mois", "AnMois",
    # Labels temporels enrichis (pour slicers Power BI)
    "trimestre", "libelle_trimestre", "semestre", "saison", "libelle_mois",
    # Metriques de volume
    "nb_deces",
    # Statistiques d'age
    "age_moyen", "age_median", "age_min", "age_max",
    # Repartition sexe
    "nb_hommes", "nb_femmes", "pct_hommes", "pct_femmes",
    # Sous-populations
    "nb_centenaires", "nb_mineurs",
    # Indicateurs temporels
    "variation_vs_n_1", "variation_pct",
    "moyenne_mobile_12m",
    "indice_saisonnalite",
    # Anomalie
    "z_score", "anomalie_flag",
]

df_mart_mensuel = (
    df_agg_mensuel
    .withColumn("trimestre",         trimestre)
    .withColumn("libelle_trimestre", libelle_trimestre)
    .withColumn("semestre",          semestre)
    .withColumn("saison",            saison)
    .withColumn("libelle_mois",      libelle_mois)
    .select(COLONNES_MART_MENSUEL)
    .orderBy("annee", "mois")
)

# ─────────────────────────────────────────────────────────
# Sous-partie 3c — Ecriture Delta
# ─────────────────────────────────────────────────────────
try:
    (
        df_mart_mensuel.write.format("delta")
        .mode(CONFIG["write_mode"])
        .option("overwriteSchema", "true")
        .save(CONFIG["mart_mensuel_path"])
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["mart_mensuel_table"]}
        USING DELTA LOCATION '{CONFIG["mart_mensuel_path"]}'
    """)
    nb_mart_mensuel = spark.read.table(CONFIG["mart_mensuel_table"]).count()
    log.info("mart_deces_mensuel ecrit : %d lignes | %d colonnes",
             nb_mart_mensuel, len(COLONNES_MART_MENSUEL))
except Exception as e:
    log.error("Echec ecriture mart_deces_mensuel : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — CONSTRUCTION DE MART_DECES_GEOGRAPHIQUE
# ============================================================================
#
# Enrichissement de agg_mortalite_commune avec les labels geographiques
# de dim_lieu. agg_mortalite_commune ne conserve que id_commune comme
# cle geographique — le mart restitue la hierarchie complete (commune,
# departement, region) et les attributs necessaires aux cartes Power BI
# (latitude, longitude, type_commune, superficie, densite).
#
# Jointure LEFT : toutes les communes avec metriques sont conservees,
# meme celles absentes de dim_lieu (codes etrangers, DOM-TOM non couverts).

# ─────────────────────────────────────────────────────────
# Sous-partie 4a — Jointure agg_commune x dim_lieu
# ─────────────────────────────────────────────────────────
df_mart_geo = (
    df_agg_commune
    .join(df_dim_lieu, on="id_commune", how="left")
)

# Diagnostic de couverture
nb_avec_label = df_mart_geo.filter(F.col("commune").isNotNull()).count()
nb_sans_label = df_mart_geo.count() - nb_avec_label
log.info("Jointure dim_lieu : %d communes matchees | %d non matchees (etrangers/DOM)",
         nb_avec_label, nb_sans_label)

# ─────────────────────────────────────────────────────────
# Sous-partie 4b — Selection finale
# ─────────────────────────────────────────────────────────
COLONNES_MART_GEO = [
    # Cle geographique
    "id_commune",
    # Hierarchie geographique (labels pour slicers Power BI)
    "commune",
    "département",
    "région",
    "code_region",
    "type_commune",
    "epci",
    # Geolocalisation (pour cartes Power BI)
    "latitude",
    "longitude",
    # Caracteristiques territoriales
    "population_commune",
    "superficie_commune",
    "densite_commune",
    # Metriques de mortalite
    "nb_deces",
    "taux_mortalite",
    "age_moyen_deces",
    "age_median_deces",
    "age_min",
    "age_max",
    # Repartition sexe
    "nb_hommes",
    "nb_femmes",
    "pct_hommes",
    "pct_femmes",
    # Sous-populations
    "nb_centenaires",
    "nb_mineurs",
    # Indicateurs analytiques
    "indice_vieillissement",
    "densite_deces",
    "anomalie_mortalite",
]

df_mart_geo_final = (
    df_mart_geo
    .select(COLONNES_MART_GEO)
    .orderBy(F.col("nb_deces").desc())
)

# ─────────────────────────────────────────────────────────
# Sous-partie 4c — Ecriture Delta
# ─────────────────────────────────────────────────────────
try:
    (
        df_mart_geo_final.write.format("delta")
        .mode(CONFIG["write_mode"])
        .option("overwriteSchema", "true")
        .save(CONFIG["mart_geo_path"])
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["mart_geo_table"]}
        USING DELTA LOCATION '{CONFIG["mart_geo_path"]}'
    """)
    nb_mart_geo = spark.read.table(CONFIG["mart_geo_table"]).count()
    log.info("mart_deces_geographique ecrit : %d lignes | %d colonnes",
             nb_mart_geo, len(COLONNES_MART_GEO))
except Exception as e:
    log.error("Echec ecriture mart_deces_geographique : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — RAPPORT QUALITE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 5a — mart_deces_mensuel
# ─────────────────────────────────────────────────────────
print(f"\n{'=' * 64}")
print(f"  MART : mart_deces_mensuel")
print(f"  Lignes : {nb_mart_mensuel:,}  |  Colonnes : {len(COLONNES_MART_MENSUEL)}")
print(f"{'=' * 64}")

df_check_m = spark.read.table(CONFIG["mart_mensuel_table"])
df_check_m.printSchema()

print("--- Apercu (3 lignes) ---")
df_check_m.select(
    "annee", "mois", "libelle_mois", "trimestre", "libelle_trimestre",
    "saison", "nb_deces", "variation_pct", "indice_saisonnalite", "anomalie_flag"
).show(3, truncate=False)

print("--- Distribution par saison (cumul) ---")
df_check_m.groupBy("saison")     .agg(F.sum("nb_deces").alias("total_deces"),
         F.round(F.avg("nb_deces"), 0).alias("moy_mensuel"))     .orderBy("saison").show(truncate=False)

print("--- Distribution par trimestre (cumul) ---")
df_check_m.groupBy("trimestre", "libelle_trimestre")     .agg(F.sum("nb_deces").alias("total_deces"))     .orderBy("trimestre").show(truncate=False)

# ─────────────────────────────────────────────────────────
# Sous-partie 5b — mart_deces_geographique
# ─────────────────────────────────────────────────────────
print(f"\n{'=' * 64}")
print(f"  MART : mart_deces_geographique")
print(f"  Lignes : {nb_mart_geo:,}  |  Colonnes : {len(COLONNES_MART_GEO)}")
print(f"{'=' * 64}")

df_check_g = spark.read.table(CONFIG["mart_geo_table"])
df_check_g.printSchema()

print("--- Apercu top 5 communes par nb_deces ---")
df_check_g.select(
    "id_commune", "commune", "département", "région",
    "nb_deces", "taux_mortalite", "indice_vieillissement", "anomalie_mortalite"
).show(5, truncate=False)

print("--- Repartition par type_commune ---")
df_check_g.groupBy("type_commune")     .agg(F.count("*").alias("nb_communes"),
         F.sum("nb_deces").alias("total_deces"),
         F.round(F.avg("taux_mortalite"), 3).alias("taux_moyen"))     .orderBy(F.col("total_deces").desc()).show(truncate=False)

print("--- % nulls colonnes cles ---")
cols_check = ["commune", "département", "région", "latitude", "longitude",
              "taux_mortalite", "indice_vieillissement"]
exprs = [F.round(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))
                 / F.count("*") * 100, 2).alias(c) for c in cols_check]
df_check_g.select(exprs).show(truncate=False)

# ─────────────────────────────────────────────────────────
# Sous-partie 5c — Coherence volumetrique inter-marts
# ─────────────────────────────────────────────────────────
total_m = df_check_m.agg(F.sum("nb_deces")).collect()[0][0]
total_g = df_check_g.agg(F.sum("nb_deces")).collect()[0][0]
print(f"\nSomme nb_deces mart_mensuel     : {total_m:,}")
print(f"Somme nb_deces mart_geographique : {total_g:,}")
print("(Ecart normal : lignes Silver sans code_lieu_deces ne figurent pas dans le mart geo)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE FINALE — CLEANUP & REPORTING
# ============================================================================

# ─────────────────────────────────────────────────────────
# Liberation du cache Spark
# ─────────────────────────────────────────────────────────
df_agg_mensuel.unpersist()
df_agg_commune.unpersist()
df_dim_lieu.unpersist()
spark.catalog.clearCache()
log.info("Cache Spark libere.")

# ─────────────────────────────────────────────────────────
# Calcul de la duree
# ─────────────────────────────────────────────────────────
duree_sec = (datetime.now(timezone.utc) - _debut_pipeline).seconds

# ─────────────────────────────────────────────────────────
# Rapport
# ─────────────────────────────────────────────────────────
print(f"\n{'=' * 60}")
print(f"  RAPPORT GOLD-MARTS — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  mart_deces_mensuel")
print(f"    Lignes : {nb_mart_mensuel:>8,}  |  Colonnes : {len(COLONNES_MART_MENSUEL)}")
print(f"  mart_deces_geographique")
print(f"    Lignes : {nb_mart_geo:>8,}  |  Colonnes : {len(COLONNES_MART_GEO)}")
print(f"    Communes sans label dim_lieu : {nb_sans_label:,}")
print(f"{'─' * 60}")
print(f"  Duree totale : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Gold-Marts termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
