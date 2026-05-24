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
#  Notebook : 03_DK-DE-DECES-API-Gold-AggTemporel
#  Couche   : Gold
#  Domaine  : Sante - Deces
#  Objectif : Agregation de la mortalite mensuelle avec indicateurs temporels
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook charge de produire la table agg_mortalite_mensuelle, agregee
# a la granularite annee x mois. Calcule les volumes de deces, les
# statistiques d'age, la repartition sexe, les indicateurs temporels
# (variation YoY, moyenne mobile 12 mois, indice de saisonnalite) et
# detecte les mois statistiquement anormaux via z-score global.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter decomposition tendance / saisonnalite (STL ou regression)
# [ ] Ajouter prevision N+1 / N+3 par lissage exponentiel
# [ ] Ajouter comparaison avec esperance de vie nationale (INSEE)
# [ ] Ajouter partitionnement Delta par annee
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

import logging
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.window import Window

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

_LOG_LEVEL_PAR_ENV = {"dev": logging.INFO, "test": logging.INFO, "prod": logging.WARNING}
logging.basicConfig(level=_LOG_LEVEL_PAR_ENV[ENVIRONMENT],
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("agg_mortalite_mensuelle")

spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

CONFIG = {
    "silver_path" :      "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/0359305b-a30c-48e1-be9d-e06c3f6eca4f/Tables/",
    "silver_table":      "silver_deces",
    "output_table_path": "Tables/agg_mortalite_mensuelle",
    "output_table_name": "agg_mortalite_mensuelle",
    "write_mode":        "overwrite",
    "zscore_seuil":      2.0,
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Gold-AggTemporel — demarrage | env : %s | seuil z-score : %.1f",
         ENVIRONMENT, CONFIG["zscore_seuil"])
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — LECTURE SILVER
# ============================================================================

full_path = f"{CONFIG['silver_path']}{CONFIG['silver_table']}"

try:
    df_silver = (
        spark.read.format('delta').load(full_path)
        .select("annee_deces", "mois_deces", "age_au_deces", "sexe")
        .filter(F.col("annee_deces").isNotNull() & F.col("mois_deces").isNotNull())
    )
    df_silver.cache()
    nb_silver = df_silver.count()
    log.info("Lignes Silver charges : %d", nb_silver)
except Exception as e:
    log.error("Echec lecture Silver '%s' : %s", CONFIG["silver_table"], e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — AGREGATS DE BASE PAR ANNEE x MOIS
# ============================================================================

df = (
    df_silver
    .withColumn("est_homme",      F.when(F.col("sexe") == "M", 1).otherwise(0))
    .withColumn("est_femme",      F.when(F.col("sexe") == "F", 1).otherwise(0))
    .withColumn("est_centenaire", F.when(F.col("age_au_deces") >= 100, 1).otherwise(0))
    .withColumn("est_mineur",     F.when(F.col("age_au_deces") < 18, 1).otherwise(0))
)

df_agg = (
    df
    .groupBy("annee_deces", "mois_deces")
    .agg(
        F.count("*")                                                   .alias("nb_deces"),
        F.round(F.avg("age_au_deces"), 1)                             .alias("age_moyen"),
        F.round(F.percentile_approx("age_au_deces", 0.5, 1000), 1)   .alias("age_median"),
        F.min("age_au_deces")                                          .alias("age_min"),
        F.max("age_au_deces")                                          .alias("age_max"),
        F.sum("est_homme")                                             .alias("nb_hommes"),
        F.sum("est_femme")                                             .alias("nb_femmes"),
        F.sum("est_centenaire")                                        .alias("nb_centenaires"),
        F.sum("est_mineur")                                            .alias("nb_mineurs"),
    )
    .withColumn("AnMois",
        F.concat(F.col("annee_deces").cast(StringType()),
                 F.lpad(F.col("mois_deces").cast(StringType()), 2, "0")).cast(IntegerType()))
    .withColumn("_nb_sexe", F.col("nb_hommes") + F.col("nb_femmes"))
    .withColumn("pct_hommes",
        F.when(F.col("_nb_sexe") > 0,
            F.round(F.col("nb_hommes").cast(DoubleType()) / F.col("_nb_sexe") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .withColumn("pct_femmes",
        F.when(F.col("_nb_sexe") > 0,
            F.round(F.col("nb_femmes").cast(DoubleType()) / F.col("_nb_sexe") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .drop("_nb_sexe")
    .withColumnRenamed("annee_deces", "annee")
    .withColumnRenamed("mois_deces",  "mois")
    .orderBy("annee", "mois")
)

log.info("Agregat de base : %d lignes", df_agg.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — METRIQUES TEMPORELLES (WINDOW FUNCTIONS)
# ============================================================================

# ─────────────────────────────────────────────────────────
# Definition des fenetres
# ─────────────────────────────────────────────────────────
df_agg = df_agg.withColumn("_ordre_chrono",
    (F.col("annee") * 100 + F.col("mois")).cast(IntegerType()))

w_mois   = Window.partitionBy("mois").orderBy("annee")
w_12m    = Window.orderBy("_ordre_chrono").rowsBetween(-11, 0)
w_annee  = Window.partitionBy("annee")
w_global = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# ─────────────────────────────────────────────────────────
# Sous-partie 4a — Variation YoY (meme mois, annee N-1)
# ─────────────────────────────────────────────────────────
df_agg = (
    df_agg
    .withColumn("variation_vs_n_1",
        (F.col("nb_deces") - F.lag("nb_deces", 1).over(w_mois)).cast(IntegerType()))
    .withColumn("variation_pct",
        F.when(F.lag("nb_deces", 1).over(w_mois).isNotNull()
               & (F.lag("nb_deces", 1).over(w_mois) != 0),
            F.round(F.col("variation_vs_n_1").cast(DoubleType())
                    / F.lag("nb_deces", 1).over(w_mois) * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType())))
)

# ─────────────────────────────────────────────────────────
# Sous-partie 4b — Moyenne mobile 12 mois
# ─────────────────────────────────────────────────────────
df_agg = df_agg.withColumn("moyenne_mobile_12m",
    F.round(F.avg("nb_deces").over(w_12m), 1))

# ─────────────────────────────────────────────────────────
# Sous-partie 4c — Indice de saisonnalite
# ─────────────────────────────────────────────────────────
df_agg = (
    df_agg
    .withColumn("_moy_mens_annee",
        F.round(F.sum("nb_deces").over(w_annee).cast(DoubleType()) / 12, 1))
    .withColumn("indice_saisonnalite",
        F.when(F.col("_moy_mens_annee") > 0,
            F.round(F.col("nb_deces").cast(DoubleType()) / F.col("_moy_mens_annee"), 3)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .drop("_moy_mens_annee")
)

# ─────────────────────────────────────────────────────────
# Sous-partie 4d — Z-score global et anomalie_flag
# ─────────────────────────────────────────────────────────
df_agg = (
    df_agg
    .withColumn("_mean_g", F.avg("nb_deces").over(w_global))
    .withColumn("_std_g",  F.stddev("nb_deces").over(w_global))
    .withColumn("z_score",
        F.when(F.col("_std_g").isNotNull() & (F.col("_std_g") != 0),
            F.round((F.col("nb_deces").cast(DoubleType()) - F.col("_mean_g"))
                    / F.col("_std_g"), 3)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .withColumn("anomalie_flag",
        F.when(F.col("z_score").isNotNull(),
            F.abs(F.col("z_score")) > F.lit(CONFIG["zscore_seuil"])
        ).otherwise(F.lit(False)))
    .drop("_mean_g", "_std_g", "_ordre_chrono")
)

log.info("Metriques temporelles calculees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — SELECTION FINALE ET ORDRE DES COLONNES
# ============================================================================

COLONNES_FINALES = [
    "annee", "mois", "AnMois",
    "nb_deces",
    "age_moyen", "age_median", "age_min", "age_max",
    "nb_hommes", "nb_femmes", "pct_hommes", "pct_femmes",
    "nb_centenaires", "nb_mineurs",
    "variation_vs_n_1", "variation_pct",
    "moyenne_mobile_12m",
    "indice_saisonnalite",
    "z_score", "anomalie_flag",
]

df_final = df_agg.select(COLONNES_FINALES).orderBy("annee", "mois")
log.info("Selection finale : %d colonnes", len(COLONNES_FINALES))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — ECRITURE DELTA
# ============================================================================

try:
    (
        df_final.write.format("delta")
        .mode(CONFIG["write_mode"])
        .option("overwriteSchema", "true")
        .save(CONFIG["output_table_path"])
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["output_table_name"]}
        USING DELTA LOCATION '{CONFIG["output_table_path"]}'
    """)
    log.info("Table '%s' ecrite et enregistree.", CONFIG["output_table_name"])
except Exception as e:
    log.error("Echec ecriture : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 7 — RAPPORT QUALITE
# ============================================================================

df_check  = spark.read.table(CONFIG["output_table_name"])
nb_final  = df_check.count()

print(f"\n{'=' * 64}")
print(f"  TABLE : {CONFIG['output_table_name']}")
print(f"  Lignes : {nb_final:,}  |  Colonnes : {len(df_check.columns)}")
print(f"{'=' * 64}\n")

df_check.printSchema()

print("--- Apercu (6 lignes) ---")
df_check.show(6, truncate=False)

nb_anomalies = df_check.filter(F.col("anomalie_flag") == True).count()
print(f"--- Mois avec anomalie (|z| > {CONFIG['zscore_seuil']}) : {nb_anomalies} ---")
(df_check.filter(F.col("anomalie_flag") == True)
    .select("annee", "mois", "nb_deces", "z_score", "variation_pct", "indice_saisonnalite")
    .orderBy(F.col("z_score").desc()).show(20, truncate=False))

total_agg = df_check.agg(F.sum("nb_deces")).collect()[0][0]
ecart = nb_silver - total_agg
print(f"\nSomme nb_deces : {total_agg:,}  |  Silver : {nb_silver:,}  |  Ecart : {ecart:,}")
if ecart != 0:
    print("Info : ecart = lignes Silver sans annee_deces ou mois_deces valide")
else:
    print("Totaux coherents Silver <-> Agregat.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE FINALE — CLEANUP & REPORTING
# ============================================================================

df_silver.unpersist()
spark.catalog.clearCache()
log.info("Cache Spark libere.")

duree_sec = (datetime.now(timezone.utc) - _debut_pipeline).seconds

print(f"\n{'=' * 60}")
print(f"  RAPPORT GOLD-AGGTEMPOREL — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Lignes Silver en entree : {nb_silver:>10,}")
print(f"  Lignes ecrites          : {nb_final:>10,}")
print(f"  Anomalies detectees     : {nb_anomalies:>10,}")
print(f"{'─' * 60}")
print(f"  Table ecrite : {CONFIG['output_table_name']}")
print(f"  Duree totale : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Gold-AggTemporel termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
