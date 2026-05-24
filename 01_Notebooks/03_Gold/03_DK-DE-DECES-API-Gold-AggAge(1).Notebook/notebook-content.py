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
#  Notebook : 03_DK-DE-DECES-API-Gold-AggAge
#  Couche   : Gold
#  Domaine  : Sante - Deces
#  Objectif : Agregation de la mortalite par age exact et periode mensuelle
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook charge de produire la table agg_mortalite_age, agregee a la
# granularite age_exact x id_date (AnMois yyyyMM). Calcule les volumes
# de deces, la repartition par sexe, les metriques Window (pct_total,
# age_moyen, age_median, indice_survie) et detecte les anomalies de
# distribution intra-mois via z-score.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter partitionnement Delta par annee pour optimiser les requetes BI
# [ ] Ajouter tranche_age (0-4, 5-9 ... 95-99, 100+) comme cle analytique
# [ ] Affiner age_median avec percentile_approx au lieu de l'approche cumul
# [ ] Ajouter comparaison YoY (meme age, meme mois, annee N-1)
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

# ─────────────────────────────────────────────────────────
# Configuration du logging
# ─────────────────────────────────────────────────────────
_LOG_LEVEL_PAR_ENV = {"dev": logging.INFO, "test": logging.INFO, "prod": logging.WARNING}
logging.basicConfig(level=_LOG_LEVEL_PAR_ENV[ENVIRONMENT],
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("agg_mortalite_age")

# ─────────────────────────────────────────────────────────
# Configuration Spark
# ─────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    "silver_path" :       "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/0359305b-a30c-48e1-be9d-e06c3f6eca4f/Tables/",
    "silver_table":       "silver_deces",          
    "output_table_path":  "Tables/agg_mortalite_age",
    "output_table_name":  "agg_mortalite_age",
    "write_mode":         "overwrite",
    "seuil_centenaire":   100,
    "seuil_mineur":       18,
    "zscore_seuil_anomalie": 2.5,
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Gold-AggAge — demarrage | env : %s", ENVIRONMENT)
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

# ─────────────────────────────────────────────────────────
# Lecture via le metastore — colonnes utiles uniquement
# Granularite cible : age_exact x id_date (AnMois yyyyMM)
# ─────────────────────────────────────────────────────────
# ============================================================================
#  ETAPE 2 — LECTURE SILVER
# ============================================================================

full_path = f"{CONFIG['silver_path']}{CONFIG['silver_table']}"

try:
    df_silver = (
        spark.read.format("delta").load(full_path)
            .select("age_au_deces", "annee_deces", "mois_deces", "sexe")
            .filter(
                F.col("age_au_deces").isNotNull()
                & F.col("annee_deces").isNotNull()
                & F.col("mois_deces").isNotNull()
            )
    )

    df_silver.cache()
    nb_silver = df_silver.count()
    log.info("Lignes Silver (age + date non null) : %d", nb_silver)

except Exception as e:
    log.error("Echec lecture Silver '%s' : %s", CONFIG["silver_final_table"], e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — CONSTRUCTION DE ID_DATE ET COLONNES INDICATRICES
# ============================================================================

# ─────────────────────────────────────────────────────────
# id_date : cle analytique AnMois yyyyMM (jointure avec dim_date)
# Colonnes indicatrices pour agregats conditionnels
# ─────────────────────────────────────────────────────────
df = (
    df_silver
    .withColumn(
        "id_date",
        F.concat(
            F.col("annee_deces").cast(StringType()),
            F.lpad(F.col("mois_deces").cast(StringType()), 2, "0")
        ).cast(IntegerType())
    )
    .withColumn("est_homme",      F.when(F.col("sexe") == "M", 1).otherwise(0))
    .withColumn("est_femme",      F.when(F.col("sexe") == "F", 1).otherwise(0))
    .withColumn("est_centenaire", F.when(F.col("age_au_deces") >= CONFIG["seuil_centenaire"], 1).otherwise(0))
    .withColumn("est_mineur",     F.when(F.col("age_au_deces") < CONFIG["seuil_mineur"], 1).otherwise(0))
)
log.info("id_date et colonnes indicatrices construits")

# ─────────────────────────────────────────────────────────
# Contruction key_age
# ─────────────────────────────────────────────────────────

df_keyage= df.withColumn(
    "key_age",
    F.when(
    F.col("age_au_deces").isNotNull()
    & F.col("annee_deces").isNotNull()
    & F.col("mois_deces").isNotNull(),
    F.concat_ws("|",
        F.col("age_au_deces").cast(StringType()),
        F.col("id_date"))
).otherwise(F.lit(None).cast(StringType()))
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — AGREGAT PRINCIPAL PAR AGE x ID_DATE
# ============================================================================

df_agg = (df_keyage
    .groupBy("age_au_deces", "id_date", "key_age")
    .agg(
        F.count("*")           .alias("nb_deces"),
        F.sum("est_homme")     .alias("nb_hommes"),
        F.sum("est_femme")     .alias("nb_femmes"),
        F.sum("est_centenaire").alias("nb_centenaires"),
        F.sum("est_mineur")    .alias("nb_mineurs"),
    )
    .withColumnRenamed("age_au_deces", "age")
    .orderBy("id_date", "age")
)

nb_agg = df_agg.count()
log.info("Combinaisons age x id_date : %d", nb_agg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — METRIQUES DE RATIO ET POURCENTAGES
# ============================================================================

df_agg = (
    df_agg
    .withColumn(
        "ratio_h_f",
        F.when(F.col("nb_femmes") > 0,
            F.round(F.col("nb_hommes").cast(DoubleType()) / F.col("nb_femmes"), 3)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    .withColumn("_nb_sexe", F.col("nb_hommes") + F.col("nb_femmes"))
    .withColumn(
        "pct_hommes",
        F.when(F.col("_nb_sexe") > 0,
            F.round(F.col("nb_hommes").cast(DoubleType()) / F.col("_nb_sexe") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    .withColumn(
        "pct_femmes",
        F.when(F.col("_nb_sexe") > 0,
            F.round(F.col("nb_femmes").cast(DoubleType()) / F.col("_nb_sexe") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    .drop("_nb_sexe")
)
log.info("Ratios et pourcentages calcules")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — METRIQUES WINDOW
# ============================================================================
#
# Fenetres utilisees :
#   w_global  : ensemble du dataset -> pct_total
#   w_id_date : partition par id_date -> age_moyen, age_median, indice_survie
#   w_cumul   : cumul croissant par age dans chaque mois -> indice_survie

# ─────────────────────────────────────────────────────────
# Definition des fenetres
# ─────────────────────────────────────────────────────────
w_global  = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_id_date = Window.partitionBy("id_date")
w_cumul   = Window.partitionBy("id_date").orderBy("age").rowsBetween(Window.unboundedPreceding, 0)

# ─────────────────────────────────────────────────────────
# Application des metriques Window
# ─────────────────────────────────────────────────────────
df_agg = (
    df_agg

    # pct_total : part de cet age/mois sur l'ensemble du dataset
    .withColumn("_total_global", F.sum("nb_deces").over(w_global))
    .withColumn("pct_total",
        F.round(F.col("nb_deces").cast(DoubleType()) / F.col("_total_global") * 100, 4))
    .drop("_total_global")

    # age_moyen : moyenne ponderee par nb_deces sur la fenetre id_date
    .withColumn("age_moyen",
        F.round(
            F.sum(F.col("age").cast(DoubleType()) * F.col("nb_deces")).over(w_id_date)
            / F.sum("nb_deces").over(w_id_date), 1))

    # age_median : premier age pour lequel le cumul depasse 50% du mois
    .withColumn("_cumul_deces", F.sum("nb_deces").over(w_cumul))
    .withColumn("_total_mois",  F.sum("nb_deces").over(w_id_date))
    .withColumn("age_median",
        F.first(
            F.when(F.col("_cumul_deces").cast(DoubleType())
                   >= F.col("_total_mois").cast(DoubleType()) * 0.5, F.col("age")),
            ignorenulls=True
        ).over(w_id_date))
    .drop("_cumul_deces", "_total_mois")

    # indice_survie : 1 - (cumul_deces_jusqu_a_cet_age / total_deces_mois)
    .withColumn("_cumul_age",   F.sum("nb_deces").over(w_cumul))
    .withColumn("_total_mois2", F.sum("nb_deces").over(w_id_date))
    .withColumn("indice_survie",
        F.when(F.col("_total_mois2") > 0,
            F.round(1.0 - F.col("_cumul_age").cast(DoubleType())
                    / F.col("_total_mois2").cast(DoubleType()), 4)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .drop("_cumul_age", "_total_mois2")
)
log.info("Metriques Window calculees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 7 — DETECTION D'ANOMALIES (Z-SCORE INTRA-MOIS)
# ============================================================================
#
# Detecte les pics de mortalite a un age donne dans un mois donne.
# Z-score calcule par partition id_date : compare nb_deces de cet age
# aux autres ages du meme mois.

w_z = Window.partitionBy("id_date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_agg = (
    df_agg
    .withColumn("_mean_mois", F.avg("nb_deces").over(w_z))
    .withColumn("_std_mois",  F.stddev("nb_deces").over(w_z))
    .withColumn("_z_score",
        F.when(F.col("_std_mois").isNotNull() & (F.col("_std_mois") != 0),
            F.round((F.col("nb_deces").cast(DoubleType()) - F.col("_mean_mois"))
                    / F.col("_std_mois"), 3)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .withColumn("anomalie_distribution",
        F.when(F.col("_z_score").isNotNull(),
            F.abs(F.col("_z_score")) > F.lit(CONFIG["zscore_seuil_anomalie"])
        ).otherwise(F.lit(False)))
    .drop("_mean_mois", "_std_mois", "_z_score")
)

nb_anomalies = df_agg.filter(F.col("anomalie_distribution") == True).count()
log.info("Combinaisons age x id_date avec anomalie : %d", nb_anomalies)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 8 — SELECTION FINALE ET ORDRE DES COLONNES
# ============================================================================

COLONNES_FINALES = [
    "age", "id_date","key_age",
    "nb_deces", "pct_total",
    "nb_hommes", "nb_femmes", "pct_hommes", "pct_femmes", "ratio_h_f",
    "age_moyen", "age_median",
    "nb_centenaires", "nb_mineurs",
    "indice_survie", "anomalie_distribution",
]

df_final = df_agg.select(COLONNES_FINALES).orderBy("id_date", "age")
log.info("Selection finale : %d colonnes | %d lignes", len(COLONNES_FINALES), df_final.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 9 — ECRITURE DELTA
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
#  ETAPE 10 — RAPPORT QUALITE
# ============================================================================

df_check = spark.read.table(CONFIG["output_table_name"])
nb_rows  = df_check.count()

print(f"\n{'=' * 64}")
print(f"  TABLE : {CONFIG['output_table_name']}")
print(f"  Lignes : {nb_rows:,}  |  Colonnes : {len(df_check.columns)}")
print(f"{'=' * 64}\n")

df_check.printSchema()

print("--- Apercu (6 lignes) ---")
df_check.show(6, truncate=False)

print("--- Top 15 ages par nb_deces (cumul tous mois) ---")
df_check.groupBy("age").agg(F.sum("nb_deces").alias("total_deces"))     .orderBy(F.col("total_deces").desc()).show(15, truncate=False)

print(f"--- Anomalies (|z| > {CONFIG['zscore_seuil_anomalie']}) : {nb_anomalies} ---")
df_check.filter(F.col("anomalie_distribution") == True)     .select("id_date", "age", "nb_deces", "pct_total", "ratio_h_f", "indice_survie")     .orderBy(F.col("nb_deces").desc()).show(20, truncate=False)

# ─────────────────────────────────────────────────────────
# Verification monotonie indice_survie sur un mois exemple
# ─────────────────────────────────────────────────────────
id_date_exemple = df_check.select("id_date").orderBy("id_date").first()[0]
print(f"--- Indice survie pour id_date={id_date_exemple} (doit decroitre avec l'age) ---")
df_check.filter(F.col("id_date") == id_date_exemple)     .select("age", "nb_deces", "indice_survie", "age_moyen", "age_median")     .orderBy("age").show(20, truncate=False)

print("--- % nulls colonnes cles ---")
cols_check = ["ratio_h_f", "age_moyen", "age_median", "indice_survie", "pct_total"]
exprs = [F.round(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / F.count("*") * 100, 2).alias(c)
         for c in cols_check]
df_check.select(exprs).show(truncate=False)

total_deces_agg = df_check.agg(F.sum("nb_deces")).collect()[0][0]
ecart = nb_silver - total_deces_agg
print(f"\nSomme nb_deces : {total_deces_agg:,}  |  Silver (age non null) : {nb_silver:,}")
if ecart != 0:
    print(f"Info : ecart {ecart:,} = lignes Silver avec age_au_deces null")
else:
    print("Totaux coherents.")

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
df_silver.unpersist()
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
print(f"  RAPPORT GOLD-AGGAGE — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Lignes Silver en entree : {nb_silver:>10,}")
print(f"  Combinaisons age x mois : {nb_agg:>10,}")
print(f"  Lignes ecrites          : {nb_rows:>10,}")
print(f"  Anomalies detectees     : {nb_anomalies:>10,}")
print(f"{'─' * 60}")
print(f"  Table ecrite : {CONFIG['output_table_name']}")
print(f"  Duree totale : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Gold-AggAge termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
