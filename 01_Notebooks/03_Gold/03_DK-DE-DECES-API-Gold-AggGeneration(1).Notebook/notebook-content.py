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
#  Notebook : 03_DK-DE-DECES-API-Gold-AggGeneration
#  Couche   : Gold
#  Domaine  : Sante - Deces
#  Objectif : Agregation de la mortalite par cohorte generationnelle
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook charge de produire la table agg_mortalite_generation, agregee
# a la granularite id_date (AnMois naissance) x age x id_commune.
# Calcule les indicateurs de surmortalite inter-cohortes (z-score a age
# egal), la tendance de mortalite intra-cohorte (variation age N vs N-1)
# et detecte les anomalies generationnelles. Permet l'analyse des
# cohortes historiques (ex : generation 1914-1918, baby-boom, etc.).
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter agregation par decennie de naissance pour simplifier BI
# [ ] Ajouter esperance de vie residuelle par cohorte
# [ ] Ajouter jointure avec dim_generation quand disponible
# [ ] Optimiser la window tendance_mortalite (couteuse sur grandes cohortes)
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
log = logging.getLogger("agg_mortalite_generation")

spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

CONFIG = {
    "silver_path" :          "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/0359305b-a30c-48e1-be9d-e06c3f6eca4f/Tables/",
    "silver_table":          "silver_deces",
    "output_table_path":     "Tables/agg_mortalite_generation",
    "output_table_name":     "agg_mortalite_generation",
    "write_mode":            "overwrite",
    "seuil_centenaire":      100,
    "seuil_mineur":          18,
    "seuil_senior":          65,
    "zscore_seuil_anomalie": 2.5,
    "annee_naissance_min":   1850,
    "annee_naissance_max":   2025,
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Gold-AggGeneration — demarrage | env : %s", ENVIRONMENT)
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
        spark.read.format("delta").load(full_path)
            .select("date_naissance", "age_au_deces", "annee_deces",
                "mois_deces", "code_lieu_deces", "sexe")
            .filter(
                F.col("age_au_deces").isNotNull()
                & F.col("annee_deces").isNotNull()
                & F.col("mois_deces").isNotNull()
                & F.col("date_naissance").isNotNull()
            )
    )
    df_silver.cache()
    nb_silver = df_silver.count()
    log.info("Lignes Silver (age + date_naissance non null) : %d", nb_silver)
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
#  ETAPE 3 — CONSTRUCTION DES CLES GENERATIONNELLES
# ============================================================================
#
# id_date    : AnMois naissance (yyyyMM) — cle analytique cohorte fine
# annee_naissance : annee de naissance — cohorte annuelle pour agregats
# id_commune : code_lieu_deces normalise zfill-5

df = (
    df_silver
    .withColumn("annee_naissance", F.year("date_naissance").cast(IntegerType()))
    .withColumn("mois_naissance",  F.month("date_naissance").cast(IntegerType()))

    # Filtre qualite annee_naissance
    .filter(F.col("annee_naissance").between(
        CONFIG["annee_naissance_min"], CONFIG["annee_naissance_max"]))

    # id_date = AnMois naissance (yyyyMM)
    .withColumn("id_date",
        F.concat(F.col("annee_naissance").cast(StringType()),
                 F.lpad(F.col("mois_naissance").cast(StringType()), 2, "0")
        ).cast(IntegerType()))

    # id_commune : normalisation zfill-5
    .withColumn("id_commune",
        F.when(F.col("code_lieu_deces").isNull() | (F.col("code_lieu_deces") == ""),
            F.lit(None).cast(StringType())
        ).otherwise(F.lpad(F.col("code_lieu_deces"), 5, "0")))

    # Colonnes indicatrices
    .withColumn("est_homme",      F.when(F.col("sexe") == "M", 1).otherwise(0))
    .withColumn("est_femme",      F.when(F.col("sexe") == "F", 1).otherwise(0))
    .withColumn("est_centenaire", F.when(F.col("age_au_deces") >= CONFIG["seuil_centenaire"], 1).otherwise(0))
    .withColumn("est_mineur",     F.when(F.col("age_au_deces") < CONFIG["seuil_mineur"], 1).otherwise(0))
    .withColumn("est_senior",     F.when(F.col("age_au_deces") >= CONFIG["seuil_senior"], 1).otherwise(0))
)

nb_apres_filtre = df.count()
log.info("Apres filtre qualite annee_naissance [%d-%d] : %d lignes (/%d)",
         CONFIG["annee_naissance_min"], CONFIG["annee_naissance_max"],
         nb_apres_filtre, nb_silver)

# ─────────────────────────────────────────────────────────
# Construction key_generation
# ─────────────────────────────────────────────────────────
df_keygen = df.withColumn("key_generation",
    F.when(
    F.col("date_naissance").isNotNull()
    & F.col("age_au_deces").isNotNull()
    & F.col("code_lieu_deces").isNotNull()
    & (F.col("code_lieu_deces") != ""),
    F.concat_ws("|",
        F.col("id_date").cast(StringType()),
        F.col("age_au_deces").cast(StringType()),
        F.col("id_commune").cast(StringType()))
).otherwise(F.lit(None).cast(StringType()))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — AGREGAT PRINCIPAL
# ============================================================================
#
# Granularite : id_date (AnMois naissance) x age x id_commune

df_agg = (
    df_keygen
    .groupBy("id_date","key_generation", "annee_naissance", "age_au_deces", "id_commune")
    .agg(
        F.count("*")                                                   .alias("nb_deces"),
        F.round(F.avg("age_au_deces"), 1)                             .alias("age_moyen_deces"),
        F.round(F.percentile_approx("age_au_deces", 0.5, 1000), 1)   .alias("age_median_deces"),
        F.min("age_au_deces")                                          .alias("age_min"),
        F.max("age_au_deces")                                          .alias("age_max"),
        F.sum("est_homme")                                             .alias("nb_hommes"),
        F.sum("est_femme")                                             .alias("nb_femmes"),
        F.sum("est_centenaire")                                        .alias("nb_centenaires"),
        F.sum("est_mineur")                                            .alias("nb_mineurs"),
        F.sum("est_senior")                                            .alias("_nb_seniors"),
        F.min("annee_deces")                                           .alias("annee_deces_min"),
        F.max("annee_deces")                                           .alias("annee_deces_max"),
    )
    .withColumnRenamed("age_au_deces", "age")
)

nb_agg = df_agg.count()
log.info("Lignes apres agregat (id_date x age x id_commune) : %d", nb_agg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — RATIOS ET POURCENTAGES
# ============================================================================

df_agg = (
    df_agg
    .withColumn("_nb_sexe", F.col("nb_hommes") + F.col("nb_femmes"))
    .withColumn("pct_hommes",
        F.when(F.col("_nb_sexe") > 0,
            F.round(F.col("nb_hommes").cast(DoubleType()) / F.col("_nb_sexe") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .withColumn("pct_femmes",
        F.when(F.col("_nb_sexe") > 0,
            F.round(F.col("nb_femmes").cast(DoubleType()) / F.col("_nb_sexe") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .withColumn("ratio_h_f",
        F.when(F.col("nb_femmes") > 0,
            F.round(F.col("nb_hommes").cast(DoubleType()) / F.col("nb_femmes"), 3)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .drop("_nb_sexe")
    .withColumn("indice_vieillissement",
        F.when(F.col("nb_deces") > 0,
            F.round(F.col("_nb_seniors").cast(DoubleType()) / F.col("nb_deces") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .drop("_nb_seniors")
)
log.info("Ratios et pourcentages calcules")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — PCT_TOTAL (WINDOW GLOBAL)
# ============================================================================

w_global = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_agg = (
    df_agg
    .withColumn("_total_global", F.sum("nb_deces").over(w_global))
    .withColumn("pct_total",
        F.round(F.col("nb_deces").cast(DoubleType()) / F.col("_total_global") * 100, 5))
    .drop("_total_global")
)
log.info("pct_total calcule")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 7 — SURMORTALITE (Z-SCORE INTER-COHORTES A AGE EGAL)
# ============================================================================
#
# Pour chaque age X : compare nb_deces de cette cohorte a la moyenne
# de toutes les cohortes ayant atteint cet age.
# > 0 : plus de deces que la moyenne inter-cohortes a cet age
# > 2 : surmortalite significative (ex : cohorte 1914-1918)

w_age = Window.partitionBy("age").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_agg = (
    df_agg
    .withColumn("_mean_age", F.avg("nb_deces").over(w_age))
    .withColumn("_std_age",  F.stddev("nb_deces").over(w_age))
    .withColumn("surmortalite",
        F.when(F.col("_std_age").isNotNull() & (F.col("_std_age") != 0),
            F.round((F.col("nb_deces").cast(DoubleType()) - F.col("_mean_age"))
                    / F.col("_std_age"), 3)
        ).otherwise(F.lit(0.0).cast(DoubleType())))
    .drop("_mean_age", "_std_age")
)
log.info("Surmortalite calculee")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 8 — TENDANCE MORTALITE
# ============================================================================
#
# Variation de nb_deces de cet age par rapport a l'age precedent,
# au sein de la meme cohorte (annee_naissance) et commune.
# Valeur positive : mortalite en hausse avec l'age (normal apres 50 ans)
# Valeur negative : mortalite en baisse (cohorte qui s'eteint)

w_tendance = Window.partitionBy("annee_naissance", "id_commune").orderBy("age")

df_agg = df_agg.withColumn("tendance_mortalite",
    F.when(
        F.lag("nb_deces", 1).over(w_tendance).isNotNull()
        & (F.lag("nb_deces", 1).over(w_tendance) != 0),
        F.round(
            (F.col("nb_deces").cast(DoubleType()) - F.lag("nb_deces", 1).over(w_tendance))
            / F.lag("nb_deces", 1).over(w_tendance) * 100, 2)
    ).otherwise(F.lit(None).cast(DoubleType())))

log.info("Tendance mortalite calculee")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 9 — ANOMALIE GENERATION
# ============================================================================
#
# Z-score intra-cohorte : nb_deces de ce groupe (age x commune) compare
# aux autres groupes de la meme cohorte (annee_naissance).

w_cohorte = Window.partitionBy("annee_naissance").rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing)

df_agg = (
    df_agg
    .withColumn("_mean_cohorte", F.avg("nb_deces").over(w_cohorte))
    .withColumn("_std_cohorte",  F.stddev("nb_deces").over(w_cohorte))
    .withColumn("_z_cohorte",
        F.when(F.col("_std_cohorte").isNotNull() & (F.col("_std_cohorte") != 0),
            F.round((F.col("nb_deces").cast(DoubleType()) - F.col("_mean_cohorte"))
                    / F.col("_std_cohorte"), 3)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .withColumn("anomalie_generation",
        F.when(F.col("_z_cohorte").isNotNull(),
            F.abs(F.col("_z_cohorte")) > F.lit(CONFIG["zscore_seuil_anomalie"])
        ).otherwise(F.lit(False)))
    .drop("_mean_cohorte", "_std_cohorte", "_z_cohorte")
)

nb_anomalies = df_agg.filter(F.col("anomalie_generation") == True).count()
log.info("Groupes avec anomalie_generation = True : %d", nb_anomalies)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 10 — SELECTION FINALE ET ORDRE DES COLONNES
# ============================================================================

COLONNES_FINALES = [
    "id_date","key_generation", "annee_naissance", "age", "id_commune",
    "nb_deces", "pct_total",
    "age_moyen_deces", "age_median_deces", "age_min", "age_max",
    "nb_hommes", "nb_femmes", "pct_hommes", "pct_femmes", "ratio_h_f",
    "nb_centenaires", "nb_mineurs",
    "annee_deces_min", "annee_deces_max",
    "surmortalite", "tendance_mortalite",
    "indice_vieillissement", "anomalie_generation",
]

df_final = (
    df_agg
    .select(COLONNES_FINALES)
    .orderBy("annee_naissance", "age", "id_commune")
)

nb_final = df_final.count()
log.info("Selection finale : %d colonnes | %d lignes", len(COLONNES_FINALES), nb_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 11 — ECRITURE DELTA
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
#  ETAPE 12 — RAPPORT QUALITE
# ============================================================================

df_check = spark.read.table(CONFIG["output_table_name"])
nb_rows  = df_check.count()

print(f"\n{'=' * 64}")
print(f"  TABLE : {CONFIG['output_table_name']}")
print(f"  Lignes : {nb_rows:,}  |  Colonnes : {len(df_check.columns)}")
print(f"{'=' * 64}\n")

df_check.printSchema()

print("--- Apercu (5 lignes) ---")
df_check.show(5, truncate=False)

print("--- Top 10 cohortes (annee_naissance) par nb_deces ---")
df_check.groupBy("annee_naissance")     .agg(F.sum("nb_deces").alias("total_deces"),
         F.round(F.avg("age_moyen_deces"), 1).alias("age_moyen"),
         F.round(F.avg("surmortalite"), 3).alias("surmortalite_moy"))     .orderBy(F.col("total_deces").desc()).show(10, truncate=False)

print(f"--- Cohortes avec surmortalite > {CONFIG['zscore_seuil_anomalie']} (top 20) ---")
df_check.filter(F.col("surmortalite") > CONFIG["zscore_seuil_anomalie"])     .select("annee_naissance", "age", "id_commune",
            "nb_deces", "surmortalite", "tendance_mortalite")     .orderBy(F.col("surmortalite").desc()).show(20, truncate=False)

print(f"--- Groupes avec anomalie_generation : {nb_anomalies:,} ---")

print("--- % nulls colonnes cles ---")
cols_check = ["id_commune", "age_moyen_deces", "age_median_deces",
              "ratio_h_f", "surmortalite", "tendance_mortalite", "indice_vieillissement"]
exprs = [F.round(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / F.count("*") * 100, 2).alias(c)
         for c in cols_check]
df_check.select(exprs).show(truncate=False)

total_deces_agg = df_check.agg(F.sum("nb_deces")).collect()[0][0]
ecart = nb_apres_filtre - total_deces_agg
print(f"\nSomme nb_deces : {total_deces_agg:,}  |  Silver filtre : {nb_apres_filtre:,}")
if ecart != 0:
    print(f"Info : ecart {ecart:,} = deces avec id_commune null (groupBy conserve null)")
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

df_silver.unpersist()
spark.catalog.clearCache()
log.info("Cache Spark libere.")

duree_sec = (datetime.now(timezone.utc) - _debut_pipeline).seconds

print(f"\n{'=' * 60}")
print(f"  RAPPORT GOLD-AGGGENERATION — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Lignes Silver en entree       : {nb_silver:>10,}")
print(f"  Lignes apres filtre naissance : {nb_apres_filtre:>10,}")
print(f"  Lignes agregees               : {nb_agg:>10,}")
print(f"  Lignes ecrites                : {nb_final:>10,}")
print(f"  Anomalies detectees           : {nb_anomalies:>10,}")
print(f"{'─' * 60}")
print(f"  Table ecrite : {CONFIG['output_table_name']}")
print(f"  Duree totale : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Gold-AggGeneration termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
