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
#  Notebook : 03_DK-DE-DECES-API-Gold-AggGeographie
#  Couche   : Gold
#  Domaine  : Sante - Deces
#  Objectif : Agregation de la mortalite par commune avec enrichissement geo
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook charge de produire la table agg_mortalite_commune, agregee
# par commune de deces. Realise une jointure LEFT avec dim_lieu pour
# enrichir les indicateurs geographiques (population, superficie,
# densite, type de commune). Calcule le taux de mortalite pour 1 000
# habitants, la densite de deces, l'indice de vieillissement et detecte
# les communes avec un taux de mortalite statistiquement anormal.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter agregation departementale et regionale (ROLLUP)
# [ ] Ajouter evolution temporelle par commune (jointure avec AggTemporel)
# [ ] Ajouter carte choropleth via donnees latitude/longitude
# [ ] Remplacer pop_min_taux par un seuil dynamique (percentile)
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
log = logging.getLogger("agg_mortalite_commune")

spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

CONFIG = {
    "silver_path" :          "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/0359305b-a30c-48e1-be9d-e06c3f6eca4f/Tables/",
    "silver_table":          "silver_deces",
    "dim_path" :              "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/",        
    "dim_lieu_table":        "dim_lieu",              
    "output_table_path":     "Tables/agg_mortalite_commune",
    "output_table_name":     "agg_mortalite_commune",
    "write_mode":            "overwrite",
    "zscore_seuil_anomalie": 2.5,
    "seuil_centenaire":      100,
    "seuil_mineur":          18,
    "seuil_senior":          65,
    "pop_min_taux":          50,
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Gold-AggGeographie — demarrage | env : %s", ENVIRONMENT)
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
# Sous-partie 2a — Silver
# ─────────────────────────────────────────────────────────

full_path_silver =f"{CONFIG['silver_path']}{CONFIG['silver_table']}"

try:
    df_silver = (
        spark.read.format('delta').load(full_path_silver)
        .select("code_lieu_deces", "age_au_deces", "sexe")
        .filter(F.col("code_lieu_deces").isNotNull())
    )
    df_silver.cache()
    nb_silver = df_silver.count()
    log.info("Lignes Silver (code_lieu_deces non null) : %d", nb_silver)
except Exception as e:
    log.error("Echec lecture Silver '%s' : %s", CONFIG["silver_table"], e)
    raise

# ─────────────────────────────────────────────────────────
# Sous-partie 2b — dim_lieu
# ─────────────────────────────────────────────────────────

full_path_dim =f"{CONFIG['dim_path']}{CONFIG['dim_lieu_table']}"

try:
    df_lieu = (
        spark.read.format('delta').load(full_path_dim)
        .select(
            "id_commune", "commune", "département", "région",
            "code_region", "latitude", "longitude",
            "population_commune", "superficie_commune",
            "densite_commune", "type_commune", "epci",
        )
        # Normalisation cle de jointure : zfill 5 pour matcher code_lieu_deces Silver
        .withColumn("id_commune",
            F.lpad(F.col("id_commune").cast(StringType()), 5, "0"))
    )
    df_lieu.cache()
    nb_lieu = df_lieu.count()
    log.info("Lignes dim_lieu : %d communes", nb_lieu)
    df_lieu.printSchema()
except Exception as e:
    log.error("Echec lecture dim_lieu '%s' : %s", CONFIG["dim_lieu_table"], e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — AGREGATS DECES PAR COMMUNE
# ============================================================================

df = (
    df_silver
    .withColumn("est_homme",      F.when(F.col("sexe") == "M", 1).otherwise(0))
    .withColumn("est_femme",      F.when(F.col("sexe") == "F", 1).otherwise(0))
    .withColumn("est_centenaire", F.when(F.col("age_au_deces") >= CONFIG["seuil_centenaire"], 1).otherwise(0))
    .withColumn("est_mineur",     F.when(F.col("age_au_deces") < CONFIG["seuil_mineur"], 1).otherwise(0))
    .withColumn("est_senior",     F.when(F.col("age_au_deces") >= CONFIG["seuil_senior"], 1).otherwise(0))
)

df_agg_deces = (
    df
    .groupBy("code_lieu_deces")
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
    )
)

log.info("Communes avec deces dans Silver : %d", df_agg_deces.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — JOINTURE AVEC DIM_LIEU
# ============================================================================
#
# LEFT JOIN : on conserve toutes les communes Silver,
# meme celles absentes de dim_lieu (codes etrangers, DOM-TOM non couverts).

df_joint = (
    df_agg_deces
    .join(df_lieu, df_agg_deces["code_lieu_deces"] == df_lieu["id_commune"], how="left")
    .withColumn("id_commune",
        F.coalesce(F.col("id_commune"), F.col("code_lieu_deces")))
    .drop("code_lieu_deces")
)

nb_joint     = df_joint.count()
nb_sans_lieu = df_joint.filter(F.col("commune").isNull()).count()
nb_avec_lieu = nb_joint - nb_sans_lieu
log.info("Jointure : %d communes | %d matchees dim_lieu | %d non matchees",
         nb_joint, nb_avec_lieu, nb_sans_lieu)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — METRIQUES DERIVEES
# ============================================================================

df_metriques = (
    df_joint

    # Taux de mortalite : deces / population (pour 1 000 habitants)
    # Null si population inconnue ou inferieure au seuil (micro-communes)
    .withColumn("taux_mortalite",
        F.when(F.col("population_commune").isNotNull()
               & (F.col("population_commune") >= CONFIG["pop_min_taux"]),
            F.round(F.col("nb_deces").cast(DoubleType())
                    / F.col("population_commune") * 1000, 4)
        ).otherwise(F.lit(None).cast(DoubleType())))

    # Densite de deces : deces / superficie (km2)
    .withColumn("densite_deces",
        F.when(F.col("superficie_commune").isNotNull()
               & (F.col("superficie_commune") > 0),
            F.round(F.col("nb_deces").cast(DoubleType())
                    / F.col("superficie_commune"), 4)
        ).otherwise(F.lit(None).cast(DoubleType())))

    # Indice de vieillissement : % deces 65+ sur total
    .withColumn("indice_vieillissement",
        F.when(F.col("nb_deces") > 0,
            F.round(F.col("_nb_seniors").cast(DoubleType()) / F.col("nb_deces") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType())))

    # % sexe sur sexe renseigne uniquement
    .withColumn("_nb_sexe", F.col("nb_hommes") + F.col("nb_femmes"))
    .withColumn("pct_hommes",
        F.when(F.col("_nb_sexe") > 0,
            F.round(F.col("nb_hommes").cast(DoubleType()) / F.col("_nb_sexe") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .withColumn("pct_femmes",
        F.when(F.col("_nb_sexe") > 0,
            F.round(F.col("nb_femmes").cast(DoubleType()) / F.col("_nb_sexe") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .drop("_nb_sexe", "_nb_seniors")
)

log.info("Metriques derivees calculees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — DETECTION D'ANOMALIES (Z-SCORE SUR TAUX_MORTALITE)
# ============================================================================
#
# Identifie les communes avec un taux de mortalite statistiquement anormal.
# Comparaison inter-communes sur celles avec taux calcule.

w_global = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_final_calc = (
    df_metriques
    .withColumn("_mean_taux", F.avg("taux_mortalite").over(w_global))
    .withColumn("_std_taux",  F.stddev("taux_mortalite").over(w_global))
    .withColumn("_z_taux",
        F.when(F.col("taux_mortalite").isNotNull()
               & F.col("_std_taux").isNotNull() & (F.col("_std_taux") != 0),
            F.round((F.col("taux_mortalite") - F.col("_mean_taux"))
                    / F.col("_std_taux"), 3)
        ).otherwise(F.lit(None).cast(DoubleType())))
    .withColumn("anomalie_mortalite",
        F.when(F.col("_z_taux").isNotNull(),
            F.abs(F.col("_z_taux")) > F.lit(CONFIG["zscore_seuil_anomalie"])
        ).otherwise(F.lit(False)))
    .drop("_mean_taux", "_std_taux", "_z_taux")
)

nb_anomalies = df_final_calc.filter(F.col("anomalie_mortalite") == True).count()
log.info("Communes avec anomalie_mortalite = True : %d", nb_anomalies)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 7 — SELECTION FINALE ET ORDRE DES COLONNES
# ============================================================================

COLONNES_FINALES = [
    "id_commune",
    "population_commune",
    "nb_deces", "taux_mortalite",
    "age_moyen_deces", "age_median_deces", "age_min", "age_max",
    "nb_hommes", "nb_femmes", "pct_hommes", "pct_femmes",
    "nb_centenaires", "nb_mineurs",
    "indice_vieillissement", "densite_deces",
    "anomalie_mortalite",
]

df_final = (
    df_final_calc
    .select(COLONNES_FINALES)
    .orderBy(F.col("nb_deces").desc())
)

log.info("Selection finale : %d colonnes | %d communes",
         len(COLONNES_FINALES), df_final.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 8 — ECRITURE DELTA
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
#  ETAPE 9 — RAPPORT QUALITE
# ============================================================================

df_check = spark.read.table(CONFIG["output_table_name"])
nb_rows  = df_check.count()

print(f"\n{'=' * 64}")
print(f"  TABLE : {CONFIG['output_table_name']}")
print(f"  Lignes : {nb_rows:,}  |  Colonnes : {len(df_check.columns)}")
print(f"{'=' * 64}\n")

df_check.printSchema()

print("--- Top 10 communes par nb_deces ---")
df_check.select("id_commune", "nb_deces", "taux_mortalite",
                "age_moyen_deces", "indice_vieillissement", "anomalie_mortalite")     .show(10, truncate=False)

print(f"--- Communes avec anomalie_mortalite (|z| > {CONFIG['zscore_seuil_anomalie']}) : {nb_anomalies} ---")
df_check.filter(F.col("anomalie_mortalite") == True)     .select("id_commune", "nb_deces", "taux_mortalite",
            "population_commune", "indice_vieillissement")     .orderBy(F.col("taux_mortalite").desc()).show(20, truncate=False)

print("--- % nulls colonnes cles ---")
cols_check = ["id_commune", "population_commune", "taux_mortalite",
              "densite_deces", "age_moyen_deces", "indice_vieillissement"]
exprs = [F.round(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / F.count("*") * 100, 2).alias(c)
         for c in cols_check]
df_check.select(exprs).show(truncate=False)

total_deces_agg = df_check.agg(F.sum("nb_deces")).collect()[0][0]
ecart = nb_silver - total_deces_agg
print(f"\nSomme nb_deces : {total_deces_agg:,}  |  Silver (code non null) : {nb_silver:,}")
if ecart != 0:
    print(f"Info : ecart {ecart:,} = deces avec code_lieu_deces null dans Silver")
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
df_lieu.unpersist()
spark.catalog.clearCache()
log.info("Cache Spark libere.")

duree_sec = (datetime.now(timezone.utc) - _debut_pipeline).seconds

print(f"\n{'=' * 60}")
print(f"  RAPPORT GOLD-AGGGEOGRAPHIE — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Lignes Silver en entree : {nb_silver:>10,}")
print(f"  Communes dim_lieu       : {nb_lieu:>10,}")
print(f"  Communes matchees       : {nb_avec_lieu:>10,}")
print(f"  Communes non matchees   : {nb_sans_lieu:>10,}")
print(f"  Anomalies detectees     : {nb_anomalies:>10,}")
print(f"{'─' * 60}")
print(f"  Table ecrite : {CONFIG['output_table_name']}")
print(f"  Duree totale : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Gold-AggGeographie termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
