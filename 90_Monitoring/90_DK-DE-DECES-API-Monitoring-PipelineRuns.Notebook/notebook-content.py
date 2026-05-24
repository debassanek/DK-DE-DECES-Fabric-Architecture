# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4bed95d1-cf69-4084-8c1e-1753a1ddb55d",
# META       "default_lakehouse_name": "DK_DE_DECES_API_Monitoring",
# META       "default_lakehouse_workspace_id": "35193659-8177-497e-ae34-111479e85809",
# META       "known_lakehouses": [
# META         {
# META           "id": "4bed95d1-cf69-4084-8c1e-1753a1ddb55d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
#  Notebook : 90_DK-DE-DECES-API-Monitoring-PipelineRuns
#  Couche   : Monitoring
#  Domaine  : Sante - Deces
#  Objectif : Historique des executions du pipeline Data Engineering
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook charge d'enregistrer l'historique des executions pipeline.
# A chaque execution, collecte les metriques de volumetrie par couche
# (Bronze, Silver, Gold), calcule les taux de rejet et les anomalies
# detectees dans les tables Gold, puis insere une ligne dans la table
# de suivi monitoring_pipeline_runs (mode APPEND — accumule l'historique).
# Toujours execute en fin de pipeline, meme si celui-ci est en echec.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Recevoir run_id en parametre depuis le MasterPipeline pour
#     corr eler les enregistrements PipelineRuns et Metrics :
#     run_id = notebookutils.notebook.run(..., {"run_id": run_id})
# [ ] Ajouter la duree reelle du pipeline (passee en parametre depuis
#     le MasterPipeline qui connait les temps par couche)
# [ ] Ajouter notification Teams / email si statut != success
# [ ] Ajouter comparaison vs execution precedente (derive volumetrique)
# [ ] Ajouter lecture du manifest Bronze (_manifest.json) pour
#     les metriques de telechargement et de parsing par fichier
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
from pyspark.sql import SparkSession, Row, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType,
)
from delta.tables import DeltaTable

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
log = logging.getLogger("monitoring_pipeline_runs")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "true")

CONFIG = {
    # Paths sources
    "bronze_path"   :        "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/592237df-630e-4111-ad48-dc45b1a5a5e0/Tables/",
    "silver_path"   :        "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/0359305b-a30c-48e1-be9d-e06c3f6eca4f/Tables/",
    "gold_path"   :          "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/",
    # Tables sources
    "bronze_table":          "bronze_deces",
    "silver_clean_table":    "silver_deces_clean",
    "silver_final_table":    "silver_deces",
    "fact_table":            "fact_deces",
    "mart_table":            "mart_deces_mensuel",
    "agg_age_table":         "agg_mortalite_age",
    "agg_gen_table":         "agg_mortalite_generation",
    "agg_temporel_table":    "agg_mortalite_mensuelle",
    "agg_geo_table":         "agg_mortalite_commune",
    # Table de sortie
    "output_table_name":     "monitoring_pipeline_runs",
    "output_table_path":     "Tables/monitoring_pipeline_runs",
}

# ─────────────────────────────────────────────────────────
# Identifiant de l'execution courante
# Format : YYYYMMDD_HHMMSS — unique par execution
# Note : a terme, a recevoir en parametre depuis le MasterPipeline
# ─────────────────────────────────────────────────────────
_now            = datetime.now(timezone.utc)
RUN_ID          = _now.strftime("%Y%m%d_%H%M%S")
RUN_TIMESTAMP   = _now

_debut_pipeline = _now
log.info("=" * 60)
log.info("Monitoring-PipelineRuns — demarrage | env : %s | run_id : %s",
         ENVIRONMENT, RUN_ID)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — FONCTIONS UTILITAIRES DE COLLECTE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Lecture securisee d'un count de table
# Retourne -1 si la table est absente ou inaccessible
# ─────────────────────────────────────────────────────────

def safe_count(table_path: str, table_name: str) -> int:
    try:
        if table_path.startswith("abfss://"):
            full_path = f"{table_path}{table_name}"
            return spark.read.format("delta").load(full_path).count()
        else:
            full_table_ref = f"{table_path}.{table_name}" if table_path else table_name
            return spark.read.table(full_table_ref).count()
    except Exception as e:
        log.warning("Table inaccessible '%s' : %s", table_name, str(e)[:80])
        return -1

def safe_count_filter(table_path: str, table_name: str, condition: str) -> int:
    try:
        if table_path.startswith("abfss://"):
            full_path = f"{table_path}{table_name}"
            return spark.read.format("delta").load(full_path).filter(condition).count()
        else:
            full_table_ref = f"{table_path}.{table_name}" if table_path else table_name
            return spark.read.table(full_table_ref).filter(condition).count()
    except Exception as e:
        log.warning("Filtre inaccessible '%s' [%s] : %s", table_name, condition, str(e)[:80])
        return -1

def safe_delta_version(table_path: str, table_name: str) -> int:
    try:
        if table_path.startswith("abfss://"):
            full_path = f"{table_path}{table_name}"
            dt = DeltaTable.forPath(spark, full_path)
        else:
            full_table_ref = f"{table_path}.{table_name}" if table_path else table_name
            dt = DeltaTable.forName(spark, full_table_ref)
        return int(dt.history(1).collect()[0]["version"])
    except Exception:
        return -1

log.info("Fonctions utilitaires de collecte OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — COLLECTE BRONZE
# ============================================================================

log.info("Collecte Bronze...")

nb_bronze = safe_count(CONFIG["bronze_path"],CONFIG["bronze_table"])
version_bronze = safe_delta_version(CONFIG["bronze_path"],CONFIG["bronze_table"])

log.info("  bronze_deces : %d lignes | version Delta : %d", nb_bronze, version_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — COLLECTE SILVER
# ============================================================================

log.info("Collecte Silver...")

nb_silver_clean = safe_count(CONFIG["silver_path"],CONFIG["silver_clean_table"])
nb_silver_final = safe_count(CONFIG["silver_path"],CONFIG["silver_final_table"])

# ─────────────────────────────────────────────────────────
# Taux de rejet Bronze -> Silver
# ─────────────────────────────────────────────────────────
if nb_bronze > 0 and nb_silver_final >= 0:
    taux_rejet_pct = round((nb_bronze - nb_silver_final) / nb_bronze * 100, 4)
else:
    taux_rejet_pct = None

log.info("  silver_deces_clean : %d lignes", nb_silver_clean)
log.info("  silver_deces final : %d lignes", nb_silver_final)
log.info("  Taux de rejet Bronze->Silver : %s%%",
         str(taux_rejet_pct) if taux_rejet_pct is not None else "N/A")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — COLLECTE GOLD
# ============================================================================

log.info("Collecte Gold...")

nb_fact = safe_count(CONFIG["gold_path"],CONFIG["fact_table"])

# ─────────────────────────────────────────────────────────
# Anomalies detectees dans les tables Gold
# ─────────────────────────────────────────────────────────
nb_anomalies_age      = safe_count_filter(CONFIG["gold_path"],
    CONFIG["agg_age_table"], "anomalie_distribution = true")
nb_anomalies_gen      = safe_count_filter(CONFIG["gold_path"],
    CONFIG["agg_gen_table"], "anomalie_generation = true")
nb_anomalies_temporel = safe_count_filter(CONFIG["gold_path"],
    CONFIG["agg_temporel_table"], "anomalie_flag = true")
nb_anomalies_geo      = safe_count_filter(CONFIG["gold_path"],
    CONFIG["agg_geo_table"], "anomalie_mortalite = true")

log.info("  fact_deces           : %d lignes", nb_fact)
log.info("  Anomalies age        : %d", nb_anomalies_age)
log.info("  Anomalies gen        : %d", nb_anomalies_gen)
log.info("  Anomalies temporel   : %d", nb_anomalies_temporel)
log.info("  Anomalies geographie : %d", nb_anomalies_geo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — DETERMINATION DU STATUT GLOBAL
# ============================================================================

# ─────────────────────────────────────────────────────────
# Logique de statut :
#   success : toutes les tables cles sont alimentees
#   partial : certaines tables manquantes ou vides
#   failed  : aucune table cle disponible
# ─────────────────────────────────────────────────────────
tables_cles = {
    "bronze_deces": nb_bronze,
    "silver_deces": nb_silver_final,
    "fact_deces":   nb_fact,
}

nb_ok     = sum(1 for v in tables_cles.values() if v > 0)
nb_absent = sum(1 for v in tables_cles.values() if v < 0)

if nb_ok == len(tables_cles):
    statut = "success"
elif nb_absent == len(tables_cles):
    statut = "failed"
else:
    statut = "partial"

notes_parts = []
for t, v in tables_cles.items():
    if v < 0:
        notes_parts.append(f"{t} inaccessible")
    elif v == 0:
        notes_parts.append(f"{t} vide")
notes = " | ".join(notes_parts) if notes_parts else None

log.info("Statut global : %s", statut.upper())
if notes:
    log.warning("Notes : %s", notes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 7 — SCHEMA ET INITIALISATION DE LA TABLE DE SUIVI
# ============================================================================

# ─────────────────────────────────────────────────────────
# Schema de la table d'historique
# ─────────────────────────────────────────────────────────
SCHEMA_PIPELINE_RUNS = StructType([
    StructField("run_id",                    StringType(),    False),
    StructField("run_timestamp",             TimestampType(), False),
    StructField("environment",               StringType(),    True),
    StructField("statut",                    StringType(),    True),
    StructField("nb_lignes_bronze",          LongType(),      True),
    StructField("nb_lignes_silver_clean",    LongType(),      True),
    StructField("nb_lignes_silver_final",    LongType(),      True),
    StructField("taux_rejet_pct",            DoubleType(),    True),
    StructField("nb_lignes_fact",            LongType(),      True),
    StructField("nb_anomalies_age",          LongType(),      True),
    StructField("nb_anomalies_gen",          LongType(),      True),
    StructField("nb_anomalies_temporel",     LongType(),      True),
    StructField("nb_anomalies_geographie",   LongType(),      True),
    StructField("version_delta_bronze",      LongType(),      True),
    StructField("notes",                     StringType(),    True),
])

# ─────────────────────────────────────────────────────────
# Creation de la table si premiere execution
# ─────────────────────────────────────────────────────────
OUTPUT_PATH = CONFIG["output_table_path"]
OUTPUT_NAME = CONFIG["output_table_name"]

if not DeltaTable.isDeltaTable(spark, OUTPUT_PATH):
    log.info("Premiere execution — creation de la table de suivi...")
    (
        spark.createDataFrame([], SCHEMA_PIPELINE_RUNS)
        .write.format("delta")
        .option("overwriteSchema", "true")
        .save(OUTPUT_PATH)
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {OUTPUT_NAME}
        USING DELTA LOCATION '{OUTPUT_PATH}'
    """)
    log.info("Table '%s' creee.", OUTPUT_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 8 — ECRITURE DE L'EXECUTION COURANTE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Construction de la ligne d'execution
# ─────────────────────────────────────────────────────────
def _to_long(v):
    return int(v) if v is not None and v >= 0 else None

run_row = Row(
    run_id                  = RUN_ID,
    run_timestamp           = RUN_TIMESTAMP,
    environment             = ENVIRONMENT,
    statut                  = statut,
    nb_lignes_bronze        = _to_long(nb_bronze),
    nb_lignes_silver_clean  = _to_long(nb_silver_clean),
    nb_lignes_silver_final  = _to_long(nb_silver_final),
    taux_rejet_pct          = taux_rejet_pct,
    nb_lignes_fact          = _to_long(nb_fact),
    nb_anomalies_age        = _to_long(nb_anomalies_age),
    nb_anomalies_gen        = _to_long(nb_anomalies_gen),
    nb_anomalies_temporel   = _to_long(nb_anomalies_temporel),
    nb_anomalies_geographie = _to_long(nb_anomalies_geo),
    version_delta_bronze    = _to_long(version_bronze),
    notes                   = notes,
)

df_run = spark.createDataFrame([run_row], schema=SCHEMA_PIPELINE_RUNS)

# ─────────────────────────────────────────────────────────
# Append — accumule l'historique (ne pas overwrite)
# ─────────────────────────────────────────────────────────
try:
    (
        df_run.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(OUTPUT_PATH)
    )
    log.info("Execution '%s' enregistree dans '%s'.", RUN_ID, OUTPUT_NAME)
except Exception as e:
    log.error("Echec ecriture monitoring_pipeline_runs : %s", e)
    raise

# ─────────────────────────────────────────────────────────
# Apercu des 10 dernieres executions
# ─────────────────────────────────────────────────────────
print(f"\n{'─' * 70}")
print("  Historique des 10 dernieres executions")
print(f"{'─' * 70}")
(
    spark.read.table(OUTPUT_NAME)
    .orderBy(F.col("run_timestamp").desc())
    .select("run_id", "run_timestamp", "environment", "statut",
            "nb_lignes_bronze", "nb_lignes_silver_final",
            "nb_lignes_fact", "taux_rejet_pct", "notes")
    .show(10, truncate=50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE FINALE — CLEANUP & REPORTING
# ============================================================================

spark.catalog.clearCache()
log.info("Cache Spark libere.")

duree_sec = (datetime.now(timezone.utc) - _debut_pipeline).seconds

print(f"\n{'=' * 60}")
print(f"  RAPPORT MONITORING-PIPELINERUNS — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  run_id               : {RUN_ID}")
print(f"  Statut global        : {statut.upper()}")
print(f"  Bronze               : {nb_bronze:>10,} lignes")
print(f"  Silver clean         : {nb_silver_clean:>10,} lignes")
print(f"  Silver final         : {nb_silver_final:>10,} lignes")
print(f"  Taux de rejet        : {str(taux_rejet_pct) + '%' if taux_rejet_pct else 'N/A':>10}")
print(f"  Fact deces           : {nb_fact:>10,} lignes")
print(f"  Anomalies detectees  : {max(0, nb_anomalies_age) + max(0, nb_anomalies_temporel) + max(0, nb_anomalies_geo):>10,}")
print(f"{'─' * 60}")
print(f"  Table de suivi : {OUTPUT_NAME}")
print(f"  Duree collecte : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Monitoring-PipelineRuns termine. run_id : %s | statut : %s | duree : %d sec",
         RUN_ID, statut, duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
