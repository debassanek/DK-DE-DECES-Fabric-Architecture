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
#  Notebook : 90_DK-DE-DECES-API-Monitoring-Metrics
#  Couche   : Monitoring
#  Domaine  : Sante - Deces
#  Objectif : Collecte et consolidation des metriques techniques du pipeline
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook responsable de la collecte et de la consolidation des metriques
# techniques et fonctionnelles du pipeline. Pour chaque table du projet,
# collecte : nb_lignes, nb_colonnes, version Delta, % nulls sur la cle
# principale, et les indicateurs qualite specifiques par couche.
# Insere une ligne par table dans monitoring_metrics (mode APPEND).
# Fournit un tableau de bord complet de l'etat du parc de tables.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Recevoir run_id en parametre depuis le MasterPipeline
# [ ] Ajouter la taille physique des tables Delta (via DeltaTable.detail())
# [ ] Ajouter le nombre de fichiers Delta par table
# [ ] Ajouter alerte automatique si derive de volumetrie > seuil
# [ ] Ajouter comparaison avec les metriques de l'execution precedente
# [ ] Ajouter metriques de performance Spark (shuffle bytes, stages)
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
    StringType, LongType, IntegerType, DoubleType, TimestampType,
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
log = logging.getLogger("monitoring_metrics")

# ─────────────────────────────────────────────────────────
# Configuration Spark
# ─────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "true")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    "output_table_name": "monitoring_metrics",
    "output_table_path": "Tables/monitoring_metrics",
}

# ─────────────────────────────────────────────────────────
# Catalogue des tables du projet a surveiller
# Format : (couche, table_path, table_name, cle_principale)
# La cle_principale sert au calcul du % nulls
# ─────────────────────────────────────────────────────────
TABLES_CATALOGUE = [
    # Bronze
    ("Bronze", "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/592237df-630e-4111-ad48-dc45b1a5a5e0/Tables/", "bronze_deces",               "_source_fichier"),
    # Silver
    ("Silver", "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/0359305b-a30c-48e1-be9d-e06c3f6eca4f/Tables/", "silver_deces_clean",          "nom"),
    ("Silver", "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/0359305b-a30c-48e1-be9d-e06c3f6eca4f/Tables/", "silver_deces_normalized",     "nom"),
    ("Silver", "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/0359305b-a30c-48e1-be9d-e06c3f6eca4f/Tables/", "silver_deces",                "nom"),
    # Gold — Fact
    ("Gold",   "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/", "fact_deces",                  "key_aggAge"),
    # Gold — Agregations
    ("Gold",   "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/", "agg_mortalite_age",           "age"),
    ("Gold",   "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/", "agg_mortalite_mensuelle",     "AnMois"),
    ("Gold",   "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/", "agg_mortalite_commune",       "id_commune"),
    ("Gold",   "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/",  "agg_mortalite_generation",   "id_date"),
    # Gold — Marts
    ("Gold",   "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/", "mart_deces_mensuel",          "AnMois"),
    ("Gold",   "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/", "mart_deces_geographique",     "id_commune"),
    # Dimensions
    ("Dim", "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/",    "dim_age",                     "age_key"),
    ("Dim",  "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/",   "dim_date",                    "id_date"),
    ("Dim",  "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/",   "dim_generation",              "annee_naissance"),
    ("Dim",  "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/ef922306-2cca-4783-b2d3-96e5b020eeb5/Tables/",   "dim_lieu",                    "id_commune"),
    # Monitoring
    ("Monitoring", "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/4bed95d1-cf69-4084-8c1e-1753a1ddb55d/Tables/", "monitoring_pipeline_runs", "run_id"),
]

# ─────────────────────────────────────────────────────────
# Identifiant d'execution
# ─────────────────────────────────────────────────────────
_now          = datetime.now(timezone.utc)
RUN_ID        = _now.strftime("%Y%m%d_%H%M%S")
RUN_TIMESTAMP = _now

_debut_pipeline = _now
log.info("=" * 60)
log.info("Monitoring-Metrics — demarrage | env : %s | run_id : %s",
         ENVIRONMENT, RUN_ID)
log.info("Tables a surveiller : %d", len(TABLES_CATALOGUE))
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

def safe_read(layer: str, table_path: str, table_name: str):
    """Retourne un DataFrame ou None si la table est inaccessible."""
    try:
        if table_path.startswith("abfss://"):
            # Bronze : lecture directe via chemin ABFSS
            full_path = f"{table_path}{table_name}"
            return spark.read.format("delta").load(full_path)
        else:
            # Silver / Gold / Dim / Monitoring : lecture via catalogue
            full_table_ref = f"{table_path}.{table_name}" if table_path else table_name
            return spark.read.format("delta").load(full_table_ref)
    except Exception as e:
        log.warning("Table inaccessible : '%s' — %s", table_name, str(e)[:80])
        return None

def get_delta_version(layer: str, table_path: str, table_name: str) -> int:
    try:
        if table_path.startswith("abfss://"):
            full_path = f"{table_path}{table_name}"
            dt = DeltaTable.forPath(spark, full_path)   # ← forPath pour ABFSS
        else:
            full_table_ref = f"{table_path}.{table_name}" if table_path else table_name
            dt = DeltaTable.forName(spark, full_table_ref)  # ← forName pour catalogue
        return int(dt.history(1).collect()[0]["version"])
    except Exception:
        return -1
def get_pct_null(df, col_name: str) -> float:
    """Retourne le % de nulls sur col_name ou None si colonne absente."""
    if col_name not in df.columns:
        return None
    try:
        total = df.count()
        if total == 0:
            return 0.0
        nb_null = df.filter(F.col(col_name).isNull()).count()
        return round(nb_null / total * 100, 4)
    except Exception:
        return None
        
log.info("Fonctions utilitaires de collecte OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — COLLECTE DES METRIQUES PAR TABLE
# ============================================================================

rows_metrics = []

for couche, table_path, table_name, cle_principale in TABLES_CATALOGUE:
    log.info("  Collecte : [%s] %s ...", couche, table_name)

    df = safe_read(couche, table_path, table_name)

    if df is None:
        # Table absente — on enregistre quand meme avec des indicateurs -1
        rows_metrics.append(Row(
            run_id          = RUN_ID,
            run_timestamp   = RUN_TIMESTAMP,
            environment     = ENVIRONMENT,
            couche          = couche,
            table_name      = table_name,
            nb_lignes       = -1,
            nb_colonnes     = -1,
            version_delta   = -1,
            pct_null_cle    = None,
            statut_table    = "inaccessible",
            notes           = "Table absente ou inaccessible lors de la collecte",
        ))
        continue

    # ─────────────────────────────────────────────────────────
    # Metriques de base
    # ─────────────────────────────────────────────────────────
    nb_lignes     = df.count()
    nb_colonnes   = len(df.columns)
    version_delta = get_delta_version(couche, table_path, table_name)
    pct_null_cle  = get_pct_null(df, cle_principale)

    # ─────────────────────────────────────────────────────────
    # Statut table
    # ─────────────────────────────────────────────────────────
    if nb_lignes > 0:
        statut_table = "ok"
    else:
        statut_table = "vide"

    # ─────────────────────────────────────────────────────────
    # Notes specifiques par couche
    # ─────────────────────────────────────────────────────────
    notes = None
    if pct_null_cle is not None and pct_null_cle > 10:
        notes = f"ATTENTION : {pct_null_cle:.2f}% nulls sur cle '{cle_principale}'"
    if nb_lignes == 0:
        notes = "Table vide — verifier l'execution du notebook producteur"

    log.info(
        "    [%s] %s : %d lignes | %d cols | version %d | null_cle=%.2f%%",
        statut_table, table_name, nb_lignes, nb_colonnes,
        version_delta, pct_null_cle if pct_null_cle is not None else 0.0,
    )

    rows_metrics.append(Row(
        run_id          = RUN_ID,
        run_timestamp   = RUN_TIMESTAMP,
        environment     = ENVIRONMENT,
        couche          = couche,
        table_name      = table_name,
        nb_lignes       = nb_lignes,
        nb_colonnes     = nb_colonnes,
        version_delta   = version_delta,
        pct_null_cle    = pct_null_cle,
        statut_table    = statut_table,
        notes           = notes,
    ))

log.info("Collecte terminee : %d tables traitees", len(rows_metrics))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — SCHEMA ET INITIALISATION DE LA TABLE DE METRIQUES
# ============================================================================

SCHEMA_METRICS = StructType([
    StructField("run_id",           StringType(),    False),
    StructField("run_timestamp",    TimestampType(), False),
    StructField("environment",      StringType(),    True),
    StructField("couche",           StringType(),    True),
    StructField("table_name",       StringType(),    True),
    StructField("nb_lignes",        LongType(),      True),
    StructField("nb_colonnes",      IntegerType(),   True),
    StructField("version_delta",    LongType(),      True),
    StructField("pct_null_cle",     DoubleType(),    True),
    StructField("statut_table",     StringType(),    True),
    StructField("notes",            StringType(),    True),
])

OUTPUT_PATH = CONFIG["output_table_path"]
OUTPUT_NAME = CONFIG["output_table_name"]

# ─────────────────────────────────────────────────────────
# Creation de la table si premiere execution
# ─────────────────────────────────────────────────────────
if not DeltaTable.isDeltaTable(spark, OUTPUT_PATH):
    log.info("Premiere execution — creation de la table de metriques...")
    (
        spark.createDataFrame([], SCHEMA_METRICS)
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
#  ETAPE 5 — ECRITURE DES METRIQUES
# ============================================================================

df_metrics = spark.createDataFrame(rows_metrics, schema=SCHEMA_METRICS)

# ─────────────────────────────────────────────────────────
# Append — accumule l'historique des metriques
# ─────────────────────────────────────────────────────────
try:
    (
        df_metrics.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(OUTPUT_PATH)
    )
    log.info("Metriques de l'execution '%s' enregistrees dans '%s'.",
             RUN_ID, OUTPUT_NAME)
except Exception as e:
    log.error("Echec ecriture monitoring_metrics : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — RAPPORT DE L'EXECUTION COURANTE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Tableau recapitulatif par couche
# ─────────────────────────────────────────────────────────
print(f"\n{'=' * 80}")
print(f"  METRIQUES DU PIPELINE — run_id : {RUN_ID} | env : {ENVIRONMENT.upper()}")
print(f"{'=' * 80}")
print(f"  {'COUCHE':<12} {'TABLE':<42} {'LIGNES':>10} {'VERSION':>8} {'NULL_CLE':>10} {'STATUT':<12}")
print(f"{'─' * 80}")
for r in sorted(rows_metrics, key=lambda x: (x.couche, x.table_name)):
    null_str = f"{r.pct_null_cle:.2f}%" if r.pct_null_cle is not None else "N/A"
    lignes_str = f"{r.nb_lignes:,}" if r.nb_lignes >= 0 else "N/A"
    print(f"  {r.couche:<12} {r.table_name:<42} {lignes_str:>10} "
          f"{r.version_delta:>8} {null_str:>10} {r.statut_table:<12}")
print(f"{'=' * 80}\n")

# ─────────────────────────────────────────────────────────
# Tables en alerte
# ─────────────────────────────────────────────────────────
alertes = [r for r in rows_metrics if r.statut_table != "ok"]
if alertes:
    print(f"{'─' * 60}")
    print(f"  ALERTES ({len(alertes)} table(s) necessitant attention)")
    print(f"{'─' * 60}")
    for r in alertes:
        print(f"  [{r.statut_table.upper()}] {r.table_name}")
        if r.notes:
            print(f"    -> {r.notes}")
    print(f"{'─' * 60}\n")
else:
    print("  Toutes les tables sont en statut OK.\n")

# ─────────────────────────────────────────────────────────
# Historique des metriques recentes dans la table
# ─────────────────────────────────────────────────────────
print("--- Historique des 5 derniers run_id ---")
(
    spark.read.table(OUTPUT_NAME)
    .groupBy("run_id", "run_timestamp", "environment")
    .agg(
        F.count("*").alias("nb_tables"),
        F.sum(F.when(F.col("statut_table") == "ok", 1).otherwise(0)).alias("nb_ok"),
        F.sum(F.when(F.col("statut_table") != "ok", 1).otherwise(0)).alias("nb_alertes"),
    )
    .orderBy(F.col("run_timestamp").desc())
    .show(5, truncate=False)
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

nb_ok_total     = sum(1 for r in rows_metrics if r.statut_table == "ok")
nb_alerte_total = sum(1 for r in rows_metrics if r.statut_table != "ok")

print(f"\n{'=' * 60}")
print(f"  RAPPORT MONITORING-METRICS — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  run_id           : {RUN_ID}")
print(f"  Tables surveillees : {len(rows_metrics):>6}")
print(f"  Tables OK          : {nb_ok_total:>6}")
print(f"  Tables en alerte   : {nb_alerte_total:>6}")
print(f"{'─' * 60}")
print(f"  Table de metriques : {OUTPUT_NAME}")
print(f"  Duree collecte     : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Monitoring-Metrics termine. run_id : %s | ok : %d | alertes : %d | duree : %d sec",
         RUN_ID, nb_ok_total, nb_alerte_total, duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
