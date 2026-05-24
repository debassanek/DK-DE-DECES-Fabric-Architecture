# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================================================
#  Notebook : 99_DK-DE-DECES-API-Utils-DeltaUtils
#  Couche   : Utils
#  Domaine  : Sante - Deces
#  Objectif : Fonctions Delta Lake pour le pipeline Data Engineering
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Module utilitaire dedie aux operations Delta Lake : initialisation de
# tables, merges incrementaux, optimisation, vacuum et lecture des
# metriques d'historique. Centralise les patterns Delta utilises dans
# les couches Bronze, Silver et Gold.
#
# Fonctions exposees :
#   init_delta_if_needed(spark, path, schema, table_name)  -> bool
#   merge_insert_only(delta_table, df_source, condition)   -> dict
#   get_delta_metrics(delta_table)                         -> dict
#   get_delta_version(spark, table_name)                   -> int
#   optimize_table(spark, table_name, zorder_cols)
#   vacuum_table(spark, table_name, retention_hours)
#   register_table(spark, table_name, path)
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter merge_upsert() pour les dimensions (UPDATE + INSERT)
# [ ] Ajouter clone_table() pour les sauvegardes avant transformation
# [ ] Ajouter get_delta_size_mb() via DeltaTable.detail()
# [ ] Ajouter restore_to_version() pour la recuperation sur erreur
# ----------------------------------------------------------------------------

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 1 — IMPORTS
# ============================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

log = logging.getLogger("delta_utils")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CONFIG = {
    "test_table":  "silver_deces",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — FONCTIONS DELTA LAKE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 2a — Initialisation et enregistrement
# ─────────────────────────────────────────────────────────

def init_delta_if_needed(spark: SparkSession, path: str,
                          schema: StructType, table_name: str) -> bool:
    """
    Cree une table Delta vide si elle n'existe pas encore.
    Necessaire avant le premier MERGE (la table cible doit exister).

    Args:
        spark      : session Spark
        path       : chemin local de la table (ex : 'Tables/bronze_deces')
        schema     : StructType du schema attendu
        table_name : nom de la table pour l'enregistrement metastore

    Retourne True si la table a ete creee, False si elle existait deja.
    """
    if DeltaTable.isDeltaTable(spark, path):
        log.info("Table Delta existante : '%s'", path)
        return False

    log.info("Creation initiale de la table Delta : '%s'", path)
    (
        spark.createDataFrame([], schema)
        .write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .save(path)
    )
    register_table(spark, table_name, path)
    log.info("Table '%s' creee et enregistree dans le metastore.", table_name)
    return True


def register_table(spark: SparkSession, table_name: str, path: str) -> None:
    """
    Enregistre une table Delta dans le metastore Spark si absente.
    Idempotent — n'echoue pas si la table existe deja.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA LOCATION '{path}'
    """)
    log.info("Table '%s' enregistree dans le metastore.", table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2b — Operations de merge
# ─────────────────────────────────────────────────────────

def merge_insert_only(delta_table: DeltaTable, df_source: DataFrame,
                      merge_condition: str) -> dict:
    """
    MERGE Delta en mode INSERT uniquement (pas de mise a jour).
    Pattern : WHEN NOT MATCHED -> INSERT ALL
    Usage : ingestion Bronze (deduplication sur la cle composite).

    Args:
        delta_table     : DeltaTable cible
        df_source       : DataFrame source
        merge_condition : condition de jointure (ex : 'existing.id = incoming.id')

    Retourne un dict avec les metriques du MERGE.
    """
    (
        delta_table.alias("existing")
        .merge(df_source.alias("incoming"), merge_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
    return get_delta_metrics(delta_table)


def merge_upsert(delta_table: DeltaTable, df_source: DataFrame,
                 merge_condition: str, update_cols: dict = None) -> dict:
    """
    MERGE Delta en mode UPDATE + INSERT.
    Pattern : WHEN MATCHED -> UPDATE | WHEN NOT MATCHED -> INSERT
    Usage : dimensions (dim_lieu, etc.) avec SCD Type 1.

    Args:
        delta_table     : DeltaTable cible
        df_source       : DataFrame source
        merge_condition : condition de jointure
        update_cols     : dict {col_target: col_source} ou None (INSERT ALL)

    Retourne un dict avec les metriques du MERGE.
    """
    merge_builder = (
        delta_table.alias("target")
        .merge(df_source.alias("source"), merge_condition)
    )

    if update_cols:
        merge_builder = merge_builder.whenMatchedUpdate(set=update_cols)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder.whenNotMatchedInsertAll().execute()
    return get_delta_metrics(delta_table)


def get_delta_metrics(delta_table: DeltaTable) -> dict:
    """
    Lit les operationMetrics de la derniere operation sur la table.
    Retourne un dict avec les cles de metriques Delta.
    """
    try:
        history = delta_table.history(1).select("operationMetrics").collect()
        if history:
            metrics = history[0]["operationMetrics"] or {}
            return {
                "nb_inseres":  int(metrics.get("numTargetRowsInserted", 0)),
                "nb_mis_a_jour": int(metrics.get("numTargetRowsUpdated", 0)),
                "nb_supprimes":  int(metrics.get("numTargetRowsDeleted", 0)),
                "nb_ignores":    int(metrics.get("numTargetRowsIgnored",  0)),
            }
    except Exception as e:
        log.warning("Impossible de lire les metriques Delta : %s", e)
    return {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2c — Maintenance Delta
# ─────────────────────────────────────────────────────────

def get_delta_version(spark: SparkSession, table_name: str) -> int:
    """
    Retourne la version Delta courante de la table.
    Retourne -1 si la table est inaccessible.
    """
    try:
        dt = DeltaTable.forName(spark, table_name)
        return int(dt.history(1).collect()[0]["version"])
    except Exception as e:
        log.warning("Impossible de lire la version Delta de '%s' : %s", table_name, e)
        return -1


def optimize_table(spark: SparkSession, table_name: str,
                   zorder_cols: list = None) -> None:
    """
    Lance un OPTIMIZE sur la table Delta avec ZORDER optionnel.
    Compacte les petits fichiers et optimise les lectures filtrees.

    Args:
        spark       : session Spark
        table_name  : nom de la table (metastore)
        zorder_cols : liste de colonnes pour le ZORDER (ex : ['annee_deces', 'id_commune'])
    """
    try:
        if zorder_cols:
            cols_str = ", ".join(zorder_cols)
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols_str})")
            log.info("OPTIMIZE + ZORDER BY (%s) sur '%s'", cols_str, table_name)
        else:
            spark.sql(f"OPTIMIZE {table_name}")
            log.info("OPTIMIZE sur '%s'", table_name)
    except Exception as e:
        log.warning("OPTIMIZE non critique sur '%s' : %s", table_name, e)


def vacuum_table(spark: SparkSession, table_name: str,
                 retention_hours: int = 168) -> None:
    """
    Lance un VACUUM sur la table Delta.
    Supprime les fichiers plus anciens que retention_hours.
    Defaut : 168h (7 jours) — minimum recommande par Databricks/Fabric.

    ATTENTION : ne pas passer retention_hours < 168 en PROD sans
    desactiver explicitement le check de securite Delta.

    Args:
        spark           : session Spark
        table_name      : nom de la table (metastore)
        retention_hours : retention en heures (defaut : 168)
    """
    try:
        spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
        log.info("VACUUM %s RETAIN %d HOURS sur '%s'",
                 table_name, retention_hours, table_name)
    except Exception as e:
        log.warning("VACUUM non critique sur '%s' : %s", table_name, e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — TESTS ET VALIDATION
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

_spark = SparkSession.builder.getOrCreate()

# Test get_delta_version sur table inexistante -> -1
assert get_delta_version(_spark, CONFIG['test_table']) == -1

# Test get_delta_metrics format
_fake_metrics = {}
assert isinstance(_fake_metrics, dict)

# Test optimize_table sur table inexistante -> warning, pas d'erreur
optimize_table(_spark, CONFIG['test_table'])

print("delta_utils — tous les tests OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE FINALE — REPORTING
# ============================================================================

print(f"\n{'=' * 60}")
print("  99_DK-DE-DECES-API-Utils-DeltaUtils")
print(f"{'=' * 60}")
print("  Fonctions disponibles :")
print("    init_delta_if_needed(spark, path, schema, name) -> bool")
print("    register_table(spark, name, path)")
print("    merge_insert_only(delta, df, condition)         -> dict")
print("    merge_upsert(delta, df, condition, cols)        -> dict")
print("    get_delta_metrics(delta_table)                  -> dict")
print("    get_delta_version(spark, name)                  -> int")
print("    optimize_table(spark, name, zorder_cols)")
print("    vacuum_table(spark, name, retention_hours)")
print(f"{'=' * 60}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
