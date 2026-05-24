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
#  Notebook : 99_DK-DE-DECES-API-Utils-SparkUtils
#  Couche   : Utils
#  Domaine  : Sante - Deces
#  Objectif : Fonctions communes Spark pour le pipeline Data Engineering
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Module utilitaire regroupant les fonctions communes Spark : configuration
# de session, optimisation des DataFrames, lecture standardisee des tables,
# gestion du cache et helpers distribues. Evite la duplication de la
# configuration Spark dans chaque notebook du pipeline.
#
# Fonctions exposees :
#   configure_spark(spark, env)              -> SparkSession
#   safe_read_table(spark, name)             -> DataFrame | None
#   safe_count(df)                           -> int
#   assert_not_empty(df, table_name)
#   unpersist_all(dfs)
#   get_abfss_path(local_path)               -> str
#   local_to_abfss(local_path, abfss_base)   -> str
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter get_spark_session_config(env) pour externaliser les confs
# [ ] Ajouter repartition_for_write(df, nb_rows) calcul auto des partitions
# [ ] Ajouter compare_schemas(df1, df2) pour la validation de schema
# [ ] Ajouter safe_read_delta_path(spark, path) pour les chemins ABFSS
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
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame, functions as F

try:
    import notebookutils
    _HAS_NOTEBOOKUTILS = True
except ImportError:
    _HAS_NOTEBOOKUTILS = False

log = logging.getLogger("spark_utils")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — FONCTIONS DE CONFIGURATION SPARK
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 2a — Configuration de session
# ─────────────────────────────────────────────────────────

# Configurations Spark par environnement
_SPARK_CONF_COMMON = {
    "spark.sql.adaptive.enabled":                "true",
    "spark.sql.adaptive.skewJoin.enabled":       "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}

_SPARK_CONF_DEV = {
    **_SPARK_CONF_COMMON,
    "spark.sql.shuffle.partitions": "8",    # reduit pour les petits volumes DEV
}

_SPARK_CONF_TEST = {
    **_SPARK_CONF_COMMON,
    "spark.sql.shuffle.partitions": "50",
}

_SPARK_CONF_PROD = {
    **_SPARK_CONF_COMMON,
    "spark.sql.shuffle.partitions": "200",
}

_SPARK_CONF_PAR_ENV = {
    "dev":  _SPARK_CONF_DEV,
    "test": _SPARK_CONF_TEST,
    "prod": _SPARK_CONF_PROD,
}


def configure_spark(spark: SparkSession, environment: str = "dev") -> SparkSession:
    """
    Applique la configuration Spark standard pour l'environnement donne.

    Args:
        spark       : session Spark active
        environment : 'dev' / 'test' / 'prod'

    Retourne la session Spark configuree (chainable).
    """
    conf = _SPARK_CONF_PAR_ENV.get(environment, _SPARK_CONF_COMMON)
    for k, v in conf.items():
        spark.conf.set(k, v)
    log.info("Spark configure pour l'environnement '%s' (%d parametres)",
             environment, len(conf))
    return spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2b — Lecture securisee des tables
# ─────────────────────────────────────────────────────────

def safe_read_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Lit une table Delta via le chemin abfss.
    Leve une RuntimeError explicite si la table est inaccessible.

    Preferer cette fonction a spark.read.table() directement pour
    obtenir un message d'erreur contextualisé et loggué.
    """
    try:
        df = spark.read.format('delta').load(table_full_path)
        log.info("Table lue : '%s'", table_name)
        return df
    except Exception as e:
        msg = (
            f"Impossible de lire la table '{table_name}' : {e}\n"
            f"Verifiez que le notebook producteur s'est execute correctement."
        )
        log.error(msg)
        raise RuntimeError(msg) from e


def safe_count(df: DataFrame) -> int:
    """
    Retourne le count d'un DataFrame. Retourne -1 si le DataFrame est None.
    Wrappé pour uniformiser la gestion du cas None dans le pipeline.
    """
    if df is None:
        return -1
    return df.count()


def assert_not_empty(df: DataFrame, table_name: str = "") -> None:
    """
    Leve une RuntimeError si le DataFrame est vide.
    Utiliser apres chaque lecture de table source critique.

    Args:
        df         : DataFrame a verifier
        table_name : nom de la table (pour le message d'erreur)
    """
    nb = df.count()
    if nb == 0:
        raise RuntimeError(
            f"Table '{table_name}' vide — "
            "verifiez que le notebook producteur s'est execute correctement."
        )
    log.info("Validation non-vide : '%s' — %d lignes", table_name, nb)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2c — Gestion du cache
# ─────────────────────────────────────────────────────────

def unpersist_all(dfs: list) -> None:
    """
    Libere le cache Spark pour une liste de DataFrames.
    Accepte les None sans erreur (compatibilite avec les cas partiels).

    Args:
        dfs : liste de DataFrames (ou None) a liberer

    Usage :
        unpersist_all([df_bronze, df_silver, df_gold])
    """
    for df in dfs:
        if df is not None:
            try:
                df.unpersist()
            except Exception:
                pass  # non critique
    log.info("Cache libere pour %d DataFrames", sum(1 for d in dfs if d is not None))


# ─────────────────────────────────────────────────────────
# Sous-partie 2d — Resolution des chemins ABFSS
# ─────────────────────────────────────────────────────────

def get_abfss_base() -> str:
    """
    Retourne le chemin ABFSS racine du Lakehouse attache au notebook.
    Compatible avec les deux formes retournees par notebookutils.

    Leve une RuntimeError si aucun Lakehouse n'est attache.
    """
    if not _HAS_NOTEBOOKUTILS:
        raise RuntimeError(
            "notebookutils non disponible — "
            "verifiez que ce code s'execute bien dans Fabric."
        )
    try:
        lh = notebookutils.lakehouse.get()
        if isinstance(lh, dict):
            return lh["properties"]["abfsPath"]
        return lh.properties.abfsPath
    except (KeyError, TypeError, AttributeError) as e:
        raise RuntimeError(
            f"Cle 'properties.abfsPath' introuvable dans notebookutils.lakehouse.get() : {e}"
        ) from e
    except Exception as e:
        raise RuntimeError(
            f"Impossible de resoudre le chemin ABFSS : {e}\n"
            "Verifiez qu'un Lakehouse est bien attache a ce notebook."
        ) from e


def local_to_abfss(local_path: str, abfss_base: str) -> str:
    """
    Convertit un chemin local FUSE en URI ABFSS utilisable par Spark.

    Args:
        local_path : chemin FUSE (ex : '/lakehouse/default/Files/bronze/...')
        abfss_base : chemin ABFSS racine retourne par get_abfss_base()

    Retourne l'URI ABFSS (ex : 'abfss://...@onelake.../Files/bronze/...')
    """
    relative = local_path.replace("/lakehouse/default", "", 1)
    return abfss_base.rstrip("/") + relative

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

_spark = SparkSession.builder.getOrCreate()

# Test configure_spark
configure_spark(_spark, "dev")
assert _spark.conf.get("spark.sql.adaptive.enabled") == "true"

# Test safe_count avec DataFrame vide
_df_empty = _spark.createDataFrame([], "id: int")
assert safe_count(_df_empty) == 0
assert safe_count(None) == -1

# Test assert_not_empty sur DataFrame non vide
_df_test = _spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
assert_not_empty(_df_test, "test_table")

# Test assert_not_empty sur DataFrame vide -> RuntimeError attendue
try:
    assert_not_empty(_df_empty, "table_vide")
    assert False, "Doit lever RuntimeError"
except RuntimeError:
    pass

# Test unpersist_all avec None
unpersist_all([_df_test, None, _df_empty])

# Test local_to_abfss
_base = "abfss://ws@onelake.dfs.fabric.microsoft.com/lh"
_result = local_to_abfss("/lakehouse/default/Files/bronze/test.json", _base)
assert _result == "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Files/bronze/test.json"

print("spark_utils — tous les tests OK")

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
print("  99_DK-DE-DECES-API-Utils-SparkUtils")
print(f"{'=' * 60}")
print("  Fonctions disponibles :")
print("    configure_spark(spark, env)          -> SparkSession")
print("    safe_read_table(spark, name)          -> DataFrame")
print("    safe_count(df)                        -> int")
print("    assert_not_empty(df, table_name)")
print("    unpersist_all(dfs)")
print("    get_abfss_base()                      -> str")
print("    local_to_abfss(local_path, base)      -> str")
print(f"{'=' * 60}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
