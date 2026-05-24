# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "592237df-630e-4111-ad48-dc45b1a5a5e0",
# META       "default_lakehouse_name": "DK_DE_DECES_API_Bronze_Ingest",
# META       "default_lakehouse_workspace_id": "35193659-8177-497e-ae34-111479e85809",
# META       "known_lakehouses": [
# META         {
# META           "id": "592237df-630e-4111-ad48-dc45b1a5a5e0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
#  Notebook : 01_DK-DE-DECES-API-Bronze-Write
#  Couche   : Bronze
#  Domaine  : Sante - Deces
#  Objectif : Ecriture des donnees Bronze dans la table Delta bronze_deces
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook responsable de l'ecriture des donnees Bronze dans la table
# Delta Lake bronze_deces. Lit la table de staging produite par
# Bronze-Parse, gere l'initialisation de la table cible, applique un
# MERGE idempotent (INSERT uniquement si la cle n'existe pas), puis
# ecrit le manifeste final (_manifest.json) consolide avec les
# statistiques des etapes precedentes.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter OPTIMIZE + ZORDER apres le MERGE (performance requetes BI)
# [ ] Ajouter VACUUM apres OPTIMIZE (retention configurable)
# [ ] Ajouter partitionnement de bronze_deces par annee
# [ ] Ajouter monitoring Delta (nombre de fichiers, taille, version)
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
import json
import os
from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType

import notebookutils

def _detect_environment() -> str:
    """Détecte l'environnement Fabric via le nom du workspace."""
    try:
        workspace_name = notebookutils.runtime.context.get("currentWorkspaceName", "")
        if "Dev" in workspace_name:
            return "dev"
        elif "Test" in workspace_name:
            return "test"
        else:
            return "prod"
    except Exception:
        return "dev"

ENVIRONMENT = _detect_environment()

# ─────────────────────────────────────────────────────────
# Configuration du logging
# ─────────────────────────────────────────────────────────
_LOG_LEVEL_PAR_ENV = {
    "dev":  logging.DEBUG,
    "test": logging.INFO,
    "prod": logging.WARNING,
}
logging.basicConfig(
    level=_LOG_LEVEL_PAR_ENV[ENVIRONMENT],
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_write")

# ─────────────────────────────────────────────────────────
# Configuration Spark
# ─────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    "path_discovery":     "/lakehouse/default/Files/bronze/deces/raw_txt/_discovery.json",
    "path_parse_stats":    "/lakehouse/default/Files/bronze/deces/raw_txt/_parse_stats.json",
    "path_manifest":      "/lakehouse/default/Files/bronze/deces/raw_txt/_manifest.json",
    "local_delta_path":    "Tables/bronze_deces",
    "local_delta_staging": "Tables/bronze_deces_staging",
}

# ─────────────────────────────────────────────────────────
# Schema Spark Bronze (reference pour init table vide)
# ─────────────────────────────────────────────────────────
BRONZE_SCHEMA = StructType([
    StructField("nom_prenoms",          StringType(), True),
    StructField("nom",                  StringType(), True),
    StructField("prenoms",              StringType(), True),
    StructField("sexe",                 StringType(), True),
    StructField("date_naissance",       StringType(), True),
    StructField("code_lieu_naissance",  StringType(), True),
    StructField("commune_naissance",    StringType(), True),
    StructField("pays_naissance",       StringType(), True),
    StructField("date_deces",           StringType(), True),
    StructField("code_lieu_deces",      StringType(), True),
    StructField("num_acte_deces",       StringType(), True),
    StructField("_source_fichier",      StringType(), True),
    StructField("_ingestion_ts",        StringType(), True),
])

# ─────────────────────────────────────────────────────────
# Cle de deduplication Bronze
# Identifie un acte de deces de maniere unique dans la source INSEE
# ─────────────────────────────────────────────────────────
CLE_MERGE = [
    "nom",
    "prenoms",
    "date_naissance",
    "date_deces",
    "code_lieu_deces",
]
MERGE_CONDITION = " AND ".join(
    [f"existing.{c} = incoming.{c}" for c in CLE_MERGE]
)

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Bronze-Write — demarrage | env : %s", ENVIRONMENT)
log.info("Condition MERGE : %s", MERGE_CONDITION)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — LECTURE DE LA TABLE DE STAGING
# ============================================================================

# ─────────────────────────────────────────────────────────
# Lecture de la table de staging produite par Bronze-Parse
# ─────────────────────────────────────────────────────────
STAGING_PATH = CONFIG["local_delta_staging"]
DELTA_PATH   = CONFIG["local_delta_path"]
TABLE_NAME   = "bronze_deces"

try:
    df_staging = spark.read.format("delta").load(STAGING_PATH)
    df_staging.cache()
    nb_staging = df_staging.count()
    log.info("Table de staging lue : %d lignes", nb_staging)
except Exception as e:
    log.error("Echec lecture table de staging : %s", e)
    raise

if nb_staging == 0:
    raise RuntimeError("Table de staging vide. Verifiez Bronze-Parse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — INITIALISATION DE LA TABLE DELTA CIBLE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Creation de la table si premiere execution
# ─────────────────────────────────────────────────────────
def _init_delta_table_if_needed(spark, path: str, schema, table_name: str) -> None:
    """
    Cree la table Delta vide si elle n'existe pas encore.
    Necessaire avant le premier MERGE (la table cible doit exister).
    """
    if not DeltaTable.isDeltaTable(spark, path):
        log.info("Table Delta absente — creation initiale vide : %s", path)
        (
            spark.createDataFrame([], schema)
            .write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .save(path)
        )
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{path}'
        """)
        log.info("Table '%s' creee dans le metastore.", table_name)
    else:
        log.info("Table Delta existante trouvee : %s", path)

try:
    _init_delta_table_if_needed(spark, DELTA_PATH, BRONZE_SCHEMA, TABLE_NAME)
except Exception as e:
    log.error("Echec initialisation table Delta : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — MERGE DELTA
# ============================================================================

# ─────────────────────────────────────────────────────────
# Merge : INSERT uniquement si la cle n'existe pas (idempotence)
# WHEN MATCHED    : rien (cle deja presente)
# WHEN NOT MATCHED: insertion de la nouvelle ligne
# ─────────────────────────────────────────────────────────
try:
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)

    (
        delta_table.alias("existing")
        .merge(
            df_staging.alias("incoming"),
            MERGE_CONDITION,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    log.info("MERGE Delta OK — table : %s", DELTA_PATH)

except Exception as e:
    log.error("Echec MERGE Delta : %s", e)
    raise

# ─────────────────────────────────────────────────────────
# Metriques post-MERGE depuis l'historique Delta
# ─────────────────────────────────────────────────────────
history = delta_table.history(1).select("version", "timestamp", "operationMetrics")
metrics = history.collect()[0]["operationMetrics"]

nb_inseres  = int(metrics.get("numTargetRowsInserted", 0))
nb_ignores  = int(metrics.get("numTargetRowsIgnored",  0))
nb_total_dt = spark.read.format("delta").load(DELTA_PATH).count()

log.info(
    "MERGE — inseres : %d | ignores (doublons) : %d | total table : %d",
    nb_inseres, nb_ignores, nb_total_dt,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — ECRITURE DU MANIFESTE FINAL
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonctions utilitaires
# ─────────────────────────────────────────────────────────
def _read_json_from_onelake(path: str) -> dict:
    """Lit un fichier JSON depuis OneLake via le montage FUSE."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Manifeste introuvable : {path}\n"
            "Verifiez que le notebook precedent s'est execute correctement."
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _write_json_to_onelake(path: str, data: dict) -> None:
    """
    Ecrit un dictionnaire en JSON dans OneLake.
    Utilise notebookutils si disponible, sinon open() via le montage FUSE.
    """
    content = json.dumps(data, ensure_ascii=False, indent=2)
    try:
        notebookutils.fs.put(path, content, overwrite=True)
        log.info("Fichier ecrit via notebookutils : %s", path)
    except Exception:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("Fichier ecrit via open() : %s", path)

# ─────────────────────────────────────────────────────────
# Lecture des manifestes des etapes precedentes
# ─────────────────────────────────────────────────────────
try:
    discovery   = _read_json_from_onelake(CONFIG["path_discovery"])
    parse_stats = _read_json_from_onelake(CONFIG["path_parse_stats"])
except Exception as e:
    log.error("Echec lecture manifestes precedents : %s", e)
    raise

# ─────────────────────────────────────────────────────────
# Construction du manifeste consolide
# ─────────────────────────────────────────────────────────
manifest = {
    "run_timestamp":         parse_stats.get("parse_timestamp"),
    "write_timestamp":       datetime.now(timezone.utc).isoformat(),
    "environment":           ENVIRONMENT,
    "dataset_id":            discovery.get("dataset_id"),
    "annees_cibles":         discovery.get("annees_cibles"),
    "total_lignes_staging":  nb_staging,
    "total_lignes_inseres":  nb_inseres,
    "total_lignes_doublons": nb_ignores,
    "total_lignes_table":    nb_total_dt,
    "fichiers":              [],
}

# Enrichissement par fichier avec les stats de parsing
parse_by_fichier = {s["fichier"]: s for s in parse_stats.get("fichiers", [])}

for fic in discovery.get("fichiers", []):
    stats = parse_by_fichier.get(fic["titre"], {})
    manifest["fichiers"].append({
        "titre":            fic["titre"],
        "annee":            fic["annee"],
        "url_source":       fic["url"],
        "date_maj_source":  fic["date_maj"],
        "taille_bytes":     fic.get("taille_bytes", 0),
        "lignes_brutes":    stats.get("lignes_brutes",   0),
        "lignes_parsees":   stats.get("lignes_parsees",  0),
        "lignes_filtrees":  stats.get("lignes_filtrees", 0),
    })

# ─────────────────────────────────────────────────────────
# Ecriture
# ─────────────────────────────────────────────────────────
try:
    _write_json_to_onelake(CONFIG["path_manifest"], manifest)
    log.info("Manifeste final ecrit : %s", CONFIG["path_manifest"])
except Exception as e:
    log.error("Echec ecriture manifeste final : %s", e)
    raise

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
df_staging.unpersist()
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
print(f"  RAPPORT BRONZE-WRITE — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Lignes en staging     : {nb_staging:>10,}")
print(f"  Lignes inserees       : {nb_inseres:>10,}")
print(f"  Doublons ignores      : {nb_ignores:>10,}")
print(f"  Total dans la table   : {nb_total_dt:>10,}")
print(f"{'─' * 60}")
print(f"  Table Delta           : {DELTA_PATH}")
print(f"  Manifeste final       : {CONFIG['path_manifest']}")
print(f"  Duree totale          : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Bronze-Write termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
