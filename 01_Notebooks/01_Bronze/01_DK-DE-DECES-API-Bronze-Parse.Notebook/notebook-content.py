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
#  Notebook : 01_DK-DE-DECES-API-Bronze-Parse
#  Couche   : Bronze
#  Domaine  : Sante - Deces
#  Objectif : Parsing positionnel des fichiers TXT INSEE vers DataFrame Spark
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook charge de parser les fichiers bruts positionnels INSEE afin
# de produire une structure tabulaire exploitable. Lit le manifeste de
# telechargement (_download.json), applique le parsing selon le schema
# positionnel officiel INSEE (176 caracteres), ecrit les donnees parsees
# dans la table de staging Delta (bronze_deces_staging) et produit
# les statistiques de parsing (_parse_stats.json) pour Bronze-Write.
# Le parsing reste fide le a la source : tout est StringType.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter detection et log des encodages non latin-1
# [ ] Ajouter controle de la longueur maximale des lignes (> 176 chars)
# [ ] Ajouter partitionnement de la table de staging par annee
# [ ] Ajouter metriques de parsing par champ (taux de vide, anomalies)
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
import re
from datetime import datetime, timezone
from functools import reduce
from pyspark.sql import Row, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F


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
log = logging.getLogger("bronze_parse")

# ─────────────────────────────────────────────────────────
# Configuration Spark
# ─────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.hadoop.textinputformat.record.delimiter", "\n")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    "path_download":           "/lakehouse/default/Files/bronze/deces/raw_txt/_download.json",
    "path_parse_stats":        "/lakehouse/default/Files/bronze/deces/raw_txt/_parse_stats.json",
    "min_line_length":         162,
    "local_delta_staging":     "Tables/bronze_deces_staging",
    "abfss_base":              "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/592237df-630e-4111-ad48-dc45b1a5a5e0",
}


# ─────────────────────────────────────────────────────────
# Schema positionnel officiel INSEE (longueur totale = 176 chars)
# ─────────────────────────────────────────────────────────
POSITIONS = {
    "nom_prenoms":          (0,   80),
    "sexe":                 (80,  81),
    "date_naissance":       (81,  89),
    "code_lieu_naissance":  (89,  94),
    "commune_naissance":    (94,  124),
    "pays_naissance":       (124, 154),
    "date_deces":           (154, 162),
    "code_lieu_deces":      (162, 167),
    "num_acte_deces":       (167, 176),
}

# ─────────────────────────────────────────────────────────
# Schema Spark Bronze — tout StringType (casts faits en Silver)
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

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Bronze-Parse — demarrage | env : %s", ENVIRONMENT)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — LECTURE DU MANIFESTE DE TELECHARGEMENT
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction de lecture JSON
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

# ─────────────────────────────────────────────────────────
# Lecture et validation 
# ─────────────────────────────────────────────────────────
try:
    download_manifest     = _read_json_from_onelake(CONFIG["path_download"])
    fichiers_telecharges  = download_manifest["fichiers"]
    fichiers_a_parser     = [
        f for f in fichiers_telecharges
        if f.get("action") in ("downloaded", "skipped")
    ]
    log.info("Manifeste de telechargement lu : %d fichier(s) a parser", len(fichiers_a_parser))
except Exception as e:
    log.error("Echec lecture manifeste de telechargement : %s", e)
    raise

if not fichiers_a_parser:
    raise RuntimeError("Aucun fichier disponible pour le parsing. Verifiez Bronze-Download.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — RESOLUTION DU CHEMIN ABFSS
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonctions de resolution ABFSS
# ─────────────────────────────────────────────────────────

def _get_abfss_base() -> str:
    """Retourne l'ABFSS base depuis CONFIG si disponible, sinon via notebookutils."""
    if CONFIG.get("abfss_base"):
        log.info("ABFSS base depuis CONFIG : %s", CONFIG["abfss_base"])
        return CONFIG["abfss_base"]

    # Fallback dynamique si CONFIG non renseigne
    lh = notebookutils.lakehouse.get()
    try:
        return lh["properties"]["abfsPath"]
    except Exception:
        return lh.properties.abfsPath

# ─────────────────────────────────────────────────────────
# Resolution — une seule fois pour tous les fichiers
# ─────────────────────────────────────────────────────────
try:
    abfss_base = _get_abfss_base()
except Exception as e:
    log.error("Echec resolution ABFSS : %s", e)
    raise

# ─────────────────────────────────────────────────────────
# Fonction de conversion du chemin local en chemin abfss
# ─────────────────────────────────────────────────────────

def _local_to_abfss(local_path: str, abfss_base: str) -> str:
    partie_a_conserver = local_path.removeprefix("/lakehouse/default")
    if local_path is None:
        raise ValueError( "local_path ne peut pas être None")
    
    if not local_path.startswith("/lakehouse/default"):
        raise ValueError(f" local_path inatendu: {local_path}")
    
    return abfss_base.rstrip("/") + partie_a_conserver

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — PARSING POSITIONNEL
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction de parsing d'une ligne INSEE
# ─────────────────────────────────────────────────────────
def _parse_line(line: str, source_fichier: str, ingestion_ts: str):
    """
    Parse une ligne du fichier TXT positionnel INSEE.
    Retourne un Row Spark ou None si la ligne est trop courte.
    Les fichiers INSEE sont encodes en latin-1 — Spark gere nativement.
    """
    if len(line) < 162:
        return None

    nom_prenoms_raw = line[0:80].strip()
    if "*" in nom_prenoms_raw:
        parts  = nom_prenoms_raw.split("*", 1)
        nom    = parts[0].strip()
        prenoms = parts[1].strip() if len(parts) > 1 else ""
    else:
        nom     = nom_prenoms_raw
        prenoms = ""

    return Row(
        nom_prenoms=nom_prenoms_raw,
        nom=nom,
        prenoms=prenoms,
        sexe=line[80:81].strip(),
        date_naissance=line[81:89].strip(),
        code_lieu_naissance=line[89:94].strip(),
        commune_naissance=line[94:124].strip(),
        pays_naissance=line[124:154].strip(),
        date_deces=line[154:162].strip(),
        code_lieu_deces=line[162:167].strip(),
        num_acte_deces=line[167:176].strip() if len(line) >= 176 else "",
        _source_fichier=source_fichier,
        _ingestion_ts=ingestion_ts,
    )

# ─────────────────────────────────────────────────────────
# Boucle de parsing par fichier
# ─────────────────────────────────────────────────────────
ingestion_ts    = datetime.now(timezone.utc).isoformat()
dfs_par_fichier = []
stats_parsing   = []
min_len         = CONFIG["min_line_length"]

for fic in fichiers_a_parser:
    local_path  = fic["local_path"]
    source_nom  = fic["titre"]
    spark_path  = _local_to_abfss(local_path, abfss_base)

    log.info("Parsing : %s", source_nom)
    log.info("  Chemin ABFSS : %s", spark_path)

    try:
        rdd_raw = spark.sparkContext.textFile(spark_path)

        source_bc = spark.sparkContext.broadcast(source_nom)
        ts_bc     = spark.sparkContext.broadcast(ingestion_ts)

        rdd_rows = (
            rdd_raw
            .filter(lambda l: len(l) >= min_len)
            .map(lambda l: _parse_line(l, source_bc.value, ts_bc.value))
            .filter(lambda r: r is not None)
        )

        df = spark.createDataFrame(rdd_rows, schema=BRONZE_SCHEMA)
        df.cache()
        nb_parsed = df.count()

        log.info("  %s — parsees : %d", source_nom, nb_parsed)

        stats_parsing.append({
            "fichier":        source_nom,
            "annee":          fic.get("annee"),
            "lignes_parsees": nb_parsed,
        })
        dfs_par_fichier.append(df)

    except Exception as e:
        log.error("Erreur parsing %s : %s", source_nom, e)
        raise

if not dfs_par_fichier:
    raise RuntimeError("Aucun DataFrame produit — verifiez les fichiers sources.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — UNION DES DATAFRAMES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Union de tous les fichiers en un DataFrame unique
# ─────────────────────────────────────────────────────────
df_bronze = reduce(DataFrame.unionByName, dfs_par_fichier)
for df in dfs_par_fichier:
    df.unpersist()

total_lignes = sum(s["lignes_parsees"] for s in stats_parsing)
log.info("Union OK — total lignes : %d", total_lignes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — ECRITURE EN TABLE DE STAGING
# ============================================================================

# ─────────────────────────────────────────────────────────
# Ecriture en mode overwrite (idempotence)
# ─────────────────────────────────────────────────────────
STAGING_PATH = CONFIG["local_delta_staging"]

try:
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(STAGING_PATH)
    )
    log.info("Table de staging ecrite : %s (%d lignes)", STAGING_PATH, total_lignes)

    if not spark.catalog.tableExists("bronze_deces_staging"):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze_deces_staging
            USING DELTA
            LOCATION '{STAGING_PATH}'
        """)
        log.info("Table bronze_deces_staging enregistree dans le metastore.")

except Exception as e:
    log.error("Echec ecriture table de staging : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 7 — ECRITURE DES STATISTIQUES DE PARSING
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction d'ecriture JSON
# ─────────────────────────────────────────────────────────
def _fuse_to_nu(path: str) -> str:
    """Convertit un chemin FUSE absolu en chemin notebookutils relatif."""
    return re.sub(r"^/lakehouse/default/", "", path)

def _write_json_to_onelake(path: str, data: dict) -> None:
    """
    Ecrit un dictionnaire en JSON dans OneLake.
    Utilise notebookutils si disponible, sinon open() via le montage FUSE.
    """
    content = json.dumps(data, ensure_ascii=False, indent=2)
    try:
        notebookutils.fs.put(_fuse_to_nu(path), content, overwrite=True)
        log.info("Fichier ecrit via notebookutils : %s", path)
    except Exception:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("Fichier ecrit via open() : %s", path)

# ─────────────────────────────────────────────────────────
# Construction et ecriture des statistiques
# ─────────────────────────────────────────────────────────
parse_stats = {
    "parse_timestamp": ingestion_ts,
    "environment":     ENVIRONMENT,
    "total_lignes":    total_lignes,
    "nb_fichiers":     len(stats_parsing),
    "staging_path":    STAGING_PATH,
    "fichiers":        stats_parsing,
}

try:
    _write_json_to_onelake(CONFIG["path_parse_stats"], parse_stats)
    log.info("Statistiques de parsing ecrites : %s", CONFIG["path_parse_stats"])
except Exception as e:
    log.error("Echec ecriture statistiques de parsing : %s", e)
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
print(f"  RAPPORT BRONZE-PARSE — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
for s in stats_parsing:
    print(f"  {s['fichier']:<30}  parsees : {s['lignes_parsees']:>8,}")
print(f"{'─' * 60}")
print(f"  Total lignes parsees  : {total_lignes:>10,}")
print(f"  Table de staging      : {STAGING_PATH}")
print(f"  Duree totale          : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Bronze-Parse termine. Duree : %d sec", duree_sec)

# Signal obligatoire pour Fabric Pipeline
notebookutils.notebook.exit(json.dumps({
    "status":       "success",
    "total_lignes": total_lignes,
    "duree_sec":    duree_sec,
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
