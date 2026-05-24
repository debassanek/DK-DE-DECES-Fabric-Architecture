# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0359305b-a30c-48e1-be9d-e06c3f6eca4f",
# META       "default_lakehouse_name": "DK_DE_DECES_API_Silver_Clean",
# META       "default_lakehouse_workspace_id": "35193659-8177-497e-ae34-111479e85809",
# META       "known_lakehouses": [
# META         {
# META           "id": "0359305b-a30c-48e1-be9d-e06c3f6eca4f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
#  Notebook : 02_DK-DE-DECES-API-Silver-Clean
#  Couche   : Silver
#  Domaine  : Sante - Deces
#  Objectif : Nettoyage technique des donnees Bronze
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook charge du nettoyage technique des donnees Bronze :
# harmonisation des chaines de caracteres (trim, upper, suppression
# caracteres non conformes), standardisation du champ sexe, zfill des
# codes INSEE, rejet des lignes sans date_deces exploitable et
# suppression des doublons. Produit la table silver_deces_clean
# consommee par Silver-Normalize.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter table de rejets pour audit (lignes date_deces invalides)
# [ ] Ajouter metriques de rejet par fichier source
# [ ] Externaliser les regles de rejet dans validation_utils
# [ ] Ajouter controle de volumetrie minimale apres nettoyage
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

import notebookutils

def _detect_environment() -> str:
    """Detecte l'environnement Fabric via le nom du workspace."""
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
    "dev":  logging.INFO,
    "test": logging.INFO,
    "prod": logging.WARNING,
}
logging.basicConfig(
    level=_LOG_LEVEL_PAR_ENV[ENVIRONMENT],
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("silver_clean")

# ─────────────────────────────────────────────────────────
# Configuration Spark
# ─────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    "bronze_table":       "abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com/592237df-630e-4111-ad48-dc45b1a5a5e0/Tables/bronze_deces",          # lu via le metastore
    "silver_clean_table": "silver_deces_clean",    # ecrit via le metastore
    "silver_clean_path":  "Tables/silver_deces_clean",
    "date_vide_sentinel": "00000000",
    "pad_len_code_lieu":  5,
    "write_mode":         "overwrite",             # idempotent
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Silver-Clean — demarrage | env : %s", ENVIRONMENT)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — LECTURE DE LA TABLE BRONZE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Lecture via le metastore (table enregistree par Bronze-Write)
# ─────────────────────────────────────────────────────────
try:
    df_bronze = spark.read.format('delta').load(CONFIG["bronze_table"])
    df_bronze.cache()
    nb_bronze = df_bronze.count()
    log.info("Table Bronze lue : %d lignes", nb_bronze)
except Exception as e:
    log.error("Echec lecture table Bronze '%s' : %s", CONFIG["bronze_table"], e)
    raise

if nb_bronze == 0:
    raise RuntimeError("Table Bronze vide. Verifiez Bronze-Write.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — NETTOYAGE DES CHAINES DE CARACTERES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 3a — Trim, upper et chaines vides vers null
# ─────────────────────────────────────────────────────────
df = df_bronze

for col_str in ("nom", "prenoms", "commune_naissance", "pays_naissance"):
    df = df.withColumn(col_str, F.upper(F.trim(F.col(col_str))))

for col_str in ("commune_naissance", "pays_naissance", "prenoms"):
    df = df.withColumn(
        col_str,
        F.when(F.col(col_str) == "", None).otherwise(F.col(col_str))
    )

# ─────────────────────────────────────────────────────────
# Sous-partie 3b — Suppression du caractere "/" non conforme
# ─────────────────────────────────────────────────────────
for col_str in ("nom", "prenoms", "commune_naissance", "pays_naissance"):
    df = df.withColumn(col_str, F.regexp_replace(F.col(col_str), "/$", ""))

# ─────────────────────────────────────────────────────────
# Sous-partie 3c — Sexe : "1" -> "M", "2" -> "F", autres -> null
# ─────────────────────────────────────────────────────────
df = df.withColumn(
    "sexe",
    F.when(F.col("sexe") == "1", F.lit("M"))
     .when(F.col("sexe") == "2", F.lit("F"))
     .otherwise(F.lit(None).cast("string"))
)

# ─────────────────────────────────────────────────────────
# Sous-partie 3d — Codes INSEE : zfill 5 caracteres
# ─────────────────────────────────────────────────────────
for col_code in ("code_lieu_naissance", "code_lieu_deces"):
    df = df.withColumn(
        col_code,
        F.when(
            F.col(col_code).isNull() | (F.col(col_code) == ""), None
        ).otherwise(
            F.lpad(F.col(col_code), CONFIG["pad_len_code_lieu"], "0")
        )
    )

log.info("Nettoyage des chaines OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — REJET DES LIGNES INVALIDES ET DEDUPLICATION
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 4a — Rejet des lignes sans date_deces exploitable
# ─────────────────────────────────────────────────────────
nb_avant = nb_bronze

df = df.filter(
    F.col("date_deces").isNotNull()
    & (F.col("date_deces") != "")
    & (F.col("date_deces") != CONFIG["date_vide_sentinel"])
    & (F.length(F.col("date_deces")) == 8)
)
df.cache()
nb_apres_filtre = df.count()

log.info(
    "Rejet date_deces invalide : %d -> %d lignes (%d rejetees, %.2f%%)",
    nb_avant, nb_apres_filtre,
    nb_avant - nb_apres_filtre,
    (nb_avant - nb_apres_filtre) / nb_avant * 100 if nb_avant > 0 else 0,
)

# ─────────────────────────────────────────────────────────
# Sous-partie 4b — Detection et suppression des doublons
# ─────────────────────────────────────────────────────────
df_conct = df.withColumn(
    "cle_doublon",
    F.concat_ws("|", "nom", "prenoms", "date_naissance", "date_deces")
)

if ENVIRONMENT in ("dev", "test"):
    df_doublon = (
        df_conct
        .groupBy("cle_doublon")
        .count()
        .filter(F.col("count") > 1)
    )
    nb_doublons = df_doublon.count()
    log.info("Doublons detectes : %d", nb_doublons)
    if nb_doublons > 0:
        df_doublon.show(20, truncate=False)

df_sans_doublons = df_conct.dropDuplicates(["cle_doublon"])
df = df_sans_doublons.drop("cle_doublon")
df.cache()
nb_apres_dedup = df.count()

log.info(
    "Deduplication : %d -> %d lignes (%d doublons supprimes)",
    nb_apres_filtre, nb_apres_dedup,
    nb_apres_filtre - nb_apres_dedup,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — ECRITURE DE LA TABLE SILVER_CLEAN
# ============================================================================

# ─────────────────────────────────────────────────────────
# Ecriture Delta en mode overwrite (idempotent)
# ─────────────────────────────────────────────────────────
try:
    (
        df.write
        .format("delta")
        .mode(CONFIG["write_mode"])
        .option("overwriteSchema", "true")
        .save(CONFIG["silver_clean_path"])
    )
    log.info("Table Silver-Clean ecrite : %s", CONFIG["silver_clean_path"])

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["silver_clean_table"]}
        USING DELTA
        LOCATION '{CONFIG["silver_clean_path"]}'
    """)
    log.info("Table '%s' enregistree dans le metastore.", CONFIG["silver_clean_table"])

except Exception as e:
    log.error("Echec ecriture Silver-Clean : %s", e)
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
df_bronze.unpersist()
df.unpersist()
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
print(f"  RAPPORT SILVER-CLEAN — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Lignes Bronze en entree       : {nb_bronze:>10,}")
print(f"  Lignes apres filtre date_deces: {nb_apres_filtre:>10,}")
print(f"  Lignes apres deduplication    : {nb_apres_dedup:>10,}")
print(f"  Total rejets                  : {nb_bronze - nb_apres_dedup:>10,}")
taux = (nb_bronze - nb_apres_dedup) / nb_bronze * 100 if nb_bronze > 0 else 0
print(f"  Taux de rejet                 : {taux:>9.2f}%")
print(f"{'─' * 60}")
print(f"  Table ecrite : {CONFIG['silver_clean_table']}")
print(f"  Duree totale : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Silver-Clean termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
