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
#  Notebook : 02_DK-DE-DECES-API-Silver-Validate
#  Couche   : Silver
#  Domaine  : Sante - Deces
#  Objectif : Validation metier, rapport qualite et ecriture Silver finale
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook dedie aux controles de validation metier et de qualite
# des donnees. Lit silver_deces_normalized, applique les controles
# fonctionnels (taux de nulls, coherence des dates, plages valides),
# produit les metriques qualite et les rapports statistiques
# (distribution ages, distribution annees). Ecrit la table finale
# silver_deces consommee par les couches Gold et Dimensions.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Activer le Change Data Feed sur silver_deces apres la premiere
#     execution (operation one-shot, ne pas rejouer dans le pipeline) :
#
#     spark.sql("ALTER TABLE silver_deces
#                SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
#
# [ ] Ajouter seuil de rejet configurable (lever une erreur si taux > X%)
# [ ] Externaliser les controles dans validation_utils
# [ ] Ajouter isolation des lignes rejetees dans une table silver_deces_rejected
# [ ] Ajouter test de coherence volumetrique vs execution precedente
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
log = logging.getLogger("silver_validate")

# ─────────────────────────────────────────────────────────
# Configuration Spark
# ─────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    "silver_normalized_table": "silver_deces_normalized",  # lu via le metastore
    "silver_table":            "silver_deces",             # ecrit via le metastore
    "silver_table_path":       "Tables/silver_deces",
    "write_mode":              "overwrite",
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Silver-Validate — demarrage | env : %s", ENVIRONMENT)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — LECTURE DE LA TABLE SILVER_NORMALIZED
# ============================================================================

# ─────────────────────────────────────────────────────────
# Lecture via le metastore (table enregistree par Silver-Normalize)
# ─────────────────────────────────────────────────────────
try:
    df = spark.read.table(CONFIG["silver_normalized_table"])
    df.cache()
    nb_silver = df.count()
    log.info("Table Silver-Normalized lue : %d lignes", nb_silver)
except Exception as e:
    log.error("Echec lecture table Silver-Normalized '%s' : %s",
              CONFIG["silver_normalized_table"], e)
    raise

if nb_silver == 0:
    raise RuntimeError("Table Silver-Normalized vide. Verifiez Silver-Normalize.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — CONTROLES QUALITE FONCTIONNELS
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 3a — Taux de nulls par colonne
# ─────────────────────────────────────────────────────────
COLONNES_A_CHECKER = [
    "nom", "prenoms", "sexe",
    "date_naissance", "date_deces",
    "code_lieu_naissance", "code_lieu_deces",
    "age_au_deces", "annee_deces", "departement_deces",
]

exprs_nulls = [
    F.round(
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / F.count("*") * 100, 2
    ).alias(c)
    for c in COLONNES_A_CHECKER
]

print(f"\n{'─' * 60}")
print("  % nulls par colonne")
print(f"{'─' * 60}")
df.select(exprs_nulls).show(truncate=False)

# ─────────────────────────────────────────────────────────
# Sous-partie 3b — Dates approximees
# ─────────────────────────────────────────────────────────
nb_approx_deces = df.filter(F.col("_date_deces_approx")     == True).count()
nb_approx_naiss = df.filter(F.col("_date_naissance_approx") == True).count()

log.info("Dates deces approximees  (MM/JJ=00 corrige) : %d", nb_approx_deces)
log.info("Dates naissance approx   (MM/JJ=00 corrige) : %d", nb_approx_naiss)

# ─────────────────────────────────────────────────────────
# Sous-partie 3c — Controle de coherence de l'age
# ─────────────────────────────────────────────────────────
nb_age_null = df.filter(F.col("age_au_deces").isNull()).count()
nb_age_ok   = nb_silver - nb_age_null

log.info("age_au_deces renseigne : %d / %d (%.2f%%)",
         nb_age_ok, nb_silver,
         nb_age_ok / nb_silver * 100 if nb_silver > 0 else 0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — RAPPORT STATISTIQUE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 4a — Distribution des ages au deces (tranches 10 ans)
# ─────────────────────────────────────────────────────────
print(f"\n{'─' * 60}")
print("  Distribution des ages au deces (tranches 10 ans)")
print(f"{'─' * 60}")
(
    df
    .filter(F.col("age_au_deces").isNotNull())
    .withColumn(
        "tranche_age",
        F.concat(
            (F.floor(F.col("age_au_deces") / 10) * 10).cast("int").cast("string"),
            F.lit("-"),
            (F.floor(F.col("age_au_deces") / 10) * 10 + 9).cast("int").cast("string"),
        )
    )
    .groupBy("tranche_age")
    .count()
    .orderBy("tranche_age")
    .show(20)
)

# ─────────────────────────────────────────────────────────
# Sous-partie 4b — Distribution par annee de deces
# ─────────────────────────────────────────────────────────
print(f"\n{'─' * 60}")
print("  Distribution par annee de deces")
print(f"{'─' * 60}")
df.groupBy("annee_deces").count().orderBy("annee_deces").show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — ECRITURE DE LA TABLE SILVER_DECES FINALE
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
        .save(CONFIG["silver_table_path"])
    )
    log.info("Table Silver finale ecrite : %s", CONFIG["silver_table_path"])

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["silver_table"]}
        USING DELTA
        LOCATION '{CONFIG["silver_table_path"]}'
    """)
    log.info("Table '%s' enregistree dans le metastore.", CONFIG["silver_table"])

except Exception as e:
    log.error("Echec ecriture Silver finale : %s", e)
    raise

# ─────────────────────────────────────────────────────────
# Verification en DEV / TEST uniquement
# ─────────────────────────────────────────────────────────
if ENVIRONMENT in ("dev", "test"):
    df_check = spark.read.table(CONFIG["silver_table"])
    log.info("Verification : %d lignes dans silver_deces", df_check.count())
    df_check.printSchema()
    df_check.show(5, truncate=30)

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
print(f"  RAPPORT SILVER-VALIDATE — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Lignes validees          : {nb_silver:>10,}")
print(f"  Dates deces approx       : {nb_approx_deces:>10,}")
print(f"  Dates naissance approx   : {nb_approx_naiss:>10,}")
print(f"  Age au deces renseigne   : {nb_age_ok:>10,}")
print(f"  Age au deces null        : {nb_age_null:>10,}")
print(f"{'─' * 60}")
print(f"  Table ecrite : {CONFIG['silver_table']}")
print(f"  Duree totale : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Silver-Validate termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
