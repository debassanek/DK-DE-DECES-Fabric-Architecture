# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7ab22fb0-d88f-4278-957e-e8202da96286",
# META       "default_lakehouse_name": "DK_DE_DECES_API_Source_Dim",
# META       "default_lakehouse_workspace_id": "35193659-8177-497e-ae34-111479e85809",
# META       "known_lakehouses": [
# META         {
# META           "id": "7ab22fb0-d88f-4278-957e-e8202da96286"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
#  Notebook : 04_DK-DE-DECES-API-Dim-Generation
#  Couche   : Dimensions
#  Domaine  : Sante - Deces
#  Objectif : Construction de la dimension generationnelle
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook responsable de la creation de la dimension dim_generation.
# Genere une ligne par annee de naissance (1901-2039) avec le nom
# de la generation sociologique correspondante : Greatest Generation,
# Silent Generation, Baby Boomers, Gen X, Millennials, Gen Z,
# Gen Alpha, Gen Beta. Table de reference statique, regeneree en overwrite.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter une colonne generation_courte (ex : "Boomers", "Milleniaux")
#     pour les visuels Power BI a espace contraint
# [ ] Ajouter une colonne periode_label (ex : "Apres-Guerre", "Guerre Froide")
#     pour le contexte historique
# [ ] Verifier la coherence des bornes avec dim_age (annee_naissance_max)
# [ ] Etendre a 2040+ lors de l'arrivee des nouvelles generations
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
from functools import reduce
from pyspark.sql import SparkSession, functions as F

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
log = logging.getLogger("dim_generation")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()

CONFIG = {
    "base_name":  "DK_DE_DECES_API_Source_Dim",   # Lakehouse cible
    "table_name": "dim_generation",
    "write_mode": "overwrite",
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Dim-Generation — demarrage | env : %s", ENVIRONMENT)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — REGLES GENERATIONNELLES
# ============================================================================
#
# Chaque regle : (annee_debut, annee_fin, nom_generation)
# Source : definitions sociologiques de reference (Strauss & Howe)

GENERATION_RULES = [
    (1901, 1927, "Greatest Generation"),
    (1928, 1945, "Silent Generation"),
    (1946, 1964, "Baby Boomers"),
    (1965, 1980, "Gen X"),
    (1981, 1996, "Millennials"),
    (1997, 2012, "Gen Z"),
    (2013, 2024, "Gen Alpha"),
    (2025, 2039, "Gen Beta"),
]

annee_min = GENERATION_RULES[0][0]   # 1901
annee_max = GENERATION_RULES[-1][1]  # 2039
nb_annees = annee_max - annee_min + 1

log.info("Plage generationnelle : %d -> %d (%d annees)",
         annee_min, annee_max, nb_annees)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — GENERATION DES ANNEES ET MAPPING
# ============================================================================

# ─────────────────────────────────────────────────────────
# Generation de la plage d'annees de naissance
# ─────────────────────────────────────────────────────────
df = (
    spark.range(annee_min, annee_max + 1)
    .withColumnRenamed("id", "annee_naissance")
)

# ─────────────────────────────────────────────────────────
# Mapping annee_naissance -> nom de generation
# ─────────────────────────────────────────────────────────
generation_expr = reduce(
    lambda expr, rule: expr.when(
        F.col("annee_naissance").between(rule[0], rule[1]), rule[2]
    ),
    GENERATION_RULES[1:],
    F.when(
        F.col("annee_naissance").between(*GENERATION_RULES[0][:2]),
        GENERATION_RULES[0][2]
    )
).otherwise("Inconnue")

df = df.withColumn("generation", generation_expr)

nb_lignes = df.count()
log.info("DataFrame dim_generation construit : %d lignes", nb_lignes)

if ENVIRONMENT in ("dev", "test"):
    df.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — ECRITURE DELTA
# ============================================================================

TARGET_TABLE = f"{CONFIG['base_name']}.{CONFIG['table_name']}"

try:
    (
        df.write
        .format("delta")
        .mode(CONFIG["write_mode"])
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    log.info("Table '%s' ecrite — %d lignes.", TARGET_TABLE, nb_lignes)
except Exception as e:
    log.error("Echec ecriture '%s' : %s", TARGET_TABLE, e)
    raise

# ─────────────────────────────────────────────────────────
# Verification
# ─────────────────────────────────────────────────────────
df_check = spark.read.table(TARGET_TABLE)
print(f"\n{'─' * 50}")
print(f"  Table  : {TARGET_TABLE}")
print(f"  Lignes : {df_check.count()} | Colonnes : {len(df_check.columns)}")
print(f"{'─' * 50}")

print("--- Distribution par generation ---")
df_check.groupBy("generation")     .agg(F.count("*").alias("nb_annees"),
         F.min("annee_naissance").alias("annee_debut"),
         F.max("annee_naissance").alias("annee_fin"))     .orderBy("annee_debut")     .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE FINALE — CLEANUP & REPORTING
# ============================================================================

duree_sec = (datetime.now(timezone.utc) - _debut_pipeline).seconds

print(f"\n{'=' * 60}")
print(f"  RAPPORT DIM-GENERATION — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Plage generee    : {annee_min} -> {annee_max} ({nb_lignes} lignes)")
print(f"  Nb generations   : {len(GENERATION_RULES)}")
print(f"  Table ecrite     : {TARGET_TABLE}")
print(f"  Duree totale     : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Dim-Generation termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
