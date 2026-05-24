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
#  Notebook : 04_DK-DE-DECES-API-Dim-Age
#  Couche   : Dimensions
#  Domaine  : Sante - Deces
#  Objectif : Construction de la dimension analytique des ages
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook responsable de la creation et du maintien de la dimension
# dim_age. Genere 146 lignes (ages 0 a 145) enrichies de 4 niveaux
# de categorisation : tranche large, tranche 5 ans, tranche 10 ans,
# categorie metier (Enfant / Adolescent / Adulte / Senior / Grand senior).
# Ajoute les flags majeur_flag, centenaire_flag et ordre_tranche pour
# le tri Power BI. Table de reference statique, regeneree en overwrite.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter une colonne esperance_vie_residuelle par tranche (donnees INSEE)
# [ ] Ajouter un flag retraite_flag (age >= 62 selon legislation actuelle)
# [ ] Externaliser les regles de categorisation dans validation_utils
# [ ] Ajouter un test de non-regression (nb lignes == AGE_MAX + 1)
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
from pyspark.sql.types import IntegerType

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
log = logging.getLogger("dim_age")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()

CONFIG = {
    "base_name":  "DK_DE_DECES_API_Source_Dim",   # Lakehouse cible
    "table_name": "dim_age",
    "age_max":    145,
    "write_mode": "overwrite",
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Dim-Age — demarrage | env : %s", ENVIRONMENT)
log.info("Age max : %d", CONFIG["age_max"])
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — GENERATION DE LA PLAGE D'AGES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Plage : 0 -> AGE_MAX (inclus)
# ─────────────────────────────────────────────────────────
df = (
    spark.range(0, CONFIG["age_max"] + 1)
    .withColumnRenamed("id", "age")
)

log.info("Plage d'ages generee : 0 -> %d (%d lignes)", CONFIG["age_max"], df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — REGLES DE CATEGORISATION
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 3a — Regles par niveau de granularite
# ─────────────────────────────────────────────────────────
TRANCHE_LARGE_RULES = [
    (0,   17,  "0-17 ans"),
    (18,  29,  "18-29 ans"),
    (30,  44,  "30-44 ans"),
    (45,  59,  "45-59 ans"),
    (60,  74,  "60-74 ans"),
    (75,  89,  "75-89 ans"),
    (90,  145, "90 ans et +"),
]

TRANCHE_5_RULES = [
    (0,   4,   "0-4 ans"),   (5,   9,   "5-9 ans"),
    (10,  14,  "10-14 ans"), (15,  19,  "15-19 ans"),
    (20,  24,  "20-24 ans"), (25,  29,  "25-29 ans"),
    (30,  34,  "30-34 ans"), (35,  39,  "35-39 ans"),
    (40,  44,  "40-44 ans"), (45,  49,  "45-49 ans"),
    (50,  54,  "50-54 ans"), (55,  59,  "55-59 ans"),
    (60,  64,  "60-64 ans"), (65,  69,  "65-69 ans"),
    (70,  74,  "70-74 ans"), (75,  79,  "75-79 ans"),
    (80,  84,  "80-84 ans"), (85,  89,  "85-89 ans"),
    (90,  94,  "90-94 ans"), (95,  99,  "95-99 ans"),
    (100, 145, "100 ans et +"),
]

TRANCHE_10_RULES = [
    (0,   9,   "0-9 ans"),   (10,  19,  "10-19 ans"),
    (20,  29,  "20-29 ans"), (30,  39,  "30-39 ans"),
    (40,  49,  "40-49 ans"), (50,  59,  "50-59 ans"),
    (60,  69,  "60-69 ans"), (70,  79,  "70-79 ans"),
    (80,  89,  "80-89 ans"), (90,  99,  "90-99 ans"),
    (100, 145, "100 ans et +"),
]

CATEGORIE_RULES = [
    (0,   11,  "Enfant"),
    (12,  17,  "Adolescent"),
    (18,  64,  "Adulte"),
    (65,  79,  "Senior"),
    (80,  145, "Grand senior"),
]

ORDRE_RULES = [
    (0,   17,  1), (18,  29,  2), (30,  44,  3),
    (45,  59,  4), (60,  74,  5), (75,  89,  6), (90, 145, 7),
]

# ─────────────────────────────────────────────────────────
# Sous-partie 3b — Fonctions utilitaires de construction
# ─────────────────────────────────────────────────────────
def build_expr(rules, col_name="age"):
    """Construit un when().when()...otherwise() depuis une liste de regles (str)."""
    return reduce(
        lambda expr, rule: expr.when(F.col(col_name).between(rule[0], rule[1]), rule[2]),
        rules[1:],
        F.when(F.col(col_name).between(*rules[0][:2]), rules[0][2])
    ).otherwise("Inconnue")

def build_int_expr(rules, col_name="age"):
    """Meme chose pour les regles a valeur entiere (ordre_tranche)."""
    return reduce(
        lambda expr, rule: expr.when(F.col(col_name).between(rule[0], rule[1]), rule[2]),
        rules[1:],
        F.when(F.col(col_name).between(*rules[0][:2]), rules[0][2])
    ).otherwise(99)

log.info("Regles de categorisation et fonctions utilitaires OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — CONSTRUCTION DU DATAFRAME DIM_AGE
# ============================================================================

df = (
    df
    .withColumn("age_key",           F.col("age").cast(IntegerType()))
    .withColumn("tranche_age",        build_expr(TRANCHE_LARGE_RULES))
    .withColumn("tranche_age_5_ans",  build_expr(TRANCHE_5_RULES))
    .withColumn("tranche_age_10_ans", build_expr(TRANCHE_10_RULES))
    .withColumn("categorie_age",      build_expr(CATEGORIE_RULES))
    .withColumn("majeur_flag",        F.col("age") >= 18)
    .withColumn("centenaire_flag",    F.col("age") >= 100)
    .withColumn("ordre_tranche",      build_int_expr(ORDRE_RULES))
    .select(
        "age_key", "age",
        "tranche_age", "tranche_age_5_ans", "tranche_age_10_ans",
        "categorie_age", "majeur_flag", "centenaire_flag", "ordre_tranche",
    )
)

nb_lignes = df.count()
log.info("DataFrame dim_age construit : %d lignes | %d colonnes",
         nb_lignes, len(df.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — ECRITURE DELTA
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
print(f"  Table : {TARGET_TABLE}")
print(f"  Lignes : {df_check.count()} | Colonnes : {len(df_check.columns)}")
print(f"{'─' * 50}")
df_check.show(10, truncate=False)

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
print(f"  RAPPORT DIM-AGE — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Ages generes     : 0 -> {CONFIG['age_max']} ({nb_lignes} lignes)")
print(f"  Colonnes         : {len(df.columns)}")
print(f"  Table ecrite     : {TARGET_TABLE}")
print(f"  Duree totale     : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Dim-Age termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
