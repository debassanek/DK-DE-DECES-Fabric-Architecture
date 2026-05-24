# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ef922306-2cca-4783-b2d3-96e5b020eeb5",
# META       "default_lakehouse_name": "DK_DE_DECES_API_Gold_Build",
# META       "default_lakehouse_workspace_id": "35193659-8177-497e-ae34-111479e85809",
# META       "known_lakehouses": [
# META         {
# META           "id": "ef922306-2cca-4783-b2d3-96e5b020eeb5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
#  Notebook : 03_DK-DE-DECES-API-Gold-Fact
#  Couche   : Gold
#  Domaine  : Sante - Deces
#  Objectif : Construction de la table de faits fact_deces
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook charge de construire la table de faits principale du modele
# decisionnel. Repart de silver_deces (cardinalite 1:1), conserve tous
# les champs Silver et ajoute 4 cles de jointure composites vers les
# tables d'agregation Gold :
#   key_aggAge        -> agg_mortalite_age        (age, id_date_deces)
#   key_aggTemporel   -> agg_mortalite_mensuelle  (AnMois_deces)
#   key_aggGeographie -> agg_mortalite_commune    (id_commune)
#   key_aggGeneration -> agg_mortalite_generation (id_date_naissance, age, id_commune)
# Les cles sont nulles si les colonnes sources necessaires sont absentes.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Activer le Change Data Feed sur fact_deces apres la premiere
#     execution (operation one-shot, ne pas rejouer dans le pipeline) :
#
#     spark.sql("ALTER TABLE fact_deces
#                SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
#
# [ ] Ajouter partitionnement Delta par annee_deces (performance BI)
# [ ] Ajouter cle surrogate sk_deces (entier sequentiel) pour Power BI
# [ ] Ajouter jointure avec les tables de dimensions pour validation
#     referentielle (dim_date, dim_lieu, dim_age, dim_generation)
# [ ] Etudier passage en mode MERGE incremental (vs overwrite)
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
from pyspark.sql.types import IntegerType, StringType

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
log = logging.getLogger("fact_deces")

# ─────────────────────────────────────────────────────────
# Configuration Spark
# ─────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    "silver_table":      ("abfss://35193659-8177-497e-ae34-111479e85809@onelake.dfs.fabric.microsoft.com"
                         "/0359305b-a30c-48e1-be9d-e06c3f6eca4f/Tables/silver_deces"),
    "output_table_path": "Tables/fact_deces",
    "output_table_name": "fact_deces",
    "write_mode":        "overwrite",
}

# ─────────────────────────────────────────────────────────
# Separateur de cles composites
# Evite les collisions entre valeurs de colonnes concatenees
# ─────────────────────────────────────────────────────────
KEY_SEPARATOR = "|"

# ─────────────────────────────────────────────────────────
# Documentation des cles de jointure
# ─────────────────────────────────────────────────────────
# key_aggAge        -> agg_mortalite_age        : (age, id_date_deces)
# key_aggTemporel   -> agg_mortalite_mensuelle  : (AnMois_deces)
# key_aggGeographie -> agg_mortalite_commune    : (id_commune)
# key_aggGeneration -> agg_mortalite_generation : (id_date_naissance, age, id_commune)

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Gold-Fact — demarrage | env : %s", ENVIRONMENT)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — LECTURE SILVER
# ============================================================================

# ─────────────────────────────────────────────────────────
# Lecture integrale via le metastore (tous les champs Silver)
# La table fact_deces est un surensemble de silver_deces
# ─────────────────────────────────────────────────────────
try:
    df_silver = spark.read.format('delta').load(CONFIG["silver_table"])
    df_silver.cache()
    nb_silver = df_silver.count()
    log.info("Lignes Silver charges : %d", nb_silver)
    df_silver.printSchema()
except Exception as e:
    log.error("Echec lecture Silver '%s' : %s", CONFIG["silver_table"], e)
    raise

if nb_silver == 0:
    raise RuntimeError("Table Silver vide. Verifiez Silver-Validate.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — CONSTRUCTION DES CLES DE JOINTURE
# ============================================================================
#
# Chaque cle reproduit EXACTEMENT la combinaison de colonnes utilisee
# comme groupBy dans la table d'agregation cible, pour garantir la jointure.
# Format : colonnes concatenees avec KEY_SEPARATOR ("|")
# Type   : StringType — tolere les nulls partiels via coalesce
# Valeur : null si l'une des composantes necessaires est absente

# ─────────────────────────────────────────────────────────
# Expressions intermediaires recurrentes
# ─────────────────────────────────────────────────────────

# AnMois deces (yyyyMM) — utilise dans key_aggAge et key_aggTemporel
anmois_deces = F.concat(
    F.col("annee_deces").cast(StringType()),
    F.lpad(F.col("mois_deces").cast(StringType()), 2, "0")
)

# AnMois naissance (yyyyMM) — utilise dans key_aggGeneration
anmois_naissance = F.concat(
    F.year("date_naissance").cast(StringType()),
    F.lpad(F.month("date_naissance").cast(StringType()), 2, "0")
)

# id_commune normalise zfill-5 — utilise dans key_aggGeographie et key_aggGeneration
id_commune = F.lpad(F.col("code_lieu_deces"), 5, "0")

# ─────────────────────────────────────────────────────────
# Sous-partie 3a — key_aggAge
# Cible : agg_mortalite_age (age, id_date)
# ─────────────────────────────────────────────────────────
key_agg_age = F.when(
    F.col("age_au_deces").isNotNull()
    & F.col("annee_deces").isNotNull()
    & F.col("mois_deces").isNotNull(),
    F.concat_ws(KEY_SEPARATOR,
        F.col("age_au_deces").cast(StringType()),
        anmois_deces)
).otherwise(F.lit(None).cast(StringType()))

# ─────────────────────────────────────────────────────────
# Sous-partie 3b — key_aggTemporel
# Cible : agg_mortalite_mensuelle (AnMois)
# ─────────────────────────────────────────────────────────
key_agg_temporel = F.when(
    F.col("annee_deces").isNotNull() & F.col("mois_deces").isNotNull(),
    anmois_deces
).otherwise(F.lit(None).cast(StringType()))

# ─────────────────────────────────────────────────────────
# Sous-partie 3c — key_aggGeographie
# Cible : agg_mortalite_commune (id_commune)
# ─────────────────────────────────────────────────────────
key_agg_geographie = F.when(
    F.col("code_lieu_deces").isNotNull() & (F.col("code_lieu_deces") != ""),
    id_commune
).otherwise(F.lit(None).cast(StringType()))

# ─────────────────────────────────────────────────────────
# Sous-partie 3d — key_aggGeneration
# Cible : agg_mortalite_generation (id_date_naissance, age, id_commune)
# ─────────────────────────────────────────────────────────
key_agg_generation = F.when(
    F.col("date_naissance").isNotNull()
    & F.col("age_au_deces").isNotNull()
    & F.col("code_lieu_deces").isNotNull()
    & (F.col("code_lieu_deces") != ""),
    F.concat_ws(KEY_SEPARATOR,
        anmois_naissance,
        F.col("age_au_deces").cast(StringType()),
        id_commune)
).otherwise(F.lit(None).cast(StringType()))

# ─────────────────────────────────────────────────────────
# Application sur le DataFrame
# ─────────────────────────────────────────────────────────
df_fact = (
    df_silver
    .withColumn("key_aggAge",        key_agg_age)
    .withColumn("key_aggTemporel",   key_agg_temporel)
    .withColumn("key_aggGeographie", key_agg_geographie)
    .withColumn("key_aggGeneration", key_agg_generation)
)

log.info("Cles de jointure construites")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — VERIFICATION DE LA COUVERTURE DES CLES
# ============================================================================

nb_total = df_fact.count()

CLES = {
    "key_aggAge":        "agg_mortalite_age",
    "key_aggTemporel":   "agg_mortalite_mensuelle",
    "key_aggGeographie": "agg_mortalite_commune",
    "key_aggGeneration": "agg_mortalite_generation",
}

print(f"\n{'=' * 64}")
print(f"  COUVERTURE DES CLES — {nb_total:,} lignes Silver")
print(f"{'=' * 64}")
for cle, table_cible in CLES.items():
    nb_non_null = df_fact.filter(F.col(cle).isNotNull()).count()
    pct = nb_non_null / nb_total * 100 if nb_total > 0 else 0
    print(f"  {cle:<25} -> {table_cible:<30} {nb_non_null:>10,} ({pct:.2f}%)")
print(f"{'=' * 64}\n")

log.info("Verification couverture des cles OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — SELECTION ET ORDRE DES COLONNES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Colonnes Silver dans leur ordre d'origine + 4 cles en fin
# ─────────────────────────────────────────────────────────
cols_silver = [c for c in df_silver.columns]

cols_cles = [
    "key_aggAge",
    "key_aggTemporel",
    "key_aggGeographie",
    "key_aggGeneration",
]

df_final = df_fact.select(cols_silver + cols_cles)

log.info(
    "Schema final : %d colonnes Silver + %d cles = %d colonnes au total",
    len(cols_silver), len(cols_cles), len(cols_silver) + len(cols_cles)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — ECRITURE DELTA
# ============================================================================

try:
    (
        df_final.write.format("delta")
        .mode(CONFIG["write_mode"])
        .option("overwriteSchema", "true")
        .save(CONFIG["output_table_path"])
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["output_table_name"]}
        USING DELTA LOCATION '{CONFIG["output_table_path"]}'
    """)
    log.info("Table '%s' ecrite et enregistree.", CONFIG["output_table_name"])
except Exception as e:
    log.error("Echec ecriture : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 7 — RAPPORT QUALITE
# ============================================================================

df_check = spark.read.table(CONFIG["output_table_name"])
nb_final  = df_check.count()

print(f"\n{'=' * 64}")
print(f"  TABLE : {CONFIG['output_table_name']}")
print(f"  Lignes : {nb_final:,}  |  Colonnes : {len(df_check.columns)}")
print(f"{'=' * 64}\n")

df_check.printSchema()

# ─────────────────────────────────────────────────────────
# Apercu des cles sur un echantillon
# ─────────────────────────────────────────────────────────
print("--- Apercu (3 lignes) ---")
df_check.select(cols_cles + ["nom", "date_naissance", "date_deces",
                              "age_au_deces", "code_lieu_deces"])         .show(3, truncate=False)

print("--- Exemples de cles (5 lignes avec toutes les cles non nulles) ---")
df_check.filter(
    F.col("key_aggAge").isNotNull()
    & F.col("key_aggTemporel").isNotNull()
    & F.col("key_aggGeographie").isNotNull()
    & F.col("key_aggGeneration").isNotNull()
).select(cols_cles).show(5, truncate=False)

# ─────────────────────────────────────────────────────────
# Controle de cardinalite : fact_deces == silver_deces
# ─────────────────────────────────────────────────────────
print(f"\nfact_deces : {nb_final:,}  |  silver_deces : {nb_silver:,}")
if nb_final != nb_silver:
    log.warning("Ecart de cardinalite : %d lignes", abs(nb_final - nb_silver))
    print(f"ATTENTION : ecart de {abs(nb_final - nb_silver):,} lignes")
else:
    print("Cardinalite identique a Silver.")

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
df_silver.unpersist()
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
print(f"  RAPPORT GOLD-FACT — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Lignes Silver en entree  : {nb_silver:>10,}")
print(f"  Lignes fact_deces ecrites: {nb_final:>10,}")
print(f"  Colonnes Silver          : {len(cols_silver):>10}")
print(f"  Cles de jointure         : {len(cols_cles):>10}")
print(f"  Colonnes totales         : {len(cols_silver) + len(cols_cles):>10}")
print(f"{'─' * 60}")
print(f"  Table ecrite : {CONFIG['output_table_name']}")
print(f"  Duree totale : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Gold-Fact termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
