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
#  Notebook : 04_DK-DE-DECES-API-Dim-Date
#  Couche   : Dimensions
#  Domaine  : Sante - Deces
#  Objectif : Construction de la dimension calendaire dim_date
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook responsable de la creation de la dimension calendaire dim_date.
# Genere une ligne par jour de la plage DATE_DEBUT / DATE_FIN via pandas
# puis construit toutes les colonnes analytiques : id_date (yyyyMMdd),
# annee, mois, trimestre, semestre, semaine, saison, jour, jour_semaine,
# weekend, AnMois (yyyyMM) et les libelles pour les slicers Power BI.
# Table de reference statique, regeneree en overwrite.
# Note : la colonne 'annee' est nommee 'annee' avec accent (valide).
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter les jours feries francais (via api.gouv.fr/jours-feries)
# [ ] Ajouter une colonne is_dernier_jour_mois (pour agreagats fin de mois)
# [ ] Passer DATE_FIN a 2040 lors de l'arrivee de la Gen Beta dans le perimetre
# [ ] Ajouter colonne libelle_trimestre (ex : "2024 T1") pour les slicers BI
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
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType, IntegerType, StringType

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
log = logging.getLogger("dim_date")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Europe/Paris")

CONFIG = {
    "date_debut": "2010-01-01",
    "date_fin":   "2030-12-31",
    "base_name":  "DK_DE_DECES_API_Source_Dim",   # Lakehouse cible
    "table_name": "dim_date",
    "write_mode": "overwrite",
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Dim-Date — demarrage | env : %s", ENVIRONMENT)
log.info("Plage : %s -> %s", CONFIG["date_debut"], CONFIG["date_fin"])
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — GENERATION DE LA PLAGE DE DATES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Nettoyage de l'ancienne table si elle existe (compatibilite)
# Operation one-shot — supprimer cette ligne apres la premiere execution
# ─────────────────────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS Dim_Date")
log.info("Nettoyage ancienne table Dim_Date OK (compatibilite)")

# ─────────────────────────────────────────────────────────
# Generation via pandas date_range puis conversion Spark
# ─────────────────────────────────────────────────────────
date_range = pd.date_range(start=CONFIG["date_debut"], end=CONFIG["date_fin"], freq="D")
df_pd = pd.DataFrame({"date_raw": date_range})

df = spark.createDataFrame(df_pd).withColumnRenamed("date_raw", "date")
nb_jours = df.count()

log.info("Plage generee : %d jours (%s -> %s)",
         nb_jours, CONFIG["date_debut"], CONFIG["date_fin"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — CONSTRUCTION DES COLONNES CALENDAIRES
# ============================================================================
#
# Colonnes produites :
#   id_date      : entier yyyyMMdd (cle primaire)
#   date         : DateType
#   année        : annee (accent valide — nom de colonne valide dans Delta)
#   mois         : numero de mois (1-12)
#   mois_libelle : nom du mois (locale JVM)
#   tri_mois     : "01"..."12" pour tri alphanum correct dans Power BI
#   trimestre    : 1-4
#   semestre     : 1-2
#   semaine      : numero de semaine ISO
#   saison       : Hiver / Printemps / Ete / Automne
#   jour         : jour du mois (1-31)
#   jour_semaine : Lundi...Dimanche (locale JVM)
#   weekend      : True si samedi ou dimanche
#   AnMois       : entier yyyyMM (cle de jointure avec les tables Gold)

df_dim = (
    df
    .withColumn("date",         F.col("date").cast(DateType()))
    .withColumn("id_date",      F.date_format("date", "yyyyMMdd").cast(IntegerType()))
    .withColumn("année",        F.year("date"))
    .withColumn("mois",         F.month("date"))
    .withColumn("mois_libelle", F.date_format("date", "MMMM").cast(StringType()))
    .withColumn("tri_mois",     F.lpad(F.month("date").cast(StringType()), 2, "0"))
    .withColumn("trimestre",    F.quarter("date"))
    .withColumn("semestre",     F.when(F.month("date") <= 6, 1).otherwise(2))
    .withColumn("semaine",      F.weekofyear("date"))
    .withColumn("saison",
        F.when(F.month("date").isin(6, 7, 8),   "Ete")
         .when(F.month("date").isin(9, 10, 11), "Automne")
         .when(F.month("date").isin(12, 1, 2),  "Hiver")
         .otherwise("Printemps"))
    .withColumn("jour",         F.dayofmonth("date"))
    .withColumn("jour_semaine", F.date_format("date", "EEEE"))
    .withColumn("weekend",
        F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False))
    .withColumn("AnMois",       F.date_format("date", "yyyyMM").cast(IntegerType()))
    .select(
        "id_date", "date", "année", "mois", "mois_libelle",
        "tri_mois", "trimestre", "semestre", "semaine",
        "saison", "jour", "jour_semaine", "weekend", "AnMois",
    )
)

log.info("Colonnes calendaires construites : %d colonnes", len(df_dim.columns))

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
        df_dim.write
        .format("delta")
        .mode(CONFIG["write_mode"])
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    log.info("Table '%s' ecrite.", TARGET_TABLE)
except Exception as e:
    log.error("Echec ecriture '%s' : %s", TARGET_TABLE, e)
    raise

# ─────────────────────────────────────────────────────────
# Verification
# ─────────────────────────────────────────────────────────
df_check = spark.read.table(TARGET_TABLE)
nb_lignes = df_check.count()

print(f"\n{'─' * 50}")
print(f"  Table  : {TARGET_TABLE}")
print(f"  Lignes : {nb_lignes:,} | Colonnes : {len(df_check.columns)}")
print(f"{'─' * 50}")
df_check.show(5, truncate=False)

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
print(f"  RAPPORT DIM-DATE — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Plage generee    : {CONFIG['date_debut']} -> {CONFIG['date_fin']}")
print(f"  Lignes           : {nb_lignes:,} jours")
print(f"  Colonnes         : {len(df_dim.columns)}")
print(f"  Table ecrite     : {TARGET_TABLE}")
print(f"  Duree totale     : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Dim-Date termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
