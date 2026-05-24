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
#  Notebook : 02_DK-DE-DECES-API-Silver-Normalize
#  Couche   : Silver
#  Domaine  : Sante - Deces
#  Objectif : Normalisation metier et typage des donnees Silver-Clean
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook responsable de la normalisation metier des donnees :
# typage des colonnes de dates (AAAAMMJJ -> DateType avec gestion
# des mois/jours inconnus a 00), calcul des colonnes derivees
# (age_au_deces, annee_deces, mois_deces, departement_naissance,
# departement_deces), selection et renommage des colonnes pour
# le modele analytique. Produit la table silver_deces_normalized
# consommee par Silver-Validate.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter enrichissement du code commune via referentiel COG INSEE
# [ ] Ajouter colonne generation (tranche de naissance par decennie)
# [ ] Externaliser _build_date_exprs dans parsing_utils
# [ ] Ajouter gestion des departements DOM-TOM 976 (Mayotte)
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
from pyspark.sql.types import DateType, IntegerType

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
log = logging.getLogger("silver_normalize")

# ─────────────────────────────────────────────────────────
# Configuration Spark
# ─────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled",          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    "silver_clean_table":      "silver_deces_clean",       # lu via le metastore
    "silver_normalized_table": "silver_deces_normalized",  # ecrit via le metastore
    "silver_normalized_path":  "Tables/silver_deces_normalized",
    "write_mode":              "overwrite",
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Silver-Normalize — demarrage | env : %s", ENVIRONMENT)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — LECTURE DE LA TABLE SILVER_CLEAN
# ============================================================================

# ─────────────────────────────────────────────────────────
# Lecture via le metastore (table enregistree par Silver-Clean)
# ─────────────────────────────────────────────────────────
try:
    df = spark.read.table(CONFIG["silver_clean_table"])
    df.cache()
    nb_clean = df.count()
    log.info("Table Silver-Clean lue : %d lignes", nb_clean)
except Exception as e:
    log.error("Echec lecture table Silver-Clean '%s' : %s", CONFIG["silver_clean_table"], e)
    raise

if nb_clean == 0:
    raise RuntimeError("Table Silver-Clean vide. Verifiez Silver-Clean.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — TYPAGE DES DATES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction de construction des expressions de typage
# Strategie INSEE : MM=00 ou JJ=00 -> remplace par 01, flag _approx=True
# ─────────────────────────────────────────────────────────
def _build_date_exprs(col_name: str):
    """
    Pour une colonne AAAAMMJJ (StringType) retourne deux expressions :
    - date_parsed  : DateType (null si annee invalide ou format errone)
    - date_approx  : BooleanType (True si MM ou JJ etait 00)
    """
    c     = F.col(col_name)
    annee = F.substring(c, 1, 4)
    mois  = F.substring(c, 5, 2)
    jour  = F.substring(c, 7, 2)

    mois_corr  = F.when(mois == "00", F.lit("01")).otherwise(mois)
    jour_corr  = F.when(jour == "00", F.lit("01")).otherwise(jour)
    flag_approx = (mois == "00") | (jour == "00")

    date_str = F.concat_ws("-", annee, mois_corr, jour_corr)

    date_parsed = F.when(
        (annee == "0000") | c.isNull() | (F.length(c) != 8),
        F.lit(None).cast(DateType())
    ).otherwise(
        F.to_date(date_str, "yyyy-MM-dd")
    )

    return date_parsed, flag_approx

# ─────────────────────────────────────────────────────────
# Application sur date_naissance et date_deces
# ─────────────────────────────────────────────────────────
date_naiss_expr, approx_naiss_expr = _build_date_exprs("date_naissance")
date_deces_expr, approx_deces_expr = _build_date_exprs("date_deces")

df = (
    df
    .withColumn("date_naissance_parsed",  date_naiss_expr)
    .withColumn("_date_naissance_approx", approx_naiss_expr)
    .withColumn("date_deces_parsed",      date_deces_expr)
    .withColumn("_date_deces_approx",     approx_deces_expr)
)
df.cache()

# ─────────────────────────────────────────────────────────
# Verification rapide : % de dates nulles apres conversion
# ─────────────────────────────────────────────────────────
nb_total       = df.count()
nb_deces_null  = df.filter(F.col("date_deces_parsed").isNull()).count()
log.info(
    "date_deces_parsed null : %d / %d (%.2f%%)",
    nb_deces_null, nb_total,
    nb_deces_null / nb_total * 100 if nb_total > 0 else 0,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — COLONNES CALCULEES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 4a — Age au deces
# ─────────────────────────────────────────────────────────
df = (
    df
    .withColumn(
        "age_au_deces",
        F.when(
            F.col("date_naissance_parsed").isNotNull()
            & F.col("date_deces_parsed").isNotNull(),
            F.floor(
                F.datediff(
                    F.col("date_deces_parsed"),
                    F.col("date_naissance_parsed")
                ) / 365.25
            ).cast(IntegerType())
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    # Age negatif ou aberrant (> 145 ans) -> null
    .withColumn(
        "age_au_deces",
        F.when(
            (F.col("age_au_deces") < 0) | (F.col("age_au_deces") > 145),
            F.lit(None).cast(IntegerType())
        ).otherwise(F.col("age_au_deces"))
    )
)

# ─────────────────────────────────────────────────────────
# Sous-partie 4b — Dimensions temporelles du deces
# ─────────────────────────────────────────────────────────
df = (
    df
    .withColumn("annee_deces", F.year(F.col("date_deces_parsed")).cast(IntegerType()))
    .withColumn("mois_deces",  F.month(F.col("date_deces_parsed")).cast(IntegerType()))
)

# ─────────────────────────────────────────────────────────
# Sous-partie 4c — Departements (DOM-TOM sur 3 chars si 97x/98x)
# ─────────────────────────────────────────────────────────
for col_code, col_dept in [
    ("code_lieu_naissance", "departement_naissance"),
    ("code_lieu_deces",     "departement_deces"),
]:
    df = df.withColumn(
        col_dept,
        F.when(F.col(col_code).isNull(), None)
         .when(
             F.substring(F.col(col_code), 1, 2).isin("97", "98"),
             F.substring(F.col(col_code), 1, 3)
         )
         .otherwise(F.substring(F.col(col_code), 1, 2))
    )

log.info("Colonnes calculees OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — SELECTION ET RENOMMAGE DES COLONNES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Selection et ordre final des colonnes Silver
# ─────────────────────────────────────────────────────────
COLONNES_SILVER = [
    # Identite
    "nom", "prenoms", "sexe",
    # Naissance
    "date_naissance_parsed", "code_lieu_naissance",
    "commune_naissance",     "pays_naissance",
    "departement_naissance", "_date_naissance_approx",
    # Deces
    "date_deces_parsed",     "code_lieu_deces",
    "departement_deces",     "num_acte_deces",
    "_date_deces_approx",
    # Metriques calculees
    "age_au_deces",          "annee_deces", "mois_deces",
    # Tracabilite
    "_source_fichier",       "_ingestion_ts",
]

# ─────────────────────────────────────────────────────────
# Renommage pour noms propres dans le modele analytique
# ─────────────────────────────────────────────────────────
df_normalized = (
    df
    .select(COLONNES_SILVER)
    .withColumnRenamed("date_naissance_parsed", "date_naissance")
    .withColumnRenamed("date_deces_parsed",     "date_deces")
)

log.info("Selection et renommage des colonnes OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — ECRITURE DE LA TABLE SILVER_NORMALIZED
# ============================================================================

# ─────────────────────────────────────────────────────────
# Ecriture Delta en mode overwrite (idempotent)
# ─────────────────────────────────────────────────────────
try:
    (
        df_normalized.write
        .format("delta")
        .mode(CONFIG["write_mode"])
        .option("overwriteSchema", "true")
        .save(CONFIG["silver_normalized_path"])
    )
    log.info("Table Silver-Normalized ecrite : %s", CONFIG["silver_normalized_path"])

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["silver_normalized_table"]}
        USING DELTA
        LOCATION '{CONFIG["silver_normalized_path"]}'
    """)
    log.info("Table '%s' enregistree dans le metastore.", CONFIG["silver_normalized_table"])

except Exception as e:
    log.error("Echec ecriture Silver-Normalized : %s", e)
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
print(f"  RAPPORT SILVER-NORMALIZE — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Lignes en entree         : {nb_clean:>10,}")
print(f"  Lignes apres typage/calc : {nb_total:>10,}")
print(f"  date_deces_parsed null   : {nb_deces_null:>10,}")
print(f"{'─' * 60}")
print(f"  Table ecrite : {CONFIG['silver_normalized_table']}")
print(f"  Duree totale : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Silver-Normalize termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
