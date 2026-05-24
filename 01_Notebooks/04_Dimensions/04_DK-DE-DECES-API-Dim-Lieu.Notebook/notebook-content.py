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
#  Notebook : 04_DK-DE-DECES-API-Dim-Lieu
#  Couche   : Dimensions
#  Domaine  : Sante - Deces
#  Objectif : Construction de la dimension geographique des communes
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook responsable de la creation et du maintien de la dimension
# dim_lieu. Assemble les donnees geographiques depuis deux sources :
#   1. geo.api.gouv.fr (IGN / Etalab) : communes, departements, regions,
#      coordonnees, superficie, EPCI, type de commune
#   2. INSEE Melodie API : population municipale officielle
#      (annee la plus recente sur les 10 dernieres annees)
# Applique un MERGE Delta a chaque execution : UPDATE si les donnees
# changent, INSERT pour les nouvelles communes. Idempotent.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter le code postal (via data.gouv.fr/datasets/codes-postaux)
# [ ] Ajouter le zonage rural/urbain (ZRR, QPV) depuis data.gouv.fr
# [ ] Ajouter gestion des communes fusionnees (nouvelles communes 2016+)
# [ ] Activer le DELETE dans le MERGE pour les communes supprimees
#     (decommenter la ligne .whenNotMatchedBySourceDelete() dans la fonction)
# [ ] Externaliser fetch_json dans un module utils reseau
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
import warnings
import pandas as pd
import requests
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

warnings.filterwarnings("ignore")

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
log = logging.getLogger("dim_lieu")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()

_ANNEE_FIN = datetime.now().year

CONFIG = {
    "base_name":          "DK_DE_DECES_API_Source_Dim",   # Lakehouse cible
    "table_name":         "dim_lieu",
    "target_path":        "/lakehouse/default/Tables/dim_lieu",
    "annee_debut_pop":    _ANNEE_FIN - 10,
    "annee_fin_pop":      _ANNEE_FIN,
    "api_communes_url":   (
        "https://geo.api.gouv.fr/communes"
        "?fields=nom,code,codeDepartement,codeRegion,"
        "centre,surface,population,type,epci"
        "&format=json&geometry=centre"
    ),
    "api_depts_url":      "https://geo.api.gouv.fr/departements?fields=nom,code&format=json",
    "api_regions_url":    "https://geo.api.gouv.fr/regions?fields=nom,code&format=json",
    "api_insee_url":      "https://api.insee.fr/melodi/data/DS_POPULATIONS_HISTORIQUES?GEO=COM",
    "http_timeout_sec":   180,
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Dim-Lieu — demarrage | env : %s | pop annees : %d->%d",
         ENVIRONMENT, CONFIG["annee_debut_pop"], CONFIG["annee_fin_pop"])
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — COLLECTE DES COMMUNES (geo.api.gouv.fr)
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction utilitaire de fetch HTTP
# ─────────────────────────────────────────────────────────
def fetch_json(url: str, label: str, timeout: int = 120) -> list:
    """Appel GET avec raise_for_status. Retourne la reponse JSON parsee."""
    log.info("  Appel API : %s", label)
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ─────────────────────────────────────────────────────────
# Collecte et normalisation des communes
# ─────────────────────────────────────────────────────────
try:
    communes_raw = fetch_json(CONFIG["api_communes_url"], "communes", CONFIG["http_timeout_sec"])
except Exception as e:
    log.error("Echec collecte communes : %s", e)
    raise

rows_communes = []
for c in communes_raw:
    coords = c.get("centre", {}).get("coordinates", [None, None])
    epci   = c.get("epci") or {}
    rows_communes.append({
        "id_commune":         c.get("code"),
        "commune":            c.get("nom"),
        "code_dept":          c.get("codeDepartement"),
        "code_region":        c.get("codeRegion"),
        "longitude":          float(coords[0]) if coords[0] is not None else None,
        "latitude":           float(coords[1]) if coords[1] is not None else None,
        "superficie_commune": float(c["surface"])     if c.get("surface")    else None,
        "population_geo":     int(c["population"])    if c.get("population") else None,
        "type_commune":       c.get("type"),
        "epci":               epci.get("nom"),
    })

df_communes = pd.DataFrame(rows_communes)
log.info("Communes chargees : %d", len(df_communes))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — COLLECTE DES DEPARTEMENTS ET REGIONS
# ============================================================================

# ─────────────────────────────────────────────────────────
# Departements
# ─────────────────────────────────────────────────────────
try:
    depts_raw = fetch_json(CONFIG["api_depts_url"], "departements", CONFIG["http_timeout_sec"])
except Exception as e:
    log.error("Echec collecte departements : %s", e)
    raise

df_depts = pd.DataFrame([
    {"code_dept": d["code"], "département": d["nom"]}
    for d in depts_raw
])
log.info("Departements charges : %d", len(df_depts))

# ─────────────────────────────────────────────────────────
# Regions
# ─────────────────────────────────────────────────────────
try:
    regions_raw = fetch_json(CONFIG["api_regions_url"], "regions", CONFIG["http_timeout_sec"])
except Exception as e:
    log.error("Echec collecte regions : %s", e)
    raise

df_regions = pd.DataFrame([
    {"code_region": r["code"], "région": r["nom"]}
    for r in regions_raw
])
log.info("Regions chargees : %d", len(df_regions))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — COLLECTE DE LA POPULATION INSEE (Melodie)
# ============================================================================
#
# Source : DS_POPULATIONS_HISTORIQUES — population municipale la plus recente
# Strategie : 1 ligne par commune, annee la plus recente sur ANNEE_DEB -> ANNEE_FIN

try:
    log.info("Appel API INSEE Melodie (peut prendre jusqu'a 3 min)...")
    response = requests.get(CONFIG["api_insee_url"], verify=False,
                            timeout=CONFIG["http_timeout_sec"])
    data = json.loads(response.content)
except Exception as e:
    log.error("Echec collecte population INSEE : %s", e)
    raise

observations = data.get("observations", [])
extracted = []
for obs in observations:
    dims    = obs.get("dimensions", {})
    mesures = obs.get("measures", {}).get("OBS_VALUE_NIVEAU", {})
    value   = mesures.get("value") if mesures else None
    extracted.append({
        "id_commune":  dims.get("GEO"),
        "time_period": dims.get("TIME_PERIOD"),
        "population":  value,
    })

df_pop_all = pd.DataFrame(extracted)
df_pop_all["time_period"] = pd.to_numeric(df_pop_all["time_period"], errors="coerce")
df_pop_all["population"]  = pd.to_numeric(df_pop_all["population"],  errors="coerce")

df_pop = (
    df_pop_all[df_pop_all["time_period"] >= CONFIG["annee_debut_pop"]]
    .sort_values("time_period", ascending=False)
    .drop_duplicates(subset="id_commune", keep="first")  # annee la plus recente
    [["id_commune", "population"]]
    .rename(columns={"population": "population_insee"})
)
log.info("Population INSEE chargee : %d communes", len(df_pop))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — ASSEMBLAGE ET NETTOYAGE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Jointures Pandas
# ─────────────────────────────────────────────────────────
df = (
    df_communes
    .merge(df_depts,   on="code_dept",   how="left")
    .merge(df_regions, on="code_region", how="left")
    .merge(df_pop,     on="id_commune",  how="left")
)

# ─────────────────────────────────────────────────────────
# Population : INSEE en priorite, geo.api en fallback
# ─────────────────────────────────────────────────────────
df["population_commune"] = (
    df["population_insee"]
    .combine_first(df["population_geo"])
    .astype("Int64")
)

# ─────────────────────────────────────────────────────────
# Densite (hab/km2), arrondie a 1 decimale
# ─────────────────────────────────────────────────────────
df["densite_commune"] = (
    df["population_commune"].astype(float) / df["superficie_commune"]
).round(1)

# ─────────────────────────────────────────────────────────
# Selection et nettoyage final (NaN -> None pour Spark)
# ─────────────────────────────────────────────────────────
df_final = df[[
    "id_commune", "commune", "département", "région", "code_region",
    "latitude", "longitude",
    "population_commune", "superficie_commune", "densite_commune",
    "type_commune", "epci",
]].copy()

df_final = df_final.where(pd.notna(df_final), other=None)

log.info("Assemblage OK : %d communes | %d colonnes",
         len(df_final), len(df_final.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 6 — CONVERSION PANDAS VERS SPARK
# ============================================================================

# ─────────────────────────────────────────────────────────
# Conversion avec inference de schema depuis le DataFrame pandas
# ─────────────────────────────────────────────────────────
try:
    df_spark = spark.createDataFrame(df_final)
    df_spark.cache()
    nb_spark = df_spark.count()
    log.info("Conversion Pandas -> Spark OK : %d lignes", nb_spark)
    df_spark.printSchema()
except Exception as e:
    log.error("Echec conversion Pandas -> Spark : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 7 — ECRITURE DELTA (UPSERT / MERGE)
# ============================================================================

TARGET_TABLE = f"{CONFIG['base_name']}.{CONFIG['table_name']}"
TARGET_PATH  = CONFIG["target_path"]

# ─────────────────────────────────────────────────────────
# Fonction d'upsert Delta sur id_commune
# ─────────────────────────────────────────────────────────
def upsert_dim_lieu(df_src: DataFrame, target_table: str, target_path: str) -> None:
    """
    MERGE sur id_commune :
      - Ligne existante + donnees differentes -> UPDATE
      - Nouvelle commune                      -> INSERT
      - Commune disparue de la source         -> DELETE (decommenter si souhaite)
    """

    # ── Cas 1 : table absente -> ecriture initiale ────────────────────────────
    if not DeltaTable.isDeltaTable(spark, target_path):
        log.info("Premiere execution — creation initiale de la table...")
        (
            df_src.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        log.info("Table creee — %d lignes inserees.", df_src.count())
        return

    # ── Cas 2 : table existante -> MERGE ─────────────────────────────────────
    delta_target = DeltaTable.forName(spark, target_table)

    update_cols = {
        col: f"source.{col}"
        for col in df_src.columns
        if col != "id_commune"
    }

    (
        delta_target.alias("target")
        .merge(df_src.alias("source"), "target.id_commune = source.id_commune")

        # UPDATE uniquement si au moins une colonne a change (comparaison null-safe)
        .whenMatchedUpdate(
            condition=" OR ".join([
                f"target.{c} <=> source.{c} = false"
                for c in update_cols.keys()
            ]),
            set=update_cols
        )

        # INSERT pour les nouvelles communes
        .whenNotMatchedInsertAll()

        # DELETE des communes supprimees (decommenter si souhaite)
        # .whenNotMatchedBySourceDelete()

        .execute()
    )

    history = delta_target.history(1).select("operationMetrics").collect()[0][0]
    log.info(
        "MERGE termine — mises a jour : %s | insertions : %s",
        history.get("numTargetRowsUpdated", 0),
        history.get("numTargetRowsInserted", 0),
    )

# ─────────────────────────────────────────────────────────
# Execution du MERGE
# ─────────────────────────────────────────────────────────
try:
    upsert_dim_lieu(df_spark, TARGET_TABLE, TARGET_PATH)
    log.info("Upsert dim_lieu OK -> '%s'", TARGET_TABLE)
except Exception as e:
    log.error("Echec upsert dim_lieu : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 8 — OPTIMISATION DELTA (OPTIMIZE + ZORDER)
# ============================================================================
#
# ZORDER BY id_commune optimise les lectures filtrees par commune
# dans les notebooks Gold (AggGeographie, Marts).
# A executer apres chaque MERGE significatif.

try:
    spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (id_commune)")
    log.info("OPTIMIZE + ZORDER BY id_commune OK sur '%s'", TARGET_TABLE)
except Exception as e:
    log.warning("OPTIMIZE non critique : %s", e)

# ─────────────────────────────────────────────────────────
# Verification post-optimisation
# ─────────────────────────────────────────────────────────
df_check  = spark.read.table(TARGET_TABLE)
nb_lignes = df_check.count()

print(f"\n{'─' * 60}")
print(f"  Table  : {TARGET_TABLE}")
print(f"  Lignes : {nb_lignes:,} | Colonnes : {len(df_check.columns)}")
print(f"{'─' * 60}")
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

# ─────────────────────────────────────────────────────────
# Liberation du cache Spark
# ─────────────────────────────────────────────────────────
df_spark.unpersist()
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
print(f"  RAPPORT DIM-LIEU — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Communes assemblees      : {len(df_final):>8,}")
print(f"  Communes dans la table   : {nb_lignes:>8,}")
print(f"  Population INSEE couverte: {len(df_pop):>8,} communes")
print(f"  Colonnes                 : {len(df_final.columns):>8}")
print(f"  Table ecrite             : {TARGET_TABLE}")
print(f"  Duree totale             : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Dim-Lieu termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
