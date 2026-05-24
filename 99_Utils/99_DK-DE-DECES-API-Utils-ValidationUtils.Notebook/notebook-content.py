# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================================================
#  Notebook : 99_DK-DE-DECES-API-Utils-ValidationUtils
#  Couche   : Utils
#  Domaine  : Sante - Deces
#  Objectif : Fonctions de validation qualite et de controle fonctionnel
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Module utilitaire regroupant les regles de validation technique et
# metier : controles de nullite, plages de valeurs, coherence de dates,
# detection d'anomalies par z-score et calcul de scores qualite.
# Generalise les controles disperses dans Silver-Validate et les
# notebooks Gold.
#
# Fonctions exposees :
#   compute_null_rates(df, columns)             -> dict
#   check_not_null(df, columns)                 -> DataFrame (avec flag)
#   check_value_range(df, col, min_v, max_v)    -> DataFrame
#   check_date_coherence(df, col_naiss, col_dc) -> DataFrame
#   detect_zscore_anomalies(df, col, threshold) -> DataFrame
#   compute_quality_report(df, rules)           -> dict
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter check_referential_integrity(df, col, ref_df, ref_col)
# [ ] Ajouter compute_completeness_score() ponderable par colonne
# [ ] Ajouter export_quality_report_to_delta() pour archivage
# [ ] Ajouter check_schema_drift(df, expected_schema)
# ----------------------------------------------------------------------------

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 1 — IMPORTS
# ============================================================================

import logging
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, BooleanType
from pyspark.sql.window import Window

log = logging.getLogger("validation_utils")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — FONCTIONS DE VALIDATION
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 2a — Metriques de nullite
# ─────────────────────────────────────────────────────────

def compute_null_rates(df: DataFrame, columns: List[str] = None) -> Dict[str, float]:
    """
    Calcule le taux de nulls (%) pour chaque colonne demandee.

    Args:
        df      : DataFrame a analyser
        columns : liste de colonnes a verifier. Toutes les colonnes si None.

    Retourne un dict {col_name: pct_null (0.0 -> 100.0)}.
    Leve ValueError si une colonne demandee n'existe pas dans df.

    Usage :
        rates = compute_null_rates(df_silver, ["nom", "date_deces", "age_au_deces"])
        for col, pct in rates.items():
            if pct > 5:
                log.warning("Taux de nulls eleve sur '%s' : %.2f%%", col, pct)
    """
    cols = columns or df.columns
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes absentes du DataFrame : {missing}")

    total = df.count()
    if total == 0:
        return {c: 0.0 for c in cols}

    exprs = [
        F.round(
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / F.lit(total) * 100, 4
        ).alias(c)
        for c in cols
    ]
    row = df.select(exprs).collect()[0]
    return {c: float(row[c]) if row[c] is not None else 0.0 for c in cols}


def check_not_null(df: DataFrame, columns: List[str],
                   flag_col: str = "_has_null") -> DataFrame:
    """
    Ajoute un flag booleen True si l'une des colonnes listees est nulle.

    Args:
        df       : DataFrame a controler
        columns  : colonnes dont la nullite est inacceptable
        flag_col : nom de la colonne de flag (defaut '_has_null')

    Usage :
        df = check_not_null(df, ["nom", "date_deces"])
        df_rejets = df.filter(F.col("_has_null"))
    """
    condition = F.lit(False)
    for c in columns:
        if c in df.columns:
            condition = condition | F.col(c).isNull()
    return df.withColumn(flag_col, condition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2b — Controles de plage et de coherence
# ─────────────────────────────────────────────────────────

def check_value_range(df: DataFrame, col_name: str,
                      min_val=None, max_val=None,
                      flag_col: str = "_out_of_range") -> DataFrame:
    """
    Ajoute un flag booleen True si la valeur est hors de la plage attendue.

    Args:
        df        : DataFrame a controler
        col_name  : colonne a verifier
        min_val   : valeur minimale incluse (None = pas de borne basse)
        max_val   : valeur maximale incluse (None = pas de borne haute)
        flag_col  : nom de la colonne de flag

    Usage :
        df = check_value_range(df, "age_au_deces", min_val=0, max_val=145)
        df_invalides = df.filter(F.col("_out_of_range"))
    """
    if col_name not in df.columns:
        log.warning("Colonne '%s' absente du DataFrame — controle ignore.", col_name)
        return df.withColumn(flag_col, F.lit(False))

    condition = F.lit(False)
    col = F.col(col_name)

    if min_val is not None:
        condition = condition | (col < F.lit(min_val))
    if max_val is not None:
        condition = condition | (col > F.lit(max_val))

    # Les nulls ne sont pas hors plage — utiliser check_not_null en complement
    condition = condition & col.isNotNull()
    return df.withColumn(flag_col, condition)


def check_date_coherence(df: DataFrame,
                          col_naissance: str = "date_naissance",
                          col_deces: str = "date_deces",
                          flag_col: str = "_date_incoherente") -> DataFrame:
    """
    Ajoute un flag True si date_naissance > date_deces (incoherence temporelle).
    Traite les nulls comme coherents (flag = False).

    Args:
        df             : DataFrame a controler (colonnes de type DateType)
        col_naissance  : colonne de date de naissance
        col_deces      : colonne de date de deces
        flag_col       : nom du flag de sortie
    """
    if col_naissance not in df.columns or col_deces not in df.columns:
        log.warning("Colonnes de date absentes — controle ignore.")
        return df.withColumn(flag_col, F.lit(False))

    condition = (
        F.col(col_naissance).isNotNull()
        & F.col(col_deces).isNotNull()
        & (F.col(col_naissance) > F.col(col_deces))
    )
    return df.withColumn(flag_col, condition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2c — Detection d'anomalies statistiques
# ─────────────────────────────────────────────────────────

def detect_zscore_anomalies(df: DataFrame, col_name: str,
                             partition_cols: List[str] = None,
                             threshold: float = 2.5,
                             flag_col: str = "_anomalie") -> DataFrame:
    """
    Detecte les anomalies statistiques via z-score.
    Ajoute un flag booleen True si |z-score| > threshold.

    Generalise la detection d'anomalies utilisee dans :
    Gold-AggAge (anomalie_distribution), Gold-AggTemporel (anomalie_flag),
    Gold-AggGeographie (anomalie_mortalite), Gold-AggGeneration (anomalie_generation).

    Args:
        df             : DataFrame source
        col_name       : colonne numerique a analyser
        partition_cols : colonnes de partition pour le z-score local.
                         None = z-score global sur tout le DataFrame.
        threshold      : seuil |z-score| au-dela duquel une ligne est anomalie
        flag_col       : nom de la colonne de flag booleenne

    Usage :
        df = detect_zscore_anomalies(df, "nb_deces", partition_cols=["id_date"], threshold=2.5)
        df_anomalies = df.filter(F.col("_anomalie"))
    """
    if col_name not in df.columns:
        log.warning("Colonne '%s' absente — detection ignoree.", col_name)
        return df.withColumn(flag_col, F.lit(False))

    if partition_cols:
        w = Window.partitionBy(*partition_cols).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)
    else:
        w = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df = (
        df
        .withColumn("_mean_tmp", F.avg(col_name).over(w))
        .withColumn("_std_tmp",  F.stddev(col_name).over(w))
        .withColumn("_z_tmp",
            F.when(
                F.col("_std_tmp").isNotNull() & (F.col("_std_tmp") != 0),
                F.round(
                    (F.col(col_name).cast(DoubleType()) - F.col("_mean_tmp"))
                    / F.col("_std_tmp"), 3)
            ).otherwise(F.lit(None).cast(DoubleType())))
        .withColumn(flag_col,
            F.when(F.col("_z_tmp").isNotNull(),
                F.abs(F.col("_z_tmp")) > F.lit(threshold)
            ).otherwise(F.lit(False)))
        .drop("_mean_tmp", "_std_tmp", "_z_tmp")
    )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2d — Rapport qualite consolide
# ─────────────────────────────────────────────────────────

def compute_quality_report(df: DataFrame, rules: dict) -> dict:
    """
    Calcule un rapport qualite consolide depuis un ensemble de regles.

    Args:
        df    : DataFrame a analyser
        rules : dict de regles, chaque regle est un dict avec :
            {
              "type"    : "not_null" | "range" | "date_coherence",
              "columns" : [liste de colonnes] (pour not_null)
              "column"  : nom de colonne (pour range)
              "min"     : valeur min (pour range, optionnel)
              "max"     : valeur max (pour range, optionnel)
            }

    Retourne un dict avec :
        - "nb_lignes"        : total de lignes
        - "null_rates"       : {col: pct_null}
        - "regle_resultats"  : {nom_regle: nb_invalides}
        - "score_qualite"    : float 0.0-100.0 (% lignes sans aucune anomalie)

    Usage :
        rapport = compute_quality_report(df_silver, {
            "nulls_cles": {"type": "not_null", "columns": ["nom", "date_deces"]},
            "age_valide":  {"type": "range", "column": "age_au_deces", "min": 0, "max": 145},
        })
    """
    nb_total = df.count()
    if nb_total == 0:
        return {"nb_lignes": 0, "null_rates": {}, "regle_resultats": {}, "score_qualite": 100.0}

    df_work        = df
    flag_cols      = []
    regle_resultats = {}

    for nom_regle, regle in rules.items():
        flag = f"_flag_{nom_regle}"
        t = regle.get("type")

        if t == "not_null":
            df_work = check_not_null(df_work, regle.get("columns", []), flag)
        elif t == "range":
            df_work = check_value_range(
                df_work, regle.get("column", ""),
                regle.get("min"), regle.get("max"), flag)
        elif t == "date_coherence":
            df_work = check_date_coherence(
                df_work,
                regle.get("col_naissance", "date_naissance"),
                regle.get("col_deces", "date_deces"), flag)
        else:
            log.warning("Type de regle inconnu : '%s'", t)
            continue

        flag_cols.append(flag)
        nb_invalides = df_work.filter(F.col(flag) == True).count()
        regle_resultats[nom_regle] = nb_invalides
        log.info("Regle '%s' : %d lignes invalides / %d", nom_regle, nb_invalides, nb_total)

    # Score : % de lignes sans aucune anomalie
    if flag_cols:
        condition_ok = F.lit(True)
        for fc in flag_cols:
            condition_ok = condition_ok & (F.col(fc) == False)
        nb_ok = df_work.filter(condition_ok).count()
        score = round(nb_ok / nb_total * 100, 4)
    else:
        score = 100.0

    null_rates = compute_null_rates(df, [
        r.get("column") or c
        for r in rules.values()
        for c in ([r.get("column")] if r.get("column") else r.get("columns", []))
        if (r.get("column") or c) in df.columns
    ][:10])  # max 10 colonnes pour la performance

    return {
        "nb_lignes":       nb_total,
        "null_rates":      null_rates,
        "regle_resultats": regle_resultats,
        "score_qualite":   score,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — TESTS ET VALIDATION
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import datetime

_spark = SparkSession.builder.getOrCreate()

# DataFrame de test
_schema = StructType([
    StructField("nom",            StringType(),  True),
    StructField("age_au_deces",   IntegerType(), True),
    StructField("date_naissance", DateType(),    True),
    StructField("date_deces",     DateType(),    True),
])
_data = [
    ("DUPONT",  75,  datetime.date(1945, 3, 10), datetime.date(2020, 8, 5)),
    ("MARTIN",  None, None,                       datetime.date(2021, 1, 1)),
    ("DURAND",  -5,  datetime.date(2030, 1, 1),  datetime.date(1980, 5, 20)),  # incoherence
    (None,      200, datetime.date(1920, 6, 1),  datetime.date(2021, 3, 1)),
]
_df = _spark.createDataFrame(_data, _schema)

# Test compute_null_rates
_rates = compute_null_rates(_df, ["nom", "age_au_deces"])
assert "nom" in _rates and "age_au_deces" in _rates
assert _rates["nom"] == 25.0       # 1 null sur 4

# Test check_not_null
_df_flagged = check_not_null(_df, ["nom"])
assert "_has_null" in _df_flagged.columns
assert _df_flagged.filter(F.col("_has_null")).count() == 1

# Test check_value_range
_df_range = check_value_range(_df, "age_au_deces", min_val=0, max_val=145)
assert "_out_of_range" in _df_range.columns
assert _df_range.filter(F.col("_out_of_range")).count() == 2  # -5 et 200

# Test check_date_coherence
_df_dates = check_date_coherence(_df)
assert "_date_incoherente" in _df_dates.columns
assert _df_dates.filter(F.col("_date_incoherente")).count() == 1  # DURAND

# Test detect_zscore_anomalies
_df_anom = detect_zscore_anomalies(_df, "age_au_deces", threshold=2.0)
assert "_anomalie" in _df_anom.columns

# Test compute_quality_report
_rapport = compute_quality_report(_df, {
    "nulls_cles":  {"type": "not_null", "columns": ["nom", "age_au_deces"]},
    "age_valide":  {"type": "range", "column": "age_au_deces", "min": 0, "max": 145},
    "dates_ok":    {"type": "date_coherence"},
})
assert "score_qualite" in _rapport
assert _rapport["nb_lignes"] == 4
assert _rapport["regle_resultats"]["age_valide"] == 2

print("validation_utils — tous les tests OK")
print(f"  Score qualite du DataFrame de test : {_rapport['score_qualite']}%")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE FINALE — REPORTING
# ============================================================================

print(f"\n{'=' * 60}")
print("  99_DK-DE-DECES-API-Utils-ValidationUtils")
print(f"{'=' * 60}")
print("  Fonctions disponibles :")
print("    compute_null_rates(df, columns)             -> dict")
print("    check_not_null(df, columns, flag_col)       -> DataFrame")
print("    check_value_range(df, col, min, max, flag)  -> DataFrame")
print("    check_date_coherence(df, col_n, col_d, flag)-> DataFrame")
print("    detect_zscore_anomalies(df, col, parts, th) -> DataFrame")
print("    compute_quality_report(df, rules)           -> dict")
print(f"{'=' * 60}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
