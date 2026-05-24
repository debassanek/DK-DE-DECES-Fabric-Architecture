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
#  Notebook : 99_DK-DE-DECES-API-Utils-LoggingUtils
#  Couche   : Utils
#  Domaine  : Sante - Deces
#  Objectif : Fonctions standardisees de logging et de tracabilite pipeline
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Module utilitaire centralisant la configuration du logging, les formats
# de logs, les niveaux par environnement et les fonctions standardisees
# de tracabilite pipeline. A inclure en debut de chaque notebook via :
#
#   notebookutils.notebook.run("99_DK-DE-DECES-API-Utils-LoggingUtils")
#
# Fonctions exposees :
#   detect_environment()       -> str
#   setup_logging(name)        -> logging.Logger
#   log_step_start(log, name)  -> datetime
#   log_step_end(log, name, t) -> int
#   log_pipeline_summary(log, results)
#   log_quality_metrics(log, metrics_dict)
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter un handler vers Azure Monitor / Log Analytics (PROD)
# [ ] Ajouter structured logging (JSON) pour l'ingestion dans monitoring
# [ ] Ajouter correlation_id pour tracker une execution cross-notebooks
# [ ] Remplacer les appels duplicates dans chaque notebook par %run de ce module
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
import sys
from datetime import datetime, timezone

try:
    import notebookutils
    _HAS_NOTEBOOKUTILS = True
except ImportError:
    _HAS_NOTEBOOKUTILS = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — FONCTIONS DE CONFIGURATION
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 2a — Detection de l'environnement
# ─────────────────────────────────────────────────────────

def detect_environment() -> str:
    """
    Detecte l'environnement Fabric courant depuis le nom du workspace.

    Retourne : 'dev', 'test' ou 'prod'.
    Retourne 'dev' par defaut si la detection echoue (local, test unitaire).
    """
    if not _HAS_NOTEBOOKUTILS:
        return "dev"
    try:
        workspace_name = notebookutils.runtime.context.get("currentWorkspaceName", "")
        if "Dev" in workspace_name:    return "dev"
        elif "Test" in workspace_name: return "test"
        else:                          return "prod"
    except Exception:
        return "dev"


# ─────────────────────────────────────────────────────────
# Niveaux de log par environnement
# ─────────────────────────────────────────────────────────
_LOG_LEVEL_PAR_ENV = {
    "dev":  logging.DEBUG,
    "test": logging.INFO,
    "prod": logging.WARNING,
}

_LOG_FORMAT      = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(notebook_name: str, environment: str = None) -> logging.Logger:
    """
    Configure le logging pour un notebook et retourne un logger pret a l'emploi.

    Args:
        notebook_name : nom logique du notebook (ex : 'bronze_discover')
        environment   : 'dev'/'test'/'prod'. Detecte automatiquement si None.

    Retourne un logging.Logger configure avec le bon niveau et format.

    Usage type :
        from logging_utils import setup_logging
        log = setup_logging("bronze_discover")
    """
    env   = environment or detect_environment()
    level = _LOG_LEVEL_PAR_ENV.get(env, logging.INFO)

    # Evite la duplication de handlers si le notebook est relance
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format=_LOG_FORMAT,
            datefmt=_LOG_DATE_FORMAT,
            stream=sys.stdout,
        )
    else:
        root.setLevel(level)

    logger = logging.getLogger(notebook_name)
    logger.setLevel(level)
    return logger

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2b — Tracabilite des etapes
# ─────────────────────────────────────────────────────────

def log_step_start(log: logging.Logger, step_name: str, **context) -> datetime:
    """
    Logue le debut d'une etape et retourne l'horodatage de debut.

    Args:
        log       : logger du notebook appelant
        step_name : nom de l'etape (ex : 'ETAPE 3 — PARSING')
        **context : paires cle=valeur a logguer en contexte

    Retourne datetime UTC de debut (a passer a log_step_end).
    """
    debut = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info("Debut : %s", step_name)
    for k, v in context.items():
        log.info("  %s : %s", k, v)
    return debut


def log_step_end(log: logging.Logger, step_name: str, debut: datetime,
                 **metrics) -> int:
    """
    Logue la fin d'une etape avec duree et metriques optionnelles.

    Args:
        log       : logger du notebook appelant
        step_name : nom de l'etape
        debut     : datetime retourne par log_step_start
        **metrics : paires cle=valeur de metriques (ex : nb_lignes=1234)

    Retourne la duree en secondes.
    """
    duree_sec = (datetime.now(timezone.utc) - debut).seconds
    log.info("Fin : %s | duree : %d sec", step_name, duree_sec)
    for k, v in metrics.items():
        if isinstance(v, int):
            log.info("  %s : %d", k, v)
        elif isinstance(v, float):
            log.info("  %s : %.4f", k, v)
        else:
            log.info("  %s : %s", k, v)
    return duree_sec

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2c — Rapports standardises
# ─────────────────────────────────────────────────────────

def log_pipeline_summary(log: logging.Logger, results: list) -> None:
    """
    Logue un rapport synthetique d'execution de pipeline.

    Args:
        log     : logger du notebook appelant
        results : liste de dicts avec cles 'nom', 'statut', 'duree_sec', 'erreur'
                  (format retourne par _run_notebook dans le MasterPipeline)
    """
    duree_totale = sum(r.get("duree_sec", 0) for r in results)
    nb_ok     = sum(1 for r in results if r.get("statut") == "success")
    nb_failed = sum(1 for r in results if r.get("statut") == "failed")
    nb_skip   = sum(1 for r in results if r.get("statut") == "skipped")

    log.info("=" * 60)
    log.info("RAPPORT PIPELINE")
    log.info("  Notebooks : %d total | %d success | %d failed | %d skipped",
             len(results), nb_ok, nb_failed, nb_skip)
    log.info("  Duree totale : %d sec (%d min %d sec)",
             duree_totale, duree_totale // 60, duree_totale % 60)
    for r in results:
        log.info("  [%s] %s — %d sec", r.get("statut", "?").upper(),
                 r.get("nom", "?"), r.get("duree_sec", 0))
        if r.get("erreur"):
            log.error("    Erreur : %s", str(r["erreur"])[:120])
    log.info("=" * 60)


def log_quality_metrics(log: logging.Logger, metrics: dict) -> None:
    """
    Logue un dictionnaire de metriques qualite de maniere standardisee.

    Args:
        log     : logger du notebook appelant
        metrics : dict {nom_metrique: valeur} (valeurs numeriques ou str)

    Usage :
        log_quality_metrics(log, {
            "nb_lignes_bronze":  1_500_000,
            "nb_lignes_silver":  1_480_000,
            "taux_rejet_pct":    1.33,
        })
    """
    log.info("Metriques qualite :")
    for k, v in metrics.items():
        if isinstance(v, float):
            log.info("  %-35s : %.4f", k, v)
        elif isinstance(v, int):
            log.info("  %-35s : %d", k, v)
        else:
            log.info("  %-35s : %s", k, str(v))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — TESTS ET VALIDATION
# ============================================================================

from datetime import datetime, timezone

_env  = detect_environment()
_log  = setup_logging("logging_utils_test", _env)

_log.info("Test detect_environment : %s", _env)

_t0   = log_step_start(_log, "ETAPE TEST", fichier="test.txt", nb_lignes=42)
import time; time.sleep(0.01)
_dur  = log_step_end(_log, "ETAPE TEST", _t0, nb_lignes_parsees=40, taux=0.952)
assert _dur >= 0, "log_step_end doit retourner une duree >= 0"

log_quality_metrics(_log, {"nb_bronze": 1_000_000, "taux_rejet_pct": 1.23, "env": _env})

log_pipeline_summary(_log, [
    {"nom": "bronze", "statut": "success", "duree_sec": 120, "erreur": None},
    {"nom": "silver", "statut": "failed",  "duree_sec": 30,  "erreur": "OOM"},
    {"nom": "gold",   "statut": "skipped", "duree_sec": 0,   "erreur": None},
])

print("logging_utils — tous les tests OK")

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
print("  99_DK-DE-DECES-API-Utils-LoggingUtils")
print(f"{'=' * 60}")
print("  Fonctions disponibles :")
print("    detect_environment()        -> str")
print("    setup_logging(name, env)    -> logging.Logger")
print("    log_step_start(log, name)   -> datetime")
print("    log_step_end(log, name, t)  -> int (duree sec)")
print("    log_pipeline_summary(log, results)")
print("    log_quality_metrics(log, metrics)")
print(f"{'=' * 60}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
