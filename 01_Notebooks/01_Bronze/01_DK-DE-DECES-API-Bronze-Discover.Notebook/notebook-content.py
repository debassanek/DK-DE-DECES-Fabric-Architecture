# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "592237df-630e-4111-ad48-dc45b1a5a5e0",
# META       "default_lakehouse_name": "DK_DE_DECES_API_Bronze_Ingest",
# META       "default_lakehouse_workspace_id": "35193659-8177-497e-ae34-111479e85809",
# META       "known_lakehouses": [
# META         {
# META           "id": "592237df-630e-4111-ad48-dc45b1a5a5e0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
#  Notebook : 01_DK-DE-DECES-API-Bronze-Discover
#  Couche   : Bronze
#  Domaine  : Sante - Deces
#  Objectif : Decouverte des ressources disponibles sur data.gouv.fr
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook charge de decouvrir dynamiquement les ressources sources
# disponibles depuis l'API data.gouv.fr. Il recupere les metadonnees
# des fichiers, filtre les ressources annuelles selon l'environnement
# (DEV / TEST / PROD), et ecrit le manifeste de decouverte
# (_discovery.json) utilise par le notebook Bronze-Download.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter gestion de la pagination API si > 100 ressources
# [ ] Ajouter validation du schema JSON retourne par l'API
# [ ] Ajouter controle de fraicheur du manifeste (re-discover si > N jours)
# [ ] Ajouter support des fichiers partiels en fin d'annee courante
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
import os
import re
import requests
from datetime import datetime, timezone

import notebookutils

def _detect_environment() -> str:
    """Détecte l'environnement Fabric via le nom du workspace."""
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

workspace_name = notebookutils.runtime.context.get("currentWorkspaceName", "")
# ─────────────────────────────────────────────────────────
# Configuration du logging
# ─────────────────────────────────────────────────────────
_LOG_LEVEL_PAR_ENV = {
    "dev":  logging.DEBUG,
    "test": logging.INFO,
    "prod": logging.WARNING,
}
logging.basicConfig(
    level=_LOG_LEVEL_PAR_ENV[ENVIRONMENT],
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_discover")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
_ANNEES_PAR_ENV = {
    "dev":  [2023, 2024, 2025],
    "test": [2020, 2021, 2022, 2023, 2024, 2025],
    "prod": list(range(2015, 2026)),
}

CONFIG = {
    "dataset_id":     "5de8f397634f4164071119c5",
    "api_base_url":   "https://www.data.gouv.fr/api/1",
    "annees_cibles":  _ANNEES_PAR_ENV[ENVIRONMENT],
    "http_timeout_sec": 120,
    "path_raw_txt":    "Files/bronze/deces/raw_txt",

    "path_discovery":  "Files/bronze/deces/raw_txt/_discovery.json",
}
# ─────────────────────────────────────────────────────────
# Patterns de filtrage des ressources
# ─────────────────────────────────────────────────────────
RE_ANNUAL    = re.compile(r"^deces-(\d{4})\.txt$",      re.IGNORECASE)
RE_MONTHLY   = re.compile(r"^deces-\d{4}m\d{2}\.txt$", re.IGNORECASE)
RE_QUARTERLY = re.compile(r"^deces-\d{4}t\d\.txt$",    re.IGNORECASE)

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Bronze-Discover — demarrage | env : %s", ENVIRONMENT)
log.info("Annees cibles : %s", CONFIG["annees_cibles"])
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — APPEL API DATA.GOUV.FR
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction de recuperation des ressources du dataset
# ─────────────────────────────────────────────────────────
def _get_dataset_resources(dataset_id: str, api_base_url: str, timeout: int) -> list:
    """
    Appelle l'API data.gouv.fr et retourne la liste des ressources
    du dataset. Leve une exception si l'appel echoue.
    """
    url = f"{api_base_url}/datasets/{dataset_id}/"
    log.info("Appel API : GET %s", url)

    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    dataset_json = response.json()
    resources = dataset_json.get("resources", [])
    log.info("Ressources trouvees : %d", len(resources))
    return resources

# ─────────────────────────────────────────────────────────
# Execution
# ─────────────────────────────────────────────────────────
try:
    all_resources = _get_dataset_resources(
        CONFIG["dataset_id"],
        CONFIG["api_base_url"],
        CONFIG["http_timeout_sec"],
    )
except Exception as e:
    log.error("Echec appel API : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — FILTRAGE DES RESSOURCES ANNUELLES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction de filtrage
# ─────────────────────────────────────────────────────────
def _filter_annual_resources(resources: list, annees_cibles: list) -> list:
    """
    Filtre les ressources pour ne garder que les fichiers annuels
    des annees cibles. Exclut mensuel (mXX) et trimestriel (tX).
    """
    selected = []
    for r in resources:
        title = (r.get("title") or "").strip()
        url   = r.get("url") or r.get("latest") or ""
        m = RE_ANNUAL.match(title)
        if not m:
            if RE_MONTHLY.match(title):
                log.debug("Exclusion mensuel : %s", title)
            elif RE_QUARTERLY.match(title):
                log.debug("Exclusion trimestriel : %s", title)
            continue
        annee = int(m.group(1))
        if annee not in annees_cibles:
            log.debug("Exclusion hors perimetre : %s (annee %d)", title, annee)
            continue
        selected.append({
            "titre":        title,
            "annee":        annee,
            "url":          url,
            "taille_bytes": r.get("filesize") or r.get("file_size") or 0,
            "date_maj":     r.get("last_modified") or r.get("updated") or "",
        })
        log.info("[OK] Selectionne : %s", title)

    if not selected:
        raise ValueError(
            f"Aucun fichier annuel trouve pour les annees {annees_cibles}. "
            "Verifiez l'ID du dataset ou les annees cibles dans CONFIG."
        )
    log.info("Fichiers selectionnes : %d", len(selected))
    return selected

# ─────────────────────────────────────────────────────────
# Execution
# ─────────────────────────────────────────────────────────
try:
    fichiers_selectionnes = _filter_annual_resources(
        all_resources, CONFIG["annees_cibles"]
    )
except Exception as e:
    log.error("Echec filtrage ressources : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — ECRITURE DU MANIFESTE DE DECOUVERTE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonctions utilitaires
# ─────────────────────────────────────────────────────────
def _ensure_dir(path: str) -> None:
    """Cree le repertoire dans OneLake si absent."""
    try:
        notebookutils.fs.mkdirs(path)
    except Exception:
        os.makedirs(path, exist_ok=True)

def _write_json_to_onelake(path: str, data: dict) -> None:
    """
    Ecrit un dictionnaire en JSON dans OneLake.
    Utilise notebookutils si disponible, sinon open() via le montage FUSE.
    """
    content = json.dumps(data, ensure_ascii=False, indent=2)
    try:
        notebookutils.fs.put(path, content, overwrite=True)
        log.info("Fichier ecrit via notebookutils : %s", path)
    except Exception:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("Fichier ecrit via open() : %s", path)

# ─────────────────────────────────────────────────────────
# Construction du manifeste de decouverte
# ─────────────────────────────────────────────────────────
discovery_manifest = {
    "discover_timestamp": datetime.now(timezone.utc).isoformat(),
    "environment":        ENVIRONMENT,
    "dataset_id":         CONFIG["dataset_id"],
    "annees_cibles":      CONFIG["annees_cibles"],
    "nb_fichiers":        len(fichiers_selectionnes),
    "fichiers":           fichiers_selectionnes,
}

# ─────────────────────────────────────────────────────────
# Ecriture
# ─────────────────────────────────────────────────────────
try:
    _ensure_dir(CONFIG["path_raw_txt"])
    _write_json_to_onelake(CONFIG["path_discovery"], discovery_manifest)
    log.info("Manifeste de decouverte ecrit : %s", CONFIG["path_discovery"])
except Exception as e:
    log.error("Echec ecriture manifeste de decouverte : %s", e)
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
# Calcul de la duree
# ─────────────────────────────────────────────────────────
duree_sec = (datetime.now(timezone.utc) - _debut_pipeline).seconds

# ─────────────────────────────────────────────────────────
# Rapport
# ─────────────────────────────────────────────────────────
print(f"\n{'=' * 60}")
print(f"  RAPPORT BRONZE-DISCOVER — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
print(f"  Ressources API trouvees  : {len(all_resources):>6}")
print(f"  Fichiers selectionnes    : {len(fichiers_selectionnes):>6}")
for f in fichiers_selectionnes:
    print(f"    - {f['titre']:<30} {f['taille_bytes'] // 1024:>8} Ko")
print(f"{'─' * 60}")
print(f"  Manifeste ecrit          : {CONFIG['path_discovery']}")
print(f"  Duree totale             : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Bronze-Discover termine. Duree : %d sec", duree_sec)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
