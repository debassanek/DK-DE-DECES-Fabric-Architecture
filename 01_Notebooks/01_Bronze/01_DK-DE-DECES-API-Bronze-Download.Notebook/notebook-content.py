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
#  Notebook : 01_DK-DE-DECES-API-Bronze-Download
#  Couche   : Bronze
#  Domaine  : Sante - Deces
#  Objectif : Telechargement securise des fichiers bruts vers OneLake
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Notebook responsable du telechargement resilient des fichiers bruts
# depuis data.gouv.fr vers OneLake Bronze Files. Lit le manifeste
# produit par Bronze-Discover (_discovery.json), gere les retries,
# les timeouts, les telechargements incrementaux (skip_existing) et
# les validations minimales. Produit _download.json pour Bronze-Parse.
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter validation du checksum (md5 / sha256) apres telechargement
# [ ] Ajouter decoupe en download partiel si fichier > seuil configurable
# [ ] Ajouter metriques de bande passante par fichier
# [ ] Ajouter timeout par taille de fichier (timeout adaptatif)
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
import time
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
log = logging.getLogger("bronze_download")

# ─────────────────────────────────────────────────────────
# Configuration centralisee
# ─────────────────────────────────────────────────────────
CONFIG = {
    "path_raw_txt":    "/lakehouse/default/Files/bronze/deces/raw_txt",
    "path_discovery":  "/lakehouse/default/Files/bronze/deces/raw_txt/_discovery.json",
    "path_download":    "/lakehouse/default/Files/bronze/deces/raw_txt/_download.json",
    "skip_existing":   True,
    "chunk_size_bytes": 8 * 1024,
    "http_timeout_sec": 120,
    "max_retries":     3,
    "retry_delay_sec": 5,
}

_debut_pipeline = datetime.now(timezone.utc)
log.info("=" * 60)
log.info("Bronze-Download — demarrage | env : %s", ENVIRONMENT)
log.info("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — LECTURE DU MANIFESTE DE DECOUVERTE
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction de lecture JSON
# ─────────────────────────────────────────────────────────
def _read_json_from_onelake(path: str) -> dict:
    """Lit un fichier JSON depuis OneLake via le montage FUSE."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Manifeste introuvable : {path}\n"
            "Verifiez que le notebook precedent s'est execute correctement."
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ─────────────────────────────────────────────────────────
# Lecture et validation
# ─────────────────────────────────────────────────────────
try:
    discovery = _read_json_from_onelake(CONFIG["path_discovery"])
    fichiers_selectionnes = discovery["fichiers"]
    log.info("Manifeste de decouverte lu : %d fichier(s)", len(fichiers_selectionnes))
    log.info("Annees cibles : %s", discovery.get("annees_cibles"))
except Exception as e:
    log.error("Echec lecture manifeste de decouverte : %s", e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — PREPARATION DES REPERTOIRES
# ============================================================================

# ─────────────────────────────────────────────────────────
# Creation du repertoire de stockage si absent
# ─────────────────────────────────────────────────────────

def _fuse_to_nu(path: str) -> str:
    """Convertit un chemin FUSE absolu en chemin notebookutils relatif."""
    return re.sub(r"^/lakehouse/default/", "", path)

def _write_json_to_onelake(path: str, data: dict) -> None:
    """
    Ecrit un dictionnaire en JSON dans OneLake.
    path : toujours au format FUSE absolu (/lakehouse/default/Files/...)
    La conversion vers le format attendu par chaque API est faite en interne.
    """
    content = json.dumps(data, ensure_ascii=False, indent=2)

    try:
        notebookutils.fs.put(_fuse_to_nu(path), content, overwrite=True)
        log.info("JSON ecrit via notebookutils : %s", _fuse_to_nu(path))
    except Exception as e:
        log.debug("notebookutils echec (%s) — fallback open()", e)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("JSON ecrit via open() : %s", path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 4 — TELECHARGEMENT DES FICHIERS
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction de telechargement avec retry
# ─────────────────────────────────────────────────────────
def _download_file(url: str, local_path: str, chunk_size: int,
                   timeout: int, max_retries: int, retry_delay: int) -> int:
    """
    Telecharge un fichier en streaming par chunks.
    Retourne le nombre d'octets ecrits.
    Leve IOError apres epuisement des tentatives.
    """
    for attempt in range(1, max_retries + 1):
        try:
            log.info("  Tentative %d/%d : %s", attempt, max_retries, url)
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                total = 0
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            total += len(chunk)
            log.info("  [OK] %s — %d Mo", os.path.basename(local_path), total // (1024 * 1024))
            return total
        except requests.RequestException as exc:
            log.warning("  Echec tentative %d : %s", attempt, exc)
            if attempt < max_retries:
                time.sleep(retry_delay)
    raise IOError(f"Impossible de telecharger {url} apres {max_retries} tentatives.")

# ─────────────────────────────────────────────────────────
# Boucle de telechargement
# ─────────────────────────────────────────────────────────
fichiers_telecharges = []

for fic in fichiers_selectionnes:
    local_path     = os.path.join(CONFIG["path_raw_txt"], fic["titre"])
    fic_result     = {**fic, "local_path": local_path}

    if CONFIG["skip_existing"] and os.path.exists(local_path):
        taille = os.path.getsize(local_path)
        log.info("[SKIP] Deja present (%d Mo) : %s", taille // (1024 * 1024), fic["titre"])
        fic_result["action"]             = "skipped"
        fic_result["taille_reelle_bytes"] = taille
    else:
        try:
            taille = _download_file(
                fic["url"], local_path,
                CONFIG["chunk_size_bytes"],
                CONFIG["http_timeout_sec"],
                CONFIG["max_retries"],
                CONFIG["retry_delay_sec"],
            )
            fic_result["action"]             = "downloaded"
            fic_result["taille_reelle_bytes"] = taille
        except Exception as e:
            log.error("[FAIL] Echec telechargement %s : %s", fic["titre"], e)
            fic_result["action"]             = "failed"
            fic_result["taille_reelle_bytes"] = 0

    fichiers_telecharges.append(fic_result)

nb_ok     = sum(1 for f in fichiers_telecharges if f["action"] in ("downloaded", "skipped"))
nb_failed = sum(1 for f in fichiers_telecharges if f["action"] == "failed")

if nb_ok == 0:
    raise RuntimeError("Aucun fichier disponible apres telechargement. Arret du pipeline.")

log.info("Telechargement termine : %d OK | %d echec(s)", nb_ok, nb_failed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 5 — ECRITURE DU MANIFESTE DE TELECHARGEMENT
# ============================================================================

# ─────────────────────────────────────────────────────────
# Fonction d'ecriture JSON
# ─────────────────────────────────────────────────────────
def _fuse_to_nu(path: str) -> str:
    """Convertit un chemin FUSE absolu en chemin notebookutils relatif."""
    return re.sub(r"^/lakehouse/default/", "", path)

def _write_json_to_onelake(path: str, data: dict) -> None:
    """
    Ecrit un dictionnaire en JSON dans OneLake.
    Utilise notebookutils si disponible, sinon open() via le montage FUSE.
    """
    content = json.dumps(data, ensure_ascii=False, indent=2)
    try:
        notebookutils.fs.put(_fuse_to_nu(path), content, overwrite=True)
        log.info("Fichier ecrit via notebookutils : %s", path)
    except Exception:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("Fichier ecrit via open() : %s", path)

# ─────────────────────────────────────────────────────────
# Construction et ecriture du manifeste
# ─────────────────────────────────────────────────────────
download_manifest = {
    "download_timestamp": datetime.now(timezone.utc).isoformat(),
    "environment":        ENVIRONMENT,
    "nb_ok":              nb_ok,
    "nb_failed":          nb_failed,
    "fichiers":           fichiers_telecharges,
}

try:
    _write_json_to_onelake(CONFIG["path_download"], download_manifest)
    log.info("Manifeste de telechargement ecrit : %s", CONFIG["path_download"])
except Exception as e:
    log.error("Echec ecriture manifeste de telechargement : %s", e)
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
print(f"  RAPPORT BRONZE-DOWNLOAD — {ENVIRONMENT.upper()}")
print(f"{'=' * 60}")
for f in fichiers_telecharges:
    statut = f["action"].upper()
    taille = f["taille_reelle_bytes"] // (1024 * 1024)
    print(f"  [{statut:<10}]  {f['titre']:<30}  {taille:>5} Mo")
print(f"{'─' * 60}")
print(f"  Fichiers OK      : {nb_ok}")
print(f"  Fichiers en echec: {nb_failed}")
print(f"  Duree totale     : {duree_sec} sec")
print(f"{'=' * 60}\n")

log.info("Bronze-Download termine. Duree : %d sec", duree_sec)

if nb_failed > 0:
    log.warning("%d fichier(s) en echec — pipeline continue avec les fichiers disponibles.", nb_failed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
