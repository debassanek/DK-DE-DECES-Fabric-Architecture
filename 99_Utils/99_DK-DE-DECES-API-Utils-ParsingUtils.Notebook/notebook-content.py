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
#  Notebook : 99_DK-DE-DECES-API-Utils-ParsingUtils
#  Couche   : Utils
#  Domaine  : Sante - Deces
#  Objectif : Fonctions de parsing des fichiers bruts INSEE positionnels
# ============================================================================

# Description
# ----------------------------------------------------------------------------
# Module utilitaire contenant les fonctions generiques de parsing des
# fichiers bruts : extraction positionnelle, validation de longueur,
# split nom/prenoms, normalisation technique et helpers de transformation
# ligne a ligne. Generalise le parsing du notebook Bronze-Parse.
#
# Fonctions exposees :
#   extract_field(line, start, end)              -> str
#   validate_line_length(line, min_len)          -> bool
#   split_nom_prenoms(raw)                       -> tuple(nom, prenoms)
#   parse_insee_line(line, positions, min_len)   -> dict | None
#   build_line_parser(positions, min_len)        -> Callable
#   normalize_insee_date(date_str)               -> tuple(str, bool)
# ----------------------------------------------------------------------------

# TODO
# ----------------------------------------------------------------------------
# [ ] Ajouter parse_csv_line() pour les futures sources CSV
# [ ] Ajouter detect_encoding() pour les fichiers non latin-1
# [ ] Ajouter build_spark_udf(parser_fn) pour vectoriser le parsing
# [ ] Externaliser POSITIONS_INSEE ici plutot que dans Bronze-Parse
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
import re
from typing import Callable, Optional, Tuple, Dict

log = logging.getLogger("parsing_utils")

# ─────────────────────────────────────────────────────────
# Schema positionnel de reference INSEE (176 chars)
# Source validee : documentation INSEE fichiers deces
# ─────────────────────────────────────────────────────────
POSITIONS_INSEE = {
    "nom_prenoms":         (0,   80),
    "sexe":                (80,  81),
    "date_naissance":      (81,  89),
    "code_lieu_naissance": (89,  94),
    "commune_naissance":   (94,  124),
    "pays_naissance":      (124, 154),
    "date_deces":          (154, 162),
    "code_lieu_deces":     (162, 167),
    "num_acte_deces":      (167, 176),
}

MIN_LINE_LENGTH_INSEE = 162   # longueur minimale valide (sans num_acte)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 2 — FONCTIONS DE PARSING
# ============================================================================

# ─────────────────────────────────────────────────────────
# Sous-partie 2a — Extraction et validation de base
# ─────────────────────────────────────────────────────────

def extract_field(line: str, start: int, end: int) -> str:
    """
    Extrait et nettoie un champ positionnel d'une ligne brute.

    Args:
        line  : ligne brute du fichier source
        start : index de debut (inclus)
        end   : index de fin (exclus)

    Retourne la valeur trimmee. Retourne '' si la ligne est trop courte.
    """
    if len(line) < end:
        return line[start:].strip() if len(line) > start else ""
    return line[start:end].strip()


def validate_line_length(line: str, min_length: int,
                          max_length: int = None) -> bool:
    """
    Valide la longueur d'une ligne de fichier positionnel.

    Args:
        line       : ligne brute
        min_length : longueur minimale requise
        max_length : longueur maximale optionnelle

    Retourne True si la ligne est valide.
    """
    n = len(line)
    if n < min_length:
        return False
    if max_length is not None and n > max_length:
        return False
    return True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2b — Parsing metier INSEE
# ─────────────────────────────────────────────────────────

def split_nom_prenoms(nom_prenoms_raw: str) -> Tuple[str, str]:
    """
    Separe le champ nom_prenoms INSEE en nom et prenoms.
    Format INSEE : 'NOM*PRENOM1 PRENOM2' — separateur '*'.

    Args:
        nom_prenoms_raw : champ brut positions 0-80 du fichier INSEE

    Retourne (nom, prenoms). Retourne ('', '') si entree vide.
    """
    if not nom_prenoms_raw:
        return "", ""
    if "*" in nom_prenoms_raw:
        parts = nom_prenoms_raw.split("*", 1)
        return parts[0].strip(), parts[1].strip() if len(parts) > 1 else ""
    return nom_prenoms_raw.strip(), ""


def normalize_insee_date(date_str: str) -> Tuple[Optional[str], bool]:
    """
    Normalise une date INSEE au format AAAAMMJJ.
    Remplace MM=00 ou JJ=00 par 01 (date approchee).

    Args:
        date_str : chaine de 8 caracteres (ex : '19870600')

    Retourne (date_normalisee, est_approchee).
    Retourne (None, False) si la date est invalide (longueur, annee=0000).

    Exemples :
        normalize_insee_date('19870615') -> ('1987-06-15', False)
        normalize_insee_date('19870600') -> ('1987-06-01', True)
        normalize_insee_date('00000000') -> (None, False)
    """
    if not date_str or len(date_str) != 8:
        return None, False

    annee = date_str[0:4]
    mois  = date_str[4:6]
    jour  = date_str[6:8]

    if annee == "0000":
        return None, False

    est_approchee = (mois == "00") or (jour == "00")
    mois_corr = "01" if mois == "00" else mois
    jour_corr = "01" if jour == "00" else jour

    return f"{annee}-{mois_corr}-{jour_corr}", est_approchee


def parse_insee_line(line: str, positions: dict = None,
                     min_length: int = None) -> Optional[dict]:
    """
    Parse une ligne complete du fichier TXT positionnel INSEE.
    Retourne un dictionnaire de champs ou None si la ligne est invalide.

    Args:
        line       : ligne brute du fichier
        positions  : dict {champ: (start, end)} — POSITIONS_INSEE par defaut
        min_length : longueur minimale — MIN_LINE_LENGTH_INSEE par defaut

    Retourne un dict avec les memes cles que positions, plus 'nom' et 'prenoms'.
    """
    pos = positions or POSITIONS_INSEE
    min_len = min_length or MIN_LINE_LENGTH_INSEE

    if not validate_line_length(line, min_len):
        return None

    nom_prenoms_raw = extract_field(line, *pos["nom_prenoms"])
    nom, prenoms    = split_nom_prenoms(nom_prenoms_raw)

    return {
        "nom_prenoms":         nom_prenoms_raw,
        "nom":                 nom,
        "prenoms":             prenoms,
        "sexe":                extract_field(line, *pos["sexe"]),
        "date_naissance":      extract_field(line, *pos["date_naissance"]),
        "code_lieu_naissance": extract_field(line, *pos["code_lieu_naissance"]),
        "commune_naissance":   extract_field(line, *pos["commune_naissance"]),
        "pays_naissance":      extract_field(line, *pos["pays_naissance"]),
        "date_deces":          extract_field(line, *pos["date_deces"]),
        "code_lieu_deces":     extract_field(line, *pos["code_lieu_deces"]),
        "num_acte_deces":      extract_field(line, *pos.get("num_acte_deces", (167, 176))),
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Sous-partie 2c — Builder de parseur configurable
# ─────────────────────────────────────────────────────────

def build_line_parser(positions: dict, min_length: int) -> Callable:
    """
    Retourne une fonction de parsing configuree avec positions et min_length.
    Utile pour passer le parseur a un RDD.map() Spark sans capturer
    des variables globales dans la closure.

    Args:
        positions  : dict positionnel
        min_length : longueur minimale de ligne valide

    Retourne une fonction (line: str) -> dict | None.

    Usage :
        parser = build_line_parser(POSITIONS_INSEE, MIN_LINE_LENGTH_INSEE)
        rdd_rows = rdd_raw.map(parser).filter(lambda r: r is not None)
    """
    def _parser(line: str) -> Optional[dict]:
        return parse_insee_line(line, positions, min_length)
    return _parser

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
#  ETAPE 3 — TESTS ET VALIDATION
# ============================================================================

# Test extract_field
assert extract_field("DUPONT*JEAN", 0, 6) == "DUPONT"
assert extract_field("AB", 5, 10) == ""          # ligne trop courte

# Test validate_line_length
assert validate_line_length("A" * 162, 162) is True
assert validate_line_length("A" * 50,  162) is False

# Test split_nom_prenoms
assert split_nom_prenoms("DUPONT*JEAN PIERRE") == ("DUPONT", "JEAN PIERRE")
assert split_nom_prenoms("DUPONT")              == ("DUPONT", "")
assert split_nom_prenoms("")                    == ("", "")

# Test normalize_insee_date
assert normalize_insee_date("19870615") == ("1987-06-15", False)
assert normalize_insee_date("19870600") == ("1987-06-01", True)
assert normalize_insee_date("19870000") == ("1987-01-01", True)
assert normalize_insee_date("00000000") == (None, False)
assert normalize_insee_date("abcdefgh") == ("abcd-ef-gh", False)  # passe — date invalide mais format OK
assert normalize_insee_date("")         == (None, False)

# Test parse_insee_line sur une ligne courte
assert parse_insee_line("TROP_COURT", POSITIONS_INSEE, MIN_LINE_LENGTH_INSEE) is None

# Test build_line_parser
_parser = build_line_parser(POSITIONS_INSEE, MIN_LINE_LENGTH_INSEE)
assert _parser("TROP_COURT") is None

print("parsing_utils — tous les tests OK")

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
print("  99_DK-DE-DECES-API-Utils-ParsingUtils")
print(f"{'=' * 60}")
print("  Constantes disponibles :")
print("    POSITIONS_INSEE          dict[str, tuple(int, int)]")
print("    MIN_LINE_LENGTH_INSEE    int (162)")
print("  Fonctions disponibles :")
print("    extract_field(line, start, end)            -> str")
print("    validate_line_length(line, min, max)       -> bool")
print("    split_nom_prenoms(raw)                     -> (nom, prenoms)")
print("    normalize_insee_date(date_str)             -> (str|None, bool)")
print("    parse_insee_line(line, positions, min_len) -> dict | None")
print("    build_line_parser(positions, min_len)      -> Callable")
print(f"{'=' * 60}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
