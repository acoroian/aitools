"""Shared severity mappings, source constants, citation ID derivation.

Keeping every severity-related constant in one module prevents drift
between ingest modules and the rollup SQL helper functions (which
re-encode the same mapping in SQL — see migration 004).
"""

from __future__ import annotations

from datetime import date

SOURCE_CMS_NH = "cms_nh_compare"
SOURCE_CDPH_SEA = "cdph_sea"

# CMS scope/severity grid (letters A-L):
#   rows = severity (1=No actual harm, 4=Immediate jeopardy)
#   cols = scope (isolated / pattern / widespread)
#
#         isolated  pattern  widespread
#   S1:     A         B         C
#   S2:     D         E         F
#   S3:     G         H         I
#   S4:     J         K         L
#
# Our normalized level scale (0-10) conflates severity and scope
# slightly so the rollup can rank letters cross-source:
_CMS_SEVERITY_LEVEL: dict[str, int] = {
    "A": 1,
    "B": 2,
    "C": 3,
    "D": 4,
    "E": 5,
    "F": 6,
    "G": 6,
    "H": 7,
    "I": 7,
    "J": 8,
    "K": 9,
    "L": 10,
}

_CMS_SCOPE: dict[str, str] = {
    "A": "isolated",
    "B": "pattern",
    "C": "widespread",
    "D": "isolated",
    "E": "pattern",
    "F": "widespread",
    "G": "isolated",
    "H": "pattern",
    "I": "widespread",
    "J": "isolated",
    "K": "pattern",
    "L": "widespread",
}

# CDPH citation classes (from Health & Safety Code §1280):
#   Class AA — willful violation resulting in death (IJ-equivalent, rare)
#   Class A  — imminent danger of death or serious harm (IJ-equivalent)
#   Class B  — direct or immediate relationship to health/safety, no imminent danger
_CDPH_SEVERITY_LEVEL: dict[str, int] = {
    "AA": 10,
    "A": 8,
    "B": 4,
}

# Any level >= 8 counts as immediate jeopardy in the rollup.
_IJ_THRESHOLD = 8


def cms_severity_level(letter: str | None) -> int | None:
    if letter is None:
        return None
    return _CMS_SEVERITY_LEVEL.get(letter.upper())


def cms_severity_to_scope(letter: str | None) -> str | None:
    if letter is None:
        return None
    return _CMS_SCOPE.get(letter.upper())


def cdph_severity_level(code: str | None) -> int | None:
    if code is None:
        return None
    return _CDPH_SEVERITY_LEVEL.get(code.upper())


def is_immediate_jeopardy(source: str, severity: str | None) -> bool:
    if severity is None:
        return False
    if source == SOURCE_CMS_NH:
        level = cms_severity_level(severity)
    elif source == SOURCE_CDPH_SEA:
        level = cdph_severity_level(severity)
    else:
        return False
    return level is not None and level >= _IJ_THRESHOLD


def derive_cms_citation_id(ccn: str, survey_date: date, tag: str, scope_severity: str) -> str:
    """Deterministic composite key for CMS rows, which have no native citation ID.

    Format: {ccn}_{YYYY-MM-DD}_{tag}_{scope_severity_letter}
    Used as the stable key for the `(source, citation_id)` unique constraint.
    """
    return f"{ccn}_{survey_date.isoformat()}_{tag}_{scope_severity}"
