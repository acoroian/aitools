"""
CA HCAI Long-Term Care Annual Financial Disclosure ingest.

HCAI (Health Care Access and Information, formerly OSHPD) publishes an
annual "Selected File" XLSX of long-term care financial disclosures for
SNFs, CLHFs, SNF/RES, and ICFs. The 2022 extract has 1,344 facilities
and 221 columns.

Source index:
  https://data.chhs.ca.gov/dataset/long-term-care-facility-disclosure-report-data

Join strategy:
  HCAI uses its own facility numbering (FAC_NO) which does NOT match the
  CDPH license_number nor an OSHPD ID in our DB. We join by
  normalized (name, zip_code) — verified against sample rows that the
  HCAI FAC_NAME + ZIP_CODE uniquely identifies facilities already in
  our CDPH-sourced facilities table.

HCAI Selected File column names (verified against lafd-1222-sub-selected.xlsx):
  FAC_NO           9-digit HCAI facility number
  FAC_NAME         facility name
  CITY             city
  ZIP_CODE         5-digit ZIP
  LIC_CAT          SNF | CLHF | SNF/RES | ICF
  TOT_HC_REV       total healthcare revenue   → gross_revenue
  OTH_OP_REV       other operating revenue
  NET_INCOME       net income                 → net_income
  GR_RT_MCAR       Medicare gross revenue     → medicare_revenue
  GR_RT_MCAL       Medi-Cal gross revenue     → medicaid_revenue
"""

from __future__ import annotations

import io
import logging
import re

import httpx
import pandas as pd

from pipeline.config import settings
from pipeline.db import get_session
from pipeline.models import Facility, FacilityFinancial

log = logging.getLogger(__name__)


# HCAI column → FacilityFinancial attribute
_REVENUE_COLS: dict[str, str] = {
    "TOT_HC_REV": "gross_revenue",
    "NET_INCOME": "net_income",
    "GR_RT_MCAR": "medicare_revenue",
    "GR_RT_MCAL": "medicaid_revenue",
}


def _normalize_name(s: object) -> str:
    """Uppercase, strip punctuation/whitespace for fuzzy name matching."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).upper().strip()
    # Strip common punctuation and collapse whitespace
    s = re.sub(r"[.,'&/-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalize_zip(s: object) -> str:
    """Return first 5 digits of a zip; empty if not parseable."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    digits = re.sub(r"\D", "", str(s))
    return digits[:5] if len(digits) >= 5 else ""


def _download(url: str) -> bytes:
    log.info("Downloading HCAI LTC Excel from %s", url)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    log.info("Downloaded %.1f MB", len(resp.content) / 1_048_576)
    return resp.content


def _parse_excel(raw: bytes) -> pd.DataFrame:
    """Read the single sheet from the HCAI Selected File."""
    xl = pd.ExcelFile(io.BytesIO(raw), engine="openpyxl")
    sheet = xl.sheet_names[0]
    df = xl.parse(sheet)
    log.info("Parsed %d rows x %d cols from sheet '%s'", len(df), len(df.columns), sheet)
    return df


def _load_facility_index() -> dict[tuple[str, str], str]:
    """
    Return {(normalized_name, zip5): facility_id} for all CA facilities
    that have a non-null name and zip. Used to join HCAI rows to our
    facilities table.
    """
    with get_session() as session:
        rows = (
            session.query(Facility.id, Facility.name, Facility.zip)
            .filter(Facility.name.isnot(None))
            .filter(Facility.zip.isnot(None))
            .all()
        )
    index: dict[tuple[str, str], str] = {}
    for r in rows:
        key = (_normalize_name(r.name), _normalize_zip(r.zip))
        if key[0] and key[1]:
            index[key] = str(r.id)
    return index


def run() -> dict[str, int]:
    """Ingest HCAI LTC annual financial data. Returns {upserted, skipped, no_match}."""
    raw = _download(settings.hcai_snf_url)
    df = _parse_excel(raw)

    required = ["FAC_NAME", "ZIP_CODE"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"HCAI file missing required columns {missing}. Available: {list(df.columns)[:30]}"
        )

    index = _load_facility_index()
    log.info("CA facilities in name+zip index: %d", len(index))

    if not index:
        log.warning("No facilities loaded — run CDPH ingest first")
        return {"upserted": 0, "skipped": 0, "no_match": 0}

    year = settings.hcai_year
    source_tag = "hcai"
    upserted = skipped = no_match = 0

    def _int(val: object) -> int | None:
        if val is None or pd.isna(val):
            return None
        try:
            return int(float(str(val).replace("$", "").replace(",", "").strip()))
        except (ValueError, OverflowError):
            return None

    # Dedup by (name, zip) — take first row per facility (Selected File is
    # already one row per facility, but be defensive)
    seen: set[tuple[str, str]] = set()

    with get_session() as session:
        for _, row in df.iterrows():
            key = (_normalize_name(row.get("FAC_NAME")), _normalize_zip(row.get("ZIP_CODE")))
            if not key[0] or not key[1]:
                skipped += 1
                continue
            if key in seen:
                continue
            seen.add(key)

            facility_id = index.get(key)
            if not facility_id:
                no_match += 1
                continue

            existing = (
                session.query(FacilityFinancial)
                .filter_by(facility_id=facility_id, year=year, source=source_tag)
                .first()
            )
            if existing is None:
                existing = FacilityFinancial(facility_id=facility_id, year=year, source=source_tag)
                session.add(existing)

            for hcai_col, field in _REVENUE_COLS.items():
                if hcai_col in df.columns and hasattr(existing, field):
                    val = _int(row.get(hcai_col))
                    if val is not None:
                        setattr(existing, field, val)

            upserted += 1

    log.info("HCAI LTC upserted=%d skipped=%d no_match=%d", upserted, skipped, no_match)
    return {"upserted": upserted, "skipped": skipped, "no_match": no_match}
