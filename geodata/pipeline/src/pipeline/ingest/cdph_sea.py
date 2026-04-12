"""CDPH Health Facilities State Enforcement Actions ingest.

Discovers the current XLSX via the CHHS CKAN `package_show` endpoint,
downloads it, and upserts into facility_violations with source =
'cdph_sea'. CDPH publishes the full historical file annually, so this
ingest is a full refresh — `ON CONFLICT DO UPDATE` keeps the composite
(source, citation_id) key stable across years.

Schema reference: docs/superpowers/specs/2026-04-11-phase-3-violation-enrichment-design.md
"""

from __future__ import annotations

import io
import logging
import time
from datetime import date, datetime

import httpx
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from pipeline.config import settings
from pipeline.violations.normalize import SOURCE_CDPH_SEA
from pipeline.violations.snapshots import archive_raw

log = logging.getLogger(__name__)


# Map from our internal names → real CDPH XLSX column names.
# Captured from sea_final_20240730.xlsx (2024-07-30 release).
COLUMN_MAP: dict[str, str] = {
    "facid": "FACID",
    "facname": "FACILITY_NAME",
    "factype": "FAC_TYPE_CODE",
    "citation_id": "PENALTY_NUMBER",
    "citation_issue_date": "PENALTY_ISSUE_DATE",
    "citation_class_initial": "CLASS_ASSESSED_INITIAL",
    "citation_class_final": "CLASS_ASSESSED_FINAL",
    "penalty_amount": "TOTAL_AMOUNT_DUE_FINAL",
    "penalty_detail": "PENALTY_DETAIL",
    "penalty_category": "PENALTY_CATEGORY",
}


class SchemaDriftError(RuntimeError):
    """Raised when a required column is absent from the downloaded XLSX."""


def discover_latest_xlsx_url() -> str:
    """Hit the CHHS CKAN package_show endpoint and return the main SEA XLSX URL.

    The package bundles several resources (data dictionary, lookups, narratives).
    We pick the first XLSX whose name contains 'state enforcement actions' and
    looks like a data file (not the dictionary/lookup).
    """
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        resp = client.get(settings.cdph_sea_metadata_url)
        resp.raise_for_status()
    result = resp.json().get("result", {})
    resources = result.get("resources", []) or []

    def _is_data_xlsx(res: dict) -> bool:
        if (res.get("format") or "").lower() != "xlsx":
            return False
        name = (res.get("name") or "").lower()
        if "dictionary" in name or "lookup" in name:
            return False
        return "state enforcement actions" in name

    for res in resources:
        if _is_data_xlsx(res):
            url = res.get("url")
            if url:
                return url
    raise RuntimeError(f"No SEA XLSX downloadURL found in CDPH metadata: {result}")


def download_xlsx(url: str) -> bytes:
    log.info("Downloading CDPH SEA XLSX from %s", url)
    with httpx.Client(
        timeout=300,
        follow_redirects=True,
        transport=httpx.HTTPTransport(retries=3),
    ) as client:
        resp = client.get(url)
        resp.raise_for_status()
    log.info("Downloaded %.1f MB", len(resp.content) / 1_048_576)
    return resp.content


def parse_xlsx(raw: bytes) -> pd.DataFrame:
    """Parse raw XLSX bytes into a DataFrame, preserving FACID as string."""
    df = pd.read_excel(
        io.BytesIO(raw),
        dtype={COLUMN_MAP["facid"]: str},
    )
    required = {
        COLUMN_MAP["facid"],
        COLUMN_MAP["citation_id"],
        COLUMN_MAP["citation_issue_date"],
        COLUMN_MAP["citation_class_final"],
    }
    missing = required - set(df.columns)
    if missing:
        raise SchemaDriftError(f"CDPH SEA XLSX missing required columns: {sorted(missing)}")
    return df


def _clean(val: object) -> str | None:
    """Return stripped string, or None for NaN/empty."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s or None


def _parse_date(val: object) -> date | None:
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.to_pydatetime().date()
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_penalty(val: object) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _normalize_severity(val: object) -> str | None:
    """Extract an AA/A/B class code from the raw CDPH severity string.

    CDPH's CLASS_ASSESSED_FINAL contains values like 'A', 'B', 'AA',
    'A Trebled', 'B First', 'Dismissed by Court'. We keep AA/A/B and
    collapse any suffix; anything else returns None.
    """
    raw = _clean(val)
    if raw is None:
        return None
    token = raw.split()[0].upper().rstrip("\t").strip()
    if token in {"AA", "A", "B"}:
        return token
    return None


def normalize_rows(df: pd.DataFrame) -> list[dict]:
    """Convert a DataFrame to a list of dicts matching facility_violations columns."""
    rows: list[dict] = []
    for _, r in df.iterrows():
        cdph_id = _clean(r.get(COLUMN_MAP["facid"]))
        citation_id = _clean(r.get(COLUMN_MAP["citation_id"]))
        survey_date = _parse_date(r.get(COLUMN_MAP["citation_issue_date"]))
        if not cdph_id or not citation_id or survey_date is None:
            continue
        # Prefer the final assessed class; fall back to initial if final is blank.
        severity = _normalize_severity(r.get(COLUMN_MAP["citation_class_final"]))
        if severity is None:
            severity = _normalize_severity(r.get(COLUMN_MAP["citation_class_initial"]))
        # Description: prefer PENALTY_DETAIL, fall back to PENALTY_CATEGORY.
        description = _clean(r.get(COLUMN_MAP["penalty_detail"])) or _clean(
            r.get(COLUMN_MAP["penalty_category"])
        )
        penalty_amount = _parse_penalty(r.get(COLUMN_MAP["penalty_amount"]))

        rows.append(
            {
                "source": SOURCE_CDPH_SEA,
                "cdph_id": cdph_id,
                "citation_id": citation_id,
                "survey_date": survey_date,
                "deficiency_tag": None,
                "category": None,
                "severity": severity,
                "scope": None,
                "description": description,
                "corrective_action": None,
                "resolved": False,
                "resolved_date": None,
                "penalty_amount": penalty_amount,
            }
        )
    return rows


_UPSERT_SQL = text("""
    INSERT INTO facility_violations (
        facility_id, source, citation_id, survey_date,
        deficiency_tag, category, severity, scope,
        description, corrective_action, resolved, resolved_date
    ) VALUES (
        :facility_id, :source, :citation_id, :survey_date,
        :deficiency_tag, :category, :severity, :scope,
        :description, :corrective_action, :resolved, :resolved_date
    )
    ON CONFLICT (source, citation_id) DO UPDATE SET
        facility_id = EXCLUDED.facility_id,
        survey_date = EXCLUDED.survey_date,
        severity = EXCLUDED.severity,
        description = EXCLUDED.description
""")


def _load_ca_cdph_map(session: Session) -> dict[str, str]:
    """Return {cdph_id: facility_id} for CA facilities with a CDPH ID."""
    rows = session.execute(
        text("SELECT cdph_id, id::text FROM facilities WHERE cdph_id IS NOT NULL AND state = 'CA'")
    ).all()
    return {r[0].strip(): r[1] for r in rows}


def run_with_xlsx(session: Session, raw: bytes) -> dict[str, object]:
    """Run the full ingest using already-downloaded bytes — used in tests and by run()."""
    start = time.monotonic()
    df = parse_xlsx(raw)
    rows_downloaded = len(df)
    rows = normalize_rows(df)

    cdph_map = _load_ca_cdph_map(session)
    ingested = unmatched = invalid = 0

    for row in rows:
        fid = cdph_map.get(row["cdph_id"])
        if not fid:
            unmatched += 1
            continue
        try:
            session.execute(
                _UPSERT_SQL,
                {
                    "facility_id": fid,
                    "source": row["source"],
                    "citation_id": row["citation_id"],
                    "survey_date": row["survey_date"],
                    "deficiency_tag": row["deficiency_tag"],
                    "category": row["category"],
                    "severity": row["severity"],
                    "scope": row["scope"],
                    "description": row["description"],
                    "corrective_action": row["corrective_action"],
                    "resolved": row["resolved"],
                    "resolved_date": row["resolved_date"],
                },
            )
            ingested += 1
        except Exception as exc:
            log.warning("CDPH SEA row failed upsert: %s — %s", row["citation_id"], exc)
            invalid += 1

    runtime = time.monotonic() - start
    return {
        "source": SOURCE_CDPH_SEA,
        "rows_downloaded": rows_downloaded,
        "rows_ingested": ingested,
        "rows_unmatched": unmatched,
        "rows_invalid": invalid,
        "runtime_seconds": round(runtime, 3),
    }


def run() -> dict[str, object]:
    """Production entry point: discover URL, download, archive, ingest, return result."""
    from pipeline.db import get_session

    url = discover_latest_xlsx_url()
    raw = download_xlsx(url)
    filename = url.rsplit("/", 1)[-1]
    snapshot_path = archive_raw(SOURCE_CDPH_SEA, filename, raw)

    with get_session() as session:
        result = run_with_xlsx(session, raw)
        session.commit()

    result["source_url"] = url
    result["snapshot_path"] = snapshot_path
    log.info("CDPH SEA ingest complete: %s", result)
    return result
