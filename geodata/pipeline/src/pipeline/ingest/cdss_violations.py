"""CA CDSS Community Care Licensing inspection/violation ingest.

Downloads violation/inspection data from the CDSS CCL system and upserts
into facility_violations with source = 'cdss_ccl'.

CDSS violations are joined to facilities via CDSS License Number
(cdss_id on the facilities table).

Source: https://www.ccld.dss.ca.gov/carefacilitysearch/
"""

from __future__ import annotations

import io
import logging

import httpx
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from pipeline.config import settings
from pipeline.db import get_session

log = logging.getLogger(__name__)

# CDSS severity mapping — Type A is most serious, Type B less so
CDSS_SEVERITY_MAP: dict[str, tuple[str, int]] = {
    "TYPE A": ("serious", 8),
    "A": ("serious", 8),
    "TYPE B": ("moderate", 5),
    "B": ("moderate", 5),
    "INCIDENTAL": ("minor", 2),
    "TECHNICAL": ("minor", 1),
}


def _normalize_severity(raw: str | None) -> tuple[str | None, int]:
    """Return (severity_label, severity_level) from raw CDSS violation type."""
    if not raw:
        return None, 0
    key = str(raw).strip().upper()
    if key in CDSS_SEVERITY_MAP:
        return CDSS_SEVERITY_MAP[key]
    return key.lower(), 3


def download_csv(url: str) -> pd.DataFrame:
    log.info("Downloading CDSS violations from %s", url)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    df = pd.read_csv(io.BytesIO(resp.content), dtype=str, low_memory=False)
    log.info("Downloaded %d violation rows", len(df))
    return df


def normalize_rows(df: pd.DataFrame) -> list[dict]:
    """Normalize CDSS violation rows.

    Expected columns (case-insensitive): FACILITY_NUMBER, VISIT_DATE,
    CITATION_NUMBER, VIOLATION_TYPE, VIOLATION_SECTION, VIOLATION_DESCRIPTION,
    CORRECTIVE_ACTION_PLAN, POC_DATE
    """
    df.columns = [c.strip().upper() for c in df.columns]
    rows = []

    for _, r in df.iterrows():
        cdss_id = str(r.get("FACILITY_NUMBER", "")).strip()
        if not cdss_id or cdss_id == "nan":
            continue

        citation_id = str(r.get("CITATION_NUMBER", "")).strip()
        if not citation_id or citation_id == "nan":
            citation_id = f"cdss-{cdss_id}-{r.get('VISIT_DATE', '')}-{r.name}"

        severity_label, _ = _normalize_severity(r.get("VIOLATION_TYPE"))

        visit_date = None
        raw_date = r.get("VISIT_DATE")
        if raw_date and str(raw_date).strip() not in ("", "nan"):
            try:
                visit_date = pd.to_datetime(raw_date).date()
            except Exception:
                pass

        poc_date = None
        raw_poc = r.get("POC_DATE")
        if raw_poc and str(raw_poc).strip() not in ("", "nan"):
            try:
                poc_date = pd.to_datetime(raw_poc).date()
            except Exception:
                pass

        desc = r.get("VIOLATION_DESCRIPTION")
        desc = str(desc).strip() if desc and str(desc).strip() not in ("", "nan") else None

        corrective = r.get("CORRECTIVE_ACTION_PLAN")
        corrective = (
            str(corrective).strip()
            if corrective and str(corrective).strip() not in ("", "nan")
            else None
        )

        section = r.get("VIOLATION_SECTION")
        section = (
            str(section).strip() if section and str(section).strip() not in ("", "nan") else None
        )

        rows.append(
            {
                "cdss_id": cdss_id,
                "survey_date": visit_date,
                "citation_id": citation_id,
                "severity": severity_label,
                "deficiency_tag": section,
                "description": desc,
                "corrective_action": corrective,
                "resolved_date": poc_date,
                "resolved": poc_date is not None,
            }
        )

    log.info("Normalized %d violation rows", len(rows))
    return rows


def upsert_violations(rows: list[dict]) -> dict[str, int]:
    """Upsert CDSS violations into facility_violations. Returns counts."""
    if not rows:
        return {"inserted": 0, "updated": 0, "skipped_no_facility": 0}

    inserted = updated = skipped = 0

    with get_session() as session:
        # Build mapping of cdss_id → facility_id
        cdss_ids = list({r["cdss_id"] for r in rows})
        facility_map = _get_facility_map(session, cdss_ids)

        for row in rows:
            facility_id = facility_map.get(row["cdss_id"])
            if not facility_id:
                skipped += 1
                continue

            result = session.execute(
                text("""
                    INSERT INTO facility_violations
                        (facility_id, source, survey_date, deficiency_tag, severity,
                         description, corrective_action, citation_id, resolved, resolved_date)
                    VALUES
                        (:facility_id, 'cdss_ccl', :survey_date, :deficiency_tag, :severity,
                         :description, :corrective_action, :citation_id, :resolved, :resolved_date)
                    ON CONFLICT (source, citation_id)
                        WHERE source IS NOT NULL AND citation_id IS NOT NULL
                    DO UPDATE SET
                        survey_date = EXCLUDED.survey_date,
                        severity = EXCLUDED.severity,
                        description = EXCLUDED.description,
                        corrective_action = EXCLUDED.corrective_action,
                        resolved = EXCLUDED.resolved,
                        resolved_date = EXCLUDED.resolved_date
                """),
                {
                    "facility_id": str(facility_id),
                    "survey_date": row["survey_date"],
                    "deficiency_tag": row["deficiency_tag"],
                    "severity": row["severity"],
                    "description": row["description"],
                    "corrective_action": row["corrective_action"],
                    "citation_id": row["citation_id"],
                    "resolved": row["resolved"],
                    "resolved_date": row["resolved_date"],
                },
            )
            if result.rowcount == 1:
                inserted += 1
            else:
                updated += 1

    log.info(
        "CDSS violations upsert: %d inserted, %d updated, %d skipped (no facility)",
        inserted,
        updated,
        skipped,
    )
    return {"inserted": inserted, "updated": updated, "skipped_no_facility": skipped}


def _get_facility_map(session: Session, cdss_ids: list[str]) -> dict[str, str]:
    """Return {cdss_id: facility_id} for known facilities."""
    if not cdss_ids:
        return {}
    rows = session.execute(
        text("SELECT cdss_id, id::text FROM facilities WHERE cdss_id = ANY(:ids)"),
        {"ids": cdss_ids},
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def run() -> dict[str, int]:
    """Full CDSS violations ingest: download → normalize → upsert."""
    df_raw = download_csv(settings.cdss_violations_csv_url)
    rows = normalize_rows(df_raw)
    result = upsert_violations(rows)
    result["total_rows"] = len(rows)
    return result
