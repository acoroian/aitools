"""CMS Nursing Home Health Deficiencies ingest.

Discovers the current CSV via the data.cms.gov metastore endpoint
(filenames rotate monthly), downloads, filters to CA, and upserts
into facility_violations with source = 'cms_nh_compare'.

CMS publishes a rolling 3-year window — this ingest is append-only.
Rows that fall out of the window stay in our DB forever because
`ON CONFLICT DO UPDATE` only touches rows whose (source, citation_id)
keys are present in the new batch.

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
from pipeline.violations.normalize import (
    SOURCE_CMS_NH,
    cms_severity_to_scope,
    derive_cms_citation_id,
)
from pipeline.violations.snapshots import archive_raw

log = logging.getLogger(__name__)


class SchemaDriftError(RuntimeError):
    """Raised when a required column is absent from the downloaded CSV."""


def discover_latest_csv_url() -> str:
    """Hit the CMS metastore endpoint and return the current CSV's downloadURL.

    The response has shape `{..., "distribution": [{"downloadURL": "..."}]}`.
    """
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        resp = client.get(settings.cms_nh_metadata_url)
        resp.raise_for_status()
    meta = resp.json()
    distributions = meta.get("distribution") or []
    for dist in distributions:
        url = dist.get("downloadURL") or dist.get("data", {}).get("downloadURL")
        if url and url.lower().endswith(".csv"):
            return url
    raise RuntimeError(f"No CSV downloadURL found in CMS metadata: {meta}")


def download_csv(url: str) -> bytes:
    log.info("Downloading CMS Health Deficiencies CSV from %s", url)
    with httpx.Client(
        timeout=300,
        follow_redirects=True,
        transport=httpx.HTTPTransport(retries=3),
    ) as client:
        resp = client.get(url)
        resp.raise_for_status()
    log.info("Downloaded %.1f MB", len(resp.content) / 1_048_576)
    return resp.content


def parse_csv(raw: bytes) -> pd.DataFrame:
    """Parse raw bytes into a DataFrame, preserving strings for IDs."""
    df = pd.read_csv(
        io.BytesIO(raw),
        dtype={
            "cms_certification_number_ccn": str,
            "deficiency_tag_number": str,
            "zip_code": str,
        },
        low_memory=False,
    )
    required = {
        "cms_certification_number_ccn",
        "state",
        "survey_date",
        "deficiency_prefix",
        "deficiency_tag_number",
        "scope_severity_code",
    }
    missing = required - set(df.columns)
    if missing:
        raise SchemaDriftError(f"CMS CSV missing required columns: {sorted(missing)}")
    return df


def filter_to_ca(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["state"].str.upper() == "CA"].copy()


def _parse_date(val: object) -> date | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return datetime.strptime(str(val), "%Y-%m-%d").date()
    except ValueError:
        return None


def normalize_rows(df: pd.DataFrame) -> list[dict]:
    """Convert a filtered DataFrame to a list of dicts matching facility_violations columns."""

    def _clean(val: object) -> str | None:
        """Return stripped string, or None for NaN/empty."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        return s or None

    rows: list[dict] = []
    for _, r in df.iterrows():
        ccn = str(r["cms_certification_number_ccn"]).strip().zfill(6)
        survey_date = _parse_date(r["survey_date"])
        if survey_date is None:
            continue
        prefix = _clean(r.get("deficiency_prefix")) or "F"
        tag_num = str(r["deficiency_tag_number"]).strip().zfill(4)
        tag = f"{prefix}{tag_num}"
        scope_severity = str(r["scope_severity_code"]).strip().upper()
        corrected_val = _clean(r.get("deficiency_corrected")) or ""
        corrected_str = corrected_val.lower()
        resolved = (
            "provider has date of correction" in corrected_str or "corrected" in corrected_str
        )
        category = _clean(r.get("deficiency_category"))
        description = _clean(r.get("deficiency_description"))

        rows.append(
            {
                "source": SOURCE_CMS_NH,
                "ccn": ccn,
                "citation_id": derive_cms_citation_id(ccn, survey_date, tag, scope_severity),
                "survey_date": survey_date,
                "deficiency_tag": tag,
                "category": category,
                "severity": scope_severity,
                "scope": cms_severity_to_scope(scope_severity),
                "description": description,
                "corrective_action": None,
                "resolved": resolved,
                "resolved_date": _parse_date(r.get("correction_date")),
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
        deficiency_tag = EXCLUDED.deficiency_tag,
        category = EXCLUDED.category,
        severity = EXCLUDED.severity,
        scope = EXCLUDED.scope,
        description = EXCLUDED.description,
        resolved = EXCLUDED.resolved,
        resolved_date = EXCLUDED.resolved_date
""")


def _load_ca_ccn_map(session: Session) -> dict[str, str]:
    """Return {ccn: facility_id} for CA facilities with a CCN."""
    rows = session.execute(
        text("SELECT ccn, id::text FROM facilities WHERE ccn IS NOT NULL AND state = 'CA'")
    ).all()
    return {r[0].strip().zfill(6): r[1] for r in rows}


def run_with_csv(session: Session, raw: bytes) -> dict[str, object]:
    """Run the full ingest using already-downloaded bytes — used in tests and by run()."""
    start = time.monotonic()
    df = parse_csv(raw)
    rows_downloaded = len(df)
    df_ca = filter_to_ca(df)
    rows = normalize_rows(df_ca)

    ccn_map = _load_ca_ccn_map(session)
    ingested = unmatched = invalid = 0

    for row in rows:
        fid = ccn_map.get(row["ccn"])
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
            log.warning("CMS row failed upsert: %s — %s", row["citation_id"], exc)
            invalid += 1

    runtime = time.monotonic() - start
    return {
        "source": SOURCE_CMS_NH,
        "rows_downloaded": rows_downloaded,
        "rows_ingested": ingested,
        "rows_unmatched": unmatched,
        "rows_invalid": invalid,
        "runtime_seconds": round(runtime, 3),
    }


def run() -> dict[str, object]:
    """Production entry point: discover URL, download, archive, ingest, return result."""
    from pipeline.db import get_session

    url = discover_latest_csv_url()
    raw = download_csv(url)
    filename = url.rsplit("/", 1)[-1]
    snapshot_path = archive_raw(SOURCE_CMS_NH, filename, raw)

    with get_session() as session:
        result = run_with_csv(session, raw)
        session.commit()

    result["source_url"] = url
    result["snapshot_path"] = snapshot_path
    log.info("CMS NH Compare ingest complete: %s", result)
    return result
