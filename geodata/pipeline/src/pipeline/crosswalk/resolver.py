"""
CA Licensed Facility Crosswalk — entity resolution.

Downloads the XLSX crosswalk from CHHS and populates cms_npi and oshpd_id
on existing facility records that were ingested from CDPH.

Source: https://data.chhs.ca.gov/dataset/licensed-and-certified-healthcare-facility-crosswalk
"""

import io
import logging
import zipfile

import httpx
import pandas as pd

from pipeline.config import settings
from pipeline.db import get_session
from pipeline.models import Facility

log = logging.getLogger(__name__)


def download_crosswalk(url: str) -> pd.DataFrame:
    log.info("Downloading facility crosswalk from %s", url)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()

    # The crosswalk is distributed as a ZIP containing XLSX files
    if url.endswith(".zip") or resp.headers.get("content-type", "").startswith("application/zip"):
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            # Find the main crosswalk XLSX (largest file in the ZIP)
            xlsx_files = [f for f in zf.namelist() if f.endswith(".xlsx")]
            if not xlsx_files:
                raise ValueError("No XLSX found in crosswalk ZIP")
            target = max(xlsx_files, key=lambda f: zf.getinfo(f).file_size)
            with zf.open(target) as f:
                df = pd.read_excel(io.BytesIO(f.read()), dtype=str)
    else:
        df = pd.read_excel(io.BytesIO(resp.content), dtype=str)

    log.info("Downloaded crosswalk with %d rows, columns: %s", len(df), list(df.columns))
    return df


def normalize_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """Extract CDPH ID, NPI, and OSHPD ID columns."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Column names vary — find by fuzzy match
    def find_col(*candidates: str) -> str | None:
        for c in candidates:
            match = next((col for col in df.columns if c in col), None)
            if match:
                return match
        return None

    cdph_col = find_col("facid", "cdph_id", "elms_id", "license")
    npi_col = find_col("npi", "cms_npi")
    oshpd_col = find_col("oshpd", "hcai")

    out = pd.DataFrame()
    if cdph_col:
        out["cdph_id"] = df[cdph_col].str.strip()
    if npi_col:
        out["cms_npi"] = df[npi_col].str.strip()
    if oshpd_col:
        out["oshpd_id"] = df[oshpd_col].str.strip()

    # Drop rows with no CDPH ID
    if "cdph_id" in out.columns:
        out = out[out["cdph_id"].notna() & (out["cdph_id"] != "")]

    log.info("Crosswalk normalized: %d rows with cols %s", len(out), list(out.columns))
    return out


def apply_crosswalk(df: pd.DataFrame) -> dict[str, int]:
    """Match crosswalk rows to facility records and populate NPI/OSHPD IDs."""
    if "cdph_id" not in df.columns:
        log.warning("No cdph_id column in crosswalk — cannot resolve")
        return {"matched": 0, "unmatched": 0}

    cdph_ids = df["cdph_id"].dropna().unique().tolist()
    matched = unmatched = 0

    with get_session() as session:
        facilities: dict[str, Facility] = {
            f.cdph_id: f
            for f in session.query(Facility).filter(Facility.cdph_id.in_(cdph_ids)).all()
            if f.cdph_id
        }

        for _, row in df.iterrows():
            cdph_id = row.get("cdph_id")
            if not cdph_id or cdph_id not in facilities:
                unmatched += 1
                continue

            facility = facilities[cdph_id]
            if "cms_npi" in df.columns and pd.notna(row.get("cms_npi")):
                facility.cms_npi = str(row["cms_npi"]).strip() or None
            if "oshpd_id" in df.columns and pd.notna(row.get("oshpd_id")):
                facility.oshpd_id = str(row["oshpd_id"]).strip() or None
            matched += 1

    log.info("Crosswalk applied: %d matched, %d unmatched", matched, unmatched)
    return {"matched": matched, "unmatched": unmatched}


def run() -> dict[str, int]:
    """Full crosswalk run: download → normalize → apply."""
    df_raw = download_crosswalk(settings.cdph_crosswalk_url)
    df = normalize_crosswalk(df_raw)
    return apply_crosswalk(df)
