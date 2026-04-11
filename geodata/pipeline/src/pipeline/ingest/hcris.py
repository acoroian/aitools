"""
CMS HCRIS cost report ingest — Home Health Agency + Hospice.

Downloads the annual cost report ZIP from CMS, parses the RPT (report
header) and NMRC (numeric data) CSV files, and upserts revenue figures
into facility_financials, joined via CCN → facilities.ccn.

HCRIS format:
  *_RPT_*.CSV  — one row per cost report period
    REPT_REC_NUM  unique report ID
    PRVDR_NUM     CMS Certification Number (CCN), e.g. "057000"
    FY_BGN_DT     fiscal year begin MM/DD/YYYY
    FY_END_DT     fiscal year end   MM/DD/YYYY
    NPT_STUS      settlement status: F=final, S=settled, T=tentative, U=unsubmitted

  *_NMRC_*.CSV — numeric worksheet cells
    REPT_REC_NUM  links to RPT
    WKST_CD       worksheet code, e.g. "S700000"
    LINE_NUM      5-char line, e.g. "00100"
    CLMN_NUM      4-char column, e.g. "0100"
    ITM_VAL_NUM   the numeric value

Revenue worksheet codes:
  HHA (Form 1728-18):
    Worksheet S-7 Part I — Total charges by payer
      Gross revenue:       WKST='S700000', LINE='00100', COL='0200'
      Medicare revenue:    WKST='S700000', LINE='00100', COL='0100'
      Total visits:        WKST='S600000', LINE='00200', COL='0100'

  Hospice (Form 1984-14):
    Worksheet S-3 — Revenue
      Gross revenue:       WKST='S300000', LINE='00100', COL='0200'
      Medicare revenue:    WKST='S300000', LINE='00100', COL='0100'
      Total patients:      WKST='S200000', LINE='00100', COL='0100'

Sources:
  https://www.cms.gov/Research-Statistics-Data-and-Systems/
    Downloadable-Public-Use-Files/Cost-Reports/Home-Health-Agency
  https://www.cms.gov/Research-Statistics-Data-and-Systems/
    Downloadable-Public-Use-Files/Cost-Reports/Hospice
"""

from __future__ import annotations

import io
import logging
import zipfile
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

import httpx
import pandas as pd

from pipeline.config import settings
from pipeline.db import get_session
from pipeline.models import Facility, FacilityFinancial

log = logging.getLogger(__name__)

ProviderType = Literal["hha", "hospice"]


@dataclass
class RevenueSpec:
    """Worksheet coordinates for a revenue line item."""
    wkst: str
    line: str
    col: str
    field: str          # FacilityFinancial attribute name
    scale: int = 1      # multiply raw value by this (some forms report in $1000s)


# Revenue lines to extract per provider type
REVENUE_SPECS: dict[ProviderType, list[RevenueSpec]] = {
    "hha": [
        RevenueSpec("S700000", "00100", "0200", "gross_revenue"),
        RevenueSpec("S700000", "00100", "0100", "medicare_revenue"),
        RevenueSpec("S700000", "00200", "0200", "medicaid_revenue"),
        RevenueSpec("S700000", "00400", "0200", "net_revenue"),
        RevenueSpec("S600000", "00200", "0100", "total_visits"),
    ],
    "hospice": [
        RevenueSpec("S300000", "00100", "0200", "gross_revenue"),
        RevenueSpec("S300000", "00100", "0100", "medicare_revenue"),
        RevenueSpec("S300000", "00200", "0200", "medicaid_revenue"),
        RevenueSpec("S300000", "00400", "0200", "net_revenue"),
        RevenueSpec("S200000", "00100", "0100", "total_patients"),
    ],
}


def _download_zip(url: str) -> bytes:
    log.info("Downloading HCRIS ZIP from %s", url)
    with httpx.Client(timeout=300, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    log.info("Downloaded %.1f MB", len(resp.content) / 1_048_576)
    return resp.content


def _find_csv(zf: zipfile.ZipFile, suffix: str) -> str | None:
    """Return the name of the first ZIP member whose name contains suffix (case-insensitive)."""
    for name in zf.namelist():
        if suffix.lower() in name.lower() and name.lower().endswith(".csv"):
            return name
    return None


def _parse_rpt(zf: zipfile.ZipFile, rpt_name: str) -> pd.DataFrame:
    """Parse the RPT file → DataFrame with REPT_REC_NUM, PRVDR_NUM, FY_END_DT."""
    log.info("Parsing RPT file: %s", rpt_name)
    with zf.open(rpt_name) as f:
        df = pd.read_csv(f, dtype=str, low_memory=False)
    df.columns = [c.strip().upper() for c in df.columns]

    # Keep only settled/final reports (ignore tentative/in-progress)
    if "NPT_STUS" in df.columns:
        df = df[df["NPT_STUS"].isin(["F", "S", "As Submitted"])]

    df["FY_END_DT"] = pd.to_datetime(df["FY_END_DT"], errors="coerce")
    df["fy_year"] = df["FY_END_DT"].dt.year.astype("Int64")
    return df[["REPT_REC_NUM", "PRVDR_NUM", "fy_year"]].dropna()


def _parse_nmrc_for_specs(
    zf: zipfile.ZipFile,
    nmrc_name: str,
    valid_rpt_nums: set[str],
    specs: list[RevenueSpec],
) -> pd.DataFrame:
    """
    Stream the NMRC file in chunks, keep only rows matching valid report IDs
    and the worksheet/line/col coords we care about.

    Returns DataFrame: REPT_REC_NUM, field, ITM_VAL_NUM
    """
    log.info("Parsing NMRC file: %s (filtering to %d reports)", nmrc_name, len(valid_rpt_nums))

    # Build a lookup set of (wkst, line, col) → field name
    coord_map: dict[tuple[str, str, str], str] = {
        (s.wkst, s.line, s.col): s.field for s in specs
    }

    collected: list[dict] = []
    chunk_size = 200_000

    with zf.open(nmrc_name) as f:
        for chunk in pd.read_csv(f, dtype=str, low_memory=False, chunksize=chunk_size):
            chunk.columns = [c.strip().upper() for c in chunk.columns]

            # Filter to relevant report IDs first (big reduction)
            chunk = chunk[chunk["REPT_REC_NUM"].isin(valid_rpt_nums)]
            if chunk.empty:
                continue

            # Normalise coordinate columns (strip whitespace, zero-pad if needed)
            chunk["WKST_CD"] = chunk["WKST_CD"].str.strip()
            chunk["LINE_NUM"] = chunk["LINE_NUM"].str.strip().str.zfill(5)
            chunk["CLMN_NUM"] = chunk["CLMN_NUM"].str.strip().str.zfill(4)

            # Keep only rows matching our specs
            mask = chunk.apply(
                lambda r: (r["WKST_CD"], r["LINE_NUM"], r["CLMN_NUM"]) in coord_map,
                axis=1,
            )
            chunk = chunk[mask]
            if chunk.empty:
                continue

            chunk["field"] = chunk.apply(
                lambda r: coord_map[(r["WKST_CD"], r["LINE_NUM"], r["CLMN_NUM"])],
                axis=1,
            )
            chunk["val"] = pd.to_numeric(chunk["ITM_VAL_NUM"], errors="coerce")
            collected.append(chunk[["REPT_REC_NUM", "field", "val"]])

    if not collected:
        log.warning("No matching NMRC rows found — worksheet codes may differ from expected")
        return pd.DataFrame(columns=["REPT_REC_NUM", "field", "val"])

    return pd.concat(collected, ignore_index=True)


def _load_ca_ccns() -> dict[str, str]:
    """Return {ccn: facility_id} for all CA facilities that have a CCN."""
    with get_session() as session:
        rows = (
            session.query(Facility.ccn, Facility.id)
            .filter(Facility.ccn.isnot(None))
            .all()
        )
    return {r.ccn.strip(): str(r.id) for r in rows}


def run(provider_type: ProviderType) -> dict[str, int]:
    """
    Full HCRIS ingest for a given provider type.
    Returns {upserted, skipped, no_ccn_match}.
    """
    url = settings.hcris_hha_url if provider_type == "hha" else settings.hcris_hospice_url
    source_tag = f"hcris_{provider_type}"

    # --- Download ---
    raw = _download_zip(url)
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        log.info("ZIP contents: %s", zf.namelist())

        rpt_name = _find_csv(zf, "rpt")
        nmrc_name = _find_csv(zf, "nmrc")

        if not rpt_name or not nmrc_name:
            raise FileNotFoundError(
                f"Could not find RPT/NMRC CSVs in ZIP. Contents: {zf.namelist()}"
            )

        # --- Parse RPT → {rpt_rec_num: (ccn, fy_year)} ---
        rpt_df = _parse_rpt(zf, rpt_name)
        log.info("RPT: %d settled/final reports", len(rpt_df))

        # --- Load CA facilities that have CCNs ---
        ca_ccns = _load_ca_ccns()
        log.info("CA facilities with CCN in DB: %d", len(ca_ccns))

        if not ca_ccns:
            log.warning("No CCNs loaded yet — run CDPH ingest + migration first")
            return {"upserted": 0, "skipped": 0, "no_ccn_match": 0}

        # Keep only reports for our CA facilities
        rpt_df = rpt_df[rpt_df["PRVDR_NUM"].isin(ca_ccns)]
        log.info("Reports matching CA facilities: %d", len(rpt_df))

        if rpt_df.empty:
            log.warning(
                "No HCRIS reports match CA CCNs. Sample CCNs in file: %s",
                rpt_df["PRVDR_NUM"].head(5).tolist() if len(rpt_df) else "N/A",
            )
            return {"upserted": 0, "skipped": 0, "no_ccn_match": len(ca_ccns)}

        valid_rpt_nums = set(rpt_df["REPT_REC_NUM"].tolist())

        # --- Parse NMRC → pivot to {rpt_rec_num: {field: value}} ---
        specs = REVENUE_SPECS[provider_type]
        nmrc_df = _parse_nmrc_for_specs(zf, nmrc_name, valid_rpt_nums, specs)

    if nmrc_df.empty:
        log.warning("No revenue data extracted from NMRC file")
        return {"upserted": 0, "skipped": len(rpt_df), "no_ccn_match": 0}

    # Pivot: one row per report, columns = field names
    pivoted = nmrc_df.pivot_table(
        index="REPT_REC_NUM", columns="field", values="val", aggfunc="first"
    ).reset_index()

    # Join with RPT to get CCN + year
    merged = rpt_df.merge(pivoted, on="REPT_REC_NUM", how="left")

    # --- Upsert into facility_financials ---
    upserted = skipped = no_match = 0

    with get_session() as session:
        for _, row in merged.iterrows():
            ccn = str(row["PRVDR_NUM"]).strip()
            facility_id = ca_ccns.get(ccn)
            if not facility_id:
                no_match += 1
                continue

            fy_year = row.get("fy_year")
            if pd.isna(fy_year):
                skipped += 1
                continue
            fy_year = int(fy_year)

            # Check for existing record (upsert by facility_id + year + source)
            existing = (
                session.query(FacilityFinancial)
                .filter_by(facility_id=facility_id, year=fy_year, source=source_tag)
                .first()
            )
            if existing is None:
                existing = FacilityFinancial(
                    facility_id=facility_id,
                    year=fy_year,
                    source=source_tag,
                )
                session.add(existing)

            def _int(val: object) -> int | None:
                if val is None or pd.isna(val):
                    return None
                return int(float(val))

            for spec in specs:
                val = _int(row.get(spec.field))
                if val is not None and hasattr(existing, spec.field):
                    setattr(existing, spec.field, val)

            upserted += 1

    log.info(
        "HCRIS %s upserted=%d skipped=%d no_ccn_match=%d",
        provider_type, upserted, skipped, no_match,
    )
    return {"upserted": upserted, "skipped": skipped, "no_ccn_match": no_match}
