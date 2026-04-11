"""
CA CDPH Healthcare Facility Locations ingest.

Downloads the monthly CSV from data.chhs.ca.gov and upserts facility records
into PostGIS. CDPH already provides lat/lon so no geocoding is needed.

Source: https://data.chhs.ca.gov/dataset/healthcare-facility-locations
"""

import io
import logging
from datetime import date

import httpx
import pandas as pd

from pipeline.config import settings
from pipeline.db import get_session
from pipeline.models import Facility

log = logging.getLogger(__name__)

# Map CDPH FACTYPE codes to our canonical facility type strings
FACTYPE_MAP: dict[str, str] = {
    "HOME HEALTH AGENCY": "home_health",
    "HOSPICE": "hospice",
    "SKILLED NURSING FACILITY": "snf",
    "INTERMEDIATE CARE FACILITY": "icf",
    "CONGREGATE LIVING HEALTH FACILITY": "clhf",
    "RESIDENTIAL CARE FACILITY FOR THE ELDERLY": "rcfe",
    "ADULT DAY HEALTH CARE CENTER": "adult_day_health",
    "ADULT DAY PROGRAM": "adult_day_program",
    "CLINIC": "clinic",
    "HOSPITAL": "hospital",
    "CHEMICAL DEPENDENCY RECOVERY HOSPITAL": "chemical_dependency",
    "PSYCHIATRIC HEALTH FACILITY": "psychiatric",
    "CORRECTIONAL TREATMENT CENTER": "correctional",
    "PRIMARY CARE CLINIC": "clinic",
    "SURGICAL CLINIC": "clinic",
}

# License status normalisation
STATUS_MAP: dict[str, str] = {
    "LICENSED": "active",
    "ACTIVE": "active",
    "PENDING": "pending",
    "SUSPENDED": "suspended",
    "REVOKED": "revoked",
    "EXPIRED": "expired",
    "CLOSED": "closed",
    "INACTIVE": "inactive",
}


def _canonical_type(raw: str) -> str:
    """Map raw CDPH FACTYPE to our canonical type string."""
    return FACTYPE_MAP.get(raw.strip().upper(), raw.strip().lower().replace(" ", "_"))


def _canonical_status(raw: str) -> str:
    return STATUS_MAP.get(str(raw).strip().upper(), str(raw).strip().lower())


def download_csv(url: str) -> pd.DataFrame:
    log.info("Downloading CDPH facility CSV from %s", url)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    df = pd.read_csv(io.BytesIO(resp.content), dtype=str, low_memory=False)
    log.info("Downloaded %d rows", len(df))
    return df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and values to our schema.

    Confirmed CDPH column names (uppercase, as delivered by the CSV):
      FACID, FACNAME, FAC_TYPE_CODE, FAC_FDR, ADDRESS, CITY, COUNTY_NAME,
      ZIP, LATITUDE, LONGITUDE, LICENSE_STATUS_DESCRIPTION, LICENSE_NUMBER,
      LICENSE_EXPIRATION_DATE, CCN, NPI, HCAI_ID
    """
    # Strip any accidental whitespace from column names but preserve case
    df.columns = [c.strip() for c in df.columns]

    # FAC_FDR contains the human-readable facility type description used for
    # type mapping; FAC_TYPE_CODE is the short code (kept for reference).
    type_col = "FAC_FDR" if "FAC_FDR" in df.columns else "FAC_TYPE_CODE"

    out = pd.DataFrame()
    out["cdph_id"] = df["FACID"].str.strip()
    out["name"] = df["FACNAME"].str.strip().str.title()
    out["type"] = df[type_col].apply(_canonical_type)
    out["address"] = df["ADDRESS"].str.strip().str.title()
    out["city"] = df["CITY"].str.strip().str.title()
    out["county"] = df["COUNTY_NAME"].str.strip().str.title()
    out["zip"] = df["ZIP"].str.strip().str[:10]
    # CDPH CSV does not include a phone column
    out["phone"] = None
    out["license_status"] = df["LICENSE_STATUS_DESCRIPTION"].apply(_canonical_status)
    out["license_number"] = df["LICENSE_NUMBER"].str.strip()

    # Lat/lon are provided directly — no geocoding needed
    out["lat"] = pd.to_numeric(df["LATITUDE"], errors="coerce")
    out["lon"] = pd.to_numeric(df["LONGITUDE"], errors="coerce")

    # License expiry
    out["license_expiry"] = pd.to_datetime(
        df["LICENSE_EXPIRATION_DATE"], errors="coerce"
    ).dt.date

    # Optional cross-reference IDs (may be blank for many rows)
    out["ccn"] = df["CCN"].str.strip() if "CCN" in df.columns else None
    out["cms_npi"] = df["NPI"].str.strip() if "NPI" in df.columns else None
    out["hcai_id"] = df["HCAI_ID"].str.strip() if "HCAI_ID" in df.columns else None

    out["primary_source"] = "cdph"
    out["last_verified"] = date.today()

    # Drop rows with no CDPH ID or no valid coordinates
    out = out[out["cdph_id"].notna() & (out["cdph_id"] != "")]
    out = out[out["lat"].notna() & out["lon"].notna()]
    log.info("Normalized to %d valid rows (with coordinates)", len(out))
    return out


def upsert(df: pd.DataFrame) -> tuple[int, int]:
    """Upsert facility records. Returns (inserted, updated) counts."""
    inserted = updated = 0

    with get_session() as session:
        existing: dict[str, Facility] = {
            f.cdph_id: f
            for f in session.query(Facility).filter(Facility.cdph_id.in_(df["cdph_id"].tolist())).all()
        }

        for _, row in df.iterrows():
            cdph_id = row["cdph_id"]
            if cdph_id in existing:
                facility = existing[cdph_id]
                _apply_row(facility, row)
                updated += 1
            else:
                facility = Facility()
                _apply_row(facility, row)
                session.add(facility)
                inserted += 1

    log.info("Upserted CDPH facilities: %d inserted, %d updated", inserted, updated)
    return inserted, updated


def _str(val: object) -> "str | None":
    """Convert a pandas value to str, returning None for NA/NaN/empty."""
    if val is None:
        return None
    s = str(val).strip()
    return None if s in ("", "nan", "NaT", "None") else s


def _date(val: object) -> "date | None":
    """Return a date or None, swallowing NaT and other NA sentinels."""
    if val is None or val is pd.NaT:
        return None
    try:
        if hasattr(val, "date"):
            return val.date()  # type: ignore[return-value]
        return val  # type: ignore[return-value]
    except Exception:
        return None


def _apply_row(facility: Facility, row: "pd.Series") -> None:  # type: ignore[type-arg]
    facility.cdph_id = row["cdph_id"]
    facility.name = _str(row["name"]) or "Unknown"
    facility.type = row["type"]
    facility.address = _str(row.get("address"))
    facility.city = _str(row.get("city"))
    facility.county = _str(row.get("county"))
    facility.state = "CA"
    facility.zip = _str(row.get("zip"))
    facility.phone = None
    facility.license_status = _str(row.get("license_status"))
    facility.license_number = _str(row.get("license_number"))
    facility.license_expiry = _date(row.get("license_expiry"))
    facility.lat = row.get("lat") or None
    facility.lon = row.get("lon") or None
    # Set NPI from CDPH CSV if present (crosswalk resolver may later enrich)
    npi = _str(row.get("cms_npi"))
    if npi:
        facility.cms_npi = npi
    facility.primary_source = "cdph"
    facility.last_verified = row.get("last_verified")


def run() -> dict[str, int]:
    """Full CDPH ingest: download → normalize → upsert."""
    df_raw = download_csv(settings.cdph_facility_csv_url)
    df = normalize(df_raw)
    inserted, updated = upsert(df)
    return {"inserted": inserted, "updated": updated, "total": len(df)}
