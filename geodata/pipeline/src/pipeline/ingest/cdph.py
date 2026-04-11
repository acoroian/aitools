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
    """Normalize column names and values to our schema."""
    # Lower-case all column names, strip whitespace
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Detect actual column names (they vary slightly between releases)
    col = {c: c for c in df.columns}

    # Build normalized frame
    out = pd.DataFrame()
    out["cdph_id"] = df.get(col.get("facid", "facid"), pd.Series(dtype=str)).str.strip()
    out["name"] = df.get(col.get("facname", "facname"), pd.Series(dtype=str)).str.strip().str.title()
    out["type"] = df.get(col.get("factype", "factype"), pd.Series(dtype=str)).apply(_canonical_type)
    out["address"] = df.get(col.get("address", "address"), pd.Series(dtype=str)).str.strip().str.title()
    out["city"] = df.get(col.get("city", "city"), pd.Series(dtype=str)).str.strip().str.title()
    out["county"] = df.get(col.get("county", "county"), pd.Series(dtype=str)).str.strip().str.title()
    out["zip"] = df.get(col.get("zip", "zip"), pd.Series(dtype=str)).str.strip().str[:10]
    out["phone"] = df.get(col.get("phone", "phone"), pd.Series(dtype=str)).str.strip()
    out["license_status"] = df.get(col.get("facstatus", "facstatus"), pd.Series(dtype=str)).apply(_canonical_status)
    out["license_number"] = df.get(col.get("licnum", "licnum"), pd.Series(dtype=str)).str.strip()

    # Parse lat/lon — CDPH provides these directly
    lat_col = next((c for c in df.columns if "lat" in c), None)
    lon_col = next((c for c in df.columns if "lon" in c or "lng" in c), None)
    out["lat"] = pd.to_numeric(df[lat_col], errors="coerce") if lat_col else None
    out["lon"] = pd.to_numeric(df[lon_col], errors="coerce") if lon_col else None

    # Parse license expiry
    expiry_col = next((c for c in df.columns if "expir" in c), None)
    if expiry_col:
        out["license_expiry"] = pd.to_datetime(df[expiry_col], errors="coerce").dt.date
    else:
        out["license_expiry"] = None

    out["primary_source"] = "cdph"
    out["last_verified"] = date.today()

    # Drop rows with no CDPH ID
    out = out[out["cdph_id"].notna() & (out["cdph_id"] != "")]
    log.info("Normalized to %d valid rows", len(out))
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


def _apply_row(facility: Facility, row: "pd.Series") -> None:  # type: ignore[type-arg]
    facility.cdph_id = row["cdph_id"]
    facility.name = row["name"] or "Unknown"
    facility.type = row["type"]
    facility.address = row.get("address")
    facility.city = row.get("city")
    facility.county = row.get("county")
    facility.state = "CA"
    facility.zip = row.get("zip")
    facility.phone = row.get("phone")
    facility.license_status = row.get("license_status")
    facility.license_number = row.get("license_number")
    facility.license_expiry = row.get("license_expiry") or None
    facility.lat = row.get("lat") or None
    facility.lon = row.get("lon") or None
    facility.primary_source = "cdph"
    facility.last_verified = row.get("last_verified")


def run() -> dict[str, int]:
    """Full CDPH ingest: download → normalize → upsert."""
    df_raw = download_csv(settings.cdph_facility_csv_url)
    df = normalize(df_raw)
    inserted, updated = upsert(df)
    return {"inserted": inserted, "updated": updated, "total": len(df)}
