"""CA CDSS Community Care Licensing ingest.

Downloads the facility dataset from CDSS, normalizes, geocodes addresses
(Census Geocoder + Geocodio fallback), and upserts into PostGIS.

CDSS facilities (child day care, family child care homes, etc.) have
addresses but no lat/lon — geocoding is required.

Source: https://www.ccld.dss.ca.gov/carefacilitysearch/
"""

from __future__ import annotations

import io
import logging
from datetime import date

import httpx
import pandas as pd

from pipeline.config import settings
from pipeline.db import get_session
from pipeline.geocoding.geocoder import batch_geocode
from pipeline.models import Facility

log = logging.getLogger(__name__)

# CDSS facility type mapping to our canonical types
CDSS_TYPE_MAP: dict[str, str] = {
    "DAY CARE CENTER": "daycare_center",
    "CHILD CARE CENTER": "daycare_center",
    "INFANT CENTER": "daycare_infant",
    "SCHOOL AGE CHILD CARE CENTER": "daycare_school_age",
    "FAMILY CHILD CARE HOME": "daycare_family",
    "SMALL FAMILY CHILD CARE HOME": "daycare_family_small",
    "LARGE FAMILY CHILD CARE HOME": "daycare_family_large",
    "GROUP HOME": "group_home",
    "FOSTER FAMILY AGENCY": "foster_family_agency",
    "RESIDENTIAL CARE FACILITY FOR THE ELDERLY": "rcfe",
    "ADULT RESIDENTIAL FACILITY": "adult_residential",
    "ADULT DAY CARE FACILITY": "adult_day_care",
    "SOCIAL REHABILITATION FACILITY": "social_rehab",
    "COMMUNITY CARE FACILITY": "community_care",
    "TRANSITIONAL HOUSING PLACEMENT PROVIDER": "transitional_housing",
    "CRISIS NURSERY": "crisis_nursery",
    "SMALL FAMILY HOME": "daycare_family_small",
    "LARGE FAMILY HOME": "daycare_family_large",
}

STATUS_MAP: dict[str, str] = {
    "LICENSED": "active",
    "ACTIVE": "active",
    "PENDING": "pending",
    "PROBATIONARY": "probationary",
    "SUSPENDED": "suspended",
    "REVOKED": "revoked",
    "EXPIRED": "expired",
    "CLOSED": "closed",
    "INACTIVE": "inactive",
    "UNLICENSED": "unlicensed",
}


def _canonical_type(raw: str) -> str:
    return CDSS_TYPE_MAP.get(raw.strip().upper(), raw.strip().lower().replace(" ", "_"))


def _canonical_status(raw: str) -> str:
    return STATUS_MAP.get(str(raw).strip().upper(), str(raw).strip().lower())


def download_csv(url: str) -> pd.DataFrame:
    log.info("Downloading CDSS facility data from %s", url)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    df = pd.read_csv(io.BytesIO(resp.content), dtype=str, low_memory=False)
    log.info("Downloaded %d rows", len(df))
    return df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize CDSS columns to our schema.

    Expected columns (case-insensitive): FACILITY_NUMBER, FACILITY_NAME,
    FACILITY_TYPE, FACILITY_ADDRESS, FACILITY_CITY, FACILITY_STATE,
    FACILITY_ZIP, COUNTY_NAME, PHONE_NUMBER, LICENSE_STATUS,
    LICENSE_NUMBER, FACILITY_CAPACITY
    """
    # Normalize column names to uppercase
    df.columns = [c.strip().upper() for c in df.columns]

    out = pd.DataFrame()
    out["cdss_id"] = df["FACILITY_NUMBER"].str.strip()
    out["name"] = df["FACILITY_NAME"].str.strip().str.title()
    out["type"] = df["FACILITY_TYPE"].apply(_canonical_type)
    out["address"] = df["FACILITY_ADDRESS"].str.strip().str.title()
    out["city"] = df["FACILITY_CITY"].str.strip().str.title()
    out["state"] = df.get("FACILITY_STATE", pd.Series("CA", index=df.index)).str.strip()
    out["county"] = df["COUNTY_NAME"].str.strip().str.title()
    out["zip"] = df["FACILITY_ZIP"].str.strip().str[:10]
    out["phone"] = df.get("PHONE_NUMBER", pd.Series(dtype="str")).str.strip()
    out["license_status"] = df["LICENSE_STATUS"].apply(_canonical_status)
    out["license_number"] = df.get("LICENSE_NUMBER", pd.Series(dtype="str")).str.strip()
    out["capacity"] = pd.to_numeric(df.get("FACILITY_CAPACITY"), errors="coerce")

    out["primary_source"] = "cdss"
    out["last_verified"] = date.today()

    # Drop rows without a CDSS ID or address
    out = out[out["cdss_id"].notna() & (out["cdss_id"] != "")]
    out = out[out["address"].notna() & (out["address"] != "")]
    log.info("Normalized to %d valid rows", len(out))
    return out


def geocode_facilities(df: pd.DataFrame) -> pd.DataFrame:
    """Geocode CDSS facilities that don't have coordinates.

    Adds lat, lon, geocode_source, geocode_confidence columns.
    """
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "uid": row["cdss_id"],
                "address": row.get("address"),
                "city": row.get("city"),
                "state": row.get("state", "CA"),
                "zip": row.get("zip"),
            }
        )

    results = batch_geocode(records)

    lats, lons, sources, confidences = [], [], [], []
    for _, row in df.iterrows():
        cdss_id = row["cdss_id"]
        if cdss_id in results:
            lat, lon, source, conf = results[cdss_id]
            lats.append(lat)
            lons.append(lon)
            sources.append(source)
            confidences.append(conf)
        else:
            lats.append(None)
            lons.append(None)
            sources.append("failed")
            confidences.append(None)

    df = df.copy()
    df["lat"] = lats
    df["lon"] = lons
    df["geocode_source"] = sources
    df["geocode_confidence"] = confidences

    geocoded = df["lat"].notna().sum()
    log.info("Geocoded %d/%d CDSS facilities", geocoded, len(df))
    return df


def upsert(df: pd.DataFrame) -> tuple[int, int]:
    """Upsert CDSS facility records. Returns (inserted, updated) counts."""
    inserted = updated = 0

    with get_session() as session:
        cdss_ids = df["cdss_id"].tolist()
        existing: dict[str, Facility] = {
            f.cdss_id: f
            for f in session.query(Facility).filter(Facility.cdss_id.in_(cdss_ids)).all()
            if f.cdss_id
        }

        for _, row in df.iterrows():
            cdss_id = row["cdss_id"]
            if cdss_id in existing:
                facility = existing[cdss_id]
                _apply_row(facility, row)
                updated += 1
            else:
                facility = Facility()
                _apply_row(facility, row)
                session.add(facility)
                inserted += 1

    log.info("Upserted CDSS facilities: %d inserted, %d updated", inserted, updated)
    return inserted, updated


def _str(val: object) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return None if s in ("", "nan", "NaT", "None") else s


def _apply_row(facility: Facility, row: pd.Series) -> None:  # type: ignore[type-arg]
    facility.cdss_id = row["cdss_id"]
    facility.name = _str(row["name"]) or "Unknown"
    facility.type = row["type"]
    facility.address = _str(row.get("address"))
    facility.city = _str(row.get("city"))
    facility.county = _str(row.get("county"))
    facility.state = _str(row.get("state")) or "CA"
    facility.zip = _str(row.get("zip"))
    facility.phone = _str(row.get("phone"))
    facility.license_status = _str(row.get("license_status"))
    facility.license_number = _str(row.get("license_number"))
    facility.lat = row.get("lat") if pd.notna(row.get("lat")) else None
    facility.lon = row.get("lon") if pd.notna(row.get("lon")) else None
    facility.geocode_source = _str(row.get("geocode_source"))
    facility.geocode_confidence = (
        float(row["geocode_confidence"]) if pd.notna(row.get("geocode_confidence")) else None
    )
    facility.primary_source = "cdss"
    facility.last_verified = row.get("last_verified")


def run() -> dict[str, int]:
    """Full CDSS ingest: download → normalize → geocode → upsert."""
    df_raw = download_csv(settings.cdss_facility_csv_url)
    df = normalize(df_raw)
    df = geocode_facilities(df)
    inserted, updated = upsert(df)
    geocoded = int(df["lat"].notna().sum())
    return {
        "inserted": inserted,
        "updated": updated,
        "total": len(df),
        "geocoded": geocoded,
        "geocode_failed": len(df) - geocoded,
    }
