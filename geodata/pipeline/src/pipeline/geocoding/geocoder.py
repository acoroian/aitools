"""Geocoding: Census Geocoder batch API + Geocodio fallback.

The Census Geocoder is free (10k addresses per batch). For unmatched
addresses (~10-20%), we fall back to Geocodio ($0.50/1k).

Usage:
    from pipeline.geocoding.geocoder import batch_geocode
    results = batch_geocode(records)  # list of dicts with address fields
"""

from __future__ import annotations

import csv
import io
import logging
import time

import httpx

from pipeline.config import settings

log = logging.getLogger(__name__)

CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
CENSUS_BATCH_SIZE = 1000  # Census API docs say 10k max but smaller batches are more reliable
GEOCODIO_BATCH_URL = "https://api.geocod.io/v1.7/geocode"
GEOCODIO_BATCH_SIZE = 500


def _build_address_string(record: dict[str, str | None]) -> str:
    """Build a single-line address from record fields."""
    parts = [
        record.get("address") or "",
        record.get("city") or "",
        record.get("state") or "CA",
        record.get("zip") or "",
    ]
    return ", ".join(p for p in parts if p.strip())


def _census_batch(
    records: list[dict[str, str | None]],
) -> dict[str, tuple[float, float, str, float]]:
    """Submit a batch to the Census Geocoder. Returns {unique_id: (lat, lon, source, confidence)}.

    Census batch CSV format (no header):
        Unique ID,Street address,City,State,ZIP
    Response CSV:
        ID,Input Address,Match Type,Matched Address,Lon/Lat,...,State,County,Tract,Block
    """
    results: dict[str, tuple[float, float, str, float]] = {}

    for batch_start in range(0, len(records), CENSUS_BATCH_SIZE):
        chunk = records[batch_start : batch_start + CENSUS_BATCH_SIZE]

        buf = io.StringIO()
        writer = csv.writer(buf)
        for rec in chunk:
            uid = rec.get("uid", "")
            writer.writerow(
                [
                    uid,
                    rec.get("address", ""),
                    rec.get("city", ""),
                    rec.get("state", "CA"),
                    rec.get("zip", ""),
                ]
            )

        csv_content = buf.getvalue()

        try:
            with httpx.Client(timeout=120) as client:
                resp = client.post(
                    CENSUS_BATCH_URL,
                    data={"benchmark": "Public_AR_Current", "vintage": "Current_Current"},
                    files={"addressFile": ("addresses.csv", csv_content, "text/csv")},
                )
                resp.raise_for_status()
        except httpx.HTTPError:
            log.exception("Census batch geocode failed for batch starting at %d", batch_start)
            continue

        reader = csv.reader(io.StringIO(resp.text))
        for row in reader:
            if len(row) < 6:
                continue
            uid = row[0].strip().strip('"')
            match_type = row[2].strip().strip('"')
            if match_type not in ("Match", "Exact"):
                continue
            coords = row[5].strip().strip('"')  # "lon,lat"
            if not coords or "," not in coords:
                continue
            lon_str, lat_str = coords.split(",", 1)
            try:
                lon, lat = float(lon_str), float(lat_str)
            except ValueError:
                continue
            confidence = 1.0 if match_type == "Exact" else 0.9
            results[uid] = (lat, lon, "census", confidence)

        log.info(
            "Census batch %d-%d: %d/%d matched",
            batch_start,
            batch_start + len(chunk),
            sum(1 for r in chunk if r.get("uid", "") in results),
            len(chunk),
        )
        # Rate limit: be polite to the Census API
        time.sleep(1)

    return results


def _geocodio_batch(
    records: list[dict[str, str | None]],
) -> dict[str, tuple[float, float, str, float]]:
    """Fallback geocoder via Geocodio API. Returns {uid: (lat, lon, source, confidence)}."""
    if not settings.geocodio_api_key:
        log.warning("GEOCODIO_API_KEY not set — skipping Geocodio fallback")
        return {}

    results: dict[str, tuple[float, float, str, float]] = {}

    for batch_start in range(0, len(records), GEOCODIO_BATCH_SIZE):
        chunk = records[batch_start : batch_start + GEOCODIO_BATCH_SIZE]

        # Geocodio batch: POST a list of address strings
        uid_to_addr: dict[str, str] = {}
        addresses: list[str] = []
        for rec in chunk:
            uid = rec.get("uid", "")
            addr = _build_address_string(rec)
            uid_to_addr[uid] = addr
            addresses.append(addr)

        # Map index back to uid
        uid_list = list(uid_to_addr.keys())

        try:
            with httpx.Client(timeout=120) as client:
                resp = client.post(
                    GEOCODIO_BATCH_URL,
                    params={"api_key": settings.geocodio_api_key},
                    json=addresses,
                )
                resp.raise_for_status()
        except httpx.HTTPError:
            log.exception("Geocodio batch failed for batch starting at %d", batch_start)
            continue

        data = resp.json()
        geocodio_results = data.get("results", [])
        for i, result in enumerate(geocodio_results):
            if i >= len(uid_list):
                break
            uid = uid_list[i]
            response = result.get("response", {})
            geo_results = response.get("results", [])
            if not geo_results:
                continue
            best = geo_results[0]
            location = best.get("location", {})
            lat = location.get("lat")
            lon = location.get("lng")
            accuracy = best.get("accuracy", 0)
            if lat and lon and accuracy >= 0.5:
                results[uid] = (float(lat), float(lon), "geocodio", min(accuracy, 1.0))

        matched = sum(1 for r in chunk if r.get("uid", "") in results)
        log.info(
            "Geocodio batch %d-%d: %d/%d matched",
            batch_start,
            batch_start + len(chunk),
            matched,
            len(chunk),
        )
        time.sleep(0.5)

    return results


def batch_geocode(
    records: list[dict[str, str | None]],
) -> dict[str, tuple[float, float, str, float]]:
    """Geocode a list of records with address fields.

    Each record must have a 'uid' key and address/city/state/zip fields.
    Returns {uid: (lat, lon, source, confidence)}.

    Strategy: Census Geocoder first (free), then Geocodio for failures.
    """
    if not records:
        return {}

    log.info("Geocoding %d records via Census Geocoder", len(records))
    results = _census_batch(records)
    matched_count = len(results)

    # Find unmatched records for Geocodio fallback
    unmatched = [r for r in records if r.get("uid", "") not in results]
    if unmatched and settings.geocodio_api_key:
        log.info("Falling back to Geocodio for %d unmatched records", len(unmatched))
        geocodio_results = _geocodio_batch(unmatched)
        results.update(geocodio_results)

    log.info(
        "Geocoding complete: %d/%d matched (Census: %d, Geocodio: %d)",
        len(results),
        len(records),
        matched_count,
        len(results) - matched_count,
    )
    return results
