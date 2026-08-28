"""
http_client.py — Shared requests.Session with the descriptive User-Agent that
disclosures-clerk.house.gov, efdsearch.senate.gov, and sec.gov all require for
automated access, plus basic retry/backoff for transient failures.
"""

from __future__ import annotations

import os
import time

import requests

DEFAULT_CONTACT = "set-CONGRESS_TRADES_CONTACT-env-var@example.com"
MAX_RETRIES = 3
BACKOFF_SECONDS = 1.5


def get_session() -> requests.Session:
    """Build a requests.Session with the required identifying User-Agent."""
    contact = os.environ.get("CONGRESS_TRADES_CONTACT", DEFAULT_CONTACT)
    session = requests.Session()
    session.headers.update({"User-Agent": f"congress-trades {contact}"})
    return session


def fetch(session: requests.Session, url: str, method: str = "GET", **kwargs) -> requests.Response:
    """GET/POST with retry+backoff on network errors and 5xx responses.

    Raises the last exception (or the last response's HTTPError) if every
    attempt fails.
    """
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            response = session.request(method, url, timeout=30, **kwargs)
            if response.status_code >= 500:
                response.raise_for_status()
            return response
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            last_exc = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_SECONDS * (attempt + 1))
    assert last_exc is not None
    raise last_exc
