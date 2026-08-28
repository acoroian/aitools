"""
http_client.py — Shared requests.Session with the descriptive User-Agent that
disclosures-clerk.house.gov, efdsearch.senate.gov, and sec.gov all require for
automated access, plus retry/backoff mounted transparently on the session.
"""

from __future__ import annotations

import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_CONTACT = "set-CONGRESS_TRADES_CONTACT-env-var@example.com"
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.5


def get_session() -> requests.Session:
    """Build a requests.Session with the required identifying User-Agent and
    retry/backoff mounted for both http:// and https://."""
    contact = os.environ.get("CONGRESS_TRADES_CONTACT", DEFAULT_CONTACT)
    session = requests.Session()
    session.headers.update({"User-Agent": f"congress-trades {contact}"})

    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
