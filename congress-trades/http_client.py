"""
http_client.py — Shared requests.Session with the descriptive User-Agent that
disclosures-clerk.house.gov, efdsearch.senate.gov, and sec.gov all require for
automated access, plus retry/backoff mounted transparently on the session.
"""

from __future__ import annotations

import os
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_CONTACT = "set-CONGRESS_TRADES_CONTACT-env-var@example.com"
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.5
DEFAULT_TIMEOUT = 30
COURTESY_DELAY_SECONDS = 0.15


class TimeoutHTTPAdapter(HTTPAdapter):
    """HTTPAdapter that injects a default request timeout when the caller
    doesn't pass one explicitly, so a plain session.get(url)/session.post(url)
    can never hang forever — requests has no default timeout on its own."""

    def send(self, request, **kwargs):
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
        return super().send(request, **kwargs)


def get_session() -> requests.Session:
    """Build a requests.Session with the required identifying User-Agent and
    retry/backoff + a default timeout mounted for both http:// and https://."""
    contact = os.environ.get("CONGRESS_TRADES_CONTACT", DEFAULT_CONTACT)
    session = requests.Session()
    session.headers.update({"User-Agent": f"congress-trades {contact}"})

    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = TimeoutHTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def courtesy_delay() -> None:
    """A small pause between requests to the same government data source, to
    keep requests modest per the SEC/House/Senate fair-access requirement."""
    time.sleep(COURTESY_DELAY_SECONDS)
