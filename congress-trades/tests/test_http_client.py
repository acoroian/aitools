from unittest.mock import MagicMock, patch

import requests
from requests.adapters import HTTPAdapter

from http_client import DEFAULT_TIMEOUT, TimeoutHTTPAdapter, courtesy_delay, get_session


def test_session_sets_descriptive_user_agent():
    session = get_session()
    ua = session.headers.get("User-Agent", "")
    assert "congress-trades" in ua
    assert ua != ""


def test_session_is_a_requests_session():
    session = get_session()
    assert isinstance(session, requests.Session)


def test_session_has_retry_mounted_for_https():
    session = get_session()
    adapter = session.get_adapter("https://example.com")
    assert isinstance(adapter, HTTPAdapter)
    assert adapter.max_retries.total == 3


def test_session_mounts_timeout_defaulting_adapter():
    """The mounted adapter must be the TimeoutHTTPAdapter subclass, not a
    plain HTTPAdapter — otherwise requests has no default timeout at all."""
    session = get_session()
    adapter = session.get_adapter("https://example.com")
    assert isinstance(adapter, TimeoutHTTPAdapter)


def test_request_without_explicit_timeout_gets_default_timeout_injected():
    """A plain session.get(url) with no timeout kwarg must still reach the
    underlying send() with a timeout set, via the mounted adapter."""
    adapter = TimeoutHTTPAdapter()
    with patch.object(HTTPAdapter, "send", return_value=MagicMock()) as mock_send:
        request = requests.Request("GET", "https://example.com").prepare()
        adapter.send(request)
    assert mock_send.call_args.kwargs.get("timeout") == DEFAULT_TIMEOUT


def test_request_with_explicit_timeout_is_not_overridden():
    adapter = TimeoutHTTPAdapter()
    with patch.object(HTTPAdapter, "send", return_value=MagicMock()) as mock_send:
        request = requests.Request("GET", "https://example.com").prepare()
        adapter.send(request, timeout=5)
    assert mock_send.call_args.kwargs.get("timeout") == 5


def test_courtesy_delay_sleeps_briefly():
    with patch("http_client.time.sleep") as mock_sleep:
        courtesy_delay()
    mock_sleep.assert_called_once()
    assert 0 < mock_sleep.call_args.args[0] <= 1
