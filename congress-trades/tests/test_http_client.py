import requests

from http_client import get_session


def test_session_sets_descriptive_user_agent():
    session = get_session()
    ua = session.headers.get("User-Agent", "")
    assert "congress-trades" in ua
    assert ua != ""


def test_session_is_a_requests_session():
    session = get_session()
    assert isinstance(session, requests.Session)
