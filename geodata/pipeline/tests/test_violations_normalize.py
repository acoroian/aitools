from datetime import date

from pipeline.violations.normalize import (
    SOURCE_CDPH_SEA,
    SOURCE_CMS_NH,
    cdph_severity_level,
    cms_severity_level,
    cms_severity_to_scope,
    derive_cms_citation_id,
    is_immediate_jeopardy,
)


def test_source_constants():
    assert SOURCE_CMS_NH == "cms_nh_compare"
    assert SOURCE_CDPH_SEA == "cdph_sea"


def test_cms_severity_level_covers_grid():
    assert cms_severity_level("A") == 1
    assert cms_severity_level("F") == 6
    assert cms_severity_level("G") == 6
    assert cms_severity_level("J") == 8
    assert cms_severity_level("K") == 9
    assert cms_severity_level("L") == 10
    assert cms_severity_level("Z") is None
    assert cms_severity_level(None) is None


def test_cms_severity_to_scope():
    # Isolated: A, D, G, J
    assert cms_severity_to_scope("A") == "isolated"
    assert cms_severity_to_scope("J") == "isolated"
    # Pattern: B, E, H, K
    assert cms_severity_to_scope("E") == "pattern"
    assert cms_severity_to_scope("K") == "pattern"
    # Widespread: C, F, I, L
    assert cms_severity_to_scope("F") == "widespread"
    assert cms_severity_to_scope("L") == "widespread"
    assert cms_severity_to_scope("Z") is None


def test_cdph_severity_level():
    assert cdph_severity_level("AA") == 10
    assert cdph_severity_level("A") == 8
    assert cdph_severity_level("B") == 4
    assert cdph_severity_level("C") is None  # unknown class


def test_is_immediate_jeopardy_crosses_sources():
    assert is_immediate_jeopardy(SOURCE_CMS_NH, "J") is True
    assert is_immediate_jeopardy(SOURCE_CMS_NH, "L") is True
    assert is_immediate_jeopardy(SOURCE_CMS_NH, "F") is False
    assert is_immediate_jeopardy(SOURCE_CDPH_SEA, "AA") is True
    assert is_immediate_jeopardy(SOURCE_CDPH_SEA, "A") is True  # class A is IJ-equivalent
    assert is_immediate_jeopardy(SOURCE_CDPH_SEA, "B") is False
    assert is_immediate_jeopardy("unknown", "X") is False


def test_derive_cms_citation_id_deterministic():
    cid1 = derive_cms_citation_id("055123", date(2024, 3, 15), "F0880", "F")
    cid2 = derive_cms_citation_id("055123", date(2024, 3, 15), "F0880", "F")
    assert cid1 == cid2
    assert cid1 == "055123_2024-03-15_F0880_F"


def test_derive_cms_citation_id_differs_for_different_inputs():
    a = derive_cms_citation_id("055123", date(2024, 3, 15), "F0880", "F")
    b = derive_cms_citation_id("055123", date(2024, 3, 15), "F0880", "G")
    assert a != b
