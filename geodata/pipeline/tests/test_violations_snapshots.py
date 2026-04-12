from pathlib import Path

from pipeline.violations.snapshots import archive_raw, snapshot_dir


def test_snapshot_dir_is_created(tmp_path, monkeypatch):
    monkeypatch.setenv("GEODATA_SNAPSHOT_ROOT", str(tmp_path))
    d = snapshot_dir("cms_nh_compare")
    assert d.exists()
    assert d.name == "cms_nh_compare"
    assert d.parent == tmp_path


def test_archive_raw_writes_file_atomically(tmp_path, monkeypatch):
    monkeypatch.setenv("GEODATA_SNAPSHOT_ROOT", str(tmp_path))
    content = b"header\n1,2,3\n"
    path = archive_raw("cms_nh_compare", "NH_HealthCitations_Mar2026.csv", content)
    assert Path(path).exists()
    assert Path(path).read_bytes() == content
    # No .tmp stragglers
    assert not any(p.suffix == ".tmp" for p in Path(path).parent.iterdir())


def test_archive_raw_overwrites_same_filename(tmp_path, monkeypatch):
    monkeypatch.setenv("GEODATA_SNAPSHOT_ROOT", str(tmp_path))
    archive_raw("cdph_sea", "sea_final_20240730.xlsx", b"first")
    path = archive_raw("cdph_sea", "sea_final_20240730.xlsx", b"second")
    assert Path(path).read_bytes() == b"second"
