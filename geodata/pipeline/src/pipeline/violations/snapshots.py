"""Raw download archival.

Each ingest writes the unmodified CSV/XLSX bytes to
`$GEODATA_SNAPSHOT_ROOT/<source>/<filename>` before parsing. Atomic
write via `*.tmp` + rename so a crashed writer never leaves a
half-file that a later run would misread.
"""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_ROOT = "/tmp/geodata/snapshots"


def _root() -> Path:
    return Path(os.environ.get("GEODATA_SNAPSHOT_ROOT", DEFAULT_ROOT))


def snapshot_dir(source: str) -> Path:
    d = _root() / source
    d.mkdir(parents=True, exist_ok=True)
    return d


def archive_raw(source: str, filename: str, content: bytes) -> str:
    """Write `content` to {root}/{source}/{filename} atomically.

    Returns the final path as a string for logging.
    """
    dest_dir = snapshot_dir(source)
    final = dest_dir / filename
    tmp = final.with_suffix(final.suffix + ".tmp")
    tmp.write_bytes(content)
    tmp.replace(final)
    return str(final)
