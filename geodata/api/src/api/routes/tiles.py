"""
Local PMTiles server — serves tiles from TILES_DIR via HTTP range requests.

In production this is replaced by Cloudflare R2 + Workers.
The pmtiles format uses HTTP range requests to read only the needed tile bytes
from a single archive file — this route implements that same protocol.
"""

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, Response

from api.config import settings

router = APIRouter()


def _get_pmtiles_path(layer_slug: str) -> Path:
    path = Path(settings.tiles_dir) / f"{layer_slug}.pmtiles"
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"PMTiles file not found for layer '{layer_slug}'. Run tile generation first.",
        )
    return path


@router.get("/{layer_slug}.pmtiles")
async def serve_pmtiles(layer_slug: str, request: Request) -> Response:
    """
    Serve a PMTiles archive with range request support.

    MapLibre GL JS uses the pmtiles:// protocol which issues range requests
    against this endpoint to read individual tile bytes.
    """
    path = _get_pmtiles_path(layer_slug)
    file_size = path.stat().st_size

    range_header = request.headers.get("Range")

    if range_header:
        # Parse Range: bytes=start-end
        try:
            range_spec = range_header.replace("bytes=", "")
            start_str, end_str = range_spec.split("-")
            start = int(start_str)
            end = int(end_str) if end_str else file_size - 1
        except (ValueError, IndexError) as exc:
            raise HTTPException(status_code=416, detail="Invalid Range header") from exc

        if start >= file_size or end >= file_size or start > end:
            raise HTTPException(status_code=416, detail="Range Not Satisfiable")

        length = end - start + 1
        with open(path, "rb") as f:
            f.seek(start)
            data = f.read(length)

        return Response(
            content=data,
            status_code=206,
            headers={
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Content-Length": str(length),
                "Accept-Ranges": "bytes",
                "Content-Type": "application/octet-stream",
                "Cache-Control": "public, max-age=3600",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Range",
            },
        )

    # Full file request
    with open(path, "rb") as f:
        data = f.read()

    return Response(
        content=data,
        status_code=200,
        headers={
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
            "Content-Type": "application/octet-stream",
            "Cache-Control": "public, max-age=3600",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Range",
        },
    )


@router.get("/{layer_slug}/info")
def tile_info(layer_slug: str) -> dict:
    """Return basic info about a PMTiles archive."""
    path = _get_pmtiles_path(layer_slug)
    stat = path.stat()
    return {
        "layer": layer_slug,
        "path": str(path),
        "size_bytes": stat.st_size,
        "size_mb": round(stat.st_size / 1024 / 1024, 2),
        "modified": stat.st_mtime,
        "tile_url": f"/tiles/{layer_slug}.pmtiles",
    }
