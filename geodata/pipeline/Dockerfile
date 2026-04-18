FROM python:3.12-slim

# Install system deps: GDAL, Tippecanoe, libpq
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    libpq-dev \
    gcc \
    curl \
    tippecanoe \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml .
RUN uv sync --no-dev

COPY src/ src/

ENV PYTHONPATH=/app/src
CMD ["uv", "run", "celery", "-A", "pipeline.celery_app", "worker", "--loglevel=info"]
