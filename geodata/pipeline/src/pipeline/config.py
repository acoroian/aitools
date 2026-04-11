from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql://geodata:geodata@localhost:5432/geodata"
    redis_url: str = "redis://localhost:6379/0"
    tiles_dir: str = "/tmp/geodata/tiles"

    # Cloudflare R2 (optional — prod only)
    cf_account_id: str = ""
    cf_r2_access_key_id: str = ""
    cf_r2_secret_access_key: str = ""
    r2_bucket_name: str = "geodata-tiles"

    # Geocoding
    geocodio_api_key: str = ""

    # Data source URLs
    cdph_facility_csv_url: str = (
        "https://data.chhs.ca.gov/dataset/e54ef530-a99c-4c2c-94f1-fcc33e0da6b0"
        "/resource/8d5a4e28-e942-4dc0-9e5c-b89e5a9b8855/download/health_facility_locations.csv"
    )
    cdph_crosswalk_url: str = (
        "https://data.chhs.ca.gov/dataset/e89100fd-1f1d-4a37-8205-d588aa42e5a1"
        "/resource/63499f73-feba-43f2-a364-7188d5cf7728/download/licensed-facility-crosswalk-um8irx9a.zip"
    )


settings = Settings()
