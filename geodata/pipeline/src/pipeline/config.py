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
        "https://data.chhs.ca.gov/dataset/3b5b80e8-6b8d-4715-b3c0-2699af6e72e5"
        "/resource/f0ae5731-fef8-417f-839d-54a0ed3a126e/download/health_facility_locations.csv"
    )
    cdph_crosswalk_url: str = (
        "https://data.chhs.ca.gov/dataset/e89100fd-1f1d-4a37-8205-d588aa42e5a1"
        "/resource/63499f73-feba-43f2-a364-7188d5cf7728/download/licensed-facility-crosswalk-um8irx9a.zip"
    )

    # CMS HCRIS cost report ZIPs — update year as new reports are released.
    # Find the direct ZIP URL at:
    #   https://www.cms.gov/data-research/statistics-trends-and-reports/
    #   medicare-provider-cost-report/home-health-agency
    # The ZIP contains *_RPT_*.CSV and *_NMRC_*.CSV files.
    # Set HCRIS_HHA_URL / HCRIS_HOSPICE_URL in .env with the correct link.
    hcris_hha_url: str = ""       # set in .env: HCRIS_HHA_URL=https://...
    hcris_hospice_url: str = ""   # set in .env: HCRIS_HOSPICE_URL=https://...
    hcris_year: int = 2022

    # CA HCAI Annual Financial Disclosure
    # Download page: https://hcai.ca.gov/data-and-reports/research-data/annual-financial-data/
    # Set HCAI_SNF_URL in .env with the direct XLSX link for SNF data.
    hcai_snf_url: str = ""        # set in .env: HCAI_SNF_URL=https://...
    hcai_year: int = 2022


settings = Settings()
