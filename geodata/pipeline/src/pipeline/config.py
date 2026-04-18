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
    # HHA data is per-fiscal-year (no all-years ZIP). URL template uses HCRIS_YEAR.
    hcris_hha_url: str = "https://downloads.cms.gov/FILES/HCRIS/HHA20FY2022.ZIP"
    hcris_hospice_url: str = "https://downloads.cms.gov/Files/hcris/HOSPC14-ALL-YEARS.zip"
    hcris_year: int = 2022

    # CA HCAI Long-Term Care Annual Financial Disclosure — Selected File
    # Index: https://data.chhs.ca.gov/dataset/long-term-care-facility-disclosure-report-data
    # File contains ~1,344 LTC facilities (SNF, CLHF, ICF) for the fiscal year.
    hcai_snf_url: str = (
        "https://data.chhs.ca.gov/dataset/70fcfed4-c9b8-4c13-8c5f-06591261cba4"
        "/resource/b8e56328-c555-48db-a181-25d1035509a5"
        "/download/lafd-1222-sub-selected.xlsx"
    )
    hcai_year: int = 2022

    # CMS Nursing Home Health Deficiencies (dataset r5ix-sfxw)
    # Dataset metadata endpoint returns the current CSV's downloadURL in distribution[0].
    cms_nh_metadata_url: str = (
        "https://data.cms.gov/provider-data/api/1/metastore/"
        "schemas/dataset/items/r5ix-sfxw?show-reference-ids"
    )

    # CDPH Health Facilities State Enforcement Actions (CA, annual)
    cdph_sea_package_id: str = "1e1e2904-1bfb-448c-97e1-cf3e228c9159"
    cdph_sea_metadata_url: str = (
        "https://data.chhs.ca.gov/api/3/action/package_show?id=1e1e2904-1bfb-448c-97e1-cf3e228c9159"
    )

    # CA CDSS Community Care Licensing — facility list and violations
    # Bulk download from: https://www.ccld.dss.ca.gov/carefacilitysearch/
    cdss_facility_csv_url: str = (
        "https://data.chhs.ca.gov/dataset/community-care-licensing-facility-information"
        "/resource/locations.csv"
    )
    cdss_violations_csv_url: str = (
        "https://data.chhs.ca.gov/dataset/community-care-licensing-facility-information"
        "/resource/violations.csv"
    )


settings = Settings()
