from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql://geodata:geodata@localhost:5432/geodata"
    tiles_dir: str = "/tmp/geodata/tiles"
    jwt_secret: str = "dev-secret-change-in-prod"


settings = Settings()
