from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from pathlib import Path

_env_file = Path(__file__).parent.parent.parent / ".env"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(_env_file), env_file_encoding="utf-8", extra="ignore")

    THOA_API_URL: str = "https://api.thoa.io"
    THOA_UI_URL: str = "https://thoa.io"
    THOA_API_KEY: Optional[str] = None
    THOA_API_DEBUG: bool = False
    THOA_API_TIMEOUT: int = 30

settings = Settings()
