from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from pathlib import Path

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=[
            str(Path(__file__).parent.parent.parent / ".env"),
            str(Path.cwd() / ".env"),
        ],
        env_file_encoding="utf-8",
        extra="ignore",
    )

    THOA_API_URL: str = "https://api.thoa.io"
    THOA_UI_URL: str = "https://thoa.io"
    THOA_API_KEY: Optional[str] = None
    THOA_API_DEBUG: bool = False
    THOA_API_TIMEOUT: int = 30

settings = Settings()
