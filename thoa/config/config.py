from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):

    THOA_API_URL: str = "https://api.thoa.io"
    THOA_UI_URL: str = "https://thoa.io"
    THOA_API_KEY: Optional[str] = None
    THOA_API_DEBUG: bool = False
    THOA_API_TIMEOUT: int = 30

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (env_settings,)

settings = Settings()
