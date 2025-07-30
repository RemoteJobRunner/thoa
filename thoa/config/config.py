from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):

    THOA_API_URL: str = "http://localhost:3000"
    THOA_API_KEY: Optional[str] = "rs_a8b6c4f5eb22b66d0d7a2383ed8405ed1406c76dc236d7d07e1fa20bd82c6911f4479de9a7"
    THOA_API_TIMEOUT: int = 30

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (env_settings,)

settings = Settings()
