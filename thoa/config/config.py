from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):

    THOA_API_URL: str = "http://localhost:21002"
    THOA_API_KEY: Optional[str] = "rs_a3b7c0d9fb1c2a25ba94704f0f7ddb261b9e2b10d70f961219b7a54c9b9fa5c61b470a7021"
    THOA_API_TIMEOUT: int = 30

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (env_settings,)

settings = Settings()
