from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):

    THOA_API_URL: str = "https://api.thoa.io"
    THOA_UI_URL: str = "https://thoa.io" # @todo-sergio move it into ENV
    THOA_API_KEY: Optional[str] = None
    THOA_API_DEBUG: bool = False
    THOA_API_TIMEOUT: int = 30
    THOA_GDRIVE_CALLBACK_HOST: str = "127.0.0.1"
    THOA_GDRIVE_CALLBACK_PORT: int = 54389
    THOA_GDRIVE_OPEN_BROWSER: bool = True

    class Config:
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (env_settings,)

settings = Settings()
