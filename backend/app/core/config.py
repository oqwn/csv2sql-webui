from typing import List, Union
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import os


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=".env"
    )

    PROJECT_NAME: str = "SQL WebUI"
    VERSION: str = "0.1.0"
    API_V1_STR: str = "/api/v1"
    
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost/sqlwebui"
    )
    
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    BACKEND_CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:8000", 
        "http://localhost",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8000"
    ]

    @field_validator("BACKEND_CORS_ORIGINS", mode='before')
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, str):
            if v.startswith("[") and v.endswith("]"):
                # Handle JSON string format
                import json
                try:
                    return json.loads(v)
                except json.JSONDecodeError:
                    pass
            # Handle comma-separated string
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, list):
            return v
        return [
            "http://localhost:3000",
            "http://localhost:8000", 
            "http://localhost",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:8000"
        ]


settings = Settings()