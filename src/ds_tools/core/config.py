"""Configuration management for ds-tools."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    log_level: str = "INFO"
    otel_enabled: bool = False
    otel_service_name: str = "ds-tools"
    otel_exporter_endpoint: str = "http://localhost:4317"

    model_config = {
        "env_prefix": "DS_TOOLS_",
        "case_sensitive": False,
    }


settings = Settings()
