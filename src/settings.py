from typing import List
from pathlib import Path
from loguru import logger

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PlexSettings(BaseModel):
    url: str = Field(
        default="http://localhost:32400", description="The URL of the Plex server."
    )
    token: str = Field(
        default="", description="The authentication token for the Plex server."
    )

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="APP_", extra="ignore")
    log_level: str = Field(
        default="INFO", description="The logging level for the application."
    )

    media_extensions: List[str] = Field(
        default_factory=lambda: [
            ".mkv",
            ".mp4",
            ".avi",
            ".mov",
            ".wmv",
            ".flv",
            ".webm",
            ".mp3",
            ".flac",
            ".wav",
            ".aac",
            ".ogg",
            ".m4a",
        ],
        description="List of media file extensions to monitor.",
    )

    library_paths: List[str] = Field(
        default_factory=lambda: ["/path/to/plex/library"],
        description="List of paths to Plex libraries to rescan.",
    )

    plex: PlexSettings = Field(
        default_factory=PlexSettings,
        description="Settings for the Plex server connection.",
    )


def load_or_create_settings(path: Path) -> Settings:
    if path.exists():
        logger.info(f"Loading settings from {path}")
        return Settings.model_validate_json(path.read_text())
    else:
        logger.warning(f"Settings file {path} not found. Creating default settings.")
        default_settings = Settings()
        path.write_text(default_settings.model_dump_json(indent=4))
        return default_settings
