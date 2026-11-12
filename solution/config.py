"""Configuration loading from environment and .env file."""

import os
from pathlib import Path
from typing import Optional

import dotenv


def load_env(env_path: Optional[Path] = None) -> None:
    """Load environment variables from .env file if present."""
    if env_path is None:
        env_path = Path(".env")
    if env_path.exists():
        dotenv.load_dotenv(env_path)


def get_required_env(key: str, default: Optional[str] = None) -> str:
    """Get required environment variable or raise if missing."""
    value = os.environ.get(key, default)
    if value is None:
        raise ConfigurationError(f"Required environment variable '{key}' is not set")
    return value


def get_optional_env(key: str, default: str = "") -> str:
    """Get optional environment variable with default."""
    return os.environ.get(key, default)


class ConfigurationError(Exception):
    """Raised when required configuration is missing."""
    pass


# Convenience getters for required keys
def get_fernet_key() -> bytes:
    """Get Fernet encryption key from ENCRYPTION_KEY env var."""
    key = get_required_env("ENCRYPTION_KEY")
    return key.encode("utf-8")


def get_hmac_key() -> bytes:
    """Get HMAC key from HMAC_KEY env var."""
    key = get_required_env("HMAC_KEY")
    return key.encode("utf-8")


def get_db_path() -> Path:
    """Get database file path from env or default."""
    db_path_str = get_optional_env("DB_PATH", "agentic_universe.db")
    return Path(db_path_str)


def get_data_path() -> Path:
    """Get data directory path from env or default."""
    data_path_str = get_optional_env("DATA_PATH", "data")
    return Path(data_path_str)
