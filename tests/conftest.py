"""Pytest fixtures for Assignment A tests."""

import os
import pytest
from pathlib import Path
from typing import Tuple

from solution.db import Database
from solution.schema_registry import SchemaRegistry


@pytest.fixture
def env_keys():
    """Set up ephemeral encryption/HMAC keys for tests."""
    # Generate test keys
    from cryptography.fernet import Fernet
    import secrets

    fernet_key = Fernet.generate_key()
    hmac_key = secrets.token_bytes(32)

    os.environ["ENCRYPTION_KEY"] = fernet_key.decode("utf-8")
    os.environ["HMAC_KEY"] = hmac_key.hex()

    yield

    # Cleanup (optional, process exits anyway)
    del os.environ["ENCRYPTION_KEY"]
    del os.environ["HMAC_KEY"]


@pytest.fixture
def schema_dir():
    """Return path to assignment-a-data/schemas."""
    return Path("assignment-a-data/schemas")


@pytest.fixture
def dispositions_path():
    """Return path to assignment-a-data/schemas/dispositions.csv."""
    return Path("assignment-a-data/schemas/dispositions.csv")


@pytest.fixture
def db_and_registry(tmp_path, assignment_data_path):
    """Create database and schema registry."""
    db_path = tmp_path / "test.db"
    db = Database(db_path)
    db.initialize()

    schema_dir = assignment_data_path / "schemas"
    dispositions_path = assignment_data_path / "schemas" / "dispositions.csv"

    registry = SchemaRegistry(schema_dir, dispositions_path)

    yield db, registry

    db.close()


@pytest.fixture
def assignment_data_path():
    return Path("assignment-a-data")


@pytest.fixture
def test_data(tmp_path):
    data_dir = tmp_path / "data"
    events_dir = data_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    leads_dir = data_dir / "leads" / "motor_renewal"
    leads_dir.mkdir(parents=True, exist_ok=True)
    yield data_dir
