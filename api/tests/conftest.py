import os
from dataclasses import replace

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import DEFAULT_SCHEMA
from app.main import create_app

DSN = os.getenv("TASKFORGE_TEST_DSN", "")
KEY = "test-key-123"


@pytest.fixture(scope="session", autouse=True)
def _schema():
    if not DSN:
        pytest.skip("TASKFORGE_TEST_DSN not set", allow_module_level=True)
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute(DEFAULT_SCHEMA.read_text())


@pytest.fixture()
def db():
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("TRUNCATE jobs, job_attempts, job_logs, dead_letters, webhook_deliveries, workers CASCADE")
        yield c


@pytest.fixture()
def settings():
    return Settings(dsn=DSN, api_keys=(KEY,))


@pytest.fixture()
def client(db, settings):
    with TestClient(create_app(settings), headers={"X-API-Key": KEY}) as c:
        yield c


def submit(client, **over):
    body = {"task_type": "flaky_api", "payload": {"endpoint": "pay/charge"}}
    body.update(over)
    return client.post("/v1/jobs", json=body)
