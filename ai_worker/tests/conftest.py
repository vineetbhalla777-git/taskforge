import os
import sys
from pathlib import Path

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # import model, job_store, main directly

DSN = os.getenv("TASKFORGE_TEST_DSN", "")
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "db" / "schema.sql"


@pytest.fixture(scope="session", autouse=True)
def _schema():
    if not DSN:
        pytest.skip("TASKFORGE_TEST_DSN not set", allow_module_level=True)
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute(SCHEMA_PATH.read_text())


@pytest.fixture()
def conn():
    with psycopg.connect(DSN, autocommit=False) as c:
        with c.cursor() as cur:
            cur.execute("TRUNCATE jobs, job_attempts, job_logs, dead_letters, webhook_deliveries, workers CASCADE")
        c.commit()
        yield c


@pytest.fixture(scope="session")
def classifier(tmp_path_factory):
    from model import Classifier
    return Classifier(tmp_path_factory.mktemp("model") / "classifier.pt")
