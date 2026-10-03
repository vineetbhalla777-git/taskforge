import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from tests.conftest import DSN, KEY, submit


# ---------------------------------------------------------------- auth
def test_healthz_is_open(db, settings):
    with TestClient(create_app(settings)) as c:
        assert c.get("/healthz").json() == {"status": "ok"}


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}])
def test_auth_rejects_bad_keys(db, settings, headers):
    with TestClient(create_app(settings)) as c:
        assert c.get("/v1/jobs", headers=headers).status_code == 401
        assert c.post("/v1/jobs", json={}, headers=headers).status_code == 401


def test_app_refuses_to_start_without_keys():
    with pytest.raises(RuntimeError):
        create_app(Settings(dsn=DSN, api_keys=()))


# ---------------------------------------------------------------- submit + validation
def test_submit_returns_id_and_persists(client, db):
    r = submit(client, priority=8, max_retries=2)
    assert r.status_code == 201
    j = r.json()
    uuid.UUID(j["id"])
    assert (j["status"], j["priority"], j["max_retries"], j["attempts"]) == ("queued", 8, 2, 0)
    row = db.execute("SELECT status, priority FROM jobs WHERE id = %s", (j["id"],)).fetchone()
    assert row == ("queued", 8)


def test_payload_defaults_are_filled_in(client):
    j = submit(client).json()
    assert j["payload"] == {"endpoint": "pay/charge", "failure_rate": 0.5, "latency_ms": 100,
                            "fail_first_n_attempts": 0, "permanent": False}


@pytest.mark.parametrize("over", [
    {"task_type": "rm_rf"},                                   # unregistered type
    {"task_type": "shell", "payload": {"cmd": "ls"}},
    {"payload": {"endpoint": "x", "unknown": 1}},             # extra field
    {"payload": {"endpoint": "x; rm -rf /"}},                 # shell metacharacters
    {"payload": {"endpoint": "x", "failure_rate": 1.5}},
    {"payload": {"endpoint": "x", "latency_ms": 10**9}},      # resource limit
    {"priority": 10}, {"priority": -1},
    {"max_retries": -1}, {"max_retries": 11},
    {"unexpected_field": True},
    {"run_at": "2030-01-01T00:00:00", },                      # naive datetime (no timezone)
    {"run_at": "2030-01-01T00:00:00Z", "delay_seconds": 5},   # both given
    {"delay_seconds": 10**9},
    {"idempotency_key": "has space"},
])
def test_validation_rejects(client, over):
    assert submit(client, **over).status_code == 422


def test_image_resize_limits_and_path_safety(client):
    def go(**p):
        base = {"source": "a.png", "width": 100, "height": 100}
        return client.post("/v1/jobs", json={"task_type": "image_resize", "payload": {**base, **p}})
    assert go().status_code == 201
    for bad in ({"source": "../secret.png"}, {"source": "/etc/passwd"}, {"source": "a/b.png"},
                {"source": "..hidden..png"}, {"width": 100000}, {"height": 0}, {"format": "exe"}):
        assert go(**bad).status_code == 422, bad


def test_report_payload(client):
    r = client.post("/v1/jobs", json={"task_type": "generate_report", "payload": {"days": 30, "format": "json"}})
    assert r.status_code == 201 and r.json()["payload"]["days"] == 30
    assert client.post("/v1/jobs", json={"task_type": "generate_report", "payload": {"days": 9999}}).status_code == 422


def test_oversized_body_is_rejected(db):
    s = Settings(dsn=DSN, api_keys=(KEY,), max_body_bytes=200)
    with TestClient(create_app(s), headers={"X-API-Key": KEY}) as c:
        r = c.post("/v1/jobs", json={"task_type": "flaky_api", "payload": {"endpoint": "x" * 150}, "priority": 1})
        assert r.status_code == 413


def test_bad_json_and_bad_uuid(client):
    assert client.post("/v1/jobs", content=b"{not json", headers={"content-type": "application/json"}).status_code == 422
    assert client.get("/v1/jobs/not-a-uuid").status_code == 422
    assert client.get(f"/v1/jobs/{uuid.uuid4()}").status_code == 404


# ---------------------------------------------------------------- delayed jobs
def test_delay_seconds_and_run_at(client, db):
    j = submit(client, delay_seconds=3600).json()
    delta = datetime.fromisoformat(j["run_at"]) - datetime.now(timezone.utc)
    assert timedelta(minutes=59) < delta < timedelta(minutes=61)
    when = datetime.now(timezone.utc) + timedelta(days=2)
    j2 = submit(client, run_at=when.isoformat()).json()
    assert abs((datetime.fromisoformat(j2["run_at"]) - when).total_seconds()) < 1
    j3 = submit(client).json()  # immediate
    assert datetime.fromisoformat(j3["run_at"]) <= datetime.now(timezone.utc)


# ---------------------------------------------------------------- idempotency
def test_idempotency_key_dedups(client, db):
    a = submit(client, idempotency_key="order-1")
    b = submit(client, idempotency_key="order-1")
    assert a.status_code == 201 and b.status_code == 200
    assert a.json()["id"] == b.json()["id"]
    assert db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1


def test_idempotency_key_conflict_on_different_request(client):
    assert submit(client, idempotency_key="k").status_code == 201
    r = submit(client, idempotency_key="k", payload={"endpoint": "other"})
    assert r.status_code == 409


def test_idempotency_header(client, db):
    body = {"task_type": "flaky_api", "payload": {"endpoint": "e"}}
    a = client.post("/v1/jobs", json=body, headers={"Idempotency-Key": "h1"})
    b = client.post("/v1/jobs", json=body, headers={"Idempotency-Key": "h1"})
    assert (a.status_code, b.status_code) == (201, 200) and a.json()["id"] == b.json()["id"]
    r = client.post("/v1/jobs", json={**body, "idempotency_key": "zzz"}, headers={"Idempotency-Key": "h1"})
    assert r.status_code == 422


def test_idempotency_race_creates_one_job(client, db):
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(8) as ex:
        rs = list(ex.map(lambda _: submit(client, idempotency_key="race"), range(16)))
    assert sorted({r.status_code for r in rs}) == [200, 201]
    assert sum(r.status_code == 201 for r in rs) == 1
    assert len({r.json()["id"] for r in rs}) == 1
    assert db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1


# ---------------------------------------------------------------- read / filter
def test_filters_and_pagination(client, db):
    for i in range(5):
        submit(client, priority=i)
    submit(client, task_type="generate_report", payload={})
    db.execute("UPDATE jobs SET status='succeeded' WHERE priority = 0 AND task_type='flaky_api'")
    assert len(client.get("/v1/jobs").json()["items"]) == 6
    assert len(client.get("/v1/jobs", params={"status": "succeeded"}).json()["items"]) == 1
    assert len(client.get("/v1/jobs", params={"task_type": "generate_report"}).json()["items"]) == 1
    assert len(client.get("/v1/jobs", params={"priority": 3}).json()["items"]) == 1
    page1 = client.get("/v1/jobs", params={"limit": 4}).json()
    assert len(page1["items"]) == 4 and page1["next_offset"] == 4
    page2 = client.get("/v1/jobs", params={"limit": 4, "offset": 4}).json()
    assert len(page2["items"]) == 2 and page2["next_offset"] is None
    ids = [j["id"] for j in page1["items"] + page2["items"]]
    assert len(set(ids)) == 6
    assert client.get("/v1/jobs", params={"status": "bogus"}).status_code == 422
    assert client.get("/v1/jobs", params={"limit": 1000}).status_code == 422


def test_job_detail_includes_history_and_logs(client, db):
    jid = submit(client).json()["id"]
    db.execute("INSERT INTO job_attempts (job_id, attempt, worker_id, outcome, error) VALUES (%s,1,'w1','failed','boom')", (jid,))
    db.execute("INSERT INTO job_logs (job_id, attempt, level, message) VALUES (%s,1,'info','hello'), (%s,1,'error','bad')", (jid, jid))
    d = client.get(f"/v1/jobs/{jid}").json()
    assert d["history"][0]["error"] == "boom" and d["history"][0]["worker_id"] == "w1"
    logs = client.get(f"/v1/jobs/{jid}/logs").json()
    assert [l["message"] for l in logs] == ["hello", "bad"]
    assert [l["message"] for l in client.get(f"/v1/jobs/{jid}/logs", params={"after_id": logs[0]["id"]}).json()] == ["bad"]
    assert client.get(f"/v1/jobs/{uuid.uuid4()}/logs").status_code == 404


# ---------------------------------------------------------------- cancel
def test_cancel_queued_job(client, db):
    jid = submit(client).json()["id"]
    r = client.post(f"/v1/jobs/{jid}/cancel")
    assert r.status_code == 200 and r.json()["status"] == "cancelled" and r.json()["finished_at"]
    assert client.post(f"/v1/jobs/{jid}/cancel").status_code == 409  # already cancelled


def test_cannot_cancel_running_or_finished(client, db):
    jid = submit(client).json()["id"]
    db.execute("UPDATE jobs SET status='running', worker_id='w', lease_expires_at=now()+interval '1 min' WHERE id=%s", (jid,))
    assert client.post(f"/v1/jobs/{jid}/cancel").status_code == 409
    db.execute("UPDATE jobs SET status='succeeded', worker_id=NULL, lease_expires_at=NULL WHERE id=%s", (jid,))
    assert client.post(f"/v1/jobs/{jid}/cancel").status_code == 409
    assert client.post(f"/v1/jobs/{uuid.uuid4()}/cancel").status_code == 404


# ---------------------------------------------------------------- dead letters
def test_dead_letter_list_and_requeue(client, db):
    jid = submit(client).json()["id"]
    db.execute("UPDATE jobs SET status='dead', attempts=4, last_error='HTTP 503' WHERE id=%s", (jid,))
    db.execute("INSERT INTO dead_letters (job_id, reason, attempts, last_error, payload) "
               "SELECT id, 'retries_exhausted', attempts, last_error, payload FROM jobs WHERE id=%s", (jid,))
    dl = client.get("/v1/dead-letters").json()
    assert len(dl) == 1 and dl[0]["reason"] == "retries_exhausted" and dl[0]["task_type"] == "flaky_api"
    r = client.post(f"/v1/dead-letters/{jid}/requeue")
    assert r.status_code == 200 and r.json()["status"] == "queued" and r.json()["attempts"] == 0
    assert client.get("/v1/dead-letters").json() == []
    assert client.post(f"/v1/dead-letters/{jid}/requeue").status_code == 404


# ---------------------------------------------------------------- misc
def test_stats(client, db):
    for _ in range(3):
        submit(client)
    db.execute("UPDATE jobs SET status='succeeded' WHERE id = (SELECT id FROM jobs LIMIT 1)")
    s = client.get("/v1/stats").json()
    assert s["by_status"]["queued"] == 2 and s["by_status"]["succeeded"] == 1 and s["by_status"]["dead"] == 0
    assert s["oldest_waiting_seconds"] is not None


def test_task_types_endpoint(client):
    t = client.get("/v1/task-types").json()
    assert set(t) == {"image_resize", "generate_report", "flaky_api", "classify_document"}
    assert "width" in t["image_resize"]["properties"]
