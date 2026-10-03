from tests.conftest import submit


def test_metrics_endpoint_is_open_and_prometheus_formatted(db, settings):
    from fastapi.testclient import TestClient
    from app.main import create_app
    with TestClient(create_app(settings)) as c:  # no API key header: /metrics must not require auth (scrapers)
        r = c.get("/metrics")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]
    assert "taskforge_jobs_by_status" in r.text


def test_metrics_reflect_job_counts(client, db):
    submit(client)
    submit(client)
    db.execute("UPDATE jobs SET status='dead' WHERE id = (SELECT id FROM jobs LIMIT 1)")
    body = client.get("/metrics").text
    assert 'taskforge_jobs_by_status{status="queued"} 1.0' in body
    assert 'taskforge_jobs_by_status{status="dead"} 1.0' in body


def test_metrics_count_requests_and_submissions(client, db):
    submit(client)
    client.get("/v1/jobs")
    body = client.get("/metrics").text
    assert 'taskforge_api_jobs_submitted_total{task_type="flaky_api"} 1.0' in body
    assert "taskforge_api_requests_total" in body
