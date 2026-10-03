"""Load test for TaskForge's REST API: submission throughput and status-lookup
latency under concurrent load. Run against a live api + scheduler + postgres:

    locust -f locustfile.py --host http://localhost:8000 --headless \
           -u 50 -r 10 -t 60s --csv results

TASKFORGE_LOAD_API_KEY must match the running API's configured key.
"""
import os
import random
import uuid

from locust import HttpUser, task, between, events

API_KEY = os.getenv("TASKFORGE_LOAD_API_KEY", "load-test-key")
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

TASK_BODIES = [
    lambda: {"task_type": "flaky_api", "priority": random.randint(0, 9),
             "payload": {"endpoint": "load/test", "failure_rate": 0.1, "latency_ms": 20}},
    lambda: {"task_type": "generate_report", "priority": 5, "payload": {"days": 1, "format": "json"}},
]

submitted_ids = []  # shared across users in this process: fine for a demo-scale load test


class ApiUser(HttpUser):
    wait_time = between(0.01, 0.05)

    @task(5)
    def submit_job(self):
        body = random.choice(TASK_BODIES)()
        with self.client.post("/v1/jobs", json=body, headers=HEADERS, catch_response=True) as r:
            if r.status_code == 201:
                jid = r.json().get("id")
                if jid:
                    submitted_ids.append(jid)
                    if len(submitted_ids) > 2000:
                        del submitted_ids[:1000]
                r.success()
            else:
                r.failure(f"unexpected status {r.status_code}: {r.text[:200]}")

    @task(3)
    def get_job_status(self):
        if not submitted_ids:
            return
        jid = random.choice(submitted_ids)
        with self.client.get(f"/v1/jobs/{jid}", headers=HEADERS, catch_response=True, name="/v1/jobs/[id]") as r:
            if r.status_code in (200, 404):  # 404 is fine: job may have rotated out of the shared list
                r.success()
            else:
                r.failure(f"unexpected status {r.status_code}")

    @task(2)
    def list_jobs(self):
        params = {"limit": 20}
        if random.random() < 0.5:
            params["status"] = random.choice(["queued", "running", "succeeded", "dead"])
        with self.client.get("/v1/jobs", params=params, headers=HEADERS, catch_response=True, name="/v1/jobs?filter") as r:
            r.success() if r.status_code == 200 else r.failure(f"status {r.status_code}")

    @task(1)
    def stats(self):
        with self.client.get("/v1/stats", headers=HEADERS, catch_response=True) as r:
            r.success() if r.status_code == 200 else r.failure(f"status {r.status_code}")


@events.test_start.add_listener
def on_start(environment, **kwargs):
    print(f"load test starting against {environment.host}, api key set: {bool(API_KEY)}")
