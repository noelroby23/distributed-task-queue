from __future__ import annotations

from datetime import datetime, timedelta, timezone


def test_submit_task_returns_created_record(client, store):
    response = client.post(
        "/tasks",
        json={"task_type": "fibonacci", "args": [8], "kwargs": {}, "timeout_seconds": 9},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["task_type"] == "fibonacci"
    assert payload["status"] == "pending"
    assert payload["priority"] == "medium"
    assert payload["run_at"] is None
    assert payload["timeout_seconds"] == 9
    assert payload["attempt_count"] == 0
    assert payload["retry_count"] == 0

    stored = store.get_task(payload["task_id"])
    assert stored is not None
    assert stored["task_type"] == "fibonacci"


def test_submit_task_accepts_legacy_task_name_alias(client):
    response = client.post(
        "/tasks",
        json={"task_name": "word_count", "kwargs": {"text": "hello world"}},
    )

    assert response.status_code == 201
    assert response.json()["task_type"] == "word_count"


def test_get_task_status_returns_extended_metadata(client):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=1)
    create_response = client.post(
        "/tasks",
        json={
            "task_type": "fibonacci",
            "args": [5],
            "timeout_seconds": 12,
            "priority": "high",
            "run_at": run_at.isoformat(),
        },
    )

    task_id = create_response.json()["task_id"]
    response = client.get(f"/tasks/{task_id}")
    payload = response.json()

    assert response.status_code == 200
    assert payload["status"] == "scheduled"
    assert payload["priority"] == "high"
    assert payload["run_at"].startswith(run_at.strftime("%Y-%m-%dT%H:%M:%S"))
    assert payload["timeout_seconds"] == 12
    assert payload["attempt_count"] == 0
    assert payload["failure_type"] is None
    assert payload["dead_lettered_at"] is None


def test_get_task_status_returns_404_for_missing_task(client):
    response = client.get("/tasks/does-not-exist")

    assert response.status_code == 404


def test_submit_task_rejects_unknown_task_type(client):
    response = client.post("/tasks", json={"task_type": "not_registered"})

    assert response.status_code == 400
    assert "Unknown task_type" in response.json()["detail"]


def test_submit_task_rejects_invalid_timeout(client):
    response = client.post(
        "/tasks",
        json={"task_type": "fibonacci", "args": [3], "timeout_seconds": 0},
    )

    assert response.status_code == 422


def test_submit_task_rejects_naive_run_at(client):
    response = client.post(
        "/tasks",
        json={"task_type": "fibonacci", "args": [3], "run_at": "2030-01-01T00:00:00"},
    )

    assert response.status_code == 422
