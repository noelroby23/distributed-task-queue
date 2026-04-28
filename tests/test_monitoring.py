from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.worker import worker_iteration


def test_store_metrics_track_pending_scheduled_and_queue_depths(store):
    store.create_task(task_type="fibonacci", args=[4], priority="high")
    store.create_task(
        task_type="fibonacci",
        args=[6],
        priority="low",
        run_at=datetime.now(timezone.utc) + timedelta(minutes=1),
    )

    metrics = store.get_metrics()

    assert metrics["status_counts"]["pending"] == 1
    assert metrics["status_counts"]["scheduled"] == 1
    assert metrics["queue_depths"] == {"high": 1, "medium": 0, "low": 0}
    assert metrics["scheduled_count"] == 1
    assert metrics["totals"]["tasks_created_total"] == 2


def test_store_metrics_update_after_completion_and_retry(store):
    task = store.create_task(task_type="flaky", kwargs={"fail_count": 1}, priority="high")

    worker_iteration(store.redis, store, "worker-metrics", store.runnable_queue_names(), timeout=1)
    after_failure = store.get_metrics()
    assert after_failure["status_counts"]["scheduled"] == 1
    assert after_failure["retry_stats"]["task_failures_total"] == 1
    assert after_failure["retry_stats"]["task_retries_total"] == 1

    store.redis.zadd(store.scheduled_queue_name, {task["task_id"]: 0})
    worker_iteration(store.redis, store, "worker-metrics", store.runnable_queue_names(), timeout=1)

    final_metrics = store.get_metrics()
    assert final_metrics["status_counts"]["completed"] == 1
    assert final_metrics["status_counts"]["running"] == 0
    assert final_metrics["totals"]["tasks_completed_total"] == 1
    assert final_metrics["workers"]["active_count"] >= 1


def test_store_metrics_track_dead_letter_and_timeout(store):
    task = store.create_task(task_type="dummy", kwargs={"sleep_seconds": 2}, timeout_seconds=1)

    worker_iteration(store.redis, store, "worker-timeout-metrics", store.runnable_queue_names(), timeout=1)
    for _ in range(3):
        store.redis.zadd(store.scheduled_queue_name, {task["task_id"]: 0})
        worker_iteration(store.redis, store, "worker-timeout-metrics", store.runnable_queue_names(), timeout=1)

    metrics = store.get_metrics()
    assert metrics["status_counts"]["dead_lettered"] == 1
    assert metrics["retry_stats"]["task_timeouts_total"] >= 1
    assert metrics["totals"]["dead_lettered_total"] == 1
    assert metrics["dead_letter_queue_depth"] == 1


def test_metrics_endpoint_returns_operational_snapshot(client, store):
    store.create_task(task_type="fibonacci", args=[5])

    response = client.get("/metrics")
    payload = response.json()

    assert response.status_code == 200
    assert payload["status_counts"]["pending"] == 1
    assert "queue_depths" in payload
    assert "retry_stats" in payload
    assert "workers" in payload


def test_dashboard_endpoint_returns_html(client):
    response = client.get("/dashboard")

    assert response.status_code == 200
    assert "Distributed Task Queue Dashboard" in response.text
    assert "/metrics" in response.text
