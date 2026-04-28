from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from PIL import Image

from app.worker import execute_task_with_timeout, process_task, worker_iteration


def make_scheduled_task_due_now(store, task_id: str) -> None:
    store.redis.zadd(store.scheduled_queue_name, {task_id: 0})


def test_worker_processes_fibonacci_task(store):
    task = store.create_task(task_type="fibonacci", args=[10], timeout_seconds=5)

    result = worker_iteration(store.redis, store, "worker-1", store.runnable_queue_names(), timeout=1)

    assert result is True
    updated = store.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "completed"
    assert updated["result"] == {"n": 10, "value": 55}
    assert updated["worker_id"] == "worker-1"
    assert updated["attempt_count"] == 1
    assert updated["failure_type"] is None


def test_worker_retries_then_succeeds(store, monkeypatch):
    task = store.create_task(task_type="flaky", kwargs={"fail_count": 2}, timeout_seconds=5)

    assert worker_iteration(store.redis, store, "worker-1", store.runnable_queue_names(), timeout=1) is True
    after_first = store.get_task(task["task_id"])
    assert after_first is not None
    assert after_first["status"] == "scheduled"
    assert after_first["priority"] == "medium"
    assert after_first["retry_count"] == 1
    assert after_first["attempt_count"] == 1
    assert after_first["failure_type"] == "exception"
    assert after_first["failure_reason"] == "Task handler raised RuntimeError."
    assert after_first["first_failure_at"] is not None
    assert after_first["last_failure_at"] is not None
    assert "Simulated failure" in after_first["last_error"]

    make_scheduled_task_due_now(store, task["task_id"])
    assert worker_iteration(store.redis, store, "worker-2", store.runnable_queue_names(), timeout=1) is True
    after_second = store.get_task(task["task_id"])
    assert after_second is not None
    assert after_second["status"] == "scheduled"
    assert after_second["retry_count"] == 2
    assert after_second["attempt_count"] == 2

    make_scheduled_task_due_now(store, task["task_id"])
    assert worker_iteration(store.redis, store, "worker-3", store.runnable_queue_names(), timeout=1) is True
    final = store.get_task(task["task_id"])
    assert final is not None
    assert final["status"] == "completed"
    assert final["attempt_count"] == 3
    assert final["result"]["total_attempts"] == 3


def test_worker_dead_letters_task_after_retry_exhaustion(store, monkeypatch):
    task = store.create_task(task_type="flaky", kwargs={"fail_count": 10}, timeout_seconds=5)

    first_attempt = worker_iteration(store.redis, store, "worker-9", store.runnable_queue_names(), timeout=1)
    assert first_attempt is True
    assert store.dead_letter_size() == 0

    for _ in range(3):
        make_scheduled_task_due_now(store, task["task_id"])
        worker_iteration(store.redis, store, "worker-9", store.runnable_queue_names(), timeout=1)

    dead_lettered = store.get_task(task["task_id"])
    assert dead_lettered is not None
    assert dead_lettered["status"] == "dead_lettered"
    assert dead_lettered["retry_count"] == 3
    assert dead_lettered["attempt_count"] == 4
    assert dead_lettered["dead_lettered_at"] is not None
    assert dead_lettered["failure_type"] == "exception"
    assert "Simulated failure" in dead_lettered["result"]
    assert store.dead_letter_size() == 1
    assert store.redis.lrange(store.dead_letter_queue_name, 0, -1) == [task["task_id"]]


def test_execute_task_with_timeout_reports_timeout():
    outcome = execute_task_with_timeout(
        task_type="dummy",
        task_args=[],
        task_kwargs={"sleep_seconds": 2},
        timeout_seconds=1,
    )

    assert outcome.succeeded is False
    assert outcome.failure_type == "timeout"
    assert outcome.error_message is not None
    assert "timeout of 1 seconds" in outcome.error_message


def test_worker_dead_letters_timed_out_task(store, monkeypatch):
    task = store.create_task(
        task_type="dummy",
        kwargs={"sleep_seconds": 2},
        timeout_seconds=1,
    )

    worker_iteration(store.redis, store, "worker-timeout", store.runnable_queue_names(), timeout=1)
    for _ in range(3):
        make_scheduled_task_due_now(store, task["task_id"])
        worker_iteration(store.redis, store, "worker-timeout", store.runnable_queue_names(), timeout=1)

    timed_out = store.get_task(task["task_id"])
    assert timed_out is not None
    assert timed_out["status"] == "dead_lettered"
    assert timed_out["failure_type"] == "timeout"
    assert timed_out["failure_reason"] == "Task exceeded its configured execution timeout."
    assert timed_out["dead_lettered_at"] is not None
    assert timed_out["timeout_seconds"] == 1
    assert timed_out["attempt_count"] == 4
    assert timed_out["retry_count"] == 3
    assert timed_out["last_error"] == timed_out["result"]
    assert timed_out["last_error"] is not None and "timeout of 1 seconds" in timed_out["last_error"]
    assert store.redis.lrange(store.dead_letter_queue_name, 0, -1) == [task["task_id"]]


def test_unknown_task_type_is_dead_lettered_immediately(store):
    task = store.create_task(task_type="missing_handler")

    process_task(task["task_id"], store.redis, store, "worker-x")
    failed = store.get_task(task["task_id"])

    assert failed is not None
    assert failed["status"] == "dead_lettered"
    assert failed["failure_type"] == "unknown_task"
    assert failed["dead_lettered_at"] is not None
    assert store.redis.lrange(store.dead_letter_queue_name, 0, -1) == [task["task_id"]]


def test_image_resize_task_creates_output_file(store, tmp_path: Path):
    input_path = tmp_path / "input.png"
    output_path = tmp_path / "resized" / "output.png"

    Image.new("RGB", (12, 12), color="red").save(input_path)

    task = store.create_task(
        task_type="image_resize",
        kwargs={
            "input_path": str(input_path),
            "output_path": str(output_path),
            "width": 4,
            "height": 6,
        },
        timeout_seconds=5,
    )

    worker_iteration(store.redis, store, "worker-img", store.runnable_queue_names(), timeout=1)
    updated = store.get_task(task["task_id"])

    assert updated is not None
    assert updated["status"] == "completed"
    assert output_path.exists()

    with Image.open(output_path) as image:
        assert image.size == (4, 6)


def test_default_priority_is_medium(store):
    task = store.create_task(task_type="fibonacci", args=[3])

    assert task["priority"] == "medium"
    assert store.runnable_queue_lengths() == {"high": 0, "medium": 1, "low": 0}


def test_high_priority_is_processed_before_low(store):
    low_task = store.create_task(task_type="fibonacci", args=[2], priority="low")
    high_task = store.create_task(task_type="fibonacci", args=[9], priority="high")

    processed = worker_iteration(store.redis, store, "worker-priority", store.runnable_queue_names(), timeout=1)

    assert processed is True
    high_state = store.get_task(high_task["task_id"])
    low_state = store.get_task(low_task["task_id"])
    assert high_state is not None and high_state["status"] == "completed"
    assert low_state is not None and low_state["status"] == "pending"


def test_same_priority_tasks_preserve_fifo_order(store):
    first_task = store.create_task(task_type="fibonacci", args=[4], priority="medium")
    second_task = store.create_task(task_type="fibonacci", args=[6], priority="medium")

    worker_iteration(store.redis, store, "worker-fifo", store.runnable_queue_names(), timeout=1)

    first_state = store.get_task(first_task["task_id"])
    second_state = store.get_task(second_task["task_id"])
    assert first_state is not None and first_state["status"] == "completed"
    assert second_state is not None and second_state["status"] == "pending"


def test_scheduled_task_does_not_run_early(store):
    run_at = datetime.now(timezone.utc) + timedelta(seconds=2)
    task = store.create_task(task_type="fibonacci", args=[8], run_at=run_at)

    processed = worker_iteration(store.redis, store, "worker-scheduled", store.runnable_queue_names(), timeout=1)

    assert processed is False
    current = store.get_task(task["task_id"])
    assert current is not None
    assert current["status"] == "scheduled"
    assert store.scheduled_count() == 1


def test_scheduled_task_runs_after_it_is_due(store):
    run_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    task = store.create_task(task_type="fibonacci", args=[8], priority="high", run_at=run_at)

    time.sleep(1.1)
    processed = worker_iteration(store.redis, store, "worker-scheduled", store.runnable_queue_names(), timeout=1)

    assert processed is True
    current = store.get_task(task["task_id"])
    assert current is not None
    assert current["status"] == "completed"
    assert current["priority"] == "high"
    assert current["run_at"] == run_at
    assert store.scheduled_count() == 0


def test_due_task_promotion_happens_once(store):
    run_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    task = store.create_task(task_type="fibonacci", args=[8], priority="low", run_at=run_at)

    time.sleep(1.1)
    assert store.promote_due_scheduled_tasks() == 1
    assert store.promote_due_scheduled_tasks() == 0

    current = store.get_task(task["task_id"])
    assert current is not None
    assert current["status"] == "pending"
    assert store.redis.lrange(store.runnable_queue_name("low"), 0, -1) == [task["task_id"]]


def test_retry_preserves_priority_and_uses_scheduled_backoff(store):
    task = store.create_task(task_type="flaky", kwargs={"fail_count": 1}, priority="high", timeout_seconds=5)

    worker_iteration(store.redis, store, "worker-retry", store.runnable_queue_names(), timeout=1)
    scheduled_retry = store.get_task(task["task_id"])

    assert scheduled_retry is not None
    assert scheduled_retry["status"] == "scheduled"
    assert scheduled_retry["priority"] == "high"
    assert scheduled_retry["retry_count"] == 1
    assert scheduled_retry["run_at"] is not None

    make_scheduled_task_due_now(store, task["task_id"])
    worker_iteration(store.redis, store, "worker-retry", store.runnable_queue_names(), timeout=1)
    final = store.get_task(task["task_id"])

    assert final is not None
    assert final["status"] == "completed"
    assert final["priority"] == "high"
