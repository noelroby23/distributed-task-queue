from __future__ import annotations

from datetime import datetime, timedelta, timezone

def test_store_create_and_mark_completed(store):
    task = store.create_task(
        task_type="word_count",
        kwargs={"text": "one two two"},
        timeout_seconds=11,
    )

    assert task["task_type"] == "word_count"
    assert task["status"] == "pending"
    assert task["priority"] == "medium"
    assert task["run_at"] is None
    assert task["timeout_seconds"] == 11
    assert task["attempt_count"] == 0

    running = store.mark_running(task["task_id"], worker_id="worker-test")
    assert running is not None
    assert running["status"] == "running"
    assert running["attempt_count"] == 1

    updated = store.mark_completed(task["task_id"], result={"word_count": 3}, worker_id="worker-test")

    assert updated is not None
    assert updated["status"] == "completed"
    assert updated["result"] == {"word_count": 3}
    assert updated["worker_id"] == "worker-test"


def test_patch_task_metadata_only_updates_non_lifecycle_fields(store):
    task = store.create_task(task_type="fibonacci", args=[5])

    patched = store.patch_task_metadata(task["task_id"], timeout_seconds=99, worker_id="worker-meta")

    assert patched is not None
    assert patched["status"] == "pending"
    assert patched["timeout_seconds"] == 99
    assert patched["worker_id"] == "worker-meta"


def test_store_creates_scheduled_task_without_runnable_enqueue(store):
    run_at = datetime.now(timezone.utc) + timedelta(minutes=1)

    task = store.create_task(task_type="fibonacci", args=[7], priority="high", run_at=run_at)

    assert task["status"] == "scheduled"
    assert task["priority"] == "high"
    assert task["run_at"] == run_at
    assert store.scheduled_count() == 1
    assert store.runnable_queue_lengths() == {"high": 0, "medium": 0, "low": 0}
