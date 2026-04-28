from __future__ import annotations

import os
from pathlib import Path

import pytest
import redis
from fastapi.testclient import TestClient


os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_DB", "15")
os.environ.setdefault("QUEUE_NAME", "test_task_queue")
os.environ.setdefault("DEAD_LETTER_QUEUE_NAME", "test_task_queue_dead_letter")
os.environ.setdefault("SCHEDULED_QUEUE_NAME", "test_task_queue_scheduled")
os.environ.setdefault("MAX_RETRIES", "3")
os.environ.setdefault("DEFAULT_TASK_TIMEOUT", "5")
os.environ.setdefault("WORKER_POLL_TIMEOUT", "1")
os.environ.setdefault("WORKER_HEARTBEAT_TTL", "30")

from app.config import Settings  # noqa: E402
from app.main import app  # noqa: E402
from app.store import TaskStore  # noqa: E402


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def redis_client(test_settings: Settings):
    client = redis.from_url(test_settings.redis_url, decode_responses=True)
    try:
        client.ping()
    except redis.RedisError as exc:
        pytest.skip(f"Redis is required for these tests: {exc}")
    yield client
    client.flushdb()


@pytest.fixture()
def store(redis_client, test_settings: Settings):
    redis_client.flushdb()
    store = TaskStore(
        redis_url=test_settings.redis_url,
        queue_name=test_settings.queue_name,
        dead_letter_queue_name=test_settings.dead_letter_queue_name,
        scheduled_queue_name=test_settings.scheduled_queue_name,
    )
    yield store
    redis_client.flushdb()


@pytest.fixture()
def client(store: TaskStore):
    import app.main as main_module

    original = main_module.get_task_store
    main_module.get_task_store = lambda: store
    with TestClient(app) as test_client:
        yield test_client
    main_module.get_task_store = original


@pytest.fixture()
def temp_output_dir(tmp_path: Path) -> Path:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir
