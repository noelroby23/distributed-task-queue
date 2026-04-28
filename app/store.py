"""
Redis-backed task repository and queue broker.

Task records live in Redis hashes under ``task:{id}``.
Runnable work lives in Redis lists split by priority:

    task_queue:high
    task_queue:medium
    task_queue:low

Future work is stored in one Redis sorted set keyed by ``run_at`` timestamps.
When a task becomes due, it is promoted into its runnable priority list.
Terminal failures are also pushed to a dedicated dead-letter list.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, cast

import redis

from app.config import settings
from app.models import TaskPriority, TaskStatus


class TaskStore:
    """Read and write task records plus queue entries in Redis."""

    def __init__(
        self,
        redis_url: str,
        queue_name: str,
        dead_letter_queue_name: str | None = None,
        scheduled_queue_name: str | None = None,
    ) -> None:
        self._redis: redis.Redis = redis.from_url(redis_url, decode_responses=True)
        self._queue_name = queue_name
        self._dead_letter_queue_name = dead_letter_queue_name or settings.dead_letter_queue_name
        self._scheduled_queue_name = scheduled_queue_name or settings.scheduled_queue_name

    @property
    def redis(self) -> redis.Redis:
        return self._redis

    @property
    def queue_name(self) -> str:
        return self._queue_name

    @property
    def dead_letter_queue_name(self) -> str:
        return self._dead_letter_queue_name

    @property
    def scheduled_queue_name(self) -> str:
        return self._scheduled_queue_name

    @property
    def metrics_status_key(self) -> str:
        return f"{self._queue_name}:metrics:status"

    @property
    def metrics_stats_key(self) -> str:
        return f"{self._queue_name}:metrics:stats"

    @property
    def workers_key(self) -> str:
        return f"{self._queue_name}:workers"

    def runnable_queue_name(self, priority: str) -> str:
        return f"{self._queue_name}:{priority}"

    def runnable_queue_names(self) -> list[str]:
        return [
            self.runnable_queue_name(TaskPriority.HIGH.value),
            self.runnable_queue_name(TaskPriority.MEDIUM.value),
            self.runnable_queue_name(TaskPriority.LOW.value),
        ]

    def _task_key(self, task_id: str) -> str:
        return f"task:{task_id}"

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _now_iso(self) -> str:
        return self._now().isoformat()

    def _normalize_dt(self, value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _priority_value(self, priority: str | TaskPriority | None) -> str:
        if isinstance(priority, TaskPriority):
            return priority.value
        if priority in {TaskPriority.HIGH.value, TaskPriority.MEDIUM.value, TaskPriority.LOW.value}:
            return cast(str, priority)
        return TaskPriority.MEDIUM.value

    def _metrics_increment(self, pipeline: Any, key: str, field: str, amount: int = 1) -> None:
        pipeline.hincrby(key, field, amount)

    def _status_transition(self, pipeline: Any, old_status: str | None, new_status: str | None) -> None:
        if old_status == new_status:
            return
        if old_status:
            self._metrics_increment(pipeline, self.metrics_status_key, old_status, -1)
        if new_status:
            self._metrics_increment(pipeline, self.metrics_status_key, new_status, 1)

    def _ensure_metrics_initialized(self) -> None:
        """Build status counters once if this Redis DB predates metrics support."""
        if self._redis.exists(self.metrics_status_key):
            return

        counts = {
            TaskStatus.PENDING.value: 0,
            TaskStatus.SCHEDULED.value: 0,
            TaskStatus.RUNNING.value: 0,
            TaskStatus.COMPLETED.value: 0,
            TaskStatus.FAILED.value: 0,
            TaskStatus.DEAD_LETTERED.value: 0,
        }
        for key in self._redis.scan_iter(match="task:*"):
            data = cast(dict[str, str], self._redis.hgetall(key))
            status = data.get("status", TaskStatus.PENDING.value)
            counts[status] = counts.get(status, 0) + 1

        pipeline = self._redis.pipeline()
        pipeline.hset(self.metrics_status_key, mapping={field: str(value) for field, value in counts.items()})
        pipeline.hsetnx(self.metrics_stats_key, "tasks_created_total", "0")
        pipeline.hsetnx(self.metrics_stats_key, "tasks_completed_total", "0")
        pipeline.hsetnx(self.metrics_stats_key, "task_failures_total", "0")
        pipeline.hsetnx(self.metrics_stats_key, "task_retries_total", "0")
        pipeline.hsetnx(self.metrics_stats_key, "task_timeouts_total", "0")
        pipeline.hsetnx(self.metrics_stats_key, "dead_lettered_total", "0")
        pipeline.hsetnx(self.metrics_stats_key, "scheduled_promotions_total", "0")
        pipeline.execute()

    def _enqueue_runnable(self, pipeline: Any, task_id: str, priority: str) -> None:
        pipeline.lpush(self.runnable_queue_name(priority), task_id)

    def _schedule_task_in_pipeline(self, pipeline: Any, task_id: str, run_at: datetime) -> None:
        pipeline.zadd(self._scheduled_queue_name, {task_id: run_at.timestamp()})

    def create_task(
        self,
        task_type: str,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
        timeout_seconds: Optional[int] = None,
        priority: str | TaskPriority | None = None,
        run_at: Optional[datetime] = None,
    ) -> dict:
        """Create a task record and either enqueue or schedule it."""
        task_id = str(uuid.uuid4())
        now = self._now()
        effective_timeout = timeout_seconds or settings.default_task_timeout
        effective_priority = self._priority_value(priority)
        normalized_run_at = self._normalize_dt(run_at)
        is_scheduled = normalized_run_at is not None and normalized_run_at > now

        task_data = {
            "task_id": task_id,
            "task_type": task_type,
            "status": TaskStatus.SCHEDULED.value if is_scheduled else TaskStatus.PENDING.value,
            "priority": effective_priority,
            "run_at": normalized_run_at.isoformat() if normalized_run_at is not None else "",
            "result": "",
            "args": json.dumps(args or []),
            "kwargs": json.dumps(kwargs or {}),
            "attempt_count": "0",
            "retry_count": "0",
            "max_retries": str(settings.max_retries),
            "timeout_seconds": str(effective_timeout),
            "last_error": "",
            "failure_type": "",
            "failure_reason": "",
            "first_failure_at": "",
            "last_failure_at": "",
            "dead_lettered_at": "",
            "worker_id": "",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        pipeline = self._redis.pipeline()
        pipeline.hset(self._task_key(task_id), mapping=task_data)
        if is_scheduled:
            assert normalized_run_at is not None
            self._schedule_task_in_pipeline(pipeline, task_id, normalized_run_at)
        else:
            self._enqueue_runnable(pipeline, task_id, effective_priority)
        self._status_transition(pipeline, None, task_data["status"])
        self._metrics_increment(pipeline, self.metrics_stats_key, "tasks_created_total")
        pipeline.execute()
        return self._deserialize_task(task_data)

    def get_task(self, task_id: str) -> Optional[dict]:
        data = cast(dict[str, str], self._redis.hgetall(self._task_key(task_id)))
        if not data:
            return None
        return self._deserialize_task(data)

    def mark_running(self, task_id: str, worker_id: str) -> Optional[dict]:
        existing = self.get_task(task_id)
        if existing is None:
            return None

        pipeline = self._redis.pipeline()
        pipeline.hset(
            self._task_key(task_id),
            mapping={
                "status": TaskStatus.RUNNING.value,
                "worker_id": worker_id,
                "attempt_count": str(existing["attempt_count"] + 1),
                "updated_at": self._now_iso(),
            },
        )
        self._status_transition(pipeline, existing["status"], TaskStatus.RUNNING.value)
        pipeline.execute()
        return self.get_task(task_id)

    def mark_completed(self, task_id: str, result: Any, worker_id: str) -> Optional[dict]:
        current = self.get_task(task_id)
        if current is None:
            return None

        pipeline = self._redis.pipeline()
        pipeline.hset(
            self._task_key(task_id),
            mapping={
                "status": TaskStatus.COMPLETED.value,
                "result": json.dumps(result),
                "last_error": "",
                "failure_type": "",
                "failure_reason": "",
                "updated_at": self._now_iso(),
                "worker_id": worker_id,
            },
        )
        self._status_transition(pipeline, current["status"], TaskStatus.COMPLETED.value)
        self._metrics_increment(pipeline, self.metrics_stats_key, "tasks_completed_total")
        pipeline.execute()
        return self.get_task(task_id)

    def record_failure(
        self,
        task_id: str,
        *,
        worker_id: str,
        error_message: str,
        failure_type: str,
        failure_reason: str,
    ) -> Optional[dict]:
        current = self.get_task(task_id)
        if current is None:
            return None

        now = self._now_iso()
        updates = {
            "last_error": error_message,
            "failure_type": failure_type,
            "failure_reason": failure_reason,
            "last_failure_at": now,
            "updated_at": now,
            "worker_id": worker_id,
        }
        if current["first_failure_at"] is None:
            updates["first_failure_at"] = now

        pipeline = self._redis.pipeline()
        pipeline.hset(self._task_key(task_id), mapping=updates)
        self._metrics_increment(pipeline, self.metrics_stats_key, "task_failures_total")
        if failure_type == "timeout":
            self._metrics_increment(pipeline, self.metrics_stats_key, "task_timeouts_total")
        pipeline.execute()
        return self.get_task(task_id)

    def requeue_task(self, task_id: str, *, retry_count: int) -> Optional[dict]:
        current = self.get_task(task_id)
        if current is None:
            return None

        pipeline = self._redis.pipeline()
        pipeline.hset(
            self._task_key(task_id),
            mapping={
                "status": TaskStatus.PENDING.value,
                "retry_count": str(retry_count),
                "updated_at": self._now_iso(),
            },
        )
        self._enqueue_runnable(pipeline, task_id, current["priority"])
        self._status_transition(pipeline, current["status"], TaskStatus.PENDING.value)
        self._metrics_increment(pipeline, self.metrics_stats_key, "task_retries_total")
        pipeline.execute()
        return self.get_task(task_id)

    def schedule_retry(
        self,
        task_id: str,
        *,
        retry_count: int,
        run_at: datetime,
    ) -> Optional[dict]:
        """Schedule a retry for the future using the shared scheduled sorted set."""
        current = self.get_task(task_id)
        if current is None:
            return None

        normalized_run_at = self._normalize_dt(run_at)
        assert normalized_run_at is not None

        pipeline = self._redis.pipeline()
        pipeline.hset(
            self._task_key(task_id),
            mapping={
                "status": TaskStatus.SCHEDULED.value,
                "retry_count": str(retry_count),
                "run_at": normalized_run_at.isoformat(),
                "updated_at": self._now_iso(),
            },
        )
        self._schedule_task_in_pipeline(pipeline, task_id, normalized_run_at)
        self._status_transition(pipeline, current["status"], TaskStatus.SCHEDULED.value)
        self._metrics_increment(pipeline, self.metrics_stats_key, "task_retries_total")
        pipeline.execute()
        return self.get_task(task_id)

    def dead_letter_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        error_message: str,
        failure_type: str,
        failure_reason: str,
    ) -> Optional[dict]:
        current = self.record_failure(
            task_id,
            worker_id=worker_id,
            error_message=error_message,
            failure_type=failure_type,
            failure_reason=failure_reason,
        )
        if current is None:
            return None

        now = self._now_iso()
        pipeline = self._redis.pipeline()
        pipeline.hset(
            self._task_key(task_id),
            mapping={
                "status": TaskStatus.DEAD_LETTERED.value,
                "result": json.dumps(error_message),
                "dead_lettered_at": now,
                "updated_at": now,
                "worker_id": worker_id,
            },
        )
        pipeline.lpush(self._dead_letter_queue_name, task_id)
        self._status_transition(pipeline, current["status"], TaskStatus.DEAD_LETTERED.value)
        self._metrics_increment(pipeline, self.metrics_stats_key, "dead_lettered_total")
        pipeline.execute()
        return self.get_task(task_id)

    def patch_task_metadata(
        self,
        task_id: str,
        result: Any = None,
        last_error: Optional[str] = None,
        worker_id: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Optional[dict]:
        key = self._task_key(task_id)
        if not self._redis.exists(key):
            return None

        updates = {"updated_at": self._now_iso()}
        if result is not None:
            updates["result"] = json.dumps(result)
        if last_error is not None:
            updates["last_error"] = last_error
        if worker_id is not None:
            updates["worker_id"] = worker_id
        if timeout_seconds is not None:
            updates["timeout_seconds"] = str(timeout_seconds)

        self._redis.hset(key, mapping=updates)
        return self.get_task(task_id)

    def promote_due_scheduled_tasks(self, limit: int = 100) -> int:
        """
        Move due scheduled tasks into their runnable priority queues.

        Each promoted task is removed from the sorted set and pushed to the
        appropriate runnable list. A small per-task transaction avoids duplicate
        promotion when multiple workers race.
        """
        script = """
local scheduled_key = KEYS[1]
local queue_prefix = ARGV[1]
local task_prefix = ARGV[2]
local now_score = ARGV[3]
local now_iso = ARGV[4]
local limit = tonumber(ARGV[5])

local due_ids = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_score, 'LIMIT', 0, limit)
local promoted = 0
local metrics_status_key = queue_prefix .. ':metrics:status'
local metrics_stats_key = queue_prefix .. ':metrics:stats'

for _, task_id in ipairs(due_ids) do
  if redis.call('ZREM', scheduled_key, task_id) == 1 then
    local task_key = task_prefix .. task_id
    local priority = redis.call('HGET', task_key, 'priority')
    if not priority or priority == '' then
      priority = 'medium'
    end
    redis.call('HSET', task_key, 'status', 'pending', 'updated_at', now_iso)
    redis.call('LPUSH', queue_prefix .. ':' .. priority, task_id)
    redis.call('HINCRBY', metrics_status_key, 'scheduled', -1)
    redis.call('HINCRBY', metrics_status_key, 'pending', 1)
    redis.call('HINCRBY', metrics_stats_key, 'scheduled_promotions_total', 1)
    promoted = promoted + 1
  end
end

return promoted
"""
        promoted = cast(
            int,
            self._redis.eval(
                script,
                1,
                self._scheduled_queue_name,
                self._queue_name,
                "task:",
                str(self._now().timestamp()),
                self._now_iso(),
                str(limit),
            ),
        )
        return int(promoted)

    def touch_worker_heartbeat(self, worker_id: str) -> None:
        """Record the worker's latest heartbeat timestamp."""
        self._redis.hset(self.workers_key, worker_id, self._now_iso())

    def get_worker_activity(self) -> dict[str, Any]:
        """Return heartbeat data for workers seen recently enough to be useful."""
        heartbeats = cast(dict[str, str], self._redis.hgetall(self.workers_key))
        now = self._now()
        workers: dict[str, dict[str, Any]] = {}
        active_count = 0
        stale_cutoff = settings.worker_heartbeat_ttl

        for worker_id, raw_timestamp in heartbeats.items():
            try:
                seen_at = datetime.fromisoformat(raw_timestamp)
            except ValueError:
                continue
            age_seconds = max(0, int((now - seen_at).total_seconds()))
            is_active = age_seconds <= stale_cutoff
            if is_active:
                active_count += 1
            workers[worker_id] = {
                "last_seen_at": seen_at.isoformat(),
                "age_seconds": age_seconds,
                "is_active": is_active,
            }

        return {"active_count": active_count, "total_seen": len(workers), "workers": workers}

    def get_metrics(self) -> dict[str, Any]:
        """Return a lightweight operational snapshot for the queue system."""
        self._ensure_metrics_initialized()
        status_counts_raw = cast(dict[str, str], self._redis.hgetall(self.metrics_status_key))
        stats_raw = cast(dict[str, str], self._redis.hgetall(self.metrics_stats_key))

        status_counts = {
            status: int(status_counts_raw.get(status, "0"))
            for status in (
                TaskStatus.PENDING.value,
                TaskStatus.SCHEDULED.value,
                TaskStatus.RUNNING.value,
                TaskStatus.COMPLETED.value,
                TaskStatus.FAILED.value,
                TaskStatus.DEAD_LETTERED.value,
            )
        }
        stats = {
            key: int(stats_raw.get(key, "0"))
            for key in (
                "tasks_created_total",
                "tasks_completed_total",
                "task_failures_total",
                "task_retries_total",
                "task_timeouts_total",
                "dead_lettered_total",
                "scheduled_promotions_total",
            )
        }

        return {
            "status_counts": status_counts,
            "queue_depths": self.runnable_queue_lengths(),
            "scheduled_count": self.scheduled_count(),
            "dead_letter_queue_depth": self.dead_letter_size(),
            "retry_stats": {
                "task_retries_total": stats["task_retries_total"],
                "task_failures_total": stats["task_failures_total"],
                "task_timeouts_total": stats["task_timeouts_total"],
                "scheduled_promotions_total": stats["scheduled_promotions_total"],
            },
            "totals": {
                "tasks_created_total": stats["tasks_created_total"],
                "tasks_completed_total": stats["tasks_completed_total"],
                "dead_lettered_total": stats["dead_lettered_total"],
            },
            "workers": self.get_worker_activity(),
        }

    def dead_letter_size(self) -> int:
        return int(cast(int, self._redis.llen(self._dead_letter_queue_name)))

    def runnable_queue_lengths(self) -> dict[str, int]:
        return {
            priority: int(cast(int, self._redis.llen(self.runnable_queue_name(priority))))
            for priority in (TaskPriority.HIGH.value, TaskPriority.MEDIUM.value, TaskPriority.LOW.value)
        }

    def scheduled_count(self) -> int:
        return int(cast(int, self._redis.zcard(self._scheduled_queue_name)))

    def flush(self) -> None:
        self._redis.flushdb()

    def _deserialize_task(self, data: dict[str, str]) -> dict:
        task_type = data.get("task_type") or data.get("task_name")
        result = data.get("result", "")

        parsed_result: Any
        if result == "":
            parsed_result = None
        else:
            try:
                parsed_result = json.loads(result)
            except (TypeError, json.JSONDecodeError):
                parsed_result = result

        def parse_dt(value: str) -> Optional[datetime]:
            return datetime.fromisoformat(value) if value else None

        return {
            "task_id": data["task_id"],
            "task_type": task_type,
            "status": data.get("status", TaskStatus.PENDING.value),
            "priority": self._priority_value(data.get("priority")),
            "run_at": parse_dt(data.get("run_at", "")),
            "result": parsed_result,
            "attempt_count": int(data.get("attempt_count", "0")),
            "retry_count": int(data.get("retry_count", "0")),
            "max_retries": int(data.get("max_retries", str(settings.max_retries))),
            "timeout_seconds": int(data.get("timeout_seconds", str(settings.default_task_timeout))),
            "last_error": data.get("last_error", "") or None,
            "failure_type": data.get("failure_type", "") or None,
            "failure_reason": data.get("failure_reason", "") or None,
            "first_failure_at": parse_dt(data.get("first_failure_at", "")),
            "last_failure_at": parse_dt(data.get("last_failure_at", "")),
            "dead_lettered_at": parse_dt(data.get("dead_lettered_at", "")),
            "worker_id": data.get("worker_id", "") or None,
            "created_at": datetime.fromisoformat(data["created_at"]),
            "updated_at": datetime.fromisoformat(data["updated_at"]),
        }


task_store = TaskStore(
    redis_url=settings.redis_url,
    queue_name=settings.queue_name,
    dead_letter_queue_name=settings.dead_letter_queue_name,
    scheduled_queue_name=settings.scheduled_queue_name,
)
