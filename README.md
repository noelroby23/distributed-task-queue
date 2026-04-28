# Distributed Task Queue

A Redis-backed distributed task queue built with `FastAPI`, `Python` workers, and `Docker Compose`.

It is intentionally smaller than Celery, but it still demonstrates the core ideas behind a production-style background job system:

- asynchronous task submission over HTTP
- multiple worker processes consuming from shared queues
- Redis-backed task persistence and broker queues
- retries with exponential backoff
- per-task timeouts and dead-letter handling
- priority queues and scheduled jobs
- lightweight monitoring, metrics, and worker heartbeats

Resume-ready description:

> Built a Dockerized distributed task queue in Python using FastAPI and Redis with multi-worker execution, retries with backoff, timeouts, dead-letter queues, priority scheduling, and operational metrics.

## What This Project Demonstrates

- distributed systems fundamentals with a shared broker and worker pool
- API design for asynchronous job submission and polling
- fault-tolerance patterns such as retries, timeouts, and dead-letter queues
- queue design using Redis hashes, lists, and sorted sets
- observability through metrics, queue depth tracking, and worker heartbeats

## Architecture

```text
Client
  |
  | POST /tasks, GET /tasks/{id}, GET /metrics
  v
FastAPI API
  |
  | HSET task:{id}
  | LPUSH task_queue:{priority} id
  | ZADD task_queue:scheduled score=id
  v
Redis
  |
  | BRPOP high -> medium -> low
  | promote due scheduled tasks
  v
Worker Pool
  |
  | execute task handler
  | update task hash
  v
Task Status / Results / Metrics
```

Core design choices:

- Redis hash per task record keeps one source of truth for status and metadata
- Redis lists hold runnable tasks by priority
- Redis sorted set holds future work and retry backoff scheduling
- workers promote due scheduled tasks, then consume runnable work in priority order
- FastAPI acts as the control plane for submission, polling, and observability

## Feature Highlights

- `POST /tasks` to submit work
- `GET /tasks/{task_id}` to poll status
- `GET /metrics` for JSON observability data
- `GET /dashboard` for a minimal HTML dashboard
- task registry with example handlers:
  - `dummy`
  - `add`
  - `flaky`
  - `word_count`
  - `fibonacci`
  - `image_resize`
- task lifecycle support:
  - `pending`
  - `scheduled`
  - `running`
  - `completed`
  - `dead_lettered`

Note: `failed` remains in the API model for completeness and backward compatibility, but the current worker path dead-letters terminal failures instead of leaving tasks in `failed`.

## API Endpoints

| Endpoint | Purpose |
| --- | --- |
| `POST /tasks` | Submit a task |
| `GET /tasks/{task_id}` | Fetch task state and result metadata |
| `GET /metrics` | Return queue, retry, and worker metrics as JSON |
| `GET /dashboard` | Minimal HTML dashboard backed by `/metrics` |
| `/docs` | Swagger UI |
| `/redoc` | ReDoc |

## Quick Start

### Option 1: Docker Compose

Recommended for demos and review.

```bash
docker compose up --build
```

This starts:

- Redis
- FastAPI API on `http://localhost:8000`
- 3 workers

Then open:

- API docs: `http://localhost:8000/docs`
- Metrics: `http://localhost:8000/metrics`
- Dashboard: `http://localhost:8000/dashboard`

### Option 2: Run Locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker run -d --name dtq-redis -p 6379:6379 redis:7-alpine
uvicorn app.main:app --reload
```

In separate terminals:

```bash
python -m app.worker --id worker-1
python -m app.worker --id worker-2
python -m app.worker --id worker-3
```

## Example Requests

Submit an immediate task:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "fibonacci", "args": [10]}'
```

Submit a high-priority task:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "fibonacci", "args": [12], "priority": "high"}'
```

Submit a scheduled task:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "fibonacci", "args": [8], "run_at": "2030-01-01T00:00:00+00:00"}'
```

Submit a timeout demo task:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "dummy", "kwargs": {"sleep_seconds": 2}, "timeout_seconds": 1}'
```

Check task status:

```bash
curl http://localhost:8000/tasks/<task_id>
```

Inspect metrics:

```bash
curl http://localhost:8000/metrics
```

## Task Lifecycle

Typical flow:

```text
submit -> pending -> running -> completed
submit -> scheduled -> pending -> running -> completed
submit -> running -> scheduled(retry) -> pending -> running -> completed
submit -> running -> scheduled(retry) -> ... -> dead_lettered
```

Behavior summary:

- `priority` defaults to `medium`
- `run_at` is optional and must be timezone-aware
- retries preserve priority
- retry backoff uses the same scheduled-job path as future jobs
- terminal failures go to the dead-letter queue and remain queryable via the main task hash

## Monitoring And Metrics

`GET /metrics` exposes a lightweight operational snapshot including:

- status counts:
  - `pending`
  - `scheduled`
  - `running`
  - `completed`
  - `failed`
  - `dead_lettered`
- queue depth by priority
- scheduled queue depth
- dead-letter queue depth
- retry/failure totals
- worker heartbeat activity

Example response:

```json
{
  "status_counts": {
    "pending": 1,
    "scheduled": 0,
    "running": 0,
    "completed": 4,
    "failed": 0,
    "dead_lettered": 1
  },
  "queue_depths": {
    "high": 0,
    "medium": 1,
    "low": 0
  },
  "scheduled_count": 0,
  "dead_letter_queue_depth": 1,
  "retry_stats": {
    "task_retries_total": 2,
    "task_failures_total": 3,
    "task_timeouts_total": 1,
    "scheduled_promotions_total": 2
  }
}
```

Metrics strategy:

- status counters are updated during lifecycle transitions in Redis
- queue depths are read directly from Redis list lengths
- scheduled depth is read from the sorted set size
- worker activity comes from last-seen heartbeat timestamps

This keeps metrics cheap to read without scanning every task hash on each request.

## Configuration

Important environment variables:

| Variable | Purpose | Default |
| --- | --- | --- |
| `REDIS_HOST` | Redis host | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `REDIS_DB` | Redis logical DB | `0` |
| `QUEUE_NAME` | Base runnable queue prefix | `task_queue` |
| `DEAD_LETTER_QUEUE_NAME` | DLQ list name | `task_queue_dead_letter` |
| `SCHEDULED_QUEUE_NAME` | Scheduled-job sorted set | `task_queue:scheduled` |
| `MAX_RETRIES` | Retries after first failure | `3` |
| `DEFAULT_TASK_TIMEOUT` | Per-task timeout fallback | `30` |
| `WORKER_POLL_TIMEOUT` | Worker BRPOP timeout | `1` |
| `WORKER_HEARTBEAT_TTL` | Worker active window in seconds | `30` |

## Project Structure

```text
app/
  main.py          FastAPI API and monitoring endpoints
  models.py        request/response models and enums
  store.py         Redis persistence, lifecycle transitions, metrics
  worker.py        worker loop, timeout execution, retry logic
  task_registry.py built-in task handlers and registry
tests/
  test_api.py
  test_worker.py
  test_monitoring.py
docker-compose.yml local multi-service runtime
Dockerfile         shared API/worker image
```

## Testing

The project includes coverage for:

- API submission and validation
- task lifecycle transitions
- retries and exponential backoff
- timeouts and dead-lettering
- priority ordering and FIFO behavior
- scheduled promotion behavior
- metrics and dashboard endpoints

Run tests:

```bash
python3 -m pytest
```

## Local Verification Checklist

1. Start the stack with `docker compose up --build`
2. Open `http://localhost:8000/docs`
3. Submit:
   - one normal task
   - one high-priority task
   - one low-priority task
   - one scheduled task
   - one timeout/retry task
4. Confirm:
   - high-priority work is processed before low-priority work
   - scheduled work stays `scheduled` until due
   - timeout/retry tasks eventually complete or dead-letter
   - `/metrics` reflects queue and lifecycle changes
   - `/dashboard` shows live counts
5. Watch logs:

```bash
docker compose logs -f worker1 worker2 worker3
```

## Design Decisions

- Why Redis hashes?
  - one canonical task record per job
  - simple polling and metadata inspection
- Why Redis lists?
  - minimal runnable queue implementation
  - easy to explain and debug
- Why a sorted set for scheduling?
  - natural fit for future eligibility timestamps
- Why subprocess timeouts?
  - practical hard timeout enforcement for synchronous Python work
- Why JSON metrics instead of external tooling?
  - keeps observability lightweight and self-contained

## Tradeoffs And Limitations

Be honest about these in interviews:

- this is a simplified queue, not a full broker with acknowledgements or consumer groups
- once a worker pops a task from a runnable list, a hard crash during execution can still lose in-flight work
- strict priority can starve low-priority work under constant high-priority load
- scheduled execution is near-real-time, not millisecond-precise, because workers poll periodically
- `image_resize` uses local file paths only and expects shared disk access under Docker Compose
- metrics are operational counters, not a full time-series monitoring system

## Interview-Friendly Summary

This project is a compact distributed systems exercise that demonstrates how to build a queueing system from first principles with Redis and FastAPI. It shows task submission, worker coordination, retries, scheduling, fault handling, and observability without depending on a large framework.

## Resume Bullet Ideas

- Built a distributed task queue in Python with FastAPI and Redis, supporting asynchronous task submission, multi-worker execution, and status polling.
- Implemented retries with exponential backoff, per-task timeouts, and dead-letter queue handling to improve fault tolerance.
- Added high/medium/low priority queues and scheduled-job promotion using Redis lists and sorted sets.
- Designed lightweight observability with Redis-backed metrics, worker heartbeats, and a FastAPI monitoring dashboard.
