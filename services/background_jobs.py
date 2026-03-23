from __future__ import annotations

import threading
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable

from database import utc_now_iso


@dataclass
class JobRecord:
    id: str
    kind: str
    status: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    result: Any | None = None
    error: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


class BackgroundJobManager:
    def __init__(self, max_workers: int = 4):
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='bybit-lab-job')
        self._jobs: dict[str, JobRecord] = {}
        self._lock = threading.Lock()

    def submit(self, kind: str, func: Callable[[], Any], meta: dict[str, Any] | None = None) -> JobRecord:
        job_id = uuid.uuid4().hex[:12]
        job = JobRecord(id=job_id, kind=kind, status='queued', created_at=utc_now_iso(), meta=meta or {})
        with self._lock:
            self._jobs[job_id] = job
        self._executor.submit(self._run, job_id, func)
        return job

    def _run(self, job_id: str, func: Callable[[], Any]) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = 'running'
            job.started_at = utc_now_iso()
        try:
            result = func()
        except Exception as exc:
            with self._lock:
                job = self._jobs[job_id]
                job.status = 'error'
                job.error = f"{exc}\n{traceback.format_exc(limit=5)}"
                job.finished_at = utc_now_iso()
            return
        with self._lock:
            job = self._jobs[job_id]
            job.status = 'success'
            job.result = result
            job.finished_at = utc_now_iso()

    def get(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise ValueError(f'Job {job_id} not found.')
            return {
                'id': job.id,
                'kind': job.kind,
                'status': job.status,
                'created_at': job.created_at,
                'started_at': job.started_at,
                'finished_at': job.finished_at,
                'result': job.result,
                'error': job.error,
                'meta': job.meta,
            }

    def list(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            items = list(self._jobs.values())
        items.sort(key=lambda x: x.created_at, reverse=True)
        return [self.get(job.id) for job in items[:limit]]


job_manager = BackgroundJobManager()
