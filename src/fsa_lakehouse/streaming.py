"""Small helper for incremental (availableNow) streams that may hit schema evolution.

Auto Loader in `addNewColumns` mode deliberately FAILS the stream the first time it
sees a new column, after recording the new schema. Restarting picks the new schema up.
Delta streaming sources behave similarly when an upstream table adds a column.
In production you let the job's task retry handle this; here we restart once in code
so you can see exactly what happens.
"""

from __future__ import annotations

from typing import Callable

SCHEMA_CHANGE_MARKERS = (
    "UNKNOWN_FIELD",
    "UnknownFieldException",
    "DELTA_SCHEMA_CHANGED",
    "schema change",
    "Detected schema change",
)


def run_with_schema_restart(start_query: Callable[[], object], max_restarts: int = 2) -> None:
    """start_query() must start and return a StreamingQuery."""
    for attempt in range(max_restarts + 1):
        try:
            q = start_query()
            q.awaitTermination()
            print(f"stream finished cleanly (attempt {attempt + 1})")
            return
        except Exception as e:  # noqa: BLE001 - we inspect the message on purpose
            msg = str(e)
            if attempt < max_restarts and any(m.lower() in msg.lower() for m in SCHEMA_CHANGE_MARKERS):
                print(f"schema evolution detected, restarting stream. First line of error:\n  {msg.splitlines()[0][:300]}")
                continue
            raise
