"""Debezium topic consumer used by the Airflow task (confluent-kafka, optional dependency)."""

from __future__ import annotations

import json
import os


def read_batch(max_events: int = 50_000, timeout_s: float = 10.0) -> list[dict]:
    from confluent_kafka import Consumer

    c = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "group.id": "warranty-lakehouse",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    c.subscribe(["dms.public.claims", "dms.public.vehicles", "dms.public.dealers"])
    out, msgs = [], c.consume(num_messages=max_events, timeout=timeout_s)
    for m in msgs:
        if m.error() or m.value() is None:  # tombstones follow deletes; the delete event itself is kept
            continue
        payload = json.loads(m.value())
        out.append(payload.get("payload", payload))
    if msgs:
        c.commit(asynchronous=False)  # commit only after the batch is read; bronze writes are idempotent
    c.close()
    return out
