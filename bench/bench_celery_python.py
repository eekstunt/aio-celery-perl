#!/usr/bin/env python3
"""
Benchmark Python aio-celery: message serialization, deserialization,
and publish to RabbitMQ.
"""
import asyncio
import json
import time
import sys
import os

# aio-celery imports
from aio_celery.amqp import create_task_message
from aio_celery import Celery

NUM_TASKS = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000
NUM_RUNS = int(sys.argv[2]) if len(sys.argv) > 2 else 5
BROKER_URL = os.environ.get("BROKER_URL", "amqp://guest:guest@127.0.0.1:5673//")

print("=" * 60)
print("Python aio-celery benchmark")
print("=" * 60)
print(f"Tasks per run:   {NUM_TASKS}")
print(f"Runs:            {NUM_RUNS}")
print(f"Broker:          {BROKER_URL}")
print(f"Python:          {sys.version.split()[0]}")
print(f"aio-celery:      0.22.0")
print("=" * 60)


# --- Benchmark 1: Message Serialization ---
def bench_serialize(n):
    start = time.perf_counter()
    for i in range(n):
        msg = create_task_message(
            task_id=f"task-{i:06d}",
            task_name="tasks.add",
            args=(i, i + 1),
            kwargs={"extra": "value"},
            priority=None,
            parent_id=None,
            root_id=f"task-{i:06d}",
        )
    elapsed = time.perf_counter() - start
    return elapsed


# --- Benchmark 2: Message Deserialization ---
def bench_deserialize(n):
    # Create a sample message to deserialize
    sample = create_task_message(
        task_id="sample-task",
        task_name="tasks.add",
        args=(1, 2),
        kwargs={"key": "val"},
        priority=None,
        parent_id=None,
        root_id="sample-task",
    )
    # Simulate what a worker does: decode body JSON
    raw_body = sample.body  # bytes
    raw_headers = dict(sample.headers)

    start = time.perf_counter()
    for i in range(n):
        body = json.loads(raw_body)
        args, kwargs, options = body
        task_id = raw_headers["id"]
        task_name = raw_headers["task"]
        retries = raw_headers.get("retries", 0)
    elapsed = time.perf_counter() - start
    return elapsed


# --- Benchmark 3: Publish to RabbitMQ ---
async def bench_publish(n):
    app = Celery()
    app.conf.update(broker_url=BROKER_URL)

    async with app.setup():
        # Use asyncio.gather() — the recommended pattern from aio-celery README
        start = time.perf_counter()
        tasks = [app.send_task("tasks.add", args=(i, i + 1)) for i in range(n)]
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start
    return elapsed


def run_benchmarks():
    results = {
        "serialize": [],
        "deserialize": [],
        "publish": [],
    }

    for run in range(1, NUM_RUNS + 1):
        print(f"\n--- Run {run}/{NUM_RUNS} ---")

        # Serialize
        t = bench_serialize(NUM_TASKS)
        results["serialize"].append(t)
        print(f"  Serialize {NUM_TASKS} messages:   {t:.3f}s  ({NUM_TASKS/t:.0f} msg/s)")

        # Deserialize
        t = bench_deserialize(NUM_TASKS)
        results["deserialize"].append(t)
        print(f"  Deserialize {NUM_TASKS} messages: {t:.3f}s  ({NUM_TASKS/t:.0f} msg/s)")

        # Publish
        t = asyncio.run(bench_publish(NUM_TASKS))
        results["publish"].append(t)
        print(f"  Publish {NUM_TASKS} to RabbitMQ:  {t:.3f}s  ({NUM_TASKS/t:.0f} msg/s)")

    # Summary
    print(f"\n{'=' * 60}")
    print("PYTHON RESULTS (averages)")
    print("=" * 60)
    for key in ["serialize", "deserialize", "publish"]:
        times = results[key]
        avg = sum(times) / len(times)
        best = min(times)
        worst = max(times)
        rate = NUM_TASKS / avg
        print(f"  {key:15s}: avg={avg:.3f}s  best={best:.3f}s  worst={worst:.3f}s  ({rate:.0f} msg/s)")

    # Machine-readable output
    for key in ["serialize", "deserialize", "publish"]:
        avg = sum(results[key]) / len(results[key])
        print(f"PYTHON_{key.upper()}_AVG={avg:.6f}")

    print()


if __name__ == "__main__":
    run_benchmarks()
