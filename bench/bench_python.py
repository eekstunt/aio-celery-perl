#!/usr/bin/env python3
"""
Benchmark: 10,000 concurrent async HTTP tasks using Python asyncio + aiohttp.
Mirror of bench_async.pl for direct comparison.
"""
import asyncio
import aiohttp
import time
import sys
import os

NUM_TASKS = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000
MAX_IN_FLIGHT = int(sys.argv[2]) if len(sys.argv) > 2 else 500
NGINX_URL = os.environ.get("NGINX_URL", "http://127.0.0.1:8888/")

completed = 0
failed = 0
in_flight = 0
max_seen = 0
second_counts: dict[int, int] = {}


async def run_task(task_id: int, semaphore: asyncio.Semaphore, session: aiohttp.ClientSession):
    global completed, failed, in_flight, max_seen

    async with semaphore:
        in_flight += 1
        if in_flight > max_seen:
            max_seen = in_flight

        second_key = int(time.time())
        try:
            async with session.get(f"{NGINX_URL}?task={task_id}") as resp:
                await resp.read()
                if resp.status == 200:
                    completed += 1
                    second_counts[second_key] = second_counts.get(second_key, 0) + 1
                else:
                    failed += 1
        except Exception:
            failed += 1
        finally:
            in_flight -= 1


async def progress_reporter(start_time: float):
    while True:
        await asyncio.sleep(0.5)
        elapsed = time.time() - start_time
        rps = completed / elapsed if elapsed > 0 else 0
        print(
            f"\r  [{elapsed:.1f}s] {completed}/{NUM_TASKS} done, "
            f"{in_flight} in-flight, {failed} failed — {rps:.0f} req/s   ",
            end="",
            flush=True,
        )


async def main():
    global completed, failed, in_flight, max_seen, second_counts
    completed = failed = in_flight = max_seen = 0
    second_counts = {}

    print("=" * 60)
    print("Python asyncio + aiohttp benchmark")
    print("=" * 60)
    print(f"Tasks:           {NUM_TASKS}")
    print(f"Max in-flight:   {MAX_IN_FLIGHT} (concurrent TCP connections)")
    print(f"Target:          {NGINX_URL}")
    print(f"PID:             {os.getpid()} (single process, single thread)")
    print(f"Python:          {sys.version.split()[0]}")
    print("=" * 60)
    print()

    semaphore = asyncio.Semaphore(MAX_IN_FLIGHT)

    connector = aiohttp.TCPConnector(limit=MAX_IN_FLIGHT, limit_per_host=MAX_IN_FLIGHT)
    async with aiohttp.ClientSession(connector=connector) as session:
        start_time = time.time()

        print(f"Launching {NUM_TASKS} tasks...")
        launch_start = time.time()

        progress_task = asyncio.create_task(progress_reporter(start_time))

        tasks = [asyncio.create_task(run_task(i, semaphore, session)) for i in range(1, NUM_TASKS + 1)]

        launch_time = time.time() - launch_start
        print(f"All {NUM_TASKS} futures created in {launch_time:.3f}s")
        print("Waiting for completion...")

        await asyncio.gather(*tasks)

        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass

        total_time = time.time() - start_time

    print(f"\n\n{'=' * 60}")
    print("RESULTS")
    print("=" * 60)
    print(f"Total tasks:     {NUM_TASKS}")
    print(f"Completed:       {completed}")
    print(f"Failed:          {failed}")
    print(f"Total time:      {total_time:.3f}s")
    print(f"Throughput:      {completed / total_time:.0f} req/s")
    print(f"Avg latency:     {(total_time / NUM_TASKS) * 1000:.3f}ms")
    print(f"Max in-flight:   {max_seen} (observed peak)")
    print(f"Process:         single thread, PID {os.getpid()}")
    print("=" * 60)

    if second_counts:
        print("\nRequests completed per second (client-side):")
        sorted_secs = sorted(second_counts.keys())
        base = sorted_secs[0]
        for sec in sorted_secs:
            print(f"  t+{sec - base}s: {second_counts[sec]} requests")

    print()
    if completed == NUM_TASKS:
        print(f"VERDICT: ALL {NUM_TASKS} tasks completed on a single thread.")
        print(f"Peak concurrency: {max_seen} simultaneous requests.")
    elif completed > 0:
        print(f"VERDICT: {completed}/{NUM_TASKS} completed. {failed} failed.")
    else:
        print(f"VERDICT: All tasks failed. Is Nginx running on {NGINX_URL}?")
    print()

    # Output machine-readable time for the runner script
    print(f"BENCH_TOTAL_TIME={total_time:.6f}")


if __name__ == "__main__":
    asyncio.run(main())
