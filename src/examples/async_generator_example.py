#!/usr/bin/env python
"""
Example demonstrating how to use the new AsyncGenerator pattern in IndexWorker.
This shows how to effectively chain multiple AsyncGenerators together.
"""

import asyncio
import datetime
from typing import AsyncGenerator, TypeVar

from mrt_downloader.http import IndexWorker, build_session
from mrt_downloader.models import CollectorFileEntry, CollectorIndexEntry

# Generic type for chaining any kind of AsyncGenerator
T = TypeVar("T")


async def achain(*generators) -> AsyncGenerator[T, None]:
    """Chain multiple async generators together"""
    for generator in generators:
        async for item in generator:
            yield item


async def process_indices(
    workers: list[IndexWorker],
) -> AsyncGenerator[CollectorFileEntry, None]:
    """Process multiple IndexWorkers in parallel and combine their results"""
    # Create a list of generators
    generators = [worker.run_generator() for worker in workers]

    # Chain them together
    async for entry in achain(*generators):
        yield entry


async def main():
    # Example usage with multiple collectors
    async with build_session() as sess:
        # Create two separate index queues for different collectors
        queue1 = asyncio.Queue()
        queue2 = asyncio.Queue()

        # Example collector entries (you would need to replace these with actual values)
        queue1.put_nowait(
            CollectorIndexEntry(
                None,  # Replace with actual collector info
                "https://example.org/collector1/",
                datetime.datetime.now(datetime.UTC),
                file_types=frozenset({"rib", "update"}),
            )
        )

        queue2.put_nowait(
            CollectorIndexEntry(
                None,  # Replace with actual collector info
                "https://example.org/collector2/",
                datetime.datetime.now(datetime.UTC),
                file_types=frozenset({"rib", "update"}),
            )
        )

        # Create workers
        worker1 = IndexWorker(sess, queue1)
        worker2 = IndexWorker(sess, queue2)

        # Method 1: Process each worker's entries as they come in
        print("Method 1: Processing entries as they arrive")
        count = 0
        async for entry in achain(worker1.run_generator(), worker2.run_generator()):
            print(f"Received entry: {entry.url}")
            count += 1
            # Process entry here...
        print(f"Total processed: {count}")

        # Method 2: Use helper function for cleaner code
        # Note: In a real example, you'd need to refill the queues here
        print("\nMethod 2: Using helper function")
        async for entry in process_indices([worker1, worker2]):
            print(f"Received entry: {entry.url}")
            # Process entry here...


if __name__ == "__main__":
    asyncio.run(main())
