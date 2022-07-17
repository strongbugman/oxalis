import asyncio

import pytest

from oxalis.pool import Pool


@pytest.mark.asyncio
async def test_schedule_task():
    pool = Pool()

    count = 0
    max_count = 10

    async def cor():
        nonlocal count
        count += 1

    for i in range(max_count):
        pool.spawn(cor())
    await pool.wait_done()
    assert count == max_count
    assert pool.done
    # test with limit
    count = 0
    running_count = 0
    max_running_count = -1
    concurrent_limit = 3

    async def cor():
        nonlocal count, running_count, max_running_count
        running_count += 1
        max_running_count = max(running_count, max_running_count)
        await asyncio.sleep(0.1)
        count += 1
        running_count -= 1

    pool = Pool(limit=concurrent_limit)
    for i in range(max_count):
        pool.spawn(cor())
    assert not pool.done
    assert pool.running
    await pool.wait_done()
    assert count == max_count
    assert max_running_count <= concurrent_limit
    # test with exception
    count = 0
    max_count = 3

    async def coro():
        nonlocal count
        count += 1
        raise Exception("Test exception")

    for i in range(max_count):
        pool.spawn(coro())
    await pool.wait_close()
    assert not pool.running
    assert count == max_count
    # test with timeout
    count = 0
    max_count = 3
    pool = Pool()

    async def coro():
        nonlocal count
        await asyncio.sleep(10)
        count += 1

    for i in range(max_count):
        pool.spawn(coro(), timeout=0.1)
    await pool.wait_close()
    assert count == 0
    # test with closed pool
    with pytest.raises(RuntimeError):
        pool.spawn(coro)
    await pool.wait_close()
