import os
import asyncio
import random
import time
import logging
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

load_dotenv()

logging.basicConfig(
    filename=f'{os.getenv("input_csv_dataset_file_name")}.log',
    encoding='utf-8',
    level=logging.INFO,
    filemode='w')


async def workload(args):
    arg1, arg2 = args
    await asyncio.sleep(arg2)
    #print(f'{arg1}: {arg2:.2f} seconds')
    logging.info(f'{arg1}: {arg2:.2f} seconds')


async def worker(queue, pbar):
    while True:
        args = await queue.get()
        await workload(args)
        queue.task_done()
        pbar.update(1)


async def main():
    queue = asyncio.Queue()

    total_sleep_time = 0
    for _ in range(200):
        sleep_for = random.uniform(0.05, 1.0)
        total_sleep_time += sleep_for

        arg1 = 'https://qwe.ok'
        arg2 = sleep_for
        
        queue.put_nowait([arg1, arg2])

    pbar = tqdm(total=queue.qsize())

    tasks = []
    for i in range(int(os.getenv("async_workers_count"))):
        task = asyncio.create_task(worker(queue, pbar))
        tasks.append(task)

    started_at = time.monotonic()
    await queue.join()
    total_slept_for = time.monotonic() - started_at

    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    print('====')
    print(f'{os.getenv("async_workers_count")} workers slept in parallel for {total_slept_for:.2f} seconds')
    print(f'total expected sleep time: {total_sleep_time:.2f} seconds')


asyncio.run(main())