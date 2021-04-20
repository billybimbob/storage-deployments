#!/usr/bin/env python3

from typing import List, Literal, Optional, Tuple, cast
from argparse import ArgumentParser
from pathlib import Path
# from pymongo import MongoClient

from asyncio.subprocess import PIPE
import asyncio as aio

from database import Database, STORAGE_FOLDER, run_ssh, write_results


Operation = Literal['write', 'read', 'meta']
LOAD_SIZES = [1_000, 10_000, 100_000]

GEN_PATH = Path('load_generation')


async def redis_bench(port: int, op: Operation, requests: int):
    out = GEN_PATH / 'redis-bench' / f'{op}_{requests}_times.txt'
    out.mkdir(exist_ok=True, parents=True)

    bench = ['redis-benchmark', '-p', str(port)]
    bench.extend(['-c', str(1), '-n', str(requests)])
    bench.extend(['--csv', '-q' '-d', str(20)])

    if op == 'write':
        bench.extend(['-t', 'set'])
    elif op == 'read':
        bench.extend(['-t', 'get'])
    elif op == 'meta':
        bench.extend(['-t', 'hset'])

    bench.extend(['>', str(out)])
    bench = ' '.join(bench)

    run = await aio.create_subprocess_shell(bench, stdout=PIPE)
    await run.wait()


async def redis_run(port: int, ssh: Optional[Tuple[str, str]]):
    if ssh:
        bench = STORAGE_FOLDER / 'benchmark.py'
        res = await run_ssh(f'./{bench}', *ssh)
        write_results(res)
        return

    for op in cast(List[Operation], ['write', 'read', 'meta']):
        for size in LOAD_SIZES:
            await redis_bench(port, op, size)



async def mongo_bench():
    mongo_load = GEN_PATH / 'load-output' / 'mongodb'

    if not mongo_load.exists():
        mongo_gen = GEN_PATH / 'mongodb_load_gen.py'
        mongo_gen = f'./{mongo_gen}'

        gen = await aio.create_subprocess_exec(mongo_gen, stdout=PIPE)
        await gen.wait()



async def main(
    database: Database,
    port: int,
    user: Optional[str],
    addr: Optional[str]):

    ssh = None
    if user and addr:
        ssh = user, addr

    elif user or addr:
        raise ValueError('only one of user or addr specified')

    if database == 'redis':
        await redis_run(port, ssh)

    # TODO: add mongo run


if __name__ == '__main__':
    args = ArgumentParser(
        description = 'runs benchmarks for a remote database')

    args.add_argument('-a', '--addr',
        help = 'ssh address where database is')

    args.add_argument('-d', '--database',
        default = 'redis',
        choices = ['mongodb','redis'],
        help = 'datbase system that is being benchmarked')

    args.add_argument('-p', '--port',
        required = True,
        type = int,
        help='port to connect to database')

    args.add_argument('-u', '--user',
        help = 'user to ssh into; addr required as well')

    args = args.parse_args()
    aio.run( main(**vars(args)) )