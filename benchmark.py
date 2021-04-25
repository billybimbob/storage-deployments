#!/usr/bin/env python3

from typing import Any, List, NamedTuple, Optional, cast
from argparse import ArgumentParser
from pathlib import Path
from time import asctime

from asyncio.subprocess import PIPE
import asyncio as aio
import json
import os

from pymongo import MongoClient
from database import Database, STORAGE_FOLDER, is_selfhost, run_ssh, write_results

from load_generation.mongodb_load_gen import (
    Command, Operation, KEY, LOAD_SIZES, generate, operation_json)



GEN_PATH = Path(os.path.realpath(__file__))
TIMESTAMP = GEN_PATH / 'mongo-timestamps.log' 

class Remote(NamedTuple):
    user: str
    address: str



async def redis_bench(port: int, op: Operation, requests: int):
    out = GEN_PATH / 'redis-bench' / f'{op}_{requests}_times.csv'
    out.parent.mkdir(exist_ok=True, parents=True)

    bench = ['redis-benchmark']
    bench += ['-p', str(port)]
    bench += ['-c', str(1)]
    bench += ['-n', str(requests)]
    bench += ['-d', str(20)]
    bench += ['--csv']
    bench += ['--cluster']

    if op == 'write':  bench += ['-t', 'set']
    elif op == 'read': bench += ['-t', 'get']
    elif op == 'meta': bench += ['-t', 'hset']

    bench += ['>', str(out)]
    bench = ' '.join(bench)

    run = await aio.create_subprocess_shell(bench, stdout=PIPE)
    await run.wait()



def mongo_bench(port: int, op: Operation, size: int):
    run_db = 'test-db'
    run_col = 'test-col1'

    with MongoClient(port=port) as cli:
        admin = cli['admin']

        if op == 'write':
            # not sure if multiple calls is ok
            admin.command("enableSharding", run_db)
            admin.command(
                "shardCollection", f"{run_db}.{run_col}",
                key = { KEY: "hashed" })

        db = cli[run_db]
        with open(operation_json(op, size)) as f:
            cmds: List[Command] = json.load(f)

        start = asctime()
        for cmd in cmds:
            if 'insert' in cmd:
                cmd['insert'] = run_col
            # elif 'aggregate' in cmd:
            #     cmd['aggregate'] = run_col
            elif 'find' in cmd:
                cmd['find'] = run_col

            db.command(cmd)

        end = asctime()
        with open(TIMESTAMP, 'a+') as f:
            f.write(f'bench {op}: {size} started {start}, ended {end}\n')

        if op == 'read':
            db.drop_collection(run_col)


async def benchmarks(database: Database, port: int):
    TIMESTAMP.touch()
    # with open(TIMESTAMP, 'w+'):
        # connect to primary data node, should pass in cluster
        # monitor = admin.command("getFreeMonitoringStatus")
        # f.write(f'monitoring state: {monitor}')
        # pass

    if database == 'mongodb':
        generate(overwrite=False)

    for op in cast(List[Operation], ['write', 'read', 'meta']):
        for size in LOAD_SIZES:

            if database == 'redis':
                await redis_bench(port, op, size)

            elif database == 'mongodb':
                mongo_bench(port, op, size)



async def remote_bench(
    ssh: Optional[Remote], database: Database, port: int):

    if not ssh:
        await benchmarks(database, port)
        return

    if is_selfhost(ssh.address):
        await benchmarks(database, port)
        return

    bench = STORAGE_FOLDER / 'benchmark.py'
    res = await run_ssh(
        f'./{bench} -p {port} -d {database}',
        ssh.user, ssh.address)

    write_results(res)



async def main(user: Optional[str], addr: Optional[str], **kwargs: Any):
    ssh = None
    if user and addr:
        ssh = Remote(user, addr)

    elif user or addr:
        raise ValueError('only one of user or addr specified')

    await remote_bench(ssh, **kwargs)



if __name__ == '__main__':
    args = ArgumentParser(
        description = 'runs benchmarks for a remote database')

    args.add_argument('-a', '--addr',
        help = 'ssh address where database is')

    args.add_argument('-d', '--database',
        required= True,
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