#!/usr/bin/env python3

import asyncio

from typing import Optional
from argparse import ArgumentParser
from pathlib import Path


async def main(
    redis: str,
    conf: str,
    sentinel: bool,
    master: Optional[str],
    master_port: Optional[int]):

    redis_server = Path(redis).with_name('redis-server')
    redis_server = [str(redis_server), conf]

    if not sentinel and master and master_port:
        redis_server.extend(['--slaveof', master, master_port])

    if sentinel and master and master_port:
        redis_server.append('--sentinel')

    print(f'cmd: {redis_server}')
    await asyncio.create_subprocess_exec(*redis_server)

    if sentinel and master and master_port:
        sentinel_monitor = Path(redis).with_name('redis-cli')

        # mymaster should be the same as in the conf files
        sentinel_monitor = [
            str(sentinel_monitor),
            'SENTINAL', 'MONITOR', 'mymaster', master, master_port]
        
        await asyncio.create_subprocess_exec(*sentinel_monitor)



if __name__ == "__main__":
    args = ArgumentParser(description="Start the redis programs")
    args.add_argument('-r', '--redis', default='', help='the path to the compiled redis binaries')
    args.add_argument('-c', '--conf', required=True, help='the redis configuration file')
    args.add_argument('-s', '--sentinel', action='store_true', help='start as a sentinel node')
    args.add_argument('-m', '--master', help='location of the master node')
    args.add_argument('-p', '--master-port', type=int, help='port of the master node')
    
    args = args.parse_args()
    asyncio.run(main(**vars(args)))