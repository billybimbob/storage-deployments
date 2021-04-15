#!/usr/bin/env python3

import asyncio

from typing import Optional
from argparse import ArgumentParser
from redis.client import Redis


async def main(
    conf: str,
    sentinel: bool,
    master: Optional[str],
    master_port: Optional[int]):

    redis_server = ['redis-server', conf]

    if not sentinel and master and master_port:
        redis_server.extend(['--slaveof', master, str(master_port)])

    elif sentinel and master and master_port:
        redis_server.append('--sentinel')

    print(f'cmd: {redis_server}')
    await asyncio.create_subprocess_exec(*redis_server)

    if sentinel and master and master_port:
        with Redis() as cli:
            # mymaster should be the same as in the conf files
            cli.sentinel_monitor('mymaster', master, master_port, 2)



if __name__ == "__main__":
    args = ArgumentParser(description="Start the redis programs")

    args.add_argument('-c', '--conf', 
        required=True,
        help='the redis configuration file')

    args.add_argument('-s', '--sentinel',
        action='store_true',
        help='start as a sentinel node')

    args.add_argument('-m', '--master',
        help='location of the master node')

    args.add_argument('-p', '--master-port',
        type=int,
        help='port of the master node')
    
    args = args.parse_args()
    asyncio.run(main(**vars(args)))