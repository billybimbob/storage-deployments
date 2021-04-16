#!/usr/bin/env python3

import asyncio
from pathlib import Path

from typing import Optional, Tuple, Union, cast
from argparse import ArgumentParser
from redis import Redis



def sentinel_monitor(conf: str, master: str, m_port: int):
    auth = get_master_auth(conf)
    if auth is None:
        raise ValueError('master auth info missing specified')

    m_name, m_pass = auth
    with Redis(password=m_pass) as cli:
        cli.sentinel_monitor(m_name, master, m_port, 2)



def get_master_auth(conf: str) -> Optional[Tuple[str, str]]:
    # expects sentinel auth-pass name cloudpass
    with open(conf) as f:
        for line in f:
            if 'auth-pass' not in line:
                continue
                
            return cast(
                Tuple[str, str],
                tuple(line.split()[-2:]))


def touch_log(log: Union[Path, str]):
    log = Path(log)
    log.parent.mkdir(exist_ok=True, parents=True)
    if not log.exists():
        with open(log, 'x') as _: pass


async def main(
    conf: str,
    log: str,
    sentinel: bool,
    master: Optional[str],
    master_port: Optional[int]):

    touch_log(log)

    redis_server = ['redis-server', conf]
    redis_server += ['--logfile', log]

    if not sentinel and master and master_port:
        redis_server.extend(['--slaveof', master, str(master_port)])

    elif sentinel and master and master_port:
        redis_server.append('--sentinel')

    print(f'cmd: {redis_server}')
    await asyncio.create_subprocess_exec(*redis_server)

    if sentinel and master and master_port:
        sentinel_monitor(conf, master, master_port)



if __name__ == "__main__":
    args = ArgumentParser(description = 'start the redis programs')

    args.add_argument('-c', '--conf', 
        required = True,
        help = 'the redis configuration file')

    args.add_argument('-s', '--sentinel',
        action = 'store_true',
        help = 'start as a sentinel node')

    args.add_argument('-l', '--log',
        required = True,
        help = 'log file location')

    args.add_argument('-m', '--master',
        help = 'location of the master node')

    args.add_argument('-p', '--master-port',
        type = int,
        help = 'port of the master node')
    
    args = args.parse_args()
    asyncio.run(main(**vars(args)))