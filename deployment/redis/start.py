#!/usr/bin/env python3

import asyncio
from pathlib import Path

from typing import NamedTuple, Optional, Union
from argparse import ArgumentParser
from redis import Redis


class Sentinel(NamedTuple):
    port: int
    m_name: str

    @classmethod
    def from_conf(cls, conf: str):
        port, m_name = [None] * 2

        with open(conf) as f:
            for line in f:
                if 'port' in line:
                    port = int(line.split()[-1])

                elif 'monitor' in line:
                    m_name = line.split()[2]

        if port and m_name:
            return cls(port, m_name)
        else:
            raise ValueError('config file is missing values')
        

def sentinel_monitor(conf: str, master: str, m_port: int):
    sent = Sentinel.from_conf(conf)

    with Redis(port=sent.port) as cli:
        cli.sentinel_monitor(sent.m_name, master, m_port, 2)



def touch_log(log: Union[Path, str]):
    log = Path(log)
    log.parent.mkdir(exist_ok=True, parents=True)
    if not log.exists():
        with open(log, 'x'): pass


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
        redis_server += ['--slaveof', master, str(master_port)]

    elif sentinel and master and master_port:
        redis_server.append('--sentinel')

    print(f'cmd: {redis_server}')
    await asyncio.create_subprocess_exec(*redis_server)

    if sentinel and master and master_port:
        await asyncio.sleep(2)
        # might run too early
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