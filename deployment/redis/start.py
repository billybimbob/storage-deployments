#!/usr/bin/env python3

import asyncio
from pathlib import Path

from typing import NamedTuple, Optional, Union
from argparse import ArgumentParser
from redis import Redis


class Configs(NamedTuple):
    port: int
    m_name: Optional[str]

    @classmethod
    def from_file(cls, conf: Union[str, Path]):
        port, m_name = [None] * 2

        with open(conf) as f:
            for line in f:
                if 'port' in line:
                    port = int(line.split()[-1])

                elif 'monitor' in line:
                    m_name = line.split()[2]

        if port is None:
            raise ValueError('config file is missing values')
        else:
            return cls(port, m_name)
        


def sentinel_monitor(conf: str, master: str, m_port: int):
    sent = Configs.from_file(conf)
    if sent.m_name is None:
        raise ValueError('config file has no master name')

    with Redis(port=sent.port) as cli:
        cli.sentinel_monitor(sent.m_name, master, m_port, 2)



def touch_log(log: Union[Path, str]):
    log = Path(log)
    log.parent.mkdir(exist_ok=True, parents=True)
    if not log.exists():
        with open(log, 'x'): pass


async def init_server(
    conf: str,
    log: str,
    sentinel: bool = False,
    master: Optional[str] = None,
    master_port: Optional[int] = None):

    redis_server = ['redis-server', conf]
    redis_server += ['--logfile', log]

    if not sentinel and master and master_port:
        redis_server += ['--replicaof', master, str(master_port)]

    elif sentinel and master and master_port:
        redis_server.append('--sentinel')

    elif master or master_port:
        raise ValueError('master args missing addr or port')

    touch_log(log)

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
    asyncio.run(init_server(**vars(args)))