#!/usr/bin/env python3

from asyncio.subprocess import PIPE
import asyncio
import json
import logging

from argparse import ArgumentParser
from os import write
from pathlib import Path

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Set, Optional, Union

from redis import Redis



@dataclass
class Addresses:
    main: List[str]
    data: List[str]
    misc: List[str]

    @classmethod
    def from_json(cls, path: Union[str, Path]):
        if isinstance(path, str):
            path = Path(path)

        path = path.with_suffix('.json')
        with open(path, 'r') as f:
            ips = json.load(f)

        return cls(**ips)


    def __bool__(self) -> bool:
        return (bool(self.main)
            and bool(self.data)
            and bool(self.misc))

    def __iter__(self):
        yield from self.main
        yield from self.data
        yield from self.misc



LineCheck = Callable[[str], bool]

def parse_conf(
    conf: Union[str, Path], 
    *args: Union[str, LineCheck]) -> Dict[str, str]:

    parse_args: Dict[str, str] = {}
    search_args: Set[Union[str, LineCheck]] = set(args)

    with open(conf) as f:
        found_args: Set[str] = set()
        for line in f:
            for search in search_args:

                if isinstance(search, str):
                    if search not in line:
                        continue

                    search_val = line.split(search)[-1]
                    parse_args[search] = search_val
                    found_args.add(search)

                elif search(line):
                    parse_args[line] = '' # not sure
                    
            search_args -= found_args
            found_args.clear()

    if any( isinstance(s, str) for s in search_args ):
        raise ValueError('config file is missing values')
    else:
        return parse_args
        


# def sentinel_monitor(conf: str, master: str, m_port: int):
#     conf_params = parse_conf(
#         conf, 'port', lambda l: l.startswith('sentinel'))

#     port = int(conf_params['port'])

#     master_name = [
#         param
#         for param in conf_params
#         if 'monitor' in param ]

#     if not master_name:
#         raise ValueError('conf missing values')

#     master_name = master_name[0].split()[0]

#     sentinel_cmds = [
#         arg.split()[1:] # drop sentinel word
#         for arg in conf_params
#         if arg.startswith('sentinel') and 'monitor' not in arg ]

#     with Redis(port=port) as cli:
#         cli.sentinel_monitor(master_name, master, m_port, 2)

#         for cmd in sentinel_cmds:
#             cli.sentinel(*cmd)



def touch_log(log: Union[Path, str]):
    log = Path(log)
    log.parent.mkdir(exist_ok=True, parents=True)

    if not log.exists():
        with open(log, 'x'): pass



async def create_cluster(conf: str, ips: str):
    port = int(parse_conf(conf, 'port')['port'])
    addrs = Addresses.from_json(ips)
    nodes = [f'{ip}:{port}' for ip in addrs]

    logging.info(nodes)

    redis_cli = ['redis-cli']
    redis_cli += ['-c']
    redis_cli += ['--cluster', 'create', *nodes]
    redis_cli += ['--cluster-replicas', str(0)]

    logging.info(redis_cli)

    proc = await asyncio.create_subprocess_exec(
        *redis_cli, stdin=PIPE, stdout=PIPE, stderr=PIPE)

    out, error = await proc.communicate("yes\n".encode("utf-8"))

    logging.info(out)
    logging.info(error)

    # with Redis(port=port) as cli:
    #     cli.cluster('create', *nodes)        


async def init_server(
    conf: str, *,
    log: Optional[str] = None,
    ips: Optional[str] = None):

    if log and ips:
        raise ValueError('only one of ips and log should be specified')
    
    elif not (log or ips):
        raise ValueError('log and ips are both not specified')

    elif log:
        redis_server = ['redis-server', conf, '--logfile', log]
        print(f'cmd: {redis_server}')

        touch_log(log)
        await asyncio.create_subprocess_exec(*redis_server)
    
    elif ips:
        await create_cluster(conf, ips)



async def end_server(conf: str):
    port = parse_conf(conf, 'port')['port']
    port = int(port)

    with Redis(port=port) as cli:
        cli.flushall()
        cli.cluster('reset')
        cli.shutdown()



async def mod_server(conf: str, shutdown: bool, **init_args: Any):
    if shutdown:
        await end_server(conf)
    else:
        await init_server(conf, **init_args)


if __name__ == "__main__":
    args = ArgumentParser(description = 'start the redis programs')

    args.add_argument('-c', '--conf', 
        required = True,
        help = 'the redis configuration file')

    # args.add_argument('-s', '--sentinel',
    #     action = 'store_true',
    #     help = 'start as a sentinel node')

    args.add_argument('-i', '--ips',
        help = 'ips file location')

    args.add_argument('-l', '--log',
        help = 'log file location')

    args.add_argument('-s', '--shutdown',
        action = 'store_true',
        help = 'run shutdown instead of init')

    # args.add_argument('-m', '--master',
    #     help = 'location of the master node')

    # args.add_argument('-p', '--master-port',
    #     type = int,
    #     help = 'port of the master node')
    
    logging.basicConfig(filename="testing.txt", filemode="w")

    args = args.parse_args()
    asyncio.run(mod_server(**vars(args)))