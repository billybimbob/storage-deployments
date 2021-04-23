#!/usr/bin/env python3

import asyncio
import json

from argparse import ArgumentParser
from pathlib import Path

from dataclasses import dataclass
from typing import Callable, Dict, List, Set, Optional, Union

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



def create_cluster(ips: str):
    addrs = Addresses.from_json(ips)
    with Redis() as cli:
        cli.cluster('create', *addrs)



def touch_log(log: Union[Path, str]):
    log = Path(log)
    log.parent.mkdir(exist_ok=True, parents=True)

    if not log.exists():
        with open(log, 'x'): pass



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
        create_cluster(ips)

    # if not sentinel and master and master_port:
    #     redis_server += ['--slaveof', master, str(master_port)]

    # elif sentinel and master and master_port:
    #     redis_server.append('--sentinel')

    # elif master or master_port:
    #     raise ValueError('master args missing addr or port')


    # if sentinel and master and master_port:
    #     await asyncio.sleep(2)
    #     # might run too early
    #     sentinel_monitor(conf, master, master_port)



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

    # args.add_argument('-m', '--master',
    #     help = 'location of the master node')

    # args.add_argument('-p', '--master-port',
    #     type = int,
    #     help = 'port of the master node')
    
    args = args.parse_args()
    asyncio.run(init_server(**vars(args)))