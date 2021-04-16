#!/usr/bin/env python3

from collections.abc import Iterable
from dataclasses import dataclass
from typing import List, Literal, NamedTuple, Optional, Tuple, Union
from pathlib import Path

import asyncio as aio
import asyncio.subprocess as proc

import argparse
import json
import os
import shlex


STORAGE_REPO = 'https://github.com/billybimbob/storage-deployments.git'
# use path to parse / and extensions
STORAGE_FOLDER = Path(STORAGE_REPO).stem


BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
MONGODB = BASE_DIR / 'mongodb'
REDIS = BASE_DIR / 'redis'



Database = Literal['redis', 'mongodb']

class ProcessOut(NamedTuple):
    out: str
    err: str

    @staticmethod
    def from_processs(std: Tuple[bytes, bytes]):
        return ProcessOut(*[s.decode() for s in std])


@dataclass(init=False)
class Remote:
    ip: str
    cmd: str

    @staticmethod
    def valid_ip(ip: str):
        return len(ip.split(".")) == 4


    def __init__(self, ip: str, cmd: Union[str, List[str]]):
        if not Remote.valid_ip(ip):
            raise ValueError('ip is not valid')

        if isinstance(cmd, list):
            cmd = ' '.join(cmd)

        self.ip = ip
        self.cmd = cmd


    def ssh(self, user: str) -> List[str]:
        return shlex.split(f'ssh {user}@{self.ip} "{self.cmd}"')


class Result(NamedTuple):
    remote: Remote
    output: Union[ProcessOut, Exception]

    @property
    def is_error(self):
        out = self.output
        return isinstance(out, Exception) or bool(out.err)



@dataclass
class Addresses:
    main: List[str]
    data: List[str]
    misc: List[str]

    def __bool__(self):
        return self.main and self.data and self.misc

    def __iter__(self):
        yield from self.main
        yield from self.data
        yield from self.misc

    @staticmethod
    def from_json(path: Union[str, Path]):
        if isinstance(path, str):
            path = Path(path)

        path = path.with_suffix('.json') 
        with open(path, 'r') as f:
            ips = json.load(f)
            return Addresses(**ips)



async def exec_remotes(user: str, remotes: List[Remote]) -> List[Result]:

    async def ssh_run(remote: Remote):
        ssh_proc = await aio.create_subprocess_exec(
            *remote.ssh(user), stdout=proc.PIPE, stderr=proc.PIPE)

        com = await ssh_proc.communicate()
        return ProcessOut.from_processs(com)

    outputs = await aio.gather(
        *[ ssh_run(remote) for remote in remotes ],
        return_exceptions=True)

    return [
        Result(*rem_out)
        for rem_out in zip(remotes, outputs) ]



async def run_ips(user: str, ips: Iterable[str], cmd: str) -> List[Result]:
    cmds = [ Remote(ip, cmd) for ip in ips ]
    if not cmds:
        return list()

    return await exec_remotes(user, cmds)



async def redis_start(user: str, ips: Addresses):
    master_node_port = 6379
    starts: List[Remote] = []
    base = ['./start.py']

    for ip in ips.main:
        cmd = list(base)
        cmd += ['-c', 'master.conf']
        starts.append( Remote(ip, cmd) )

    for ip in ips.misc:
        cmd = list(base)
        cmd += ['-c', 'sentinel.conf']
        cmd += ['-s']
        cmd += ['-m', ip]
        cmd += ['-p', str(master_node_port)]
        starts.append( Remote(ip, cmd) )

    for ip in ips.data:
        cmd = list(base)
        cmd += ['-c', 'slave.conf']
        cmd += ['-m', ip]
        cmd += ['-p', str(master_node_port)]
        starts.append( Remote(ip, cmd) )

    return await exec_remotes(user, starts)



async def mongo_start(user: str, ips: Addresses):
    starts: List[Remote] = []
    base = ['./start.py', '-c', 'cluster.json']

    for ip in ips.main:
        cmd = list(base)
        cmd += ['-r', 'mongos']
        starts.append( Remote(ip, cmd) )

    for ip in ips.misc:
        cmd = list(base)
        cmd += ['-m', str(0)]
        cmd += ['-r', 'configs']
        starts.append( Remote(ip, cmd) )

    for ip in ips.data:
        cmd = list(base)
        cmd += ['-m', str(0)]
        cmd += ['-r', 'shards']
        starts.append( Remote(ip, cmd) )

    return await exec_remotes(user, starts)



def write_results(results: List[Result], out: Optional[str]):
    res_info = [
        f"finished cmd {r.remote.cmd} at {r.remote.ip} "
        f"with output:\n{r.output}"
        for r in results ]

    res_info = '\n'.join(res_info)

    if out:
        with open(out, 'w') as f:
            f.write(res_info)
    else:
        print(res_info)



async def main(
    file: str, user: str, database: Database, out: Optional[str]):

    ips = Addresses.from_json(file)
    if not ips:
        return

    # note: fixed number of slaves and sentials for redis 
    # and fixed number of slaves for mongodb
    # if database == "redis":
    #     await redis_scp(ips, user, 1, 1)

    # elif database == "mongodb":
    #     await mongodb_scp(ips, user)

    clone = f'git clone {STORAGE_REPO}'
    results = await run_ips(user, ips, clone)

    failed = [ ip for ip, res in zip(ips, results) if res.is_error ]

    if failed:
        pull = f'cd {STORAGE_FOLDER} && git pull'
        await run_ips(user, failed, pull)

    if database == "redis":
        results = await redis_start(user, ips)
        write_results(results, out)
    
    elif database == "mongodb":
        results = await mongo_start(user, ips)
        write_results(results, out)


            
if __name__ == "__main__":
    parse = argparse.ArgumentParser(
        'Runs the command on all ssh ips in the supplied file')

    parse.add_argument('-d', '--database',
        default = 'redis',
        choices = ['redis', 'mongodb'],
        help = 'select database')

    parse.add_argument('-f', '--file',
        required = True,
        help = 'file that contains the ips')

    parse.add_argument('-o', '--out', 
        help = 'write output of ssh stdout to file')

    parse.add_argument('-u', '--user',
        default = 'cc',
        help = 'the user for the ips, for now all the same')

    args = parse.parse_args()
    aio.run(main(**vars(args)))
