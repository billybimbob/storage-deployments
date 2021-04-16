#!/usr/bin/env python3

from dataclasses import dataclass
from typing import (
    Any, Dict, Iterable, List, Literal, NamedTuple, Optional, Tuple, Union)
from pathlib import Path, PurePath, PurePosixPath

import asyncio as aio
import asyncio.subprocess as proc

import argparse
import json
import logging
import shlex


STORAGE_REPO = 'https://github.com/billybimbob/storage-deployments.git'
# use path to parse / and extensions
STORAGE_FOLDER = PurePosixPath( PurePath(STORAGE_REPO).stem )

DEPLOYMENT = STORAGE_FOLDER / 'deployment'
LOGS = STORAGE_FOLDER / 'monitor_and_graphs' / 'logs'


Database = Literal['redis', 'mongodb']

class ProcessOut(NamedTuple):
    out: str
    err: str

    @staticmethod
    def from_process(std: Tuple[bytes, bytes]):
        return ProcessOut(*[ s.decode() for s in std ])


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
        return shlex.split(f'ssh {user}@{self.ip} {self.cmd}')


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

    def __bool__(self) -> bool:
        return (bool(self.main)
            and bool(self.data)
            and bool(self.misc))

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

    async def ssh_run(remote: Remote, run_num: int):
        logging.debug(f'run: {run_num} running command {remote.ssh(user)}')

        ssh_proc = await aio.create_subprocess_exec(
            *remote.ssh(user), stdout=proc.PIPE, stderr=proc.PIPE)

        logging.debug(f'run: {run_num} waiting')

        try:
            com = await aio.wait_for(ssh_proc.communicate(), 8)
            logging.debug(f'run: {run_num} finished')
            return ProcessOut.from_process(com)

        except aio.TimeoutError:
            logging.error(f'run: {run_num} took too long')
            ssh_proc.kill()
            await ssh_proc.wait()
            raise


    outputs = await aio.gather(
        *[ ssh_run(remote, i+1) for i, remote in enumerate(remotes) ],
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
    redis = DEPLOYMENT / 'redis'
    r_log = LOGS / 'redis'

    starts: List[Remote] = []
    base = [f'./{redis}/start.py']

    for ip in ips.main:
        cmd = list(base)
        cmd += ['-l', f'{r_log}/master.log']
        cmd += ['-c', f'{redis}/confs/master.conf']
        starts.append( Remote(ip, cmd) )

    for ip in ips.misc:
        cmd = list(base)
        cmd += ['-l', f'{r_log}/sentinel.log']
        cmd += ['-c', f'{redis}/confs/sentinel.conf']
        cmd += ['-s']
        cmd += ['-m', ip]
        cmd += ['-p', str(master_node_port)]
        starts.append( Remote(ip, cmd) )

    for ip in ips.data:
        cmd = list(base)
        cmd += ['-l', f'{r_log}/slave.log']
        cmd += ['-c', f'{redis}/confs/slave.conf']
        cmd += ['-m', ip]
        cmd += ['-p', str(master_node_port)]
        starts.append( Remote(ip, cmd) )

    return await exec_remotes(user, starts)



async def mongo_start(user: str, ips: Addresses):
    mongodb = DEPLOYMENT / 'mongodb'
    m_log = LOGS / 'mongodb'
    
    # update cluster json
    cluster_loc = Path(__file__) / 'deployment' / 'cluster.json'
    with open(cluster_loc, 'r+') as f:
        cluster: Dict[str, Any] = json.load(f)

        cluster['log'].path = m_log
        # look into multiple mongos
        cluster['mongos'].host = ips.main[0]
        cluster['configs'].members = ips.misc
        cluster['shards'].members = ips.data

        json.dump(cluster, f, indent=4)

    # should  scp updated cluster

    starts: List[Remote] = []
    base = [f'./{mongodb}/start.py', '-c', f'{mongodb}/cluster.json']

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



def write_results(results: List[Result], out: Optional[str] = None):
    res_info = [
        f"finished cmd {r.remote.cmd} at {r.remote.ip} "
        f"with output:\n{r.output}"
        for r in results ]

    res_info = '\n'.join(res_info)

    if out:
        with open(out, 'w') as f:
            f.write(res_info)
    else:
        logging.debug(res_info)



async def main(
    file: Optional[str], user: str, database: Database, out: Optional[str]):

    if file is None:
        file = str(Path(__file__).parent / 'ip-addresses')

    ips = Addresses.from_json(file)
    if not ips:
        return

    logging.debug(f'cloning repo for addrs {ips}')

    clone = f'git clone {STORAGE_REPO}'
    results = await run_ips(user, ips, clone)

    failed = [ ip for ip, res in zip(ips, results) if res.is_error ]

    if failed:
        logging.debug(f'pulling git for addrs {failed}')
        pull = f'cd {STORAGE_FOLDER} && git pull'
        await run_ips(user, failed, pull)

    if database == "redis":
        logging.debug('starting redis daemons')
        results = await redis_start(user, ips)
        write_results(results, out)
    
    elif database == "mongodb":
        logging.debug('starting mongo daemons')
        results = await mongo_start(user, ips)
        write_results(results, out)


            
if __name__ == "__main__":
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.basicConfig(level=logging.DEBUG)

    parse = argparse.ArgumentParser(
        'Runs the command on all ssh ips in the supplied file')

    parse.add_argument('-d', '--database',
        default = 'redis',
        choices = ['redis', 'mongodb'],
        help = 'select database')

    parse.add_argument('-f', '--file',
        help = 'file that contains the ips')

    parse.add_argument('-o', '--out', 
        help = 'write output of ssh stdout to file')

    parse.add_argument('-u', '--user',
        default = 'cc',
        help = 'the user for the ips, for now all the same')

    args = parse.parse_args()
    aio.run(main(**vars(args)))
