#!/usr/bin/env python3

from dataclasses import dataclass, asdict
from typing import (
    Any, List, Literal, NamedTuple, Optional, Sequence, Tuple, Union)
from pathlib import Path, PurePath, PurePosixPath

import asyncio as aio
import asyncio.subprocess as proc

import argparse
import json
import logging

import shlex
import socket

from deployment.modifyconf import mod_path
from deployment.redis.start import end_server, init_server
from deployment.mongodb.start import Cluster, start_mongos


STORAGE_REPO = 'https://github.com/billybimbob/storage-deployments.git'

# use path to parse / and extensions
STORAGE_FOLDER = PurePosixPath( PurePath(STORAGE_REPO).stem )
DEPLOYMENT = Path('deployment')
LOGS = Path('monitor_and_graphs') / 'logs'

logger = logging.getLogger(__name__)


Database = Literal['redis', 'mongodb']


def is_selfhost(ip: str):
    self_info = socket.gethostbyaddr(socket.gethostname())
    ip_info = socket.gethostbyaddr(ip)

    self_hosts = [ self_info[0], *self_info[1] ]
    ip_hosts = [ ip_info[0], *ip_info[1] ]

    self_addrs = self_info[-1]
    ip_addrs = ip_info[-1]

    return (any(ip in self_addrs for addr in ip_addrs)
        or any(host in self_hosts for host in ip_hosts))


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


@dataclass(init=False)
class Remote:
    user: str
    ip: str
    cmd: str

    def __init__(
        self,
        user: str,
        ip: str,
        cmd: Union[str, List[str]]):

        # if not self.valid_ip(ip):
        #     raise ValueError('ip is not valid')

        if isinstance(cmd, list):
            cmd = ' '.join(cmd)

        self.user = user
        self.ip = ip
        self.cmd = cmd

    @staticmethod
    def valid_ip(ip: str):
        return len(ip.split(".")) == 4

    @property
    def ssh(self) -> List[str]:
        ssh_cmd = f'ssh {self.user}@{self.ip} {self.cmd}'
        return shlex.split(ssh_cmd)


class Standards(NamedTuple):
    out: str
    err: str

    @classmethod
    def from_process(cls, std: Tuple[bytes, bytes]):
        return cls(*[ s.decode() for s in std ])


class Result(NamedTuple):
    command: Sequence[str]
    output: Union[Standards, Exception]

    @property
    def is_error(self):
        out = self.output
        return isinstance(out, Exception) or bool(out.err)



async def exec_commands(*commands: List[str]) -> List[Result]:
    " Runs multiple commands with timeout, and wraps them in results "

    async def process_exec(cmd: List[str], run_num: int) -> Standards:
        logger.debug(f'run: {run_num} running command {cmd}')

        sub_proc = await aio.create_subprocess_exec(
            *cmd, stdout=proc.PIPE, stderr=proc.PIPE)

        logger.debug(f'run: {run_num} waiting')
        try:
            com = await aio.wait_for(sub_proc.communicate(), 12)
            logger.debug(f'run: {run_num} finished')

        except aio.TimeoutError:
            logger.error(f'run: {run_num} took too long')
            sub_proc.kill()
            await sub_proc.wait()
            raise

        else:
            return Standards.from_process(com)


    outputs = await aio.gather(
        *[process_exec(cmd, i+1) for i, cmd in enumerate(commands)],
        return_exceptions=True)

    return [
        Result(cmd, out)
        for cmd, out in zip(commands, outputs) ]



async def run_ssh(cmd: str, user: str, *ips: str) -> List[Result]:
    remotes = [
        Remote(user, ip, cmd)
        for ip in ips if not is_selfhost(ip) ]

    cmds = [ c.ssh for c in remotes ]
    if any( is_selfhost(ip) for ip in ips ):
        cmds.append(shlex.split(cmd))

    return await exec_commands(*cmds)



async def redis_start(user: str, ips: Addresses) -> List[Result]:
    local_conf = DEPLOYMENT / 'redis/confs/master.conf'
    redis = STORAGE_FOLDER / DEPLOYMENT / 'redis'
    r_log = STORAGE_FOLDER / LOGS / 'redis'

    start_cmds: List[Remote] = []
    cmd_base = [f'./{redis}/start.py']

    # local addr can potentially be a main addr
    in_ips = any( is_selfhost(ip) for ip in ips )

    if in_ips:
        log = LOGS / 'redis' / 'master.log'
        # run locally, no out info
        await init_server(str(local_conf), log=str(log))

    for ip in ips:
        if is_selfhost(ip):
            continue

        cmd = list(cmd_base)
        cmd += ['-l', f'{r_log}/master.log']
        cmd += ['-c', 'master.conf']
        start_cmds.append( Remote(user, ip, cmd) )
        
    # ensure master starts before other nodes
    results = await exec_commands(*[ s.ssh for s in start_cmds ])

    addrs_loc = DEPLOYMENT / 'ip-addresses'

    if in_ips:
        await init_server(str(local_conf), ips=str(addrs_loc))

    else:
        cluster_start = list(cmd_base)
        cluster_start += ['-c', str(redis / 'confs' / 'master.conf')]
        cluster_start += ['-i', str(STORAGE_FOLDER / addrs_loc)]
        ip = ips.main[0]
        cluster_start = Remote(user, ip, cluster_start)

        results += await exec_commands(cluster_start.ssh)

    return results



async def mongo_start(user: str, ips: Addresses) -> List[Result]:

    cluster_loc = DEPLOYMENT / 'mongodb/cluster.json'
    cluster = update_cluster(cluster_loc, ips)

    scp = [ # should scp updated cluster
        shlex.split(f'scp {cluster_loc} {user}@{ip}:~/cluster.json')
        for ip in ips
        if not is_selfhost(ip) ]

    results = await exec_commands(*scp)
    results += await mongo_remotes(user, ips)

    # local addr can potentially be a main addr
    for i, ip in enumerate(ips.main):
        if not is_selfhost(ip):
            continue

        mongos_conf = DEPLOYMENT / 'mongodb/confs/mongos.conf'
        # run locally, no resulting output
        await start_mongos(i, str(mongos_conf), cluster)

    return results


def update_cluster(cluster_loc: Path, ips: Addresses) -> Cluster:
    m_log = STORAGE_FOLDER / LOGS / 'mongodb'
    cluster = Cluster.from_json(cluster_loc)

    cluster.log = str(m_log)
    cluster.mongos.members = ips.main
    cluster.configs.members = ips.misc
    cluster.shards.members = ips.data

    with open(cluster_loc, 'r+') as f:
        json.dump(asdict(cluster), f, indent=4)

    return cluster


async def mongo_remotes(user: str, ips: Addresses):
    start_cmds: List[Remote] = []

    mongodb = STORAGE_FOLDER / DEPLOYMENT / 'mongodb'
    cmd_base = [f'./{mongodb}/start.py', '-c', 'cluster.json']

    for i, ip in enumerate(ips.misc):
        cmd = list(cmd_base)
        cmd += ['-r', 'configs']
        cmd += ['-m', str(i)]
        cmd += ['-c', 'config.conf']
        start_cmds.append( Remote(user, ip, cmd) )

    for i, ip in enumerate(ips.data):
        cmd = list(cmd_base)
        cmd += ['-r', 'shards']
        cmd += ['-m', str(i)]
        cmd += ['-c', 'shard.conf']
        start_cmds.append( Remote(user, ip, cmd) )

    results = await exec_commands(*[ s.ssh for s in start_cmds ])

    # add initiate cmds
    start_cmds.clear()

    if ips.misc:
        cmd = list(cmd_base)
        cmd += ['-r', 'configs']
        start_cmds.append( Remote(user, ips.misc[0], cmd) )

    if ips.data:
        cmd = list(cmd_base)
        cmd += ['-r', 'shards']
        start_cmds.append( Remote(user, ips.data[0], cmd) )

    results += await exec_commands(*[ s.ssh for s in start_cmds ])

    # make sure mongos starts after repl init ran
    start_cmds.clear()

    for i, ip in enumerate(ips.main):
        if is_selfhost(ip):
            continue

        cmd = list(cmd_base)
        cmd += ['-r', 'mongos']
        cmd += ['-m', str(i)]
        cmd += ['-c', 'mongos.conf'] # should be top level from scp
        start_cmds.append( Remote(user, ip, cmd) )

    results += await exec_commands(*[ s.ssh for s in start_cmds ])

    return results



def write_results(results: List[Result], out: Optional[str]=None):
    res_info = [
        f"finished cmd {r.command} with output:\n{r.output}"
        for r in results ]

    res_info = '\n'.join(res_info)

    if out:
        with open(out, 'w') as f:
            f.write(res_info)
    else:
        logger.debug(res_info)



async def fetch_repo(ips: Addresses, user: str):

    logger.debug(f'cloning repo for addrs {ips}')
    clone = f'git clone {STORAGE_REPO}'

    non_local = [ip for ip in ips if not is_selfhost(ip)]
    results = await run_ssh(clone, user, *non_local)

    failed = [ ip for ip, res in zip(ips, results) if res.is_error ]

    if failed:
        logger.debug(f'pulling git for addrs {failed}')
        pull = f'cd {STORAGE_FOLDER} && git pull' # && git checkout . && git pull'
        await run_ssh(pull, user, *failed)



async def run_starts(
    ips: Addresses,
    user: str,
    database: Database,
    out: Optional[str]=None):

    if database == "redis":
        logger.debug('starting redis daemons')
        results = await redis_start(user, ips)

    elif database == "mongodb":
        logger.debug('starting mongo daemons')
        results = await mongo_start(user, ips)

    write_results(results, out)



async def run_shutdown(
    ips: Addresses,
    user: str,
    database: Database,
    out: Optional[str]=None):

    # go reverse so that main nodes end last
    if database == 'redis':
        logger.debug('stopping redis daemons')

        if any(is_selfhost(ip) for ip in ips):
            master_conf = mod_path(DEPLOYMENT / 'redis/confs/master.conf')
            await end_server(str(master_conf))

        non_local = [ip for ip in ips if not is_selfhost(ip)]
        redis = STORAGE_FOLDER / DEPLOYMENT / 'redis'
        shutdown = f'./{redis}/start.py -s -c master.conf'

        results = await run_ssh(shutdown, user, *non_local)


    elif database == 'mongodb':
        logger.debug('stopping mongo daemons')
        shutdown = 'mongod --shutdown'

        # shutdown main first
        results = await run_ssh(shutdown, user, *ips.main)
        results += await run_ssh(shutdown, user, *ips.data, *ips.misc)

    write_results(results, out)



async def main(
    file: Optional[str], user: str, shutdown: bool, **run_args: Any):

    if file is None:
        file = str(Path(__file__).parent / 'ip-addresses')

    ips = Addresses.from_json(file)

    await fetch_repo(ips, user)

    if not shutdown:
        await run_starts(ips, user, **run_args)
    else:
        await run_shutdown(ips, user, **run_args)



if __name__ == "__main__":
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)

    parse = argparse.ArgumentParser(
        description = 'runs the start and shutdown commands for '
                      'database nodes')

    parse.add_argument('-d', '--database',
        required = True,
        choices = ['mongodb','redis'],
        help = 'datbase system that is being modified')

    parse.add_argument('-f', '--file',
        help = 'file that contains database node locations')

    parse.add_argument('-o', '--out',
        help = 'write output of ssh stdout to file')

    parse.add_argument('-s', '--shutdown',
        action = 'store_true',
        help = 'run shutdown process instead of default start')

    parse.add_argument('-u', '--user',
        default = 'cc',
        help = 'the user for the ips, for now all the same')

    args = parse.parse_args()
    aio.run(main(**vars(args)))
