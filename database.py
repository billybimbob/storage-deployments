#!/usr/bin/env python3

from dataclasses import dataclass
from typing import (
    Any, List, Literal, NamedTuple, Optional, Sequence, Tuple, Union)
from pathlib import Path, PurePath, PurePosixPath

import asyncio as aio
import asyncio.subprocess as proc

import argparse
import json
import logging

import random
import shlex
import socket

from deployment.modifyconf import mod_name
from deployment.redis.start import Configs, init_server
from deployment.mongodb.start import Cluster, start_mongos


STORAGE_REPO = 'https://github.com/billybimbob/storage-deployments.git'

# use path to parse / and extensions
STORAGE_FOLDER = PurePosixPath( PurePath(STORAGE_REPO).stem )
DEPLOYMENT = Path('deployment')
LOGS = Path('monitor_and_graphs') / 'logs'

logger = logging.getLogger(__name__)


Database = Literal['redis', 'mongodb']

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

        if not self.valid_ip(ip):
            raise ValueError('ip is not valid')

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
    """ Runs multiple commands with timeout, and wraps them in results """

    async def process_exec(cmd: List[str], run_num: int) -> Standards:
        logger.debug(f'run: {run_num} running command {cmd}')

        ssh_proc = await aio.create_subprocess_exec(
            *cmd, stdout=proc.PIPE, stderr=proc.PIPE)

        logger.debug(f'run: {run_num} waiting')

        try:
            com = await aio.wait_for(ssh_proc.communicate(), 8)
            logger.debug(f'run: {run_num} finished')

            return Standards.from_process(com)

        except aio.TimeoutError:
            logger.error(f'run: {run_num} took too long')
            ssh_proc.kill()
            await ssh_proc.wait()
            raise

    outputs = await aio.gather(
        *[ process_exec(cmd, i+1) for i, cmd in enumerate(commands) ],
        return_exceptions=True)

    return [
        Result(*rem_out)
        for rem_out in zip(commands, outputs) ]



async def run_ssh(cmd: str, user: str, *ips: str) -> List[Result]:
    cmds = [ Remote(user, ip, cmd) for ip in ips ]
    return await exec_commands(*[ c.ssh for c in cmds ])



async def redis_start(user: str, ips: Addresses) -> List[Result]:
    master_conf = DEPLOYMENT / 'redis' / 'confs' / 'master.conf'
    conf_info = Configs.from_file(master_conf)

    master_node_port = conf_info.port

    redis = STORAGE_FOLDER / DEPLOYMENT / 'redis'
    r_log = STORAGE_FOLDER / LOGS / 'redis'

    start_cmds: List[Remote] = []
    base = [f'./{redis}/start.py']

    self_loc = socket.gethostbyname(socket.gethostname())

    if self_loc in ips.main:
        conf = master_conf.with_name( mod_name(master_conf) )
        log = LOGS / 'redis' / 'master.log'
        # run locally, no out info
        await init_server(str(conf), str(log))

    for ip in ips.main:
        if ip == self_loc:
            continue

        cmd = list(base)
        cmd += ['-l', f'{r_log}/master.log']
        cmd += ['-c', 'master.conf']
        start_cmds.append( Remote(user, ip, cmd) )

    for ip in ips.misc:
        cmd = list(base)
        cmd += ['-l', f'{r_log}/sentinel.log']
        cmd += ['-c', 'sentinel.conf']
        cmd += ['-s']
        cmd += ['-m', random.choice(ips.main)]
        cmd += ['-p', str(master_node_port)]
        start_cmds.append( Remote(user, ip, cmd) )

    for ip in ips.data:
        cmd = list(base)
        cmd += ['-l', f'{r_log}/slave.log']
        cmd += ['-c', 'slave.conf']
        cmd += ['-m', random.choice(ips.main)]
        cmd += ['-p', str(master_node_port)]
        start_cmds.append( Remote(user, ip, cmd) )

    return await exec_commands(*[ s.ssh for s in start_cmds ])



async def mongo_start(user: str, ips: Addresses) -> List[Result]:

    mongodb = STORAGE_FOLDER / DEPLOYMENT / 'mongodb'
    m_log = STORAGE_FOLDER / LOGS / 'mongodb'

    # update cluster json
    cluster_loc = Path(__file__) / 'deployment' / 'cluster.json'
    with open(cluster_loc, 'r+') as f:
        cluster = json.load(f)
        cluster = Cluster(**cluster)

        cluster.log = str(m_log)
        cluster.mongos.members = ips.main
        cluster.configs.members = ips.misc
        cluster.shards.members = ips.data

        json.dump(cluster, f, indent=4)

    self_loc = socket.gethostbyname(socket.gethostname())

    scp = [ # should scp updated cluster
        shlex.split(f'scp {cluster_loc} {user}@{ip}:~/cluster.json')
        for ip in ips
        if ip != self_loc ]

    results = await exec_commands(*scp)

    start_cmds: List[Remote] = []
    base = [f'./{mongodb}/start.py', '-c', 'cluster.json']

    for i, ip in enumerate(ips.main):
        if ip == self_loc:
            continue

        cmd = list(base)
        cmd += ['-r', 'mongos']
        cmd += ['-m', str(i)]
        cmd += ['-c', 'mongos.conf'] # should be top level from scp
        start_cmds.append( Remote(user, ip, cmd) )

    for i, ip in enumerate(ips.misc):
        cmd = list(base)
        cmd += ['-r', 'configs']
        cmd += ['-m', str(i)]
        start_cmds.append( Remote(user, ip, cmd) )

    for i, ip in enumerate(ips.data):
        cmd = list(base)
        cmd += ['-r', 'shards']
        cmd += ['-m', str(i)]
        start_cmds.append( Remote(user, ip, cmd) )

    results += await exec_commands(*[ s.ssh for s in start_cmds ])

    # add initiate cmds
    start_cmds.clear()

    if ips.misc:
        cmd = list(base)
        cmd += ['-r', 'configs']
        start_cmds.append( Remote(user, ips.misc[0], cmd) )

    if ips.data:
        cmd = list(base)
        cmd += ['-r', 'shards']
        start_cmds.append( Remote(user, ips.data[0], cmd) )

    results += await exec_commands(*[ s.ssh for s in start_cmds ])

    if self_loc in ips.main:
        mongos_conf = DEPLOYMENT / 'mongodb' / 'confs' / 'mongos.conf'
        mongos_conf = mongos_conf.with_name( mod_name(mongos_conf) )
        # run locally, no resulting output
        await start_mongos(
            ips.main.index(self_loc), str(mongos_conf), cluster)

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



async def run_starts(
    ips: Addresses,
    user: str,
    database: Database,
    out: Optional[str]=None):

    logger.debug(f'cloning repo for addrs {ips}')
    clone = f'git clone {STORAGE_REPO}'
    results = await run_ssh(clone, user, *ips)

    failed = [ ip for ip, res in zip(ips, results) if res.is_error ]

    if failed:
        logger.debug(f'pulling git for addrs {failed}')
        pull = f'cd {STORAGE_FOLDER} && git pull'
        await run_ssh(pull, user, *failed)

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
        shutdown = 'redis-cli shutdown'

    elif database == 'mongodb':
        logger.debug('stopping mongo daemons')
        shutdown = 'mongod --shutdown'

    # shutdown main first
    results = await run_ssh(shutdown, user, *ips.main)
    results += await run_ssh(shutdown, user, *ips.data, *ips.misc)

    write_results(results, out)



async def main(file: Optional[str], shutdown: bool, **run_args: Any):
    if file is None:
        file = str(Path(__file__).parent / 'ip-addresses')

    ips = Addresses.from_json(file)

    if not shutdown:
        await run_starts(ips, **run_args)
    else:
        await run_shutdown(ips, **run_args)



if __name__ == "__main__":
    logging.getLogger('asyncio').setLevel(logging.WARNING)
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
