#!/usr/bin/env python3

from __future__ import annotations
from argparse import ArgumentParser
from dataclasses import asdict, astuple, dataclass
from typing import (
    Any, List, Literal, NamedTuple, Optional, Tuple, TypedDict, cast)

from pymongo import MongoClient

import asyncio
import json


class Log(NamedTuple):
    path: str
    level: int

    @property
    def verbosity(self):
        return max(1, min(self.level, 5))

class Mongos(NamedTuple):
    port: int
    members: List[str]

class ReplInfo(NamedTuple):
    set_name: str
    port: int
    members: List[str]


Mongot = Literal['mongos', 'configs', 'shards']

@dataclass(frozen=True)
class Cluster:
    log: Log
    mongos: Mongos
    configs: ReplInfo
    shards: ReplInfo

    class Dict(TypedDict):
        log: Log
        mongos: Mongos
        configs: ReplInfo
        shards: ReplInfo

    def as_tuple(self):
        return cast(
            Tuple[Log, Mongos, ReplInfo, ReplInfo],
            astuple(self))

    def as_dict(self):
        return cast(Cluster.Dict, asdict(self))



async def create_replica(
    mem_idx: int, info: ReplInfo, is_shard: bool, log: Log):

    verbosity = '-' + ''.join('v' for _ in range(log.verbosity))

    mongod_cmd = ['mongod', verbosity]
    mongod_cmd += ['--logpath', log.path]
    mongod_cmd += ['--shardsvr' if is_shard else '--configsvr']
    mongod_cmd += ['--replSet', info.set_name]
    mongod_cmd += ['--port', str(info.port)]
    mongod_cmd += ['--bind_ip', info.members[mem_idx]]

    await asyncio.create_subprocess_exec(*mongod_cmd)



def initiate(info: ReplInfo, configsvr: bool):
    # just use first member host by defaut
    with MongoClient(info.members[0], info.port) as cli:
        config = {
            '_id': info.set_name,
            'configsvr': configsvr,
            'members': [
                {'_id': i, 'host': f'{m}:{info.port}'}
                for i, m in enumerate(info.members) ]
        }

        cli['admin'].command("replSetInitiate", config)



async def start_mongos(mongos_idx: int, cluster: Cluster):
    log, mongos, configs, shards = cluster.as_tuple()

    verbosity = '-' + ''.join('v' for _ in range(log.verbosity))
    config_locs = [ f"{c}:{configs.port}" for c in configs.members ]
    config_set = f"{configs.set_name}/{','.join(config_locs)}"

    mongos_cmd = ['monogos', verbosity]
    mongos_cmd += ['--enableFreeMonitoring', 'on'] # should work for mongos
    mongos_cmd += ['--logpath', log.path]
    mongos_cmd += ['--configdb', config_set]
    mongos_cmd += ['--port', str(mongos.port)]
    mongos_cmd += ['--bind_ip', mongos.members[mongos_idx]]

    await asyncio.create_subprocess_exec(*mongos_cmd)

    # add shards might run too early, keep eye on
    await asyncio.sleep(2)

    shard_locs = [ f"{s}:{shards.port}" for s in shards.members ]
    shard_set = {
        'addShard': f"{shards.set_name}/{','.join(shard_locs)}" }

    with MongoClient('localhost', mongos.port) as cli:
        cli['admin'].command("addShard", shard_set)



def get_cluster(cluster_path: str):

    def convert_label(key: str, info: Any):
        if key == 'log':
            return Log(**info)
        elif key == 'monogos':
            return Mongos(**info)
        else:
            return ReplInfo(**info)

    with open(cluster_path,'r') as f:
        cluster = json.load(f) # should be a dict
        cluster = {
            label: convert_label(label, info)
            for label, info in cluster.items() }

        return Cluster(**cluster)



async def main(cluster: str, role: Mongot, member: Optional[int]):
    cluster_info = get_cluster(cluster)

    if role == 'mongos' and member is not None:
        await start_mongos(member, cluster_info)

    elif role == 'mongos':
        raise ValueError('mongos needs the member specified')

    elif member:
        await create_replica(
            member,
            cluster_info.as_dict()[role],
            role == 'shards',
            cluster_info.log)

    else:
        initiate(
            cluster_info.as_dict()[role],
            role == 'configs')



if __name__ == "__main__":
    args = ArgumentParser(description = 'start mongo daemon processes')

    args.add_argument('-c', '--cluster',
        required = True, 
        help = 'file path to cluster info')

    args.add_argument('-m', '--member',
        type = int,
        help = 'member index for either config or shard roles; '
               'runs initialize if not specified')

    args.add_argument('-r', '--role',
        choices = ['mongos', 'configs', 'shards'],
        required = True,
        help = 'the cluster role being modified')

    args = args.parse_args()
    asyncio.run(main(**vars(args)))
