#!/usr/bin/env python3

from argparse import ArgumentParser
from typing import List, Literal, NamedTuple, Optional, TypedDict

from pymongo.mongo_client import MongoClient

import asyncio
import json


class Mongos(NamedTuple):
    port: int
    host: str

class ReplInfo(NamedTuple):
    set_name: str
    port: int
    members: List[str]


ClusterType = Literal['mongos', 'configs', 'shards']

class Cluster(TypedDict):
    logpath: str
    mongos: Mongos
    configs: ReplInfo
    shards: ReplInfo



async def create_replica(
    mem_idx: int, info: ReplInfo, is_shard: bool, logpath: str):

    mongod_cmd = ['mongod']
    mongod_cmd.append('--shardsvr' if is_shard else 'configsvr')
    mongod_cmd += ['--replSet', info.set_name]
    mongod_cmd += ['--port', str(info.port)]
    mongod_cmd += ['--bind_ip', info.members[mem_idx]]
    mongod_cmd += ['--logpath', logpath]

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



async def start_mongos(cluster: Cluster):
    mongos = cluster['mongos']
    configs = cluster['configs']
    shards = cluster['shards']

    config_locs = [ f"{c}:{configs.port}" for c in configs.members ]
    config_set = f"{configs.set_name}/{','.join(config_locs)}"

    mongos_cmd = ['monogos']
    mongos_cmd += ['--configdb', config_set]
    mongos_cmd += ['--port', str(mongos.port)]
    mongos_cmd += ['--bind_ip', mongos.host]
    mongos_cmd += ['--logpath', cluster['logpath']]

    await asyncio.create_subprocess_exec(*mongos_cmd)

    shard_locs = [ f"{s}:{shards.port}" for s in shards.members ]
    shard_set = {
        'addShard': f"{shards.set_name}/{','.join(shard_locs)}" }

    with MongoClient('localhost', mongos.port) as cli:
        cli['admin'].command("addShard", shard_set)



def get_cluster(cluster_path: str):
    with open(cluster_path,'r') as f:
        cluster = json.load(f) # should be a dict
        cluster = {
            role: (ReplInfo(**info)
                   if role != 'mongos' else
                   Mongos(**info))
            for role, info in cluster.items() }

        return Cluster(**cluster)



async def main(cluster: str, role: ClusterType, member: Optional[int]):
    cluster_info = get_cluster(cluster)

    if role == 'mongos' and member is not None:
        raise ValueError('mongos role received unexpected args')

    if role == 'mongos':
        await start_mongos(cluster_info)
    elif member:
        await create_replica(
            member,
            cluster_info[role],
            role == 'shards',
            cluster_info['logpath'])
    else:
        initiate(cluster_info[role], role == 'configs')



if __name__ == "__main__":
    args = ArgumentParser(description='start mongo daemon processes')

    args.add_argument('-c', '--cluster',
        required=True, 
        help="file path to cluster info")

    # args.add_argument( '-i', '--init',
    #     action='store_true',
    #     help='run initilize command for config or shard roles')

    args.add_argument('-m', '--member',
        type=int,
        help='member index for either config or shard roles;'
            ' runs initilize if not specified')

    args.add_argument('-r', '--role',
        choices=['mongos', 'configs', 'shards'],
        required=True,
        help='the cluster role being modified')

    args = args.parse_args()
    asyncio.run(main(**vars(args)))
