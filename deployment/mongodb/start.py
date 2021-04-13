#!/usr/bin/env python3

from argparse import ArgumentParser
from typing import List, NamedTuple, Optional, TypedDict
from pymongo import MongoClient

import asyncio
import json


class Monogos(NamedTuple):
    port: int
    host: str

class ReplInfo(NamedTuple):
    set_name: str
    port: int
    members: List[str]

class Cluster(TypedDict):
    mongos: Monogos
    configs: ReplInfo
    shards: ReplInfo



async def create_replica(mem_idx: int, info: ReplInfo, is_shard: bool):
    mongod_cmd = ['mongod']
    mongod_cmd.append('--shardsvr' if is_shard else 'configsvr')
    mongod_cmd.extend(['--replSet', info.set_name])
    mongod_cmd.extend(['--port', str(info.port)])
    mongod_cmd.extend(['--bind_ip', info.members[mem_idx]])

    await asyncio.create_subprocess_exec(*mongod_cmd)


def initiate(info: ReplInfo, configsvr: bool):
    repl_set = MongoClient('localhost', info.port)
    config = {
        '_id': info.set_name,
        'configsvr': configsvr,
        'members': [
            {'_id': i, 'host': f'{m}:{info.port}'}
            for i, m in enumerate(info.members)
        ]
    }

    repl_set['admin'].command("replSetInitiate", config)
    repl_set.close()


async def start_mongos(cluster: Cluster):
    configs = [
        f"{m}:{cluster['configs'].port}"
        for m in cluster['configs'].members ]

    configs = f"{cluster['configs'].set_name}/{','.join(configs)}"

    mongos_cmd = ['monogos']
    mongos_cmd.extend(['--configdb', configs])
    mongos_cmd.extend(['--port', str(cluster['mongos'].port)])
    mongos_cmd.extend(['--bind_ip', cluster['mongos'].host])

    await asyncio.create_subprocess_exec(*mongos_cmd)

    shards = [
        f"{s}:{cluster['shards'].port}"
        for s in cluster['shards'].members ]

    shards = {
        'addShard': f"{cluster['shards'].set_name}/{','.join(shards)}"
    }

    mongos = MongoClient('localhost', cluster['mongos'].port)

    mongos['admin'].command("addShard", shards)
    mongos.close()


def get_cluster(cluster_path: str):
    with open(cluster_path,'r') as f:
        cluster = json.load(f) # should be a dict
        cluster = {
            role: ReplInfo(**info)
            for role, info in cluster.items() }

        return Cluster(**cluster)


async def main(cluster: str, role: str, member: Optional[int]):
    cluster_info = get_cluster(cluster)

    if role == 'mongos' and member is not None:
        raise ValueError('mongos role received unexpected args')

    if role == 'mongos':
        await start_mongos(cluster_info)

    elif member:
        await create_replica(member, cluster_info[role], role == 'shards')

    else:
        initiate(cluster_info[role], role == 'configs')



if __name__ == "__main__":
    args = ArgumentParser(description='start mongo daemon processes')

    args.add_argument( '-c', '--cluster',
        required=True, 
        help="file path to cluster info")

    # args.add_argument( '-i', '--init',
    #     action='store_true',
    #     help='run initilize command for config or shard roles')

    args.add_argument( '-m', '--member',
        type=int,
        help='member index for either config or shard roles;'
            ' runs initilize if not specified')

    args.add_argument( '-r', '--role',
        choices=['mongos', 'configs', 'shards'],
        required=True,
        help='the cluster role being modified')

    args = args.parse_args()
    asyncio.run(main(**vars(args)))
