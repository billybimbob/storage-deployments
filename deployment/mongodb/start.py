#!/usr/bin/env python3

from __future__ import annotations
from argparse import ArgumentParser
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any, Dict, List, Literal, Optional, TypedDict, Union)

from pymongo import MongoClient
from asyncio.subprocess import PIPE
import asyncio

import logging
import json


LOG_PATH = (Path(__file__).parents[2] 
    / 'monitor_and_graphs'
    / 'logs'
    / 'mongodb')

logger = logging.getLogger(__name__)


Log = str

@dataclass
class Mongos:
    port: int
    members: List[str]

@dataclass
class ReplInfo:
    set_name: str
    port: int
    members: List[str]


Mongot = Literal['mongos', 'configs', 'shards']

@dataclass
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

    @classmethod
    def from_json(cls, file: Union[str, Path]):
        with open(file) as f:
            clust_data = json.load(f)
            log = clust_data['log']

            mongos = Mongos(**clust_data['mongos'])
            configs = ReplInfo(**clust_data['configs'])
            shards = ReplInfo(**clust_data['shards'])

            return Cluster(
                log = log,
                mongos = mongos,
                configs = configs,
                shards = shards)


    def as_tuple(self):
        return self.log, self.mongos, self.configs, self.shards

    def as_dict(self):
        return Cluster.Dict(log=self.log, mongos=self.mongos, configs=self.configs, shards=self.shards)



async def create_replica(
    mem_idx: int,
    config: str,
    info: ReplInfo,
    is_shard: bool,
    log: Log):

    db_path = LOG_PATH / "db"
    log_path = LOG_PATH

    db_path.mkdir(parents=True, exist_ok=True)
    log_path.mkdir(parents=True, exist_ok=True)

    db = str(db_path)
    log = str(log_path / (
        f'shard_{mem_idx}.log' if is_shard else 
        f'config_{mem_idx}.log'))

    with open(log, "w"): pass

    binds = ['localhost', info.members[mem_idx]]
    binds = ','.join(binds)

    mongod_cmd = ['mongod']
    mongod_cmd += ['--config', config]
    mongod_cmd += ['--dbpath', db]
    mongod_cmd += ['--logpath', log]
    mongod_cmd += ['--shardsvr' if is_shard else '--configsvr']
    mongod_cmd += ['--replSet', info.set_name]
    mongod_cmd += ['--port', str(info.port)]
    mongod_cmd += ['--bind_ip', binds]

    print(f"mongod_cmd: {' '.join(mongod_cmd)}")

    await asyncio.create_subprocess_exec(*mongod_cmd, stdout=PIPE)




def initiate(info: ReplInfo, configsvr: bool):
    # just use first member host by default
    with MongoClient(info.members[0], info.port) as cli:
        config: Dict[str, Any] = {
            '_id': info.set_name,
            'members': [
                {'_id': i, 'host': f'{m}:{info.port}'}
                for i, m in enumerate(info.members) ]
        }

        if configsvr:
            config['configsvr'] = True

        cli['admin'].command("replSetInitiate", config)



async def start_mongos(mongos_idx: int, config: str, cluster: Cluster):
    _, mongos, configs, shards = cluster.as_tuple()
    # log = cluster.log
    # mongos = cluster.mongos
    # configs = cluster.configs
    # shards = cluster.shards

    config_locs = [ f"{c}:{configs.port}" for c in configs.members ]
    config_set = f"{configs.set_name}/{','.join(config_locs)}"

    binds = ['localhost', mongos.members[mongos_idx]]
    binds = ','.join(binds)

    mongos_cmd = ['mongos']
    mongos_cmd += ['--config', config]
    mongos_cmd += ['--logpath', str(LOG_PATH / f"mongo_{mongos_idx}.log")]
    mongos_cmd += ['--configdb', config_set]
    mongos_cmd += ['--port', str(mongos.port)]
    mongos_cmd += ['--bind_ip', binds]

    print(f"mongos cmd: {' '.join(mongos_cmd)}")

    await asyncio.create_subprocess_exec(*mongos_cmd, stdout=PIPE)

    # add shards might run too early, keep eye on
    await asyncio.sleep(2)

    shard_set = [ f"{s}:{shards.port}" for s in shards.members ]
    shard_set = f"{shards.set_name}/{','.join(shard_set)}"

    print('adding shards')

    with MongoClient(port=mongos.port) as cli:
        cli['admin'].command("addShard", shard_set)



# def get_cluster(cluster_path: str):

#     def convert_label(key: str, info: Any):
#         if key == 'log':
#             return Log(**info)
#         elif key == 'mongos':
#             return Mongos(**info)
#         else:
#             return ReplInfo(**info)

#     with open(cluster_path,'r') as f:
#         cluster = json.load(f) # should be a dict
#         cluster = {
#             label: convert_label(label, info)
#             for label, info in cluster.items() }

#         return Cluster(**cluster)


async def init_server(
    cluster: Cluster,
    role: Mongot,
    member: Optional[int],
    config: Optional[str]):

    # print(f"cluster info here: {cluster_info}")
    LOG_PATH.mkdir(exist_ok=True, parents=True)

    if role == 'mongos' and config and member is not None:
        await start_mongos(member, config, cluster)

    elif role == 'mongos':
        raise ValueError('mongos missing some args')

    elif member is None:
        initiate(
            cluster.as_dict()[role],
            role == 'configs')

    elif config:
        await create_replica(
            member,
            config,
            cluster.as_dict()[role],
            role == 'shards',
            cluster.log)

    else:
        raise ValueError('some expected args are missing')



def mongodb_stop_server(cluster: Cluster, role: Mongot):
    port = cluster.as_dict()[role].port
    with MongoClient(port=port) as cli:
        try:
            # shutdown will throw error
            cli['admin'].command('shutdown')

        except Exception:
            pass



async def main(
    cluster: str, shutdown: bool, role: Mongot, **init_args: Any):

    cluster_info = Cluster.from_json(cluster)
    
    logger.info(shutdown)

    if shutdown:
        mongodb_stop_server(cluster_info, role)

    else:
        await init_server(cluster_info, role, **init_args)




if __name__ == "__main__":
    args = ArgumentParser(description = 'start mongo daemon processes')

    args.add_argument('-c', '--cluster',
        required = True, 
        help = 'file path to cluster info')

    args.add_argument('-f', '--config',
        help = 'config file to start the daemon with')

    args.add_argument('-m', '--member',
        type = int,
        help = 'member index for either config or shard roles; '
               'runs initialize if not specified')

    args.add_argument('-r', '--role',
        choices = ['mongos', 'configs', 'shards'],
        required = True,
        help = 'the cluster role being modified')

    args.add_argument('-s', '--shutdown',
        action = 'store_true',
        help = 'run shutdown instead of init')

    logging.basicConfig(filename="test_mongo.log", filemode="w",level=logging.DEBUG)
    logger.setLevel(level=logging.DEBUG)

    args = args.parse_args()
    asyncio.run(main(**vars(args)))
