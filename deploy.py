import json
from pathlib import Path
import asyncio as aio
import time

from database import *
from modifyconf import *
from benchmark import *

DEPLOYMENT_BASE = Path(__file__).parent / 'deployment' 

USER = "cc"
IPS = Addresses.from_json(str(DEPLOYMENT /'ip-addresses'))
PARAMETER_CHANGES = json.load(open(str(DEPLOYMENT_BASE / 'parameter_changes.json'), 'r'))

REDIS_CONFIG_BASE = DEPLOYMENT_BASE / 'redis' / 'confs'
MONGODB_CONFIG_BASE = DEPLOYMENT_BASE / 'mongodb' / 'configs'

REDIS_MASTER_PORT = 6379
MONGO_MASTER_PORT = 27017

async def deploy_redis():
    for param_name in PARAMETER_CHANGES["redis"]:
        for param_val in PARAMETER_CHANGES["redis"][param_name]:
            master_mod_config_path = modify_redis(str(REDIS_CONFIG_BASE / 'master.conf'), param_name, param_val)
            cmd = [shlex.split(f'scp {master_mod_config_path} {USER}@{ip}:~/master-mod.conf') for ip in IPS]
            await exec_commands(*cmd)

            sentinel_mod_config_path = modify_redis(str(REDIS_CONFIG_BASE / 'sentinel.conf'), param_name, param_val)
            cmd = [shlex.split(f'scp {sentinel_mod_config_path} {USER}@{ip}:~/sentinel-mod.conf') for ip in IPS]
            await exec_commands(*cmd)

            slave_mod_config_path = modify_redis(str(REDIS_CONFIG_BASE / 'slave.conf'), param_name, param_val)
            cmd = [shlex.split(f'scp {slave_mod_config_path} {USER}@{ip}:~/slave-mod.conf') for ip in IPS]
            await exec_commands(*cmd)

            await run_starts(IPS, USER, "redis")

            await remote_check("redis", REDIS_MASTER_PORT)

            user_input = input("Move on to next parameter(y/n):").lower()
            while user_input != 'y':
                time.sleep(1)

            await run_shutdown(IPS, USER, "redis")

async def deploy_mongodb():
    for param_name in PARAMETER_CHANGES["mongodb"]:
        for param_val in PARAMETER_CHANGES["mongodb"][param_name]:
            mod_config_path = modify_mongo(MONGODB_CONFIG_BASE / 'mongos.conf', param_name, param_val)
            cmd = [shlex.split(f'scp {mod_config_path} {USER}@{ip}:~/mongos-mod.conf') for ip in IPS]
            await exec_commands(*cmd)

            await run_starts(IPS, USER, "mongodb")

            await remote_check("mongodb", MONGO_MASTER_PORT)

            user_input = input("Move on to next parameter(y/n):").lower()
            while user_input != 'y':
                time.sleep(1)

            await run_shutdown(IPS, USER, "mongodb")

async def main():
    await deploy_redis()
    await deploy_mongodb()

if __name__ == "__main__":
    aio.run(main())