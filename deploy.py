
from typing import Any, Dict

import asyncio as aio
import json
import shlex

from deployment.modifyconf import modify_mongo, modify_redis
from database import (
    Addresses, DEPLOYMENT,
    exec_commands, fetch_repo, run_shutdown, run_starts)

from benchmark import Remote, remote_bench



REDIS_MASTER_PORT = 6379
MONGO_MASTER_PORT = 27017

USER = "cc"
IPS = Addresses.from_json(DEPLOYMENT /'ip-addresses')

with open(DEPLOYMENT / 'parameter_changes.json', 'r') as f:
    PARAMETERS: Dict[str, Any] = json.load(f)

REDIS_CONFS = DEPLOYMENT / 'redis' / 'confs'
MONGODB_CONFS = DEPLOYMENT / 'mongodb' / 'confs'


async def deploy_redis():
    params: Dict[str, Any] = PARAMETERS["redis"]

    for param_name, vals in params.items():
        for param_val in vals:

            mod_val = param_name, param_val

            master_conf = DEPLOYMENT / modify_redis(
                REDIS_CONFS / 'master.conf', *mod_val)

            sentinel_conf = DEPLOYMENT / modify_redis(
                REDIS_CONFS / 'sentinel.conf', *mod_val)

            slave_conf = DEPLOYMENT / modify_redis(
                REDIS_CONFS / 'slave.conf', *mod_val)

            scp_cmds = [
                shlex.split(
                    f'scp {master_conf} {USER}@{ip}:~/master.conf')
                for ip in IPS ]

            scp_cmds += [
                shlex.split(
                    f'scp {sentinel_conf} {USER}@{ip}:~/sentinel.conf')
                for ip in IPS ]

            scp_cmds += [
                shlex.split(
                    f'scp {slave_conf} {USER}@{ip}:~/slave.conf')
                for ip in IPS ]

            await exec_commands(*scp_cmds)
            await run_starts(IPS, USER, "redis")

            remote = Remote(USER, IPS.main[0])
            await remote_bench(remote, "redis", REDIS_MASTER_PORT)

            prompt = "Move on to next parameter(y/n):"
            while input(prompt).lower() != 'y':
                pass

            await run_shutdown(IPS, USER, "redis")


async def deploy_mongodb():
    params: Dict[str, Any] = PARAMETERS["mongodb"]

    for param_name, vals in params.items():
        for param_val in vals:

            mongos_conf = DEPLOYMENT / modify_mongo(
                MONGODB_CONFS / 'mongos.conf', param_name, param_val)

            scp_cmds = [
                shlex.split(
                    f'scp {mongos_conf} {USER}@{ip}:~/mongos.conf')
                for ip in IPS ]

            await exec_commands(*scp_cmds)
            await run_starts(IPS, USER, "mongodb")

            remote = Remote(USER, IPS.main[0])
            await remote_bench(remote, "mongodb", MONGO_MASTER_PORT)

            prompt = "Move on to next parameter(y/n):"
            while input(prompt).lower() != 'y':
                pass

            await run_shutdown(IPS, USER, "mongodb")


async def main():
    await fetch_repo(IPS, USER)
    await deploy_redis()
    await deploy_mongodb()


if __name__ == "__main__":
    aio.run(main())