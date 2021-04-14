#!/usr/bin/env python3

from typing import List, Optional
from pathlib import Path

import asyncio as aio
import asyncio.subprocess as proc

import argparse
import os
import shlex
import subprocess

BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
MONGODB = BASE_DIR / 'mongodb'
REDIS = BASE_DIR / 'redis'


def valid_ip(ip: str):
    return len(ip.split(".")) == 4


def get_ips(file_name: str):
    with open(file_name, 'r') as f:
        ips: List[str] = []
        for line in f.readlines():
            if valid_ip(line):
                ips.append(line)

        return ips



async def redis_scp(ips: List[str], user: str, slave_count: int, sentinal_count: int):

    if len(ips) < 1 + slave_count + sentinal_count:
        print("not enough instances")
        return

    async def run_scp(idx: int):
        scp = ["scp", f"{REDIS}/install.sh", f"{REDIS}/start.py"]

        if idx == 0:
            scp.append(f"{REDIS}/confs/master.conf")

        elif idx <= slave_count:
            scp.append(f"{REDIS}/confs/slave.conf")

        elif idx <= slave_count + sentinal_count:
            scp.append(f"{REDIS}/confs/sentinel.conf")

        scp.append(f"{user}@{ips[idx]}/home")
        await aio.create_subprocess_exec(*scp, STDOUT=proc.PIPE)

    await aio.gather(*[run_scp(i) for i in range(len(ips))])    



async def mongodb_scp(ips: List[str], user: str):
    async def run_scp(ip: str):
        scp = [
            "scp",
            f"{MONGODB}/install.sh",
            f"{MONGODB}/start.py",
            f"{MONGODB}/cluster.conf",
            f"{user}@{ip}/home" ]

        await aio.create_subprocess_exec(*scp, STDOUT=proc.PIPE)

    await aio.gather(*[run_scp(ip) for ip in ips])    

async def redis_start(ips: List[str], user: str):
    master_node_port = 6379
    for i in range(len(ips)):
        if i == 0:
            conf = "master.sentinel"
            other = ""
        elif i == 1:
            conf = "sentinel.conf"
            other = f"-s -m {ips[0]} -p {master_node_port}"
        else:
            conf = "slave.conf"
            other = f"-m {ips[0]} -p {master_node_port}"

        cmd = f"./start.py -r /usr/local/bin/redis-server -c {conf} {other}"
        cmd = " ".split(f'ssh {user}@{ips[i]} "{cmd}"')
        
        await aio.create_subprocess_exec(*cmd, STDOUT=proc.PIPE)

async def mongo_start(ips: List[str], user: str):
    for i in range(len(ips)):
        if i == 0:
            role = "mongos"
        elif i == 1:
            role = "config"
        else:
            role = "shards"

        cmd = f"./start.py -c cluster.json -m {i} -r {role}"
        cmd = " ".split(f'ssh {user}@{ips[i]} "{cmd}"')
        
        await aio.create_subprocess_exec(*cmd, STDOUT=proc.PIPE)

async def run_ips(
    ips: List[str], user: str, ssh_cmd: str, write_out: Optional[str]):

    async def ssh_run(ip: str):
        cmd = shlex.split(f'ssh {user}@{ip} "{ssh_cmd}"')
        ssh_proc = await aio.create_subprocess_exec(
            *cmd, stdout=proc.PIPE, stderr=proc.STDOUT)

        ran, _ = await ssh_proc.communicate()
        return ran.decode()

    results = await aio.gather(*[ssh_run(ip) for ip in ips])

    if write_out:
        with open(write_out, "a+") as f:
            f.write('\n'.join(results))
            return

    for ip, res in zip(ips, results):
        print(f"finished cmd {ssh_cmd} at {ip} with output:\n{res}") 

async def main(file: str, user: str, database: str, out: str):
    ips = get_ips(file)

    if ips != []:

        #note: fixed number of slaves and sentials for redis and fixed number of slaves for mongodb
        if args.database == "redis":
            await redis_scp(ips, args.user, 1, 1)

        elif args.database == "mongodb":
            await mongodb_scp(ips, args.user)
    
        cmd0 = "cd ~/home"
        await run_ips(ips, user, cmd0, out)

        cmd1 = "./install.sh"
        await run_ips(ips, user, cmd1, out)

        if args.database == "redis":
            await redis_start(ips, args.user)
        
        elif args.database == "mongodb":
            await mongo_start(ips, args.user)
            
if __name__ == "__main__":
    parse = argparse.ArgumentParser(
        "Runs the command on all ssh ips in the supplied file")

    parse.add_argument("-d", "--database",
        default='redis',
        help="select database (redis or mongodb)")

    parse.add_argument("-f", "--file",
        help="file that contains the ips")

    parse.add_argument("-o", "--out", 
        help="write output of ssh stdout to file")

    parse.add_argument("-u", "--user",
        default='ubuntu',
        help="the user for the ips, for now all the same")

    args = parse.parse_args()
    aio.run(main(**vars(args)))
