#!/usr/bin/env python3

import subprocess
import shlex
import argparse
import os


base_directory = os.path.dirname(os.path.abspath(__file__))

def valid_ip(ip):
    return len(ip.split(".")) == 4

def get_ips(file_name):

    ips = []
    with open(file_name, 'r') as f:
        for line in f.readlines():
            if valid_ip(line):
                ips.append(line)
    return ips

def redis_scp(ips, user, slave_count, sentinal_count):
    if len(ips) >= 1 + slave_count + sentinal_count:
        for i in range(len(ips)):
            if i == 0:
                subprocess.run(["scp", f"{base_directory}/redis/install.sh", f"{base_directory}/redis/start.py", f"{base_directory}/redis/confs/master.conf", f"{user}@{ips[i]}/home"])
            elif i <= slave_count:
                subprocess.run(["scp", f"{base_directory}/redis/install.sh", f"{base_directory}/redis/start.py", f"{base_directory}/redis/confs/slave.conf", f"{user}@{ips[i]}/home"])
            elif i <= slave_count + sentinal_count:
                subprocess.run(["scp", f"{base_directory}/redis/install.sh", f"{base_directory}/redis/start.py", f"{base_directory}/redis/confs/sentinel.conf", f"{user}@{ips[i]}/home"])
        
    else:
        print("not enough instances")

def mongodb_scp(ips, user):
    for i in range(len(ips)):
        subprocess.run(["scp", f"{base_directory}/mongodb/install.sh", f"{base_directory}/mongodb/start.py", f"{base_directory}/mongodb/cluster.conf", f"{user}@{ips[i]}/home"])

def redis_start(ips, user):
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
        ran = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, universal_newlines=True)

        print(f"finished cmd {cmd} at {ips[i]} with output:\n{ran.stdout}") 

def mongo_start(ips, user):
    for i in range(len(ips)):
        if i == 0:
            role = "mongos"
        elif i == 1:
            role = "config"
        else:
            role = "shards"

        cmd = f"./start.py -c cluster.json -m {i} -r {role}"
        cmd = " ".split(f'ssh {user}@{ips[i]} "{cmd}"')
        ran = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, universal_newlines=True)

        print(f"finished cmd {cmd} at {ips[i]} with output:\n{ran.stdout}") 

def run_ips(ips, user, cmd, write_out=None):
    ssh_stdout = []
    for ip in ips:
        cmd = shlex.split(f'ssh {user}@{ip} "{cmd}"')
        ran = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, universal_newlines=True)
        if write_out:
            ssh_stdout.append(ran.stdout)
        else:
            print(f"finished cmd {cmd} at {ip} with output:\n{ran.stdout}") 

    if write_out:
        with open(write_out, "a+") as f:
            f.write('\n'.join(ssh_stdout))

if __name__ == "__main__":
    parse = argparse.ArgumentParser("Runs the command on all ssh ips in the supplied file")
    parse.add_argument("-d", "--database",  default='redis', help="select database (redis or mongodb)")
    parse.add_argument("-f", "--file", help="file that contains the ips")
    parse.add_argument("-o", "--out",  help="write output of ssh stdout to file")
    parse.add_argument("-u", "--user", default='ubuntu', help="the user for the ips, for now all the same")
    args = parse.parse_args()

    ips = get_ips(args.file)

    if ips != []:

        #note: fixed number of slaves and sentials for redis and fixed number of slaves for mongodb
        if args.database == "redis":
            redis_scp(ips, args.user, 1, 1)

        elif args.database == "mongodb":
            mongodb_scp(ips, args.user)
    
        cmd0 = "cd ~/home"
        run_ips(ips, args.user, cmd0, args.out)

        cmd1 = "./install.sh"
        run_ips(ips, args.user, cmd1, args.out)

        if args.database == "redis":
            redis_start(ips, args.user)
        
        elif args.database == "mongodb":
            mongo_start(ips, args.user)
            





