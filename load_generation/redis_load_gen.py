#!/usr/bin/env python3

from io import TextIOWrapper
import os
import random
import string
from typing import List, Literal


Operation = Literal['write', 'read', 'meta']

output_path = f"{os.path.dirname(os.path.abspath(__file__))}/load-output/redis"

STRING_LEN = 100
letters = string.ascii_lowercase
keys: List[str] = []


def generate_random_string(length: int):
    return ''.join(random.choice(letters) for _ in range(length))


def add_write_operations(file: TextIOWrapper):
    key = generate_random_string(STRING_LEN)
    keys.append(key)
    file.write(f'SET {key} "{generate_random_string(STRING_LEN)}"\n')


def add_read_operations(file: TextIOWrapper): 
    if not keys:
        raise RuntimeError("ERROR: No keys to GET")

    file.write(f"GET {random.choice(keys)}\n")


def add_meta_operations(file:TextIOWrapper):
    file.write(f'HSET myhash {hash} "{generate_random_string(STRING_LEN)}"\n')


def create_operations(op: Operation, load: int):
    with open(f"{output_path}/{op}_{load}_operations.txt", 'w') as f:
        for _ in range(load):
            if op == "write":
                add_write_operations(f)
                
            elif op == "read":
                add_read_operations(f)

            elif op == "meta":
                add_meta_operations(f)



if __name__ == "__main__":
    ops: List[Operation] = ['write', 'read', 'meta']
    load_sizes = [1000, 10000, 100000]

    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    for t in ops:
        for load in load_sizes:
            create_operations(t, load)
