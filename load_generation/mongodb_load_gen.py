#!/usr/bin/env python3

from io import TextIOWrapper
from typing import List, Literal

import os
import random
import string


Operation = Literal['write', 'read', 'meta']
LOAD_SIZES = [1000, 10000, 100000]

output_path = f"{os.path.dirname(os.path.abspath(__file__))}/load-output/mongodb"

STRING_LEN = 100
letters = string.ascii_lowercase
key = "key"

def generate_random_string(length: int):
    return ''.join(random.choice(letters) for _ in range(length))

def add_write_operations(operations: List[str]):
    val = generate_random_string(STRING_LEN)
    operations.append({'insert':[key, val]})

def add_read_operations(operations: List[str]):  
    operations.append({'find':[key]})

def add_meta_operations(operations: List[str]): 
    operations.append({'createCollection':[generate_random_string(STRING_LEN)]})

def create_operations(op: Operation, load: int):
    with open(f"{output_path}/{op}_{load}_operations.txt", 'w') as f:
        operations = []
        for _ in range(load): 
            if op == "write":
                add_write_operations(operations)
                
            elif op == "read":
                add_read_operations(operations)

            elif op == "meta":
                add_meta_operations(operations)
        f.write(str(operations))

if __name__ == "__main__":
    ops: List[Operation] = ['write', 'read', 'meta']

    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    for t in ops:
        for load in LOAD_SIZES:
            create_operations(t, load)
