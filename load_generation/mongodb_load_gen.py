#!/usr/bin/env python3

from typing import Any, Dict, List, Literal

import json
import os
import random
import string


Operation = Literal['write', 'read', 'meta']
Command = Dict[str, Any]


LOAD_SIZES = [1_000, 10_000, 100_000]
LOADS = f"{os.path.dirname(os.path.abspath(__file__))}/load-output/mongodb"

STRING_LEN = 50
LETTERS = string.ascii_lowercase
KEY = "key"


def generate_random_string(length: int):
    return ''.join(random.choice(LETTERS) for _ in range(length))


def add_write_operations(operations: List[Command]):
    val = generate_random_string(STRING_LEN)
    operations.append({
        "insert": "",
        "documents": [{ KEY: val }]
    })


def add_read_operations(operations: List[Command]):  
    operations.append({
        "aggregate": 1, # collection agnostic
        "pipeline": [{ "$sample": {"size": 1} }] 
    })


def add_meta_operations(operations: List[Command]): 
    operations.append({
        "create": generate_random_string(STRING_LEN)
    })



def operation_json(op: Operation, size: int):
    return f'{LOADS}/{op}_{size}_operations.json'


def create_operations(op: Operation, load: int):
    operations: List[Command] = []
    for _ in range(load): 
        if op == "write":
            add_write_operations(operations)
            
        elif op == "read":
            add_read_operations(operations)

        elif op == "meta":
            add_meta_operations(operations)

    with open(operation_json(op, load), 'w') as f:
        json.dump(operations, f, indent=4)



if __name__ == "__main__":
    ops: List[Operation] = ['write', 'read', 'meta']

    if not os.path.isdir(LOADS):
        os.makedirs(LOADS)

    for t in ops:
        for load in LOAD_SIZES:
            create_operations(t, load)
