#!/usr/bin/env python3

from typing import Any, Dict, List, Literal, Set

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

FIXED_NUM_COLLECTION = 50


def generate_random_string(length: int):
    return ''.join(random.choice(LETTERS) for _ in range(length))


def add_write_operations(operations: List[Command]):
    val = generate_random_string(STRING_LEN)
    operations.append({
        "insert": "", # collection name specified later
        "documents": [{ KEY: val }]
    })


def add_read_operations(operations: List[Command]):  
    limit = random.choice(range(1, 20))
    operations.append({
        # "aggregate": "", # collection name specified later
        # "pipeline": [{ "$sample": {"size": 1} }],
        # "cursor": {}
        "find": "",
        "limit": limit
    })


def add_meta_operations(create_drop: str ,collection_name: str, operations: List[Command]): 
    if create_drop == 'c':
        operations.append({
            "create": collection_name
        })
    elif create_drop == 'd':
        operations.append({
            "drop": collection_name
        })



def operation_json(op: Operation, size: int):
    return f'{LOADS}/{op}_{size}_operations.json'


def create_operations(op: Operation, load: int):
    free_collection_names = set(generate_random_string(STRING_LEN) for _ in range(FIXED_NUM_COLLECTION))
    used_collection_names : Set[str] = set()

    operations: List[Command] = []
    for _ in range(load): 
        if op == "write":
            add_write_operations(operations)
            
        elif op == "read":
            add_read_operations(operations)

        elif op == "meta":
            
            if not free_collection_names:
                collection_name = used_collection_names.pop()
                free_collection_names.add(collection_name)
                add_meta_operations('d', collection_name, operations)

            elif not used_collection_names:
                collection_name = free_collection_names.pop()
                used_collection_names.add(collection_name)
                add_meta_operations('c', collection_name, operations)
            
            else:
                if random.random() < 0.5:
                    collection_name = used_collection_names.pop()
                    free_collection_names.add(collection_name)
                    add_meta_operations('d', collection_name, operations)
                else:
                    collection_name = free_collection_names.pop()
                    used_collection_names.add(collection_name)
                    add_meta_operations('c', collection_name, operations)
                

    with open(operation_json(op, load), 'w') as f:
        json.dump(operations, f, indent=4)



def generate(overwrite: bool=True):
    if os.path.exists(LOADS) and not overwrite:
        return

    ops: List[Operation] = ['write', 'read', 'meta']

    if not os.path.isdir(LOADS):
        os.makedirs(LOADS)

    for t in ops:
        for load in LOAD_SIZES:
            create_operations(t, load)

    
if __name__ == '__main__':
    generate()
