#!/usr/bin/env python3

from io import TextIOWrapper
import os
import random
import string

output_path = f"{os.path.dirname(os.path.abspath(__file__))}/load-output/redis"

STRING_LEN = 100
letters = string.ascii_lowercase
keys = []

def generate_random_string(length:int):
    return ''.join(random.choice(letters) for _ in range(length))

def add_write_operations(file:TextIOWrapper):
    key = generate_random_string(STRING_LEN)
    keys.append(key)
    file.write(f'SET {key} "{generate_random_string(STRING_LEN)}" /n')

def add_read_operations(file:TextIOWrapper): 
    if len(keys) != 0:
        file.write(f"GET {random.choice(keys)}/n")
    else: 
        raise RuntimeError("ERROR: No keys to GET")

def add_meta_operations(file:TextIOWrapper):
    file.write(f'SET myhash {hash} "{generate_random_string(STRING_LEN)}"')

if __name__ == "__main__":
    load_types = ["write", "read", "metadata"]
    load_sizes = [1000, 10000, 100000]

    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    for t in load_types:
        for load in load_sizes:
            file = open(f"{output_path}/{t}_{load}_opearations.csv", 'w')
            for i in range(load):
                if t == "write":
                    add_write_operations(file)
                    
                if t == "read":
                    add_read_operations(file)

                if t == "meta":
                    add_meta_operations(file)

            file.flush()
            file.close()

