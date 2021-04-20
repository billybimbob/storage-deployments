#!/usr/bin/env python3

from typing import Any, Dict, List
from pathlib import Path

import json


def new_config(source: str):
    return f'{Path(source).stem}-mod.conf'


def modify_redis(source: str, param: str, value: Any):
    redis_params: List[str] = []
    set_param = False

    with open(source) as f:
        for line in f:
            if not line:
                redis_params.append(line)
                continue

            line_param = line.split()
            if not line_param:
                redis_params.append(line)
                continue

            line_param = line_param[0]
            if line_param == param:
                redis_params.append(f'{param} {value}')
                set_param = True
            else:
                redis_params.append(line)

    if not set_param:
        redis_params.append(f'{param} {value}')

    with open(new_config(source), 'w') as f:
        f.write(''.join(redis_params))



def modify_mongo(source: str, param: str, value: Any):
    with open(source) as f:
        configs: Dict[str, Any] = json.load(f)

    param_chain = param.split('.')
    param_key = param_chain.pop()
    param_ref = configs

    for attr in param_chain:
        if attr not in param_ref:
            param_ref[attr] = {}

        param_ref: Dict[str, Any] = param_ref[attr]

    param_ref[param_key] = value

    with open(new_config(source), 'w') as f:
        json.dump(configs, f, indent=4)


if __name__ == '__main__':
    modify_mongo(
        "deployment/mongodb/configs/config.conf",
        "storage.wiredTiger.engineConfig.cacheSizeGB",
        2)

    modify_redis("deployment/redis/confs/master.conf", "maxmemory", 1)