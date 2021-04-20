#!/usr/bin/env python3

from typing import Any, Dict, List
from collections import defaultdict
from pathlib import Path

import json


def new_config(source: str):
    return f'{Path(source).stem}-mod.conf'


def modify_redis(source: str, param: str, value: str):
    redis_params: List[str] = []

    with open(source) as f:
        for line in f:
            line_param = line.split()[0]

            if line_param == param:
                redis_params.append(f'{param} {value}')
            else:
                redis_params.append(line)

    with open(new_config(source), 'w') as f:
        f.write('\n'.join(redis_params))



def modify_mongo(source: str, param: str, value: Any):
    with open(source) as f:
        configs: Dict[str, Any] = json.load(f)
        configs = defaultdict(dict, configs)

    param_chain = param.split('.')
    param_key = param_chain.pop()
    param_ref = configs

    for attr in param_chain:
        param_ref: Dict[str, Any] = param_ref[attr]

    param_ref[param_key] = value

    with open(new_config(source), 'w') as f:
        json.dump(configs, f, indent=4)


if __name__ == '__main__':
    pass