#!/usr/bin/env python3

from typing import Any, Dict, List, Union
from pathlib import Path
import json


def mod_name(source: Union[str, Path]):
    if isinstance(source, str):
        source = Path(source)

    return f'{source.stem}-mod.conf'


def redis_conf(source: Union[str, Path]):
    return Path('redis') / 'confs' / mod_name(source)


def mongo_conf(source: Union[str, Path]):
    return Path('mongodb') / 'confs' / mod_name(source)



def modify_redis(source: Union[str, Path], param: str, value: Any):
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

    mod_config_path = redis_conf(source)
    with open(mod_config_path, 'w') as f:
        f.write(''.join(redis_params))
    
    return mod_config_path


def modify_mongo(source: Union[str, Path], param: str, value: Any):
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

    mod_config_path = mongo_conf(source)
    with open(mod_config_path, 'w') as f:
        json.dump(configs, f, indent=4)
    
    return mod_config_path



if __name__ == '__main__':
    modify_mongo(
        "deployment/mongodb/configs/config.conf",
        "storage.wiredTiger.engineConfig.cacheSizeGB",
        2)

    modify_redis("deployment/redis/confs/master.conf", "maxmemory", 1)