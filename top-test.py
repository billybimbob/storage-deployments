
from typing import Any, Dict, List, Union
from pathlib import Path

from asyncio.subprocess import PIPE
import asyncio as aio

import shlex
import json


async def top():
    top = await aio.create_subprocess_exec(
        *shlex.split('mongotop --json 2'), stdout=PIPE)
        
    if top.stdout is None:
        top.terminate()
        raise RuntimeError('stdout is not defined to a pipe')

    write = aio.create_task(write_top(top.stdout))

    # do some other stuff ...
    await aio.sleep(5)

    top.kill()
    ret = await aio.gather(top.wait(), write)

    print(f'finished with {ret=}')



async def write_top(top_stream: aio.StreamReader):
    READ_LIMIT = 20
    TOP_STORE = Path('out.json')

    count = 0
    outs: List[Dict[str, Any]] = []
    print('writing task started')

    try:
        with open(TOP_STORE, 'w') as f:
            json.dump([], f)

        while True:
            top_data = await top_stream.readuntil(b'\n')
            top_data = json.loads(top_data)

            # print(f'got data {top_data}')
            outs.append(top_data)
            count += 1

            if count == READ_LIMIT:
                flush(outs, TOP_STORE)
                outs.clear()
                count = 0

    except aio.IncompleteReadError:
        pass

    except aio.CancelledError:
        rest_top = await top_stream.read()
        for line in rest_top.split(b'\n'):
            if not line:
                continue

            outs.append(json.loads(line))

    finally:
        flush(outs, TOP_STORE)



def flush(new_data: List[Dict[str, Any]], target: Union[str, Path]):
    with open(target, 'r+') as f:
        data: List[Dict[str, Any]] = json.load(f) + new_data
        f.seek(0)
        json.dump(data, f, indent=4)



if __name__ == '__main__':
    aio.run(top())