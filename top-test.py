
from typing import Any, Dict, List

import asyncio as aio
from asyncio.subprocess import PIPE

import shlex
import json


async def top():
    top = await aio.create_subprocess_exec(
        *shlex.split('mongotop --json 2'), stdout=PIPE)
        
    try:
        if top.stdout is None:
            raise RuntimeError('stdout is not defined to a pipe')

        print('staring write proc')
        # write = aio.create_task(write_top(top.stdout, 2))

        # do some other stuff ...
        await aio.sleep(5)
        # write.cancel()

    finally:
        top.terminate()
        ret, _ = await top.communicate()

    ret = ret.decode()
    # ret = await top.wait()
    print(f'finished with {ret=}')



async def write_top(top_stream: aio.StreamReader, interval: int):
    flush_count = 5
    count = 0
    try:
        print('writing task started')
        outs: List[Dict[str, Any]] = []
        while True:
            top_data = await top_stream.read()
            top_data = top_data.decode() # need newline splits
            print(f'got data {top_data}')

            for obj in top_data.split('\n'):
                outs += json.loads(obj)

            if count == flush_count:
                count = 0

            if count == 0:
                update_out('out.json', outs)
                outs.clear()

            count += 1

            await aio.sleep(interval)

    except aio.CancelledError:
        pass


def update_out(out_file: str, new_data: List[Dict[str, Any]]):
    print(f'flushing out {new_data=}')
    with open(out_file, 'r+') as f:
        data: List[Dict[str, Any]]= json.load(f) + new_data
        json.dump(data, f, indent=4)


if __name__ == '__main__':
    aio.run(top())