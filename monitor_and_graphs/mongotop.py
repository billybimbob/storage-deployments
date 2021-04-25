
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

import asyncio as aio
import asyncio.subprocess as proc
import json


READ_LIMIT = 20


@dataclass
class MongoTop:
    _proc: proc.Process
    _writer: aio.Task

    async def __aenter__(self):
        return None

    async def __aexit__(self, exp: None, val: None, trace: None):
        # all none, since don't handle traceback
        await self.stop()

    async def stop(self):
        self._proc.kill()
        await aio.gather(self._proc.wait(), self._writer)



async def mongo_top(
    file: Union[str, Path],
    host: Optional[str] = None,
    port: Optional[int] = None):

    cmd = ['mongotop']
    if host is not None:
        cmd += ['--host', host]
    
    if port is not None:
        cmd += ['--port', str(port)]

    cmd += ['--json', '2']

    print(f'{cmd=}')
    top = await aio.create_subprocess_exec(*cmd, stdout=proc.PIPE)
        
    if top.stdout is None:
        top.kill()
        raise RuntimeError('stdout is not defined to a pipe')

    print(f"HERE1:       {file}")
    waiter = aio.create_task(write_top(top.stdout, file))

    return MongoTop(top, waiter)



async def write_top(
    top_stream: aio.StreamReader, out_file: Union[str, Path]):

    if isinstance(out_file, str):
        out_file = Path(out_file)

    print(f"HERE:       {out_file}")
    with open(out_file, 'w') as f:
        json.dump([], f)

    count = 0
    outs: List[Dict[str, Any]] = []
    print('writing task started')

    try:
        while True:
            top_data = await top_stream.readuntil(b'\n')
            top_data = json.loads(top_data)

            # print(f'got data {top_data}')
            outs.append(top_data)
            count += 1

            if count == READ_LIMIT:
                flush(outs, out_file)
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
        flush(outs, out_file)



def flush(new_data: List[Dict[str, Any]], target: Union[str, Path]):
    with open(target, 'r+') as f:
        data: List[Dict[str, Any]] = json.load(f) + new_data
        f.seek(0)
        json.dump(data, f, indent=4)



async def test_top():
    async with await mongo_top('out.json'):
        await aio.sleep(5)


if __name__ == '__main__':
    aio.run( test_top() )

