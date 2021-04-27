#!/usr/bin/env python3

from typing import Dict, List, TypedDict, Union
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path

import json
import matplotlib as plt


class Work(TypedDict):
    time: int
    "amount of time spent, in ms"
    count: int
    "number of operations"


class Poll(TypedDict):
    total: Work
    read: Work
    write: Work


class Timestamp(TypedDict):
    "rest of the date should be Poll"
    totals: Dict[str, Poll]
    time: str


class Runtime(TypedDict):
    start: str
    end: str
    rops: float


INTERVAL = 2

DB = "test-db.test-col1"
DB_CACHE = "config.cache.chunks.test-db.test-col1"

FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def parse_times(mongo_top: List[Timestamp]) -> Dict[datetime, Poll]:
    stamps: Dict[datetime, Poll] = {}

    for stamp in mongo_top:
        # just ignore the last digits and timezon
        time = datetime.strptime(stamp["time"][:-4], FORMAT)
        poll = Poll(
            total = Work(time=0, count=0),
            read = Work(time=0, count=0),
            write = Work(time=0, count=0))

        def add_poll(info: Poll):
            for poll_val in poll:
                for work_val in poll[poll_val]:
                    info_val = info[poll_val][work_val]
                    poll[poll_val][work_val] += info_val

        totals = stamp['totals']
        if DB_CACHE in totals:
            add_poll(totals[DB_CACHE])

        if DB in totals:
            add_poll(totals[DB])

        stamps[time] = poll

    return stamps



def parse_files(file: Union[str, Path]) -> Dict[str, Runtime]:
    if isinstance(file, str):
        file = Path(file)

    if file.is_dir():
        data: Dict[str, Runtime] = {}
        for f in file.iterdir():
            data = {**data, **parse_files(f)}

        return data

    if not file.is_file() or file.suffix != '.json':
        return {}

    with open(file) as f:
        json_data = json.load(f)

    times = parse_times(json_data)

    if not times:
        return {}

    avgs = [poll['total'] for poll in times.values()]
    avgs = [
        avg['count'] / avg['time'] * 1000 # might seem a bit high
        for avg in avgs
        if avg['time'] > 0]

    rops = sum(avgs) / len(avgs) if avgs else 0

    start = min(times.keys())
    end = max(times.keys())

    run = Runtime(
        start = str(start),
        end = str(end),
        rops = rops)
    
    run_name = f'{file.parent.name}: {file.stem}'
    return {run_name: run}

    # start = f'{start.minute} minutes {start.second:02d} seconds'
    # end = f'{end.minute} minutes {end.second:02d} seconds'

    # print(f'{file.name}: {start=} {end=} {a.seconds} secs')



def write_parse(times: Dict[str, Runtime], outfile: Union[str, Path]):
    with open(outfile, 'w') as f:
        json.dump(times, f, indent=4)


if __name__ == '__main__':
    args = ArgumentParser(description='parse mongotop files')

    args.add_argument('-d', '--directory',
        required = True,
        help = 'directory of mongotop json files')
    args.add_argument('-o', '--outfile',
        default = 'out.json',
        help = 'file to write parsed info to')

    args = args.parse_args()
    data = parse_files(args.directory)
    write_parse(data, args.outfile)