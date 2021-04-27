#!/usr/bin/env python3

from __future__ import annotations
from collections import defaultdict
from typing import (
    Dict, List, Literal, NamedTuple, Optional, Tuple, TypedDict, Union, cast)

from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path

import json
import re
import matplotlib.pyplot as plt


class Work(TypedDict):
    time: int
    "amount of time spent, in ms"
    count: int
    "number of operations"

WorkKey = Literal['time', 'count']


class Poll(TypedDict):
    total: Work
    read: Work
    write: Work

PollKey = Literal['total', 'read', 'write']


class Timestamp(TypedDict):
    "rest of the date should be Poll"
    totals: Dict[str, Poll]
    time: str


class Runtime(TypedDict):
    start: str
    end: str
    ops: float


class RunParams(NamedTuple):
    name: str
    value: str
    op: str
    size: int

    @staticmethod
    def file_key(file: Union[str, Path]):
        if isinstance(file, str):
            file = Path(file)

        return f'{file.parent.name}: {file.stem}'


    @classmethod
    def from_key(cls, key: str) -> Optional[RunParams]:
        match = re.match(r'mongo-([^:]+): top-([\D]+)(\d+)', key)
        if not match:
            return None

        split = re.match(r'[^-]+', match[1][::-1])
        if not split:
            return None
        
        split = split.end()
        return cls(
            name = match[1][:-split-1],
            value = match[1][-split:],
            op = match[2],
            size = int(match[3]) )


GRAPHS = Path('mongo-graphs')

DB = "test-db.test-col1"
DB_CACHE = "config.cache.chunks.test-db.test-col1"

FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def get_polls(mongo_top: List[Timestamp]) -> Dict[datetime, Poll]:
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
                poll_val = cast(PollKey, poll_val)

                for work_val in poll[poll_val]:
                    work_val = cast(WorkKey, work_val)

                    info_val = info[poll_val][work_val]
                    poll[poll_val][work_val] += info_val

        totals = stamp['totals']
        if DB_CACHE in totals:
            add_poll(totals[DB_CACHE])

        if DB in totals:
            add_poll(totals[DB])

        stamps[time] = poll

    return stamps



def get_runtimes(file: Union[str, Path]) -> Optional[Dict[str, Runtime]]:
    if isinstance(file, str):
        file = Path(file)

    if file.is_dir():
        data: Dict[str, Runtime] = {}
        for f in file.iterdir():
            new_data = get_runtimes(f)

            if new_data:
                data.update(new_data)

        return data

    if not file.is_file() or file.suffix != '.json':
        return None

    with open(file) as f:
        json_data = json.load(f)

    polls = get_polls(json_data)

    if not polls:
        return None

    totals = [ poll['total'] for poll in polls.values() ]
    avgs = [
        total['count'] / total['time'] * 1000
        for total in totals
        if total['time'] > 0 ]

    start = min(polls.keys())
    end = max(polls.keys())
    ops = sum(avgs) / len(avgs) if avgs else 0

    run = Runtime(
        start = str(start),
        end = str(end),
        ops = ops)

    return { RunParams.file_key(file): run }



def write_runtimes(times: Dict[str, Runtime], outfile: Union[str, Path]):
    with open(outfile, 'w') as f:
        json.dump(times, f, indent=4)



def graph_runtimes(runtimes: Union[str, Path, Dict[str, Runtime]]):
    if not isinstance(runtimes, dict):
        with open(runtimes) as f:
            runtimes = cast(Dict[str, Runtime], json.load(f))

    run_params = split_by_names(runtimes)

    for name, vals in run_params.items():
        graph_by_size(name, vals)
        graph_by_value(name, vals)



def split_by_names(runtimes: Dict[str, Runtime]):
    run_params: Dict[str, List[Tuple[RunParams, float]]]
    run_params = defaultdict(list)

    for run_key, run_val in runtimes.items():
        param = RunParams.from_key(run_key)
        if param is None:
            raise RuntimeError('could not parse key')

        run_params[param.name].append((param, run_val['ops']))

    return run_params


def split_by_param_value(values: List[Tuple[RunParams, float]]):
    value_split: Dict[str, List[Tuple[str, int, float]]]
    value_split = defaultdict(list)

    for param, ops in values:
        value_split[param.value].append((param.op, param.size, ops))

    return value_split



def graph_by_size(name: str, values: List[Tuple[RunParams, float]]):
    graph_params = split_by_param_value(values)

    plt.figure(figsize=(8,6), dpi=80, facecolor='w', edgecolor='k')

    for param, results in graph_params.items():
        read_vals = [r for r in results if r[0] == 'read']
        read_vals = sorted(read_vals, key=lambda t: t[1])

        x_vals = list(range(len(read_vals)))

        plt.plot(
            x_vals,
            [v[2] for v in read_vals],
            label = 'reads')

        write_vals = [w for w in results if w[0] == 'write']
        write_vals = sorted(write_vals)

        plt.plot(
            x_vals,
            [v[2] for v in write_vals],
            label = 'writes')

        plt.legend()
        plt.xticks(x_vals, [v[1] for v in read_vals])

        plt.xlabel('Number of Operations')
        plt.ylabel('Requests Per Second')
        plt.title(f'{name}: {param}')

        plt.savefig(f'{GRAPHS}/{name}-{param}')
        plt.clf()



def graph_by_value(name: str, values: List[Tuple[RunParams, float]]):
    value_names = split_by_param_value(values)
    graph_params = cast(Dict[str, Tuple[float, float]], {})

    for param, results in value_names.items():
        read_ops = [r[2] for r in results if r[0] == 'read' and r[1] > 0]
        if read_ops:
            read_ops = sum(read_ops) / len(read_ops)
        else:
            read_ops = 0

        write_ops = [w[2] for w in results if w[0] == 'write' and w[1] > 0]
        if write_ops:
            write_ops = sum(write_ops) / len(write_ops)
        else:
            write_ops = 0

        # read then write ops
        graph_params[param] = read_ops, write_ops

    def str_cmp(string: str):
        if string.isdigit():
            return int(string)
        else:
            return string

    graph_params = sorted(graph_params.items(), key=lambda t: str_cmp(t[0]))
    # width = 1.4 / (len(graph_params) * 1.5)
    width = 0.3
    slots = range(len(graph_params))

    plt.figure(figsize=(8,6), dpi=80, facecolor='w', edgecolor='k')

    plt.bar(
        [s - width/2 for s in slots],
        [r for _, (r, _) in graph_params],
        width,
        label = 'reads')

    plt.bar(
        [s + width/2 for s in slots],
        [w for _, (_, w) in graph_params],
        width,
        label = 'writes')

    plt.xticks(list(slots), [v for v, _ in graph_params])
    plt.legend()

    plt.xlabel(f'{name} Values')
    plt.ylabel('Requests Per Second')
    plt.title(f'{name} Parameter Values')

    plt.savefig(f'{GRAPHS}/{name}-parameters')



if __name__ == '__main__':
    args = ArgumentParser(description='parse mongotop files')

    args.add_argument('-d', '--directory',
        help = 'directory of mongotop json files')

    args.add_argument('-r', '--runfile',
        default = 'run.json',
        help = 'file to write runtimes to')

    args = args.parse_args()


    GRAPHS.mkdir(exist_ok=True, parents=True)

    # runtimes = get_runtimes(args.directory)
    # if runtimes:
    #     write_runtimes(runtimes, args.runfile)

    # if runtimes:
    graph_runtimes(args.runfile)