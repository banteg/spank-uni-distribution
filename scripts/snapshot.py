import json
import os
import pickle
from collections import defaultdict
from functools import wraps
from pathlib import Path

import toml
from brownie import interface, web3, chain
from eth_utils import event_abi_to_log_topic
from hexbytes import HexBytes
from toolz import groupby
from tqdm import trange, tqdm

spankbank_deploy = 6276045  # https://etherscan.io/tx/0xc6123eea98af9db149313005d9799eefd323baf1566adfaa53d25cc376229543
uniswap_v1_deploy = 6627917  # https://etherscan.io/tx/0xc1b2646d0ad4a3a151ebdaaa7ef72e3ab1aa13aa49d0b7a3ca020f5ee7b1b010
uni_deploy = 10861674  # https://etherscan.io/tx/0x4b37d2f343608457ca3322accdab2811c707acf3eb07a40dd8d9567093ea5b82
spankbank = interface.SpankBank("0x1ECB60873E495dDFa2a13A8F4140e490dd574E6F")


def main():
    logs = fetch_logs()
    events = decode_logs(logs)
    calc_points(events)
    calc_spank(events)


def cached(path):
    path = Path(path)
    codecs = {
        ".toml": {
            "read": lambda: toml.load(path.open()),
            "write": lambda result: toml.dump(result, path.open("wt")),
        },
        ".json": {
            "read": lambda: json.load(path.open()),
            "write": lambda result: json.dump(result, path.open("wt"), indent=2),
        },
        ".pickle": {
            "read": lambda: pickle.load(path.open("rb")),
            "write": lambda result: pickle.dump(result, path.open("wb")),
        },
    }
    codec = codecs[path.suffix]

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if path.exists():
                print("load from cache", path)
                return codec["read"]()
            else:
                result = func(*args, **kwargs)
                if result is None:
                    return
                os.makedirs(path.parent, exist_ok=True)
                codec["write"](result)
                print("write to cache", path)
                return result

        return wrapper

    return decorator


@cached("snapshot/01-logs.pickle")
def fetch_logs():
    logs = []
    step = 100000
    for start in trange(uniswap_v1_deploy, uni_deploy, step):
        end = min(start + step - 1, uni_deploy)
        logs.extend(
            web3.eth.getLogs(
                {"address": str(spankbank), "fromBlock": start, "toBlock": end}
            )
        )
    return logs


@cached("snapshot/02-events.pickle")
def decode_logs(logs):
    spankbank = web3.eth.contract(None, abi=interface.SpankBank.abi)
    topics = {
        HexBytes(event_abi_to_log_topic(abi)): abi["name"]
        for abi in interface.SpankBank.abi
        if abi["type"] == "event"
    }
    events = []
    for log in logs:
        name = topics[log["topics"][0]]
        events.append(spankbank.events[name]().processLog(log))

    return events


@cached("snapshot/03-spankpoints.json")
def calc_points(events):
    periods = defaultdict(dict)
    events = groupby("event", events)
    for event in events["CheckInEvent"]:
        periods[event.args.period][event.args.staker] = event.args.spankPoints
    return dict(periods)


@cached("snapshot/04-spank.json")
def calc_spank(events):
    periods = {}
    events = groupby("event", events)
    stakers = {event.args.staker for event in events["StakeEvent"]} | {
        event.args.newAddress for event in events["SplitStakeEvent"]
    }
    print(len(stakers), "stakers")
    end_time = chain[uni_deploy].timestamp
    for period in range(spankbank.currentPeriod() + 1):
        data = spankbank.periods(period)
        period_end = data[5]
        if period_end > end_time:
            break
        end_block = timestamp_to_block_number(period_end)
        periods[period] = {
            "snapshot_block": end_block,
            "period_end": period_end,
            "stakers": {},
        }
        print(f"period {period} snapshot block {end_block}")
        for staker in tqdm(stakers):
            # NOTE: the next line requires an archive node
            spank_staked, *_ = spankbank.stakers(staker, block_identifier=end_block)
            if spank_staked == 0:
                continue
            periods[period]["stakers"][staker] = spank_staked

    return dict(periods)


def timestamp_to_block_number(ts):
    lo = 0
    hi = chain.height
    threshold = 1
    while abs(hi - lo) > threshold:
        mid = (hi - lo) // 2 + lo
        if chain[mid].timestamp < ts:
            lo = mid
        else:
            hi = mid
    return hi


def to_camel_case(snake_str):
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])
