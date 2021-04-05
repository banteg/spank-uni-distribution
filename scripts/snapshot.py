import csv
import json
import os
import pickle
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction
from functools import wraps
from itertools import zip_longest
from pathlib import Path

import toml
from brownie import MerkleDistributor, Wei, accounts, chain, interface, web3
from eth_utils import encode_hex, event_abi_to_log_topic
from hexbytes import HexBytes
from toolz import groupby, valfilter
from tqdm import tqdm, trange

DISTRIBUTOR_ADDRESS = ...
DISTRIBUTION_TOTAL = Wei("695060.118 ether")
POINTS_TOTAL = Wei("111209.61888 ether")
STAKED_TOTAL = Wei("444838.47552 ether")
SNAPSHOT_TOTAL = Wei("139012.0236 ether")
DUST = Wei("6.69 ether")  # 20 usd
EXCLUDED = {
    "0x1ECB60873E495dDFa2a13A8F4140e490dd574E6F": "spankbank",
    "0x742d35Cc6634C0532925a3b844Bc454e4438f44e": "bitfinex",
    "0x876EabF441B2EE5B5b0554Fd502a8E0600950cFa": "bitfinex",
    "0xF1A5D5F652f391a906d7347F001099280D7abbF5": "vesting skip",
    "0x2a0c0DBEcC7E4D658f48E01e3fA353F44050c208": "idex",
    "0x8d12A197cB00D4747a1fe03395095ce2A5CC6819": "etherdelta",
    "0xfb54e05f36095f07f281722b805d65329db8700f": "vesting skip",
    "0x6a99e0d5065ed09433ba99faf0944faa57c1ab26": "vesting skip",
    "0x9426614d930adc9fe4c15f86b7bdd3e9b095961b": "vesting skip",
}

spankbank_deploy = 6276045  # https://etherscan.io/tx/0xc6123eea98af9db149313005d9799eefd323baf1566adfaa53d25cc376229543
uniswap_v1_deploy = 6627917  # https://etherscan.io/tx/0xc1b2646d0ad4a3a151ebdaaa7ef72e3ab1aa13aa49d0b7a3ca020f5ee7b1b010
uni_deploy = 11927314  # https://etherscan.io/tx/0x4b37d2f343608457ca3322accdab2811c707acf3eb07a40dd8d9567093ea5b82
spank_deploy = 4590304  # https://etherscan.io/tx/0x249effe35529e648be34903167e9cfaac757d9f12cc21c8a91da207519ab693e
uniswap_v2_deploy = 10000835  # https://etherscan.io/tx/0xc31d7e7e85cab1d38ce1b8ac17e821ccd47dbde00f9d57f2bd8613bff9428396
expired_by_timestamp = 1609477200  # January First 2021
spankbank = interface.SpankBank("0x1ECB60873E495dDFa2a13A8F4140e490dd574E6F")
multicall = interface.Multicall("0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441")
spank = interface.ERC20("0x42d6622deCe394b54999Fbd73D108123806f6a18")
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
UNISWAP_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
LAST_PERIOD_TO_QUALIFY = 28


def main():
    logs = fetch_logs()
    events = decode_logs(logs)
    qualified_stakers = get_qualified_stakers(events)
    points = calculate_points(events, qualified_stakers)
    write_to_csv("snapshot/distribution.csv", points)


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


def write_to_csv(path, points):
    with open(path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=
        ["staker address",
         "first stake points",
         "last check in points",
         "max points"])

        writer.writeheader()
        for staker in points:
            writer.writerow(
                {"staker address": staker,
                 "first stake points": points[staker]["firstStakePoints"][0],
                 "last check in points": points[staker]["latestCheckinPoints"][0],
                 "max points": points[staker]["maxEverSpankPoints"][0]
                 })


@cached("snapshot/01-logs.pickle")
def fetch_logs():
    logs = []
    step = 100000
    # NOTE: start from spankbank deploy since we need to catch all stakers
    for start in trange(spankbank_deploy, uni_deploy, step):
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
def calc_spankbank_points(events):
    """
    Get active points for each staker for each period from CheckInEvent.
    """
    periods = defaultdict(dict)
    events = groupby("event", events)
    for event in events["CheckInEvent"]:
        if event.blockNumber > uni_deploy:
            continue
        periods[event.args.period][event.args.staker] = event.args.spankPoints
    return dict(periods)


def get_qualified_stakers(events):
    """
    Get all staker addresses whose stakes from stakeEvent and splitStakeEvent didnt expire by Jan 1st 2021
    """
    qualifiedStakers = set()
    events = groupby("event", events)
    new_stakers = {event.args.staker for event in events["StakeEvent"]}
    split_stakers = {event.args.newAddress for event in events["SplitStakeEvent"]}
    checkin_stakers = {event.args.staker for event in events["CheckInEvent"]}

    stakers = sorted(new_stakers | split_stakers | checkin_stakers)
    print(len(stakers), "stakers")
    calls = [
        [str(spankbank), spankbank.stakers.encode_input(staker)] for staker in stakers
    ]
    _, results = multicall.aggregate.call(calls)
    staker_info = {
        staker: spankbank.stakers.decode_output(resp)
        for staker, resp in zip(stakers, results)
    }
    for staker in staker_info:
        if staker_info[staker][2] >= LAST_PERIOD_TO_QUALIFY:
            qualifiedStakers.add(staker)
    print("Number of qualified stakers: " + str(len(qualifiedStakers)))
    return {"stakers": qualifiedStakers,
            "stakerInfo": staker_info}


@cached("snapshot/points.json")
def calculate_points(events, qualified_stakers):
    """
    Get 3 different types of spankpoints for each staker - (spankpoints, period)
    - First stake event
    - latest checkin
    - highest ever
    """
    events = groupby("event", events)
    spank_points_dict = defaultdict(dict)

    for event in events["StakeEvent"]:
        if event.args.staker in qualified_stakers["stakers"]:
            if event.args.staker in spank_points_dict[event.args.staker] and \
                    "firstStakePoints" in spank_points_dict[event.args.staker]["firstStakePoints"]:
                """
                See if its an earlier period for staking
                """
                if event.args.period < spank_points_dict[event.args.staker]["firstStakePoints"][1]:
                    spank_points_dict[event.args.staker]["firstStakePoints"] = (
                        event.args.spankPoints, event.args.period)
            else:
                spank_points_dict[event.args.staker]["firstStakePoints"] = (event.args.spankPoints, event.args.period)
                """
                Check against max ever
                """
            if "maxEverSpankPoints" not in spank_points_dict[event.args.staker] or \
                    event.args.spankPoints > spank_points_dict[event.args.staker]["maxEverSpankPoints"][0]:
                spank_points_dict[event.args.staker]["maxEverSpankPoints"] = (event.args.spankPoints, event.args.period)

    for event in events["CheckInEvent"]:
        if event.args.staker in qualified_stakers["stakers"]:
            if event.args.staker in spank_points_dict and \
                    "latestCheckinPoints" in spank_points_dict[event.args.staker]:
                """
                See if its a later period for checkin
                """
                if event.args.period > spank_points_dict[event.args.staker]["latestCheckinPoints"][1]:
                    spank_points_dict[event.args.staker]["latestCheckinPoints"] = (
                        event.args.spankPoints, event.args.period)
            else:
                spank_points_dict[event.args.staker]["latestCheckinPoints"] = (
                    event.args.spankPoints, event.args.period)

            if "maxEverSpankPoints" not in spank_points_dict[event.args.staker] or \
                    event.args.spankPoints > spank_points_dict[event.args.staker]["maxEverSpankPoints"][0]:
                spank_points_dict[event.args.staker]["maxEverSpankPoints"] = (event.args.spankPoints, event.args.period)

    for staker in spank_points_dict:
        if "firstStakePoints" not in spank_points_dict[staker]:
            spank_points_dict[staker]["firstStakePoints"] = ("N/A", 0)
        if "latestCheckinPoints" not in spank_points_dict[staker]:
            spank_points_dict[staker]["latestCheckinPoints"] = ("N/A", 0)

    print("Discrepancy of: " + str(len(qualified_stakers) - len(spank_points_dict)) + " stakers")

    return dict(spank_points_dict)


@cached("snapshot/06-contracts.json")
def find_contracts(balances):
    pool = ThreadPoolExecutor(10)
    codes = pool.map(web3.eth.getCode, balances)
    contracts = {
        user: balances[user]
        for user, code in tqdm(zip(balances, codes), total=len(balances))
        if code
    }
    print(f"{len(contracts)} contracts found")
    return contracts


def timestamp_to_block_number(ts):
    lo = 0
    hi = chain.height - 30  # fix for "block not found"
    threshold = 1
    while abs(hi - lo) > threshold:
        mid = (hi - lo) // 2 + lo
        if chain[mid].timestamp < ts:
            lo = mid
        else:
            hi = mid
    return hi
