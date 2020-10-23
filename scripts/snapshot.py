import json
import os
import pickle
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from pathlib import Path

import toml
from brownie import chain, interface, web3
from brownie.exceptions import ContractNotFound
from eth_utils import event_abi_to_log_topic
from hexbytes import HexBytes
from toolz import groupby, valfilter
from tqdm import tqdm, trange

spankbank_deploy = 6276045  # https://etherscan.io/tx/0xc6123eea98af9db149313005d9799eefd323baf1566adfaa53d25cc376229543
uniswap_v1_deploy = 6627917  # https://etherscan.io/tx/0xc1b2646d0ad4a3a151ebdaaa7ef72e3ab1aa13aa49d0b7a3ca020f5ee7b1b010
uni_deploy = 10861674  # https://etherscan.io/tx/0x4b37d2f343608457ca3322accdab2811c707acf3eb07a40dd8d9567093ea5b82
spank_deploy = 4590304  # https://etherscan.io/tx/0x249effe35529e648be34903167e9cfaac757d9f12cc21c8a91da207519ab693e
spankbank = interface.SpankBank("0x1ECB60873E495dDFa2a13A8F4140e490dd574E6F")
multicall = interface.Multicall("0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441")
spank = interface.ERC20("0x42d6622deCe394b54999Fbd73D108123806f6a18")
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
UNISWAP_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"


def main():
    logs = fetch_logs()
    events = decode_logs(logs)
    points = calc_spankbank_points(events)
    staked_balances = calc_spankbank_spank(events)
    snapshot_balances = calc_spank()
    find_contracts(snapshot_balances)


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


@cached("snapshot/04-spankbank.json")
def calc_spankbank_spank(events):
    """
    Get all stakers from [StakeEvent, SplitStakeEvent].
    For each period determine the block closest to period end time.
    Get SPANK staked for each staker at end block of each period.
    """
    periods = {}
    events = groupby("event", events)
    new_stakers = {event.args.staker for event in events["StakeEvent"]}
    split_stakers = {event.args.newAddress for event in events["SplitStakeEvent"]}
    stakers = sorted(new_stakers | split_stakers)
    print(len(stakers), "stakers")
    snapshot_end_time = chain[uni_deploy].timestamp
    calls = [
        [str(spankbank), spankbank.stakers.encode_input(staker)] for staker in stakers
    ]
    _, results = multicall.aggregate.call(calls)
    staker_info = {
        staker: spankbank.stakers.decode_output(resp)
        for staker, resp in zip(stakers, results)
    }
    periods_end_times = {
        period: spankbank.periods(period)[5]
        for period in range(1, spankbank.currentPeriod() + 1)
    }
    periods_info = {
        period: {"end_time": end_time, "end_block": timestamp_to_block_number(end_time)}
        for period, end_time in periods_end_times.items()
        if end_time <= snapshot_end_time
    }
    print(periods_info)
    for period, info in periods_info.items():
        if info["end_time"] > snapshot_end_time:
            break
        periods[period] = {
            "end_block": info["end_block"],
            "end_time": info["end_time"],
            "stakers": {},
        }
        print(f"period {period} snapshot block {info['end_block']}")
        try:
            _, results = multicall.aggregate.call(
                calls, block_identifier=info["end_block"]
            )
            for staker, resp in zip(stakers, results):
                spank_staked, *_ = spankbank.stakers.decode_output(resp)
                if spank_staked > 0:
                    periods[period]["stakers"][staker] = spank_staked
        except ValueError as e:
            print("multicall reverted, fall back to slow method")
            period_stakers = [
                staker
                for staker in stakers
                if staker_info[staker][1] <= period and staker_info[staker][2] >= period
            ]
            for staker in tqdm(period_stakers):
                spank_staked, *_ = spankbank.stakers(
                    staker, block_identifier=info["end_block"]
                )
                if spank_staked > 0:
                    periods[period]["stakers"][staker] = spank_staked

    return dict(periods)


@cached("snapshot/05-spank.json")
def calc_spank():
    """
    Snapshot SPANK balances at UNI deploy block.
    """
    balances = transfers_to_balances(spank, spank_deploy, uni_deploy)
    # FIX: initial balance misses an event assigning it
    spank_deployer = "0xA7f00de671ebEB1b04C19a00842ff1d980847f0B"
    balances[spank_deployer] += 10 ** 27
    # NOTE: sanity check
    for addr in [spank_deployer, str(spankbank)]:
        assert balances[addr] == spank.balanceOf(addr, block_identifier=uni_deploy)
    return balances


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


def transfers_to_balances(contract, deploy_block, snapshot_block):
    balances = Counter()
    contract = web3.eth.contract(str(contract), abi=contract.abi)
    step = 10000
    for start in trange(deploy_block, snapshot_block, step):
        end = min(start + step - 1, snapshot_block)
        logs = contract.events.Transfer().getLogs(fromBlock=start, toBlock=end)
        for log in logs:
            if log["args"]["src"] != ZERO_ADDRESS:
                balances[log["args"]["src"]] -= log["args"]["wad"]
            if log["args"]["dst"] != ZERO_ADDRESS:
                balances[log["args"]["dst"]] += log["args"]["wad"]

    return valfilter(bool, dict(balances.most_common()))


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


def is_uniswap(address):
    try:
        pair = interface.UniswapPair(address)
        assert pair.factory() == UNISWAP_FACTORY
        print(f"{address} is a uniswap pool")
    except (AssertionError, ValueError):
        return False
    return True
