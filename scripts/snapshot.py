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
from click import secho
from eth_abi.packed import encode_abi_packed
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
expired_by_timestamp = 1609477200 # January First 2021
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
    calculate_points(events, qualified_stakers)
    points = calc_spankbank_points(events)
    # calculate_max_spank_for_qualified_stakers(qualified_stakers, points)
    staked_balances = calc_spankbank_spank(events)
    # distribution = prepare_distribution(points, staked_balances)
    # tree = prepare_merkle_tree(distribution)


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
        print(event)
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
        period: spankbank.periods(period)
        for period in range(1, spankbank.currentPeriod() + 1)
    }

    print(periods_end_times)
    periods_info = {
        period: {"end_time": period_struct[5], "end_block": timestamp_to_block_number(period_struct[5]), "startTime": period_struct[4], "startBlock": timestamp_to_block_number(period_struct[4])}
        for period, period_struct in periods_end_times.items()
        if period_struct[5] <= snapshot_end_time
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
    print(len(qualifiedStakers))
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
    first_staking_period_points = defaultdict(dict)
    post_checkin_points = defaultdict(dict)
    max_ever_spankpoints = defaultdict(dict)

    for event in events["StakeEvent"]:
        if event.args.staker in qualified_stakers["stakers"]:
            if event.args.staker in first_staking_period_points:
                """
                See if its an earlier period for staking
                """
                if event.args.period < first_staking_period_points[event.args.staker][1]:
                    first_staking_period_points[event.args.staker] = (event.args.spankPoints, event.args.period)
            else:
                first_staking_period_points[event.args.staker] = (event.args.spankPoints, event.args.period)

            if event.args.staker not in max_ever_spankpoints or event.args.spankPoints > max_ever_spankpoints[event.args.staker][0]:
                 max_ever_spankpoints[event.args.staker] = (event.args.spankPoints, event.args.period)

    for event in events["CheckInEvent"]:
        if event.args.staker in qualified_stakers["stakers"]:
            if event.args.staker in post_checkin_points:
                """
                See if its a later period for checkin
                """
                if event.args.period > post_checkin_points[event.args.staker][1]:
                    post_checkin_points[event.args.staker] = (event.args.spankPoints, event.args.period)
            else:
                post_checkin_points[event.args.staker] = (event.args.spankPoints, event.args.period)

            if event.args.staker not in max_ever_spankpoints or event.args.spankPoints > max_ever_spankpoints[event.args.staker][0]:
                 max_ever_spankpoints[event.args.staker] = (event.args.spankPoints, event.args.period)
    print(post_checkin_points)
    return dict({"points": {
        "first staking period points": first_staking_period_points,
        "post checkin period points": post_checkin_points,
        "max ever points": max_ever_spankpoints
    }})


# @cached("snapshot/result.json")
# def calculate_max_spank_for_qualified_stakers(qualified_stakers, points):
#     for staker in qualified_stakers["stakers"]:

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


@cached("snapshot/07-uniswap.json")
def calc_uniswap(contracts):
    replacements = {}
    for address in contracts:
        if not is_uniswap(address):
            continue

        # no need to check the pool contents since we already know the equivalent value
        # so we just grab the lp share distribution and distirbute the tokens pro-rata

        balances = transfers_to_balances(
            interface.ERC20(address), uniswap_v2_deploy, uni_deploy
        )
        supply = sum(balances.values())
        if not supply:
            continue
        replacements[address] = {
            user: int(Fraction(balances[user], supply) * contracts[address])
            for user in balances
        }
        assert (
            sum(replacements[address].values()) <= contracts[address]
        ), "no inflation ser"

    return replacements


@cached("snapshot/08-unwrapped.json")
def unwrap_balances(balances, replacements):
    for remove, additions in replacements.items():
        balances.pop(remove)
        for user, balance in additions.items():
            balances.setdefault(user, 0)
            balances[user] += balance
    return dict(Counter(balances).most_common())


@cached("snapshot/09-distribution.json")
def prepare_distribution(points, staked_balances):
    assert POINTS_TOTAL + STAKED_TOTAL + SNAPSHOT_TOTAL == DISTRIBUTION_TOTAL

    distribution = Counter()

    points_amounts = Counter()
    for period in points:
        for user, amount in points[period].items():
            if user in EXCLUDED:
                continue
            points_amounts[user] += amount
    ratio = Fraction(POINTS_TOTAL, sum(points_amounts.values()))
    for user, amount in points_amounts.items():
        distribution[user] += int(amount * ratio)

    staked_amounts = Counter()
    for period in staked_balances:
        for user, amount in staked_balances[period]["stakers"].items():
            if user in EXCLUDED:
                continue
            staked_amounts[user] += amount
    ratio = Fraction(STAKED_TOTAL, sum(staked_amounts.values()))
    for user, amount in staked_amounts.items():
        distribution[user] += int(amount * ratio)

    distribution = {
        user: amount for user, amount in distribution.items() if amount >= DUST
    }

    distribution_total = sum(distribution.values())
    ratio = Fraction(DISTRIBUTION_TOTAL, distribution_total)
    distribution = {user: int(amount * ratio) for user, amount in distribution.items()}
    assert sum(distribution.values()) <= DISTRIBUTION_TOTAL, "no inflation ser"

    print("target:", DISTRIBUTION_TOTAL.to("ether"))
    print("actual:", Wei(sum(distribution.values())).to("ether"))
    print("recipients:", len(distribution))

    return dict(Counter(distribution).most_common())


@cached("snapshot/10-merkle-distribution.json")
def prepare_merkle_tree(balances):
    elements = [
        (index, account, amount)
        for index, (account, amount) in enumerate(balances.items())
    ]
    nodes = [
        encode_hex(encode_abi_packed(["uint", "address", "uint"], el))
        for el in elements
    ]
    tree = MerkleTree(nodes)
    distribution = {
        "merkleRoot": encode_hex(tree.root),
        "tokenTotal": hex(sum(balances.values())),
        "claims": {
            user: {
                "index": index,
                "amount": hex(amount),
                "proof": tree.get_proof(nodes[index]),
            }
            for index, user, amount in elements
        },
    }
    print(f"merkle root: {encode_hex(tree.root)}")
    return distribution


def deploy():
    user = accounts.load(input("account: "))
    tree = json.load(open("snapshot/10-merkle-distribution.json"))
    root = tree["merkleRoot"]
    token = str(spank)
    MerkleDistributor.deploy(token, root, {"from": user})


def claim():
    claimer = accounts.load(input("account: "))
    dist = MerkleDistributor.at(DISTRIBUTOR_ADDRESS)
    tree = json.load(open("snapshot/10-merkle-distribution.json"))
    claim_other = input("Claim for another account? y/n [default: n] ") or "n"
    assert claim_other in {"y", "n"}
    user = str(claimer) if claim_other == "n" else input("Enter address to claim for: ")

    if user not in tree["claims"]:
        return secho(f"{user} is not included in the distribution", fg="red")
    claim = tree["claims"][user]
    if dist.isClaimed(claim["index"]):
        return secho(f"{user} has already claimed", fg="yellow")

    amount = Wei(int(claim["amount"], 16)).to("ether")
    secho(f"Claimable amount: {amount} UNI", fg="green")
    dist.claim(claim["index"], user, claim["amount"], claim["proof"], {"from": claimer})


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


class MerkleTree:
    def __init__(self, elements):
        self.elements = sorted(set(web3.keccak(hexstr=el) for el in elements))
        self.layers = MerkleTree.get_layers(self.elements)

    @property
    def root(self):
        return self.layers[-1][0]

    def get_proof(self, el):
        el = web3.keccak(hexstr=el)
        idx = self.elements.index(el)
        proof = []
        for layer in self.layers:
            pair_idx = idx + 1 if idx % 2 == 0 else idx - 1
            if pair_idx < len(layer):
                proof.append(encode_hex(layer[pair_idx]))
            idx //= 2
        return proof

    @staticmethod
    def get_layers(elements):
        layers = [elements]
        while len(layers[-1]) > 1:
            layers.append(MerkleTree.get_next_layer(layers[-1]))
        return layers

    @staticmethod
    def get_next_layer(elements):
        return [
            MerkleTree.combined_hash(a, b)
            for a, b in zip_longest(elements[::2], elements[1::2])
        ]

    @staticmethod
    def combined_hash(a, b):
        if a is None:
            return b
        if b is None:
            return a
        return web3.keccak(b"".join(sorted([a, b])))
