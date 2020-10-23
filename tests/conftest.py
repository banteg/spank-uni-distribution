import json
import pytest


@pytest.fixture(autouse=True)
def isolation_setup(fn_isolation):
    # enable function isolation
    pass


@pytest.fixture(scope="session")
def multisig(accounts):
    return accounts.at("0xDc9727D102f00adF0043d04431b6cd162c5114ea", force=True)


@pytest.fixture(scope="module")
def spank(interface):
    return interface.ERC20("0x42d6622deCe394b54999Fbd73D108123806f6a18")


@pytest.fixture(scope="session")
def tree():
    with open("snapshot/10-merkle-distribution.json") as fp:
        claim_data = json.load(fp)
    for value in claim_data["claims"].values():
        value["amount"] = int(value["amount"], 16)

    return claim_data


@pytest.fixture(scope="module")
def distributor(MerkleDistributor, multisig, tree, spank):
    contract = MerkleDistributor.deploy(spank, tree["merkleRoot"], {"from": multisig})
    spank.transfer(contract, tree["tokenTotal"], {"from": multisig})

    return contract
