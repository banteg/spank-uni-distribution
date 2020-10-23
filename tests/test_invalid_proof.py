import brownie
from brownie.test import given, strategy


@given(
    st_claim=strategy("decimal", min_value=0, max_value="0.9999", places=4),
)
def test_wrong_amount(distributor, multisig, tree, st_claim):
    idx = int(st_claim * len(tree["claims"]))
    account = sorted(tree["claims"])[idx]
    claim = tree["claims"][account]

    with brownie.reverts("MerkleDistributor: Invalid proof."):
        distributor.claim(
            claim["index"],
            account,
            claim["amount"] + 1,
            claim["proof"],
            {"from": multisig},
        )


@given(
    st_claim=strategy("decimal", min_value=0, max_value="0.9999", places=4),
)
def test_wrong_index(distributor, multisig, tree, st_claim):
    idx = int(st_claim * len(tree["claims"]))
    account = sorted(tree["claims"])[idx]
    claim = tree["claims"][account]

    with brownie.reverts("MerkleDistributor: Invalid proof."):
        distributor.claim(
            claim["index"] + 1,
            account,
            claim["amount"],
            claim["proof"],
            {"from": multisig},
        )


@given(
    st_claim=strategy("decimal", min_value=0, max_value="0.9999", places=4),
    st_account=strategy("address"),
)
def test_wrong_address(distributor, multisig, tree, st_claim, st_account):
    idx = int(st_claim * len(tree["claims"]))
    account = sorted(tree["claims"])[idx]
    claim = tree["claims"][account]

    with brownie.reverts("MerkleDistributor: Invalid proof."):
        distributor.claim(
            claim["index"],
            st_account,
            claim["amount"],
            claim["proof"],
            {"from": multisig},
        )
