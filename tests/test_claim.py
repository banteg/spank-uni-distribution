import brownie
from brownie.test import given, strategy
from hypothesis import assume


@given(
    st_claim=strategy("decimal", min_value=0, max_value="0.9999", places=4),
)
def test_claim(distributor, tree, spank, st_claim, multisig):
    idx = int(st_claim * len(tree["claims"]))
    account = sorted(tree["claims"])[idx]
    claim = tree["claims"][account]

    initial_balance = spank.balanceOf(account)
    distributor.claim(
        claim["index"],
        account,
        claim["amount"],
        claim["proof"],
        {"from": account},
    )

    assert spank.balanceOf(account) == initial_balance + claim["amount"]


@given(
    st_claim=strategy("decimal", min_value=0, max_value="0.9999", places=4),
    st_account=strategy("address"),
)
def test_claim_via_different_account(
    distributor, tree, spank, multisig, st_claim, st_account
):
    idx = int(st_claim * len(tree["claims"]))
    account = sorted(tree["claims"])[idx]
    claim = tree["claims"][account]

    assume(account != st_account)
    initial_balance = spank.balanceOf(account)
    distributor.claim(
        claim["index"],
        account,
        claim["amount"],
        claim["proof"],
        {"from": st_account},
    )

    assert spank.balanceOf(account) == initial_balance + claim["amount"]


@given(st_claim=strategy("decimal", min_value=0, max_value="0.9999", places=4))
def test_claim_twice(distributor, tree, spank, st_claim):
    idx = int(st_claim * len(tree["claims"]))
    account = sorted(tree["claims"])[idx]
    claim = tree["claims"][account]

    distributor.claim(
        claim["index"], account, claim["amount"], claim["proof"], {"from": account}
    )

    with brownie.reverts("MerkleDistributor: Drop already claimed."):
        distributor.claim(
            claim["index"],
            account,
            claim["amount"],
            claim["proof"],
            {"from": account},
        )
