import brownie

def test_fold(distributor, spank, multisig, chain):
    initial_balance = spank.balanceOf(multisig)
    distributor_balance = spank.balanceOf(distributor)
    with brownie.reverts('MerkleDistributor: Claim period has not passed.'):
        distributor.fold()

    chain.sleep(86400 * 180)
    distributor.fold()
    assert spank.balanceOf(multisig) == initial_balance + distributor_balance
    assert spank.balanceOf(distributor) == 0
