# SpankBank UNI Distribution

## Abstract

This is a working repo for distributing UNI claimed by Spank team to relevant stakeholders in the ecosystem.

## Distribution

- total UNI received - 1,158,433.53
- 40% for taxes - 463,373.412
- 60% to distribute - 695,060.118
    - 80% to SpankBank stakers - 556,048.0944
        - 80% to SPANK staked per period - 444,838.47552
        - 20% to SpankPoints per period - 111,209.61888
    - 20% to snapshot of SPANK holders at the UNI airdrop - 139,012.0236 

- UNI offered to spankbank will be evenly distributed by period from when UNI launched to when it was airdropped
  - for example, the UNI for period 1 is split 80% by SPANK staked in period 1 and 20% by period 1 spankpoints, and so on for each period

- UNI offered to SPANK snapshot will SKIP:
  - spankbank (bc already accounted for)
  - exchanges (bitfinex, IDEX, etherdelta)
  - uniswap v1/v2 (LPs will receive tokens directly)

- The UNI offer generally will skip anyone receiving <$20 UNI (5927 SPANK holders => 1000 UNI recipients) 
   - note this will slightly increase everyone else's UNI

- Distribution will be via merkle-drop, so you will have to claim (similar to UNI)
- ALL UNCLAIMED UNI WILL BE CLAIMED BY THE TEAM AFTER 6 MONTHS

## Deploy

To deploy the distributor on the mainnet:

```
brownie run snapshot deploy --network mainnet
```

## Claim

To claim the distribution:
```
brownie accounts import alias keystore.json
brownie run snapshot claim --network mainnet
```

## Tests

All testing is performed in a forked mainnet environment.

To run the unit tests:

```
brownie test
```

## Validation

To generate the snapshot data:

```
pip install -r requirements.txt

brownie networks add Ethereum archive host=$YOUR_ARCHIVE_NODE chainid=1

rm -rf snapshot
brownie run snapshot --network archive
```
