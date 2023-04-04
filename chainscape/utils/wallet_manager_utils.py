from typing import List, Dict
from web3 import Web3


def get_gas_costs(results: List[List[Dict]], return_eth: bool = True) -> float:
    """Find total gas cost spent from specified wallet address"""
    total_cost = 0
    for wallet, txs in results:
        for tx in txs:
            if tx['from'] == wallet.lower():
                tx_cost = int(tx['gasUsed']) * int(tx['gasPrice'])
                if return_eth:
                    total_cost += Web3.from_wei(tx_cost, 'ether')
                else:
                    total_cost += tx_cost  # wei
    return float(total_cost)
