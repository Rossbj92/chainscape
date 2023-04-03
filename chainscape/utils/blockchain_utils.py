from web3 import Web3


def get_current_gas_price(w3: Web3) -> int:
    """Returns the current gas price in ether.

    Returns:
        The current gas price in ether.
    """
    return round(float(Web3.from_wei(w3.eth.gas_price, 'gwei')))


def get_wallet_balance(w3: Web3, wallet_address: str, return_eth=True) -> float:
    """Returns the balance of the specified wallet in ether.

    Args:
        w3: Web3 instance.
        wallet_address: The wallet address.
        return_eth: If True, the balance is returned in ether. Otherwise, it is returned in wei.

    Returns:
        The balance of the specified wallet.
    """

    if wallet_address and not Web3.is_address(wallet_address):
        raise ValueError("Invalid wallet address")
    wallet = Web3.to_checksum_address(wallet_address)
    balance = w3.eth.get_balance(wallet)
    if return_eth:
        return float(Web3.from_wei(balance, 'ether'))
    else:
        return float(balance)
