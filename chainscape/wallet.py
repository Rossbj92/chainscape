from typing import Optional
from web3 import Web3


class Wallet:
    def __init__(
            self, address: str, name: Optional[str],
            private_key: Optional[str] = None, balance: Optional[int] = None
    ):
        self.address = address
        self.name = name
        self.private_key = private_key
        self.balance = balance

    def is_valid_address(self) -> bool:
        return Web3.isAddress(self.address)
