from typing import List, Union

from web3 import Web3
from web3.contract import Contract

from constants.eth_blockchain import DisperseConstants
from etherscan_api import EtherscanAPI
from transaction import ContractTransaction, Disperser
from utils import get_current_gas_price, get_wallet_balance


class Blockchain:
    """
    Wrapper class for interacting directly with the Ethereum blockchain.
    """
    def __init__(self, rpc_url: str, etherscan_api_key: str = None):
        """Initializes a new instance of the Blockchain class.

        Args:
            rpc_url: The URL for the Ethereum node's RPC endpoint.
            etherscan_api_key: The API key for accessing the Etherscan API.
        """
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.etherscan = EtherscanAPI(etherscan_api_key) if etherscan_api_key else None
        self.tx_handler = ContractTransaction(self.w3)
        self.disperser = Disperser(self.w3, etherscan_api_key)
    def get_current_gas_price(self) -> int:
        return get_current_gas_price(self.w3)

    def get_wallet_balance(self, wallet_address: str, return_eth=True) -> float:
        return get_wallet_balance(self.w3, wallet_address, return_eth)
    def load_contract(self, contract_address: str, contract_abi: str) -> Contract:
        contract_address = Web3.toChecksumAddress(contract_address)
        contract = self.w3.eth.contract(address=contract_address, abi=contract_abi)
        return contract

    def get_transaction_status(self, tx_hash: str) -> str:
        return self.tx_handler.get_transaction_status(tx_hash)

    def disperse_eth(
            self, sender_wallet: str,
            private_key: str,
            receiving_wallets: List[str],
            amounts: Union[List[float], float],
            max_fee: float = None,
            max_priority: float = None
    ) -> str:
        disperse_instance = self.load_contract(contract_address=DisperseConstants.DISPERSE_CONTRACT, contract_abi=DisperseConstants.DISPERSE_ABI)

        if isinstance(amounts, float):
            amounts = [amounts] * len(receiving_wallets)

        sender_wallet_balance = self.get_wallet_balance(sender_wallet)
        assert sender_wallet_balance > sum(amounts),\
            f'{sender_wallet} ETH balance of {sender_wallet_balance} too low for {sum(amounts)} disperse.'

        return self.disperser.disperse_eth(disperse_instance, sender_wallet, private_key, receiving_wallets, amounts, max_fee, max_priority)

    def disperse_erc721(
            self,
            holding_wallet: str,
            private_key: str,
            receiving_wallets: list,
            token_contract_address: str,
            token_ids: list,
            max_fee: float = None,
            max_priority: float = None,
            etherscan_api_key: str = None
    ) -> List[str]:
        if not etherscan_api_key:
            assert self.etherscan, 'Must include Etherscan API key.'
        else:
            self.etherscan = EtherscanAPI(etherscan_api_key)

        token_contract_address = Web3.toChecksumAddress(token_contract_address)
        contract_abi = self.etherscan.get_contract_abi(token_contract_address)
        token_contract_instance = self.load_contract(contract_address=token_contract_address, contract_abi=contract_abi)

        holding_wallet = Web3.toChecksumAddress(holding_wallet)
        receiving_wallets = [Web3.toChecksumAddress(address) for address in receiving_wallets]
        return self.disperser.disperse_erc721(token_contract_instance, holding_wallet, private_key, receiving_wallets,
                                              token_ids, max_fee, max_priority)
