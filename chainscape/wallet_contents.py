import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union, Dict

from web3 import Web3

from blockchain import Blockchain
from etherscan_api import EtherscanAPI
from log import logger
from utils.threading_utils import execute_concurrent_tasks, worker_get_wallet_balances, worker_find_tokens


class WalletContents:
    """
    Used to find eth/erc721/erc1155 wallet holdings.
    """

    def __init__(
            self, rpc_url: str = None,
            etherscan_api_key: str = None
    ):
        """
        Args:
            rpc_url: The URL for the Ethereum RPC node.
            etherscan_api_key: The API key for Etherscan.
        """
        self.blockchain = Blockchain(rpc_url)
        self.etherscanAPI = EtherscanAPI(etherscan_api_key=etherscan_api_key) if etherscan_api_key else None


    def get_wallets_balances(
            self,
            wallet_addresses: Union[List, str],
            rpc_url: str = None,
            multithread: bool = True
    ) -> Dict[str, int]:
        """Returns the balances of the specified wallets.

        By default, method sends asynchronous requests. If you are concerned about
        RPC rate limits or usage caps, it is suggested to set it to False.

        Args:
            wallet_addresses: A list of public addresses of the wallets to check.
            rpc_url: The URL for the Ethereum RPC node.
            multithread: Whether multithreading should be utilized for requests.

        Returns:
            A 'balances' column added to self.wallets.

        Raises:
            AssertionError: If the Ethereum RPC URL is not provided and the WalletManager object does not have a
                blockchain object.
        """
        if not rpc_url:
            assert self.blockchain, 'Must enter URL for ETH RPC.'
        else:
            self.blockchain = Blockchain(rpc_url)

        if isinstance(wallet_addresses, str):
            wallet = Web3.to_checksum_address(wallet_addresses)
            return self.blockchain.get_wallet_balance(wallet)

        wallets = [Web3.to_checksum_address(wallet) for wallet in wallet_addresses]

        if multithread:
            with ThreadPoolExecutor() as executor:
                results = execute_concurrent_tasks(
                    wallets,
                    worker_get_wallet_balances,
                    executor,
                    self.blockchain.get_wallet_balance
                )
            executor.shutdown(wait=True)
        else:
            results = []
            logger.info(f'Beginning search for {len(wallets)} wallets.')
            start = time.time()
            for wallet in wallets:
                result = self.blockchain.get_wallet_balance(wallet)
                results.append((wallet, result))
            end = time.time()
            logger.info(f'{len(wallets)} wallets processed in {(end - start)} seconds.')

        wallet_dict = {wallet: balance for wallet, balance in results}
        return wallet_dict


    def get_token_ids(
            self,
            wallet_address: str,
            contract_address: str,
            token_type: str = 'erc721',
            etherscan_api_key: str = None
    ) -> defaultdict(int):
        """Retrieve any contract's token IDs held in a specified wallet.

        Args:
            wallet_address: The public address of the wallet to check.
            contract_address: The address of the token contract to check.
            token_type: The type of token to check ('erc721', 'erc1155').
            etherscan_api_key: The API key for Etherscan.

        Returns:
            A list of token IDs for the specified wallet and token contract.

        Raises:
            AssertionError: If the Etherscan API key is not provided and the WalletManager object does not have an
            etherscanAPI object.
        """
        assert Web3.is_address(wallet_address), 'Invalid wallet address.'
        if not etherscan_api_key:
            assert self.etherscanAPI, 'Need Etherscan API Key for this method.'
        else:
            self.etherscanAPI = EtherscanAPI(etherscan_api_key)

        token_txs = self.etherscanAPI.get_wallet_token_transactions(
            holding_wallet=wallet_address,
            contract_address=contract_address,
            token_type=token_type
        )

        token_ids = defaultdict(int)
        if token_txs:
            for tx in token_txs:
                if token_type == 'erc721':
                    if tx['to'].lower() == wallet_address.lower():
                        token_ids[tx['tokenID']] = 1
                    elif tx['from'].lower() == wallet_address.lower():
                        del token_ids[tx['tokenID']]
                    else:
                        pass
                else:
                    if tx['to'].lower() == wallet_address.lower():
                        token_ids[tx['tokenID']] += int(tx['tokenValue'])
                    elif tx['from'].lower() == wallet_address.lower():
                        token_ids[tx['tokenID']] -= int(tx['tokenValue'])
                        if token_ids[tx['tokenID']] == 0:
                            del token_ids[tx['tokenID']]
                    else:
                        pass
        return token_ids

    def find_tokens(
            self,
            contract_address: str,
            wallets: List[str],
            etherscan_api_key: str = None,
            rpc_url: str = None,
            multithread: bool = True
    ) -> Dict[str, Dict[str, int]]:
        """Searches for tokens of a specified contract held by the specified wallets.

        By default, method sends asynchronous requests. If you are concerned about
        RPC rate limits or usage caps, it is suggested to set it to False.

        Args:
            contract_address: The address of the token contract to search for.
            wallets: A list of public addresses of wallets to check. If not provided,
                checks all wallets currrently loaded.
            etherscan_api_key: The API key for Etherscan.
            rpc_url: The URL for the Ethereum RPC node.
            multithread: Whether multithreading should be utilized for requests.

        Returns:
            A pandas DataFrame containing information about all tokens found.

        Raises:
            AssertionError: If the Etherscan API key is not provided and the WalletManager
                object does not have an etherscanAPI object.
            AssertionError: If the Ethereum RPC URL is not provided and the WalletManager
                object does not have a blockchain object.
        """
        if not etherscan_api_key:
            assert self.etherscanAPI, 'Need Etherscan API Key for this method.'
        else:
            self.etherscanAPI = EtherscanAPI(etherscan_api_key)
        if not rpc_url:
            assert self.blockchain, 'Need Eth RPC URL for this method.'
        else:
            self.blockchain = Blockchain(rpc_url)

        contract_address = Web3.to_checksum_address(contract_address)
        contract_abi = self.etherscanAPI.get_contract_abi(contract_address=contract_address)
        contract = self.blockchain.load_contract(contract_address=contract_address, contract_abi=contract_abi)

        try:
            token_name = contract.functions.name().call()
            token_type = 'erc721'
        except:
            implementation_contract = contract.functions.implementation().call()
            implementation_abi = self.etherscanAPI.get_contract_abi(contract_address=implementation_contract)
            contract = self.blockchain.load_contract(contract_address=contract_address, contract_abi=implementation_abi)
            token_name = contract.functions.name().call()
            token_type = 'erc1155'

        if multithread:
            with ThreadPoolExecutor() as executor:
                results = execute_concurrent_tasks(
                    wallets,
                    worker_find_tokens,
                    executor,
                    contract_address,
                    token_type,
                    self.get_token_ids,
                    token_name
                )
            executor.shutdown(wait=True)
        else:
            results = {}
            logger.info(f'Beginning search for {len(wallets)} wallets.')
            start = time.time()
            for wallet in wallets:
                result = self.get_token_ids(
                    wallet,
                    contract_address,
                    token_type
                )
                if result:
                    results[wallet] = dict(result)
            end = time.time()
            logger.info(f'{len(wallets)} wallets processed in {(end - start)} seconds.')
            return results

        wallet_dict = {}
        for result in results:
            if result[1]:
                wallet = result[1][0]['wallet']
                tokens = {token_pair['token_id']: token_pair['amount'] for token_pair in result[1]}
                wallet_dict[wallet] = tokens

        return wallet_dict
