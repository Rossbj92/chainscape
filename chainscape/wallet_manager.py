import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Union, Dict

import pandas as pd
from web3 import Web3

from blockchain import Blockchain
from etherscan_api import EtherscanAPI
from log import logger
from utils.csv_utils import load_wallets_from_csv, export_wallets_to_csv
from utils.wallet_manager_utils import get_gas_costs
from utils.threading_utils import execute_concurrent_tasks, worker_wallet_etherscan_call
from wallet import Wallet
from wallet_contents import WalletContents


class WalletManager:
    """
    A class for managing wallets on the Ethereum blockchain.
    """

    def __init__(
            self,
            rpc_url: str = None,
            wallets_csv_path: str = None,
            etherscan_api_key: str = None
    ):
        """
        Args:
            rpc_url: The URL for the Ethereum RPC node.
            all_wallets_csv: The file path to a CSV file containing a list of all wallets to manage.
            etherscan_api_key: The API key for Etherscan.
        """
        self.blockchain = Blockchain(rpc_url)
        self.wallets_csv_path = wallets_csv_path
        self.wallets = self.load_wallets_from_csv() if wallets_csv_path else []
        self.etherscanAPI = EtherscanAPI(etherscan_api_key=etherscan_api_key) if etherscan_api_key else None
        self.wallet_contents = WalletContents(rpc_url, etherscan_api_key)

    def load_wallets_from_csv(self, wallets_csv_path: Optional[str] = None) -> pd.DataFrame:
        """Load wallets from csv file."""
        if not self.wallets_csv_path:
            self.wallets_csv_path = wallets_csv_path
        wallets = load_wallets_from_csv(self.wallets_csv_path)
        return wallets

    def export_wallets_to_csv(self, file_path: str) -> None:
        """Exports the wallets DataFrame to a CSV file."""
        export_wallets_to_csv(self.wallets, file_path)

    def get_wallet_dataframe(self):
        """Returns current wallets as Pandas dataframe."""
        wallet_df = pd.DataFrame(
            {
                'address': [wallet.address for wallet in self.wallets],
                'name': [wallet.name for wallet in self.wallets],
                'private_key': [wallet.private_key for wallet in self.wallets],
                'balance': [wallet.balance for wallet in self.wallets]
            }
        )
        return wallet_df

    def get_wallets(self, excluded_address: Optional[str] = '', num_needed: Optional[int] = None) -> List[str]:
        """ Get a list of receiving wallets in order that they appear in the csv.

        Args:
            excluded_address: Wallet to be excluded from list.
            num_needed: The number of receiving wallets needed.

        Returns:
            A list of receiving wallets.
        """
        receiving_wallets = []
        for wallet in self.wallets:
            if wallet.address.lower() != excluded_address.lower():
                receiving_wallets.append(wallet.address)
                if num_needed and len(receiving_wallets) >= num_needed:
                    break
        return receiving_wallets

    def add_wallet(self, wallet_name: str = None, address: str = None, private_key: str = None) -> None:
        """Add a wallet to the wallet manager.

        If no address is added, public address will attempt to be recovered from private key.

        Args:
            wallet_name: Wallet nickname
            address: public address
            private_key: address's private key

        Raises:
            ValueError: If the wallet address is invalid.
        """
        if address and not Web3.is_address(address):
            raise ValueError("Invalid wallet address")
        if private_key and not address:
            assert self.blockchain, 'Need RPC url to recover public address from private key.'
            address = self.blockchain.w3.eth.account.from_key(private_key).address

        assert address not in (w.address for w in self.wallets), 'Wallet already loaded.'

        new_wallet = Wallet(name=wallet_name, address=address, private_key=private_key)
        self.wallets.append(new_wallet)
        logger.info(f'Wallet added to manager.\nName: {wallet_name}\nAddress: {address}\nPrivate key: {private_key}')

    def remove_wallet(self, address: str) -> None:
        """Removes a wallet from the wallets DataFrame. """
        assert address in (w.address for w in self.wallets), 'Address not in csv.'
        self.wallets = [wallet for wallet in self.wallets if wallet.address != address]
        logger.info(f'{address} removed from wallets.')


    def get_wallets_balances(
            self,
            wallets: Union[List, str] = None,
            rpc_url: str = None,
            multithread: bool = True
    ) -> Optional[List[str]]:
        """Returns the balances of the specified wallets.

        By default, method sends concurrent requests. If you are concerned about
        API rate limits or usage caps, it is suggested to set it to False.

        Args:
            wallets: A list of public addresses of the wallets to check. If not provided,
            checks all loaded wallets.
            rpc_url: The URL for the Ethereum RPC node.

        Returns:
            Updaed balances attribute in wallets.

        Raises:
            AssertionError: If the Ethereum RPC URL is not provided and the WalletManager object does not have a
                blockchain object.
        """
        if not rpc_url:
            assert self.blockchain, 'Must enter URL for ETH RPC.'
        else:
            self.blockchain = Blockchain(rpc_url)

        if not wallets:
            wallets = [wallet.address for wallet in self.wallets]

        wallet_balances = self.wallet_contents.get_wallets_balances(wallets, multithread=multithread)

        for wallet in self.wallets:
            if wallet in wallet_balances:
                wallet.balance = wallet_balances[wallet]
        return wallet_balances

    def get_token_ids(self, wallet: str, contract_address: str, token_type: str = 'erc721',
                      etherscan_api_key: str = None) -> List[int]:
        """Returns a list of token IDs for a specified wallet and token contract.

        Args:
            wallet: The public address of the wallet to check.
            contract_address: The address of the token contract to check.
            token_type: The type of token to check (e.g. 'erc721', 'erc1155').
            etherscan_api_key: The API key for Etherscan.

        Returns:
            A list of token IDs for the specified wallet and token contract.

        Raises:
            AssertionError: If the Etherscan API key is not provided and the WalletManager object does not have an
            etherscanAPI object.
        """
        assert Web3.is_address(wallet), 'Invalid wallet address.'
        if not etherscan_api_key:
            assert self.etherscanAPI, 'Need Etherscan API Key for this method.'
        else:
            self.etherscanAPI = EtherscanAPI(etherscan_api_key)

        token_txs = self.etherscanAPI.get_wallet_token_transactions(holding_wallet=wallet,
                                                                    contract_address=contract_address,
                                                                    token_type=token_type)
        token_ids = defaultdict(int)
        if token_txs:
            for tx in token_txs:
                if token_type == 'erc721':
                    if tx['to'].lower() == wallet.lower():
                        token_ids[tx['tokenID']] = 1
                    elif tx['from'].lower() == wallet.lower():
                        del token_ids[tx['tokenID']]
                    else:
                        pass
                else:
                    if tx['to'].lower() == wallet.lower():
                        token_ids[tx['tokenID']] += int(tx['tokenValue'])
                    elif tx['from'].lower() == wallet.lower():
                        token_ids[tx['tokenID']] -= int(tx['tokenValue'])
                        if token_ids[tx['tokenID']] == 0:
                            del token_ids[tx['tokenID']]
                    else:
                        pass
        return token_ids


    def find_tokens(
            self,
            contract_address: str,
            wallets: Union[List[str], None] = None,
            etherscan_api_key: str = None,
            rpc_url: str = None,
            multithread: bool=True
    ) -> Dict[str, Dict[str, int]]:
        """Searches for tokens of a specified contract held by the specified wallets.

        Args:
            contract_address: The address of the token contract to search for.
            wallets: A list of public addresses of wallets to check. If not provided, checks all wallets in the
                wallets DataFrame.
            etherscan_api_key: The API key for Etherscan.
            rpc_url: The URL for the Ethereum RPC node.
            multithread: Whether to utilize multithreading.

        By default, method sends concurrent requests. If you are concerned about
        RPC rate limits or usage caps, it is suggested to set it to False.

        Returns:
            A pandas DataFrame containing information about all tokens found.

        Raises:
            AssertionError: If the Etherscan API key is not provided and the WalletManager object does not have an
                etherscanAPI object.
            AssertionError: If the Ethereum RPC URL is not provided and the WalletManager object does not have a
                blockchain object.
        """
        if not etherscan_api_key:
            assert self.etherscanAPI, 'Need Etherscan API Key for this method.'
        else:
            self.etherscanAPI = EtherscanAPI(etherscan_api_key)
        if not rpc_url:
            assert self.blockchain, 'Need Eth RPC URL for this method.'
        else:
            self.blockchain = Blockchain(etherscan_api_key)
        if wallets is None:
            wallets = [wallet.address for wallet in self.wallets]

        results = self.wallet_contents.find_tokens(contract_address=contract_address,
                                                   wallets=wallets,
                                                   multithread=multithread)
        return results

    def get_wallets_gas_costs(
            self,
            wallets: Union[List, str] = None,
            etherscan_api_key: str = None,
            multithread: bool = True
    ) -> Optional[List[str]]:
        """Returns total gas costs spent by wallets in ETH.

        By default, method sends concurrent requests. If you are concerned about
        API rate limits or usage caps, it is suggested to set it to False.

        Args:
            wallets: A list of public addresses of the wallets to check. If not provided,
            checks all loaded wallets.
            etherscan_api_key: Etherscan API key.
            multithread: Whether to utilize multithreading

        Returns:
            Updaed balances attribute in wallets.

        Raises:
            AssertionError: If the Ethereum RPC URL is not provided and the WalletManager object does not have a
                blockchain object.
        """
        if not etherscan_api_key:
            assert self.etherscanAPI, 'Must enter URL for ETH RPC.'
        else:
            self.etherscanAPI = Blockchain(etherscan_api_key)

        if not wallets:
            wallets = [wallet.address for wallet in self.wallets]

        if multithread:
            with ThreadPoolExecutor() as executor:
                results = execute_concurrent_tasks(
                    wallets,
                    worker_wallet_etherscan_call,
                    executor,
                    self.etherscanAPI.get_transactions
                )
            executor.shutdown(wait=True)
        else:
            results = []
            logger.info(f'Beginning search for {len(wallets)} wallets.')
            start = time.time()
            for wallet in wallets:
                result = self.etherscanAPI.get_transactions(wallet)
                results.append((wallet, result))
            end = time.time()
            logger.info(f'{len(wallets)} wallets processed in {(end - start)} seconds.')

        total_cost = get_gas_costs(results)
        return total_cost
