import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Union

import pandas as pd
from web3 import Web3

from wallet import Wallet
from csv_utils import load_wallets_from_csv, export_wallets_to_csv
from blockchain import Blockchain
from etherscan_api import EtherscanAPI
from log import logger


class WalletManager:
    """
    A class for managing wallets on the Ethereum blockchain.
    """

    def __init__(
            self, rpc_url: str = None, wallets_csv_path: str = None,
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

    def load_wallets_from_csv(self, wallets_csv_path: Optional[str] = None) -> pd.DataFrame:
        """Load wallets from csv file."""
        if not self.wallets_csv_path:
            self.wallets_csv_path = wallets_csv_path
        wallets = load_wallets_from_csv(self.wallets_csv_path)
        return wallets

    def get_wallet_dataframe(self):
        """Returns current wallets as Pandas dataframe."""
        wallet_df = pd.DataFrame({
            'address': [wallet.address for wallet in self.wallets],
            'name': [wallet.name for wallet in self.wallets],
            'private_key': [wallet.private_key for wallet in self.wallets]
        })
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

        If no address is added, public address will be recovered from private key.

        Args:
            wallet_name: Wallet nickname
            address: public address
            private_key: address's private key

        Raises:
            ValueError: If the wallet address is invalid.
        """
        if address and not Web3.isAddress(address):
            raise ValueError("Invalid wallet address")
        if private_key and not address:
            assert self.blockchain, 'Need RPC url to recover public address from private key.'
            address = self.blockchain.w3.eth.account.from_key(private_key).address

        assert address not in (w.address for w in self.wallets), 'Wallet already loaded.'

        new_wallet = Wallet(name=wallet_name, address=address, private_key=private_key)
        self.wallets.append(new_wallet)
        logger.info(f'Wallet added to manager.\nName: {wallet_name}\nAddress: {address}\nPrivate key: {private_key}')
        return

    def remove_wallet(self, address: str) -> None:
        """Removes a wallet from the wallets DataFrame. """

        assert address in (w.address for w in self.wallets), 'Address not in csv.'
        self.wallets = [wallet for wallet in self.wallets if wallet.address != address]
        logger.info(f'{address} removed from wallets.')
        return

    # TODO: test with asyncio
    def get_wallets_balances(self, wallets: Union[List, str] = None, rpc_url: str = None) -> Optional[List[str]]:
        """Returns the balances of the specified wallets.

        Args:
            wallets: A list of public addresses of the wallets to check. If not provided, checks all wallets in the
                wallets DataFrame.
            rpc_url: The URL for the Ethereum RPC node.

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

        if isinstance(wallets, str):
            return self.blockchain.get_wallet_balance(wallets)

        if not wallets:
            wallets = self.wallets
        else:
            wallets = self.wallets[self.wallets['address'].isin(wallets)]
            assert wallets.shape[0] != 0, 'If entering multiple wallets, all must be in wallets csv.'

        wallets_needed = set(wallet['address'] for wallet in wallets.to_dict('records'))
        wallet_dict = {}
        def worker(wallet):
            try:
                balance = self.blockchain.get_wallet_balance(wallet)
                wallet_dict[wallet] = balance
                wallets_needed.remove(wallet)
            except:
                time.sleep(1)
                return wallet

        start = time.time()
        logger.info(f'Beginning search for {len(wallets_needed)} wallets.')
        with ThreadPoolExecutor() as executor:
            while wallets_needed:
                futures = {executor.submit(worker, wallet): wallet for wallet in wallets_needed}
                for future in futures:
                    failed_wallet = future.result()
                    if failed_wallet:
                        wallets_needed.add(failed_wallet)
                logger.info(f'{len(wallets) - len(wallets_needed)} wallets completed. {len(wallets_needed)} remain.')
                time.sleep(1)
            executor.shutdown(wait=False)
        end = time.time()
        logger.info(f'{len(wallets)} wallets found in {end - start} seconds.')

        self.wallets['balance'] = self.wallets['address'].replace(wallet_dict)
        return

    def export_wallets_to_csv(self, file_path: str) -> None:
        """Exports the wallets DataFrame to a CSV file.

        Args:
            file_path: The file path for the CSV file to be created.
        """
        self.wallets.to_csv(file_path, index=False)

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
        assert Web3.isAddress(wallet), 'Invalid wallet address.'
        if not etherscan_api_key:
            assert self.etherscanAPI, 'Need Etherscan API Key for this method.'
        else:
            self.etherscanAPI = EtherscanAPI(etherscan_api_key)

        token_txs = self.etherscanAPI.get_wallet_token_transactions(holding_wallet=wallet,
                                                                    contract_address=contract_address,
                                                                    token_type=token_type)
        token_ids = defaultdict(int)
        if token_txs["status"] == "1" and token_txs["result"]:
            for tx in token_txs['result']:
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


    # TODO: test with asyncio
    def find_tokens(self, contract_address: str,
                    wallets: Union[List[str], None] = None, etherscan_api_key: str = None,
                    rpc_url: str = None) -> pd.DataFrame:
        """Searches for tokens of a specified contract held by the specified wallets.

        Args:
            contract_address: The address of the token contract to search for.
            wallets: A list of public addresses of wallets to check. If not provided, checks all wallets in the
                wallets DataFrame.
            etherscan_api_key: The API key for Etherscan.
            rpc_url: The URL for the Ethereum RPC node.

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
            wallets = [wallet['address'] for wallet in self.wallets.to_dict('records')]

        contract_address = Web3.toChecksumAddress(contract_address)
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

        result = []
        wallets_needed = set(wallets)

        def worker(wallet, contract_address, token_type):
            try:
                token_ids = self.get_token_ids(wallet, contract_address, token_type)
                if token_ids:
                    for token_id, num in token_ids.items():
                        result.append({
                            'wallet': wallet,
                            'token_id': token_id,
                            'amount': num,
                            'token_name': token_name
                        })
                wallets_needed.remove(wallet)
            except:
                time.sleep(1)
                return wallet

        start = time.time()
        logger.info(f'Beginning search for {len(wallets_needed)} wallets.')
        with ThreadPoolExecutor() as executor:
            while wallets_needed:
                futures = [executor.submit(worker, wallet, contract_address, token_type) for wallet in wallets_needed]
                for future in futures:
                    failed_wallet = future.result()
                    if failed_wallet:
                        wallets_needed.add(failed_wallet)
                logger.info(f'{len(wallets) - len(wallets_needed)} wallets completed. {len(wallets_needed)} remain.')
                time.sleep(1)
            executor.shutdown(wait=False)

        end = time.time()
        logger.info(f'{len(wallets)} wallets found in {end - start} seconds.')
        return pd.DataFrame(result)
