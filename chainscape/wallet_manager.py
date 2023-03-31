import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Union

import pandas as pd
from web3 import Web3

from blockchain import Blockchain
from etherscan_api import EtherscanAPI
from log import logger


class WalletManager:
    """
    A class for managing wallets on the Ethereum blockchain.
    """

    def __init__(self, rpc_url: str = None, all_wallets_csv: str = None, etherscan_api_key: str = None,
                 binance_api_key: str = None, binance_secret_key: str = None):
        """
        Args:
            rpc_url: The URL for the Ethereum RPC node.
            all_wallets_csv: The file path to a CSV file containing a list of all wallets to manage.
            etherscan_api_key: The API key for Etherscan.
            binance_api_key: The API key for Binance.
            binance_secret_key: The secret key for Binance.
        """
        self.blockchain = Blockchain(rpc_url)
        self.all_wallets_csv = all_wallets_csv
        self.wallets = self.load_wallets_from_csv() if all_wallets_csv else []
        self.etherscanAPI = EtherscanAPI(etherscan_api_key=etherscan_api_key) if etherscan_api_key else None
        self.disperse_contract_address = "0xD152f549545093347A162Dce210e7293f1452150"  # Disperse.app contract address
        self.disperse_abi = json.loads(
            '[{"constant":false,"inputs":[{"name":"token","type":"address"},{"name":"recipients","type":"address[]"},{"name":"values","type":"uint256[]"}],"name":"disperse","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"recipients","type":"address[]"},{"name":"values","type":"uint256[]"}],"name":"disperseEther","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"}]')
        self.binance_api_key = binance_api_key
        self.binance_secret_key = binance_secret_key

    def load_wallets_from_csv(self) -> pd.DataFrame:
        """Load wallets from a CSV file.

        Returns:
            A list of all wallets loaded from the CSV file.
        """
        wallets = pd.read_csv(self.all_wallets_csv)
        return wallets

    def get_receiving_wallets(self, holding_wallet: str, num_needed: int) -> List[str]:
        """
        Get a list of receiving wallets in order that they appear in the csv.

        Args:
            holding_wallet: The wallet holding the funds to disperse.
            num_needed: The number of receiving wallets needed.

        Returns:
            A list of receiving wallets.
        """
        receiving_wallets = []
        for address in self.wallets['public_key']:
            if address != holding_wallet:
                receiving_wallets.append(address)
                if len(receiving_wallets) >= num_needed:
                    break
        return receiving_wallets

    def add_wallet(self, wallet_name: str = None, address: str = None, private_key: str = None) -> None:
        """Add a wallet to the wallets dataframe.

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

        assert self.wallets[self.wallets['public_key'] == address].shape[0] == 0, 'Wallet already present in csv.'

        self.wallets.loc[len(self.wallets), ['name', 'public_key', 'private_key']] = [wallet_name, address, private_key]
        logger.info(f'Wallet added to csv.\nName: {wallet_name}\nAddress: {address}\nPrivate key: {private_key}')
        return

    def remove_wallet(self, address: str) -> None:
        """Removes a wallet from the wallets DataFrame.

        Args:
            address: The public address of the wallet to remove.

        Raises:
            AssertionError: If the wallet address is not in the wallets DataFrame.
        """

        assert self.wallets[self.wallets['public_key'] == address].shape[0] > 0, 'Address not in csv.'
        self.wallets.drop(self.wallets[self.wallets['public_key'] == address].index, inplace=True)
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
            wallets = self.wallets[self.wallets['public_key'].isin(wallets)]
            assert wallets.shape[0] != 0, 'If entering multiple wallets, all must be in wallets csv.'

        wallets_needed = set(wallet['public_key'] for wallet in wallets.to_dict('records'))
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

        self.wallets['balance'] = self.wallets['public_key'].replace(wallet_dict)
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
            wallets = [wallet['public_key'] for wallet in self.wallets.to_dict('records')]

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
