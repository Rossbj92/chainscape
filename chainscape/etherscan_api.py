from typing import Dict, List

import requests

from log import logger


class EtherscanAPI:
    """
    Wrapper class for interacting with the Etherscan API.
    """
    BASE_URL = "https://api.etherscan.io/api"
    MODULES = {
        'ACCOUNT': 'account',
        'CONTRACT': 'contract'
    }
    ACCOUNT_MODULE = "account"
    CONTRACT_ACTIONS = {
        "GETABI": 'getabi',
        "TXLIST": "txlist",
        "TOKENNFTTX": "tokennfttx",
        "SOURCECODE": "getsourcecode",
        "TOKEN1155TX": "token1155tx"
    }
    def __init__(self, etherscan_api_key: str):
        """Initializes a new instance of the EtherscanAPI class.

        Args:
            etherscan_api_key: The API key for accessing the Etherscan API.
        """
        self.etherscan_api_key = etherscan_api_key

    def get_contract_abi(self, contract_address: str) -> Dict:
        """Retrieves the ABI of the specified contract.

        Args:
            contract_address: The address of the contract.

        Returns:
            The ABI of the contract.

        Raises:
            Exception: If the Etherscan API returns an error.
        """
        endpoint = self.CONTRACT_ACTIONS["GETABI"]
        module = self.MODULES['CONTRACT']
        url = f'{self.BASE_URL}?module={module}&action={endpoint}&address={contract_address}&apikey={self.etherscan_api_key}'
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()
        if response_json["status"] == "0":
            raise Exception(f"Etherscan API returned an error: {response_json['message']}")
        contract_abi = response_json["result"]
        logger.info(f'ABI successfully fetched for: {contract_address}')
        return contract_abi

    def get_contract_transactions(self, contract_address: str) -> List[Dict]:
            """Retrieves a list of transactions for the specified contract.

            Limited by the Etherscan API to only 10k transactions available.

            Args:
                contract_address: The address of the contract.

            Returns:
                A list of transaction dictionaries.

            Raises:
                Exception: If the Etherscan API returns an error.
            """
            endpoint = self.CONTRACT_ACTIONS["TXLIST"]
            module = self.MODULES['ACCOUNT']
            url = f"{self.BASE_URL}?module={module}&action={endpoint}&address={contract_address.lower()}&apikey={self.etherscan_api_key}"
            response = requests.get(url)
            response.raise_for_status()
            response_json = response.json()
            if response_json["status"] == "0":
                raise Exception(f"Etherscan API returned an error: {response_json['message']}")
            transactions = response_json["result"]
            logger.info(f'{len(transactions)} transactions successfully fetched for: {contract_address}')
            return transactions

    def get_contract_source_code(self, contract_address: str) -> str:
        """Retrieves the source code of the specified contract.

        Args:
            contract_address: The address of the contract.

        Returns:
            The source code of the contract.

        Raises:
            Exception: If the Etherscan API returns an error.
        """
        endpoint = self.CONTRACT_ACTIONS['SOURCECODE']
        module = self.MODULES['CONTRACT']
        url = f"{self.BASE_URL}?module={module}&action={endpoint}&address={contract_address.lower()}&apikey={self.etherscan_api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data["status"] == "0":
            raise Exception(f"Etherscan API returned an error: {data['message']}")
        source_code = data["result"][0]['SourceCode']
        logger.info(f'Source code successfully fetched for: {contract_address}')
        return source_code

    def get_wallet_token_transactions(self, holding_wallet: str, contract_address: str, token_type: str = 'erc721') -> Dict:
        """Retrieves token transactions for the specified contract and wallet.

        Method is currently adapted for ERC-721 and ERC-1155 tokens.

        Args:
            holding_wallet: The wallet address that holds the tokens.
            contract_address: The address of the contract.
            token_type: The type of the token (either 'erc721' or 'erc1155'). Default is 'erc721'.

        Returns:
            Dict: A dictionary containing the token transactions.

        Raises:
            Exception: If the Etherscan API returns an error.
        """
        module = self.MODULES['ACCOUNT']
        if token_type == 'erc721':
            endpoint = self.CONTRACT_ACTIONS["TOKENNFTTX"]
        elif token_type == 'erc1155':
            endpoint = self.CONTRACT_ACTIONS["TOKEN1155TX"]
        url = f"{self.BASE_URL}?module={module}&action={endpoint}&address={holding_wallet.lower()}&contractaddress={contract_address.lower()}&apikey={self.etherscan_api_key}"
        response = requests.get(url)
        data = response.json()
        if data["status"] == "0" and data['message'] != "No transactions found":
            raise Exception(f"Etherscan API returned an error: {data['message']}, {data['result']}")
        logger.info(f'{holding_wallet} wallet: {len(data["result"])} token transactions successfully fetched for {contract_address}')
        return data
