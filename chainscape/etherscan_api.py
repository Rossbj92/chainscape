from typing import Dict, List

import requests

from constants.etherscan import BASE_URL, MODULES, ACTIONS
from log import logger


class EtherscanAPI:
    """
    Wrapper class for interacting with the Etherscan API.
    """
    def __init__(self, etherscan_api_key: str):
        """Initializes a new instance of the EtherscanAPI class.

        Args:
            etherscan_api_key: The API key for accessing the Etherscan API.
        """
        self.etherscan_api_key = etherscan_api_key

    def _make_api_call(self, endpoint: str, module: str, address: str, **params) -> Dict:
        """Makes an API call to the Etherscan API.

        Args:
            endpoint: The API endpoint to call.
            module: The API module to use.
            address: The address to query.
            **params: Additional parameters to include in the API call.

        Returns:
            A dictionary representing the JSON response from the API.

        Raises:
            Exception: If the Etherscan API returns an error.
        """
        params.update({'module': module, 'action': endpoint, 'apikey': self.etherscan_api_key})
        url = f'{BASE_URL}?{"&".join(f"{k}={v}" for k, v in params.items())}&address={address}'
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()
        if response_json["status"] == "0" and response_json['message'] != "No transactions found":
            raise Exception(f"Etherscan API returned an error: {response_json['message']}")
        return response_json['result']

    def get_contract_abi(self, contract_address: str) -> Dict:
        """Retrieves the ABI of the specified contract.

        Args:
            contract_address: The address of the contract.

        Returns:
            The ABI of the contract.

        Raises:
            Exception: If the Etherscan API returns an error.
        """
        endpoint = ACTIONS["GETABI"]
        module = MODULES['CONTRACT']
        result = self._make_api_call(endpoint, module, contract_address)
        logger.info(f"Retrieved ABI for contract: address={contract_address}")
        return result

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
        endpoint = ACTIONS["TXLIST"]
        module = MODULES['ACCOUNT']
        result = self._make_api_call(endpoint, module, contract_address.lower())
        logger.info(f"Retrieved transactions for contract: address={contract_address}")
        return result

    def get_contract_source_code(self, contract_address: str) -> str:
        """Retrieves the source code of the specified contract.

        Args:
            contract_address: The address of the contract.

        Returns:
            The source code of the contract.

        Raises:
            Exception: If the Etherscan API returns an error.
        """
        endpoint = ACTIONS['SOURCECODE']
        module = MODULES['CONTRACT']
        result = self._make_api_call(endpoint, module, contract_address.lower())
        source_code = result[0]['SourceCode']
        logger.info(f"Retrieved source code for contract: address={contract_address}")
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
        module = MODULES['ACCOUNT']
        if token_type == 'erc721':
            endpoint = ACTIONS["TOKENNFTTX"]
        elif token_type == 'erc1155':
            endpoint = ACTIONS["TOKEN1155TX"]
        result = self._make_api_call(endpoint, module, holding_wallet.lower(), contractaddress=contract_address.lower())
        logger.info(f"Retrieved token transactions for wallet and contract: wallet={holding_wallet}, contract={contract_address}")
        return result
