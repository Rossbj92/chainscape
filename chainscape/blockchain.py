import json
from time import sleep
from typing import List, Union

from web3 import Web3
from web3.contract import Contract

from etherscan_api import EtherscanAPI
from log import logger


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
        self.disperse_contract_address = "0xD152f549545093347A162Dce210e7293f1452150"  # Disperse.app
        self.disperse_abi = json.loads('[{"constant":false,"inputs":[{"name":"token","type":"address"},{"name":"recipients","type":"address[]"},{"name":"values","type":"uint256[]"}],"name":"disperse","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"recipients","type":"address[]"},{"name":"values","type":"uint256[]"}],"name":"disperseEther","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"}]')

    def get_current_gas_price(self) -> int:
        """Returns the current gas price in ether.

        Returns:
            The current gas price in ether.
        """
        return round(float(Web3.fromWei(self.w3.eth.gasPrice, 'gwei')))

    def get_wallet_balance(self, wallet_address: str, return_eth=True) -> float:
        """Returns the balance of the specified wallet in ether.

        Args:
            wallet: The wallet address.
            return_eth: If True, the balance is returned in ether. Otherwise, it is returned in wei.

        Returns:
            The balance of the specified wallet.
        """

        if wallet_address and not Web3.isAddress(wallet_address):
            raise ValueError("Invalid wallet address")
        wallet = Web3.toChecksumAddress(wallet_address)
        balance = self.w3.eth.getBalance(wallet)
        if return_eth:
            return float(Web3.fromWei(balance, 'ether'))
        else:
            return float(balance)

    def load_contract(self, contract_address: str, contract_abi: str) -> Contract:
        """Loads a smart contract.

        Args:
            contract_address: The address of the smart contract.
            contract_abi: The ABI (Application Binary Interface) of the smart contract.

        Returns:
            The loaded smart contract.
        """
        contract_address = Web3.toChecksumAddress(contract_address)
        contract = self.w3.eth.contract(address=contract_address, abi=contract_abi)
        return contract

    def get_transaction_status(self, tx_hash: str) -> str:
        """Returns the status of the transaction with the given hash.

        Args:
            tx_hash: The hash of the transaction.

        Returns:
            The status of the transaction: "pending", "failed", or "success".
        """
        receipt = self.w3.eth.getTransactionReceipt(tx_hash)
        if receipt is None:
            return "pending"
        elif receipt["status"] == 0:
            return "failed"
        else:
            return "success"

    def disperse_eth(
            self, sender_wallet: str,
            private_key: str,
            receiving_wallets: List[str],
            amounts: Union[List[float], float],
            max_fee: float = None,
            max_priority: float = None
    ) -> str:
        """Disperse ether to a list of wallets.

        Amounts for each wallet can be specified if different, and inputting
        1 value into amounts will send that amount to all receiving wallets.
        User can either enter custom max/priority fee or gas will be estimated
        based on current network.

        Args:
            sender_wallet: The wallet holding the funds to disperse.
            private_key: The private key of the sender wallet.
            receiving_wallets: A list of recipient wallets.
            amounts: A list of amounts to disperse to the recipient wallets.

        Returns:
            The transaction hash.
        """
        disperse_instance = self.load_contract(contract_address=self.disperse_contract_address, contract_abi=self.disperse_abi)

        if isinstance(amounts, float):
            amounts = [amounts] * len(receiving_wallets)

        sender_wallet_balance = self.get_wallet_balance(sender_wallet)
        assert sender_wallet_balance > sum(amounts), f'{sender_wallet} ETH balance of {sender_wallet_balance} too low for {sum(amounts)} disperse.'

        sender_wallet = Web3.toChecksumAddress(sender_wallet)
        receiving_wallets = [Web3.toChecksumAddress(wallet) for wallet in receiving_wallets]

        amounts_wei = [Web3.toWei(amount, 'ether') for amount in amounts]

        disperse_eth_gas_estimate = disperse_instance.functions.disperseEther(receiving_wallets,
                                                                              amounts_wei).estimateGas({
            'from': sender_wallet,
            'value': sum(amounts_wei)
        })

        if max_fee and max_priority:
            max_fee_wei = Web3.toWei(max_fee, 'gwei')
            max_priority_wei = Web3.toWei(max_priority, 'gwei')

            logger.info(f'Custom gas settings: {max_fee} max fee {max_priority} priority fee.')

            disperse_tx = disperse_instance.functions.disperseEther(receiving_wallets, amounts_wei).buildTransaction({
                'from': sender_wallet,
                'value': sum(amounts_wei),
                'gas': disperse_eth_gas_estimate,
                'maxFeePerGas': max_fee_wei,
                'maxPriorityFeePerGas': max_priority_wei,
                'nonce': self.w3.eth.get_transaction_count(sender_wallet),
            })
        else:
            gas_price = self.w3.eth.gasPrice

            est_gwei = round(float(Web3.fromWei(gas_price, 'gwei')))
            logger.info(f'Gas estimate for current tx: {est_gwei} gwei.')

            disperse_tx = disperse_instance.functions.disperseEther(receiving_wallets, amounts_wei).buildTransaction({
                'from': sender_wallet,
                'value': sum(amounts_wei),
                'gas': disperse_eth_gas_estimate,
                'gasPrice': self.w3.eth.gasPrice,
                'nonce': self.w3.eth.get_transaction_count(sender_wallet),
            })

        signed_disperse_tx = self.w3.eth.account.sign_transaction(disperse_tx, private_key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_disperse_tx.rawTransaction)

        return tx_hash.hex()

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
        """Disperse ERC-721 tokens from a holding wallet to a list of receiving wallets.

        Currently, a max of 1 token will be sent to each wallet in receiving_wallets.
        User can either enter custom max/priority fee or gas will be estimated based on
        current network.

        Args:
            holding_wallet: The wallet holding the tokens to disperse.
            private_key: The private key of the wallet holding the tokens.
            receiving_wallets: The wallets to receive the tokens.
            token_ids: The token IDs to disperse.
            max_fee: Max gas fee in gwei.
            max_priority: Max gas priority fee in gwei.

        Returns:
            The transaction hash of the disperse transaction.
        """
        if not etherscan_api_key:
            assert self.etherscan, 'Must include Etherscan API key.'
        else:
            self.etherscan = EtherscanAPI(etherscan_api_key)

        token_contract_address = Web3.toChecksumAddress(token_contract_address)
        contract_abi = self.etherscan.get_contract_abi(token_contract_address)
        contract_instance = self.load_contract(contract_address=token_contract_address, contract_abi=contract_abi)

        holding_wallet = Web3.toChecksumAddress(holding_wallet)
        receiving_wallets = [Web3.toChecksumAddress(address) for address in receiving_wallets]

        tx_hashes = []
        for token_id, receiving_wallet in zip(token_ids, receiving_wallets):
            nonce = self.w3.eth.getTransactionCount(holding_wallet)
            gas_estimate = contract_instance.functions.safeTransferFrom(
                holding_wallet, receiving_wallet, int(token_id)
            ).estimateGas({'from': holding_wallet})

            if max_fee and max_priority:
                max_fee_wei = Web3.toWei(max_fee, 'gwei')
                max_priority_wei = Web3.toWei(max_priority, 'gwei')
                logger.info(f'Custom gas settings: {max_fee} max fee {max_priority} priority fee.')
                txn = {
                    'to': token_contract_address,
                    'value': 0,
                    'gas': gas_estimate,
                    'maxFeePerGas': max_fee_wei,
                    'maxPriorityFeePerGas': max_priority_wei,
                    'nonce': nonce,
                    'chainId': self.w3.eth.chainId,
                    'data': contract_instance.functions.safeTransferFrom(
                        holding_wallet, receiving_wallet, int(token_id)
                    ).buildTransaction({'gas': gas_estimate, 'nonce': nonce})['data']
                }
            else:
                gas_price = self.w3.eth.gasPrice

                est_gwei = round(float(Web3.fromWei(gas_price, 'gwei')))
                logger.info(f'Gas estimate for current tx: {est_gwei} gwei.')

                txn = {
                    'to': token_contract_address,
                    'value': 0,
                    'gas': gas_estimate,
                    'gasPrice': gas_price,
                    'nonce': nonce,
                    'chainId': self.w3.eth.chainId,
                    'data': contract_instance.functions.safeTransferFrom(
                        holding_wallet, receiving_wallet, int(token_id)
                    ).buildTransaction({'gas': gas_estimate, 'gasPrice': gas_price, 'nonce': nonce})['data']
                }

            signed_txn = self.w3.eth.account.sign_transaction(txn, private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
            hash_str = tx_hash.hex()
            logger.info(f'Token {token_id} sending from {holding_wallet} to {receiving_wallet} at hash {hash_str}.')

            self.w3.eth.wait_for_transaction_receipt(tx_hash)

            while self.get_transaction_status(tx_hash) == 'pending':
                sleep(1)
            if self.get_transaction_status(tx_hash) == 'failed':
                return f'Token {token_id} sending from {holding_wallet} to {receiving_wallet} at hash {hash_str}.'
            logger.info('Tx succeeded.')

            tx_hashes.append(hash_str)
        return tx_hashes
