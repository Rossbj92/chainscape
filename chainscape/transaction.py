from time import sleep
from typing import List, Union, Dict

from web3 import Web3

from constants.eth_blockchain import TransactionFields
from etherscan_api import EtherscanAPI
from log import logger


class ContractTransaction:
    """
    Methods relating to sending/querying eth transactions.
    """
    def __init__(self, web3_instance):
        self.w3 = web3_instance

    def build_transaction(
            self, contract_instance,
            function_name: str,
            function_args: List,
            sender_wallet: str,
            value: int = 0,
            **kwargs: Dict
    ):

        gas_estimate = getattr(contract_instance.functions, function_name)(*function_args).estimate_gas({
            'from': sender_wallet,
            'value': value
        })

        transaction_data = getattr(contract_instance.functions, function_name)(*function_args).build_transaction({
            'gas': gas_estimate,
            'nonce': self.w3.eth.get_transaction_count(sender_wallet),
        })

        transaction = {
            'to': contract_instance.address,
            'value': value,
            'gas': gas_estimate,
            'nonce': self.w3.eth.get_transaction_count(sender_wallet),
            'chainId': self.w3.eth.chain_id,
            'data': transaction_data['data']
        }

        gas_price = {k: Web3.to_wei(v, 'gwei') for k, v in kwargs.items() if v and k in [TransactionFields.MAX_FEE_KEY, TransactionFields.MAX_PRIORITY_KEY]}
        if gas_price:
            logger.info(f'Custom gas settings: {kwargs[TransactionFields.MAX_FEE_KEY]} max fee {kwargs[TransactionFields.MAX_PRIORITY_KEY]} priority fee.')
        else:
            gas_price = {'gasPrice': self.w3.eth.gas_price}
            est_gwei = round(float(Web3.from_wei(gas_price['gasPrice'], 'gwei')))
            logger.info(f'Gas estimate for current tx: {est_gwei} gwei.')

        transaction.update(gas_price)

        return transaction, gas_estimate

    def send_transaction(self, transaction: dict, private_key: str) -> str:
        """Sign and send eth transaction."""
        signed_txn = self.w3.eth.account.sign_transaction(transaction, private_key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        return tx_hash.hex()

    def get_transaction_status(self, tx_hash: str) -> str:
        """Returns the status of the transaction with the given hash.

        Args:
            tx_hash: The hash of the transaction.

        Returns:
            The status of the transaction: "pending", "failed", or "success".
        """
        receipt = self.w3.eth.get_transaction_receipt(tx_hash)
        if receipt is None:
            return "pending"
        elif receipt["status"] == 0:
            return "failed"
        else:
            return "success"


class Disperser:
    """
    Used for dispersing ether & erc-721 tokens.
    """
    def __init__(self, web3_instance, etherscan_api_key: str = None):
        self.w3 = web3_instance
        self.tx_handler = ContractTransaction(web3_instance)
        self.etherscan_api = EtherscanAPI(etherscan_api_key) if etherscan_api_key else None

    def disperse_eth(
            self, disperse_instance,
            sender_wallet: str,
            private_key: str,
            receiving_wallets: List[str],
            amounts: Union[List[float], float],
            max_fee: float = None,
            max_priority_fee: float = None
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
        assert (max_fee is not None and max_priority_fee is not None) or (
                    max_fee is None and max_priority_fee is None), "Either both max_fee and max_priority_fee should be provided or both should be None."

        sender_wallet = Web3.to_checksum_address(sender_wallet)
        receiving_wallets = [Web3.to_checksum_address(wallet) for wallet in receiving_wallets]

        amounts_wei = [Web3.to_wei(amount, 'ether') for amount in amounts]

        transaction, gas_estimate = self.tx_handler.build_transaction(
            disperse_instance,
            "disperseEther",
            [receiving_wallets, amounts_wei],
            sender_wallet,
            value=sum(amounts_wei),
            maxFeePerGas=max_fee,
            maxPriorityFeePerGas=max_priority_fee
        )

        tx_hash = self.tx_handler.send_transaction(transaction, private_key)
        logger.info(f'Dispersing {sum(amounts)} to {len(receiving_wallets)} wallets at hash {tx_hash}')
        return tx_hash

    def disperse_erc721(
            self,
            contract_instance,
            holding_wallet: str,
            private_key: str,
            receiving_wallets: list,
            token_ids: list,
            max_fee: float = None,
            max_priority_fee: float = None
    ) -> List[str]:
        """Disperse ERC-721 tokens from a holding wallet to a list of receiving wallets.

        Currently, a max of 1 token will be sent to each wallet in receiving_wallets.
        User can either enter custom max/priority fee or gas will be estimated based on
        current network.

        Args:
            contract_instance: Web3 contract instance for token contract.
            holding_wallet: The wallet holding the tokens to disperse.
            private_key: The private key of the wallet holding the tokens.
            receiving_wallets: The wallets to receive the tokens.
            token_ids: The token IDs to disperse.
            max_fee: Max gas fee in gwei.
            max_priority_fee: Max gas priority fee in gwei.

        Returns:
            The transaction hash of the disperse transaction.
        """
        assert (max_fee is not None and max_priority_fee is not None) or (
                    max_fee is None and max_priority_fee is None), \
            "Either both max_fee and max_priority_fee should be provided or both should be None."

        holding_wallet = Web3.to_checksum_address(holding_wallet)
        receiving_wallets = [Web3.to_checksum_address(address) for address in receiving_wallets]

        tx_hashes = []
        for token_id, receiving_wallet in zip(token_ids, receiving_wallets):
            transaction, gas_estimate = self.tx_handler.build_transaction(
                contract_instance,
                "safeTransferFrom",
                [holding_wallet, receiving_wallet, int(token_id)],
                holding_wallet,
                maxFeePerGas=max_fee,
                maxPriorityFeePerGas=max_priority_fee
            )

            tx_hash = self.tx_handler.send_transaction(transaction, private_key)
            logger.info(f'Token {token_id} sending from {holding_wallet} to {receiving_wallet} at hash {tx_hash}.')

            self.w3.eth.wait_for_transaction_receipt(tx_hash)

            while self.tx_handler.get_transaction_status(tx_hash) == 'pending':
                sleep(1)
            if self.tx_handler.get_transaction_status(tx_hash) == 'failed':
                return f'Token {token_id} sending from {holding_wallet} to {receiving_wallet} at hash {tx_hash} failed.'
            logger.info('Tx succeeded.')

            tx_hashes.append(tx_hash)
        return tx_hashes
