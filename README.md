# Chainscape

Chainscape is a Python library that simplifies Ethereum blockchain and wallet interactions. It's designed for developers, traders, and enthusiasts who want to streamline their workflow. The tasks across the modules such as sending transactions, querying balances, and managing wallets, are all things that I use consistently, and hopefully they can be helpful to others.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Structure](#structure)
- [Contributing](#contributing)
- [License](#license)

## Installation

Chainscape is currently not available on PyPI and can be installed directly from the GitHub repository.

1. Clone the repository:

```bash
git clone https://github.com/yourusername/chainscape.git
cd chainscape
pip install -r requirements.txt
pip install .
```

## Usage

### WalletManager

`WalletManager` is a class for managing Ethereum wallets. It provides methods to load wallets from a CSV file, add and remove wallets, get wallet balances, export wallet data to a CSV file, and interact with tokens on the Ethereum blockchain.

Example usage:

```python
from chainscape import WalletManager

wallet_manager = WalletManager(
    rpc_url="https://your.ethereum.node/rpc",
    all_wallets_csv="data/wallets.csv",
    etherscan_api_key="your_etherscan_api_key"
)

# Add a new wallet 
wallet_manager.add_wallet(wallet_name="New Wallet", address="0x123...", private_key="0xabc...")

# Remove a wallet
wallet_manager.remove_wallet(address="0x123...")

# Get wallet balances
balances = wallet_manager.get_wallets_balances()

# Export wallet data to a CSV file
wallet_manager.export_wallets_to_csv(file_path="data/updated_wallets.csv")

# Get token IDs for a wallet and contract address
token_ids = wallet_manager.get_token_ids(wallet="0x123...", contract_address="0xabc...")

# Find tokens held by list of wallets for a given contract address
tokens = wallet_manager.find_tokens(contract_address="0xabc...")
```

### EtherscanAPI

The EtherscanAPI class is a simple wrapper around the Etherscan API that allows you to interact with Ethereum contracts and their transactions. It provides several methods to retrieve contract information such as ABI, source code, and token transactions.

```python
from chainscape import EtherscanAPI

# Initialize the EtherscanAPI with your Etherscan API key
etherscan_api = EtherscanAPI(etherscan_api_key="YOUR_ETHERSCAN_API_KEY")

# Retrieve the ABI of a contract
contract_address = "0x123..."
contract_abi = etherscan_api.get_contract_abi(contract_address)
print(f"Contract ABI: {contract_abi}")

# Retrieve the source code of a contract
contract_source_code = etherscan_api.get_contract_source_code(contract_address)
print(f"Contract Source Code: {contract_source_code}")

# Get transactions for a contract
contract_transactions = etherscan_api.get_contract_transactions(contract_address)
print(f"Contract Transactions: {contract_transactions}")

# Get token transactions for a wallet's contract interactions
holding_wallet = "0x456..."
token_type = "erc721"
token_transactions = etherscan_api.get_token_transactions(holding_wallet, contract_address, token_type)
print(f"Token Transactions: {token_transactions}")

token_type = "erc1155"
token_transactions = etherscan_api.get_token_transactions(holding_wallet, contract_address, token_type)
print(f"ERC-1155 Token Transactions: {token_transactions}")

```

### Blockchain

The Blockchain class provides a simple wrapper around the Web3.py library for interacting directly with the Ethereum blockchain. It allows you to perform common tasks such as checking gas prices, wallet balances, and dispersing ether or ERC-721 tokens.

```python
from chainscape import Blockchain

# Initialize the Blockchain with your Ethereum node's RPC URL
rpc_url = "YOUR_ETHEREUM_NODE_RPC_URL"
etherscan_api_key = "YOUR_ETHERSCAN_API_KEY"
blockchain = Blockchain(rpc_url, etherscan_api_key)

# Get the current gas price
current_gas_price = blockchain.get_current_gas_price()
print(f"Current Gas Price: {current_gas_price} Gwei")

# Get the balance of a wallet
wallet_address = "0x123..."
wallet_balance = blockchain.get_wallet_balance(wallet_address)
print(f"Wallet Balance: {wallet_balance} ETH")

# Load a smart contract
contract_address = "0x456..."
contract_abi = "YOUR_CONTRACT_ABI" # can use EtherscanAPI get_contract_abi
contract = blockchain.load_contract(contract_address, contract_abi)

# Get the status of a transaction
tx_hash = "0x789..."
transaction_status = blockchain.get_transaction_status(tx_hash)
print(f"Transaction Status: {transaction_status}")

# Disperse ether to a list of wallets
sender_wallet = "0xABC..."
private_key = "YOUR_PRIVATE_KEY"
receiving_wallets = ["0xDEF...", "0xGHI..."]
amounts = [0.1, 0.2]  # In ether

tx_hash = blockchain.disperse_eth(
    sender_wallet=sender_wallet,
    private_key=private_key,
    receiving_wallets=receiving_wallets,
    amounts=amounts
)
print(f"Disperse Ether Transaction Hash: {tx_hash}")

# Disperse ERC-721 tokens
holding_wallet = "0xJKL..."
private_key = "YOUR_PRIVATE_KEY"
token_contract_address = "0x123"
token_ids = [1, 2, 3]
receiving_wallets = ["0xMNO...", "0xPQR...", "0xSTU..."]

tx_hash = blockchain.disperse_erc721(
    holding_wallet=holding_wallet,
    private_key=private_key,
    receiving_wallets=receiving_wallets,
    token_contract_address=token_contract_address,
    token_ids=token_ids
)
print(f"Disperse ERC-721 Transaction Hash: {tx_hash}")


```

## Structure
The Chainscape repository is organized into the following folders:

- `chainscape`: Contains the main Python modules for the library.
- `templates`: Includes a wallets.csv file for storing wallet data in the WalletManager. 
  - Provided csv column structure should not change
- `tutorials`: Will house tutorials to help users get started with the library.


## Contributing
Contributions to Chainscape are welcome and appreciated! To contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch.
3. Commit your changes to the new branch.
4. Create a pull request describing the changes you made.