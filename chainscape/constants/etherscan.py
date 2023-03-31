from typing import Dict

BASE_URL: str = "https://api.etherscan.io/api"

MODULES: Dict[str, str] = {
    'ACCOUNT': 'account',
    'CONTRACT': 'contract'
}

ACTIONS: Dict[str, str] = {
    "GETABI": 'getabi',
    "TXLIST": "txlist",
    "TOKENNFTTX": "tokennfttx",
    "SOURCECODE": "getsourcecode",
    "TOKEN1155TX": "token1155tx"
}