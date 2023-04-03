from typing import Dict
import json

class DisperseConstants:
    DISPERSE_CONTRACT: str = "0xD152f549545093347A162Dce210e7293f1452150"  # Disperse.app
    DISPERSE_ABI: Dict = json.loads(
        '''
        [{"constant":false,"inputs":[{"name":"token","type":"address"},{"name":"recipients","type":"address[]"},{"name":"values","type":"uint256[]"}],
        "name":"disperse","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"recipients","type":"address[]"},
        {"name":"values","type":"uint256[]"}],"name":"disperseEther","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"inputs":[],"payable":false,
        "stateMutability":"nonpayable","type":"constructor"}]
        '''
    )

class TransactionFields:
    MAX_FEE_KEY: str = "maxFeePerGas"
    MAX_PRIORITY_KEY: str = "maxPriorityFeePerGas"