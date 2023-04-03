import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Union, Callable

from log import logger


def execute_concurrent_tasks(
        wallets: List[str],
        task: Callable,
        executor: Optional[ThreadPoolExecutor] = None,
        *args, **kwargs
) -> List:
    """Execute concurrent tasks for a list of wallets.

    Args:
        wallets: A list of wallets to execute tasks on.
        task: The task to be executed on each wallet.
        executor: The ThreadPoolExecutor to use.
        *args: The arguments for the task.
        **kwargs: The keyword arguments for the task.

    Returns:
        A list of results for the executed tasks.
    """
    results = []
    wallets_needed = set(wallets)

    if executor is None:
        executor_created = True
        executor = ThreadPoolExecutor()
    else:
        executor_created = False

    start = time.time()
    logger.info(f'Beginning search for {len(wallets_needed)} wallets.')
    try:
        while wallets_needed:
            futures = [executor.submit(task, wallet, *args, **kwargs) for wallet in wallets_needed]
            for future in as_completed(futures):
                result = future.result()
                if isinstance(result, str):
                    wallets_needed.add(result)
                else:
                    wallets_needed.remove(result[0])
                    results.append(result)
            logger.info(f'{len(wallets) - len(wallets_needed)} wallets completed. {len(wallets_needed)} remain.')
    finally:
        if executor_created:
            executor.shutdown(wait=True)
    end = time.time()
    logger.info(f'{len(wallets)} wallets processed in {(end - start)} seconds.')
    return results


def worker_get_wallet_balances(wallet: str, get_wallet_balance: Callable) -> Union[tuple, str]:
    """Get the balance of a wallet.

        Args:
            wallet: The wallet to get the balance of.
            get_wallet_balance: The function to use to get the balance.

        Returns:
            A tuple containing the wallet and its balance or a string containing the wallet.
    """
    try:
        balance = get_wallet_balance(wallet)
        return (wallet, balance)
    except:
        time.sleep(1)
        return wallet


def worker_find_tokens(
        wallet: str,
        contract_address: str,
        token_type: str,
        get_token_ids: Callable,
        token_name: str
) -> Union[tuple, str]:
    """Find tokens held by a wallet.

    Args:
        wallet: The wallet to find tokens in.
        contract_address: The address of the contract to search for tokens in.
        token_type: The type of token to search for (e.g. "erc721", "erc1155").
        get_token_ids: The function to use to get the token IDs.
        token_name: The name of the token.

    Returns:
        A tuple containing the wallet and a list of dictionaries describing the
        tokens found or a string containing the wallet.
    """
    try:
        token_ids = get_token_ids(wallet, contract_address, token_type)
        if token_ids:
            return (wallet, [
                {'wallet': wallet, 'token_id': token_id, 'amount': num, 'token_name': token_name}
                for token_id, num in token_ids.items()
            ]
                    )
        return (wallet, None)
    except:
        return wallet