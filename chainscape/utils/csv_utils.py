import pandas as pd
from typing import List
from wallet import Wallet


def load_wallets_from_csv(file_path: str) -> List[Wallet]:
    """Load wallets from a CSV file."""
    wallets_df = pd.read_csv(file_path)
    wallets = [
        Wallet(name=row['name'], address=row['address'], private_key=row['private_key'])
        for _, row in wallets_df.iterrows()
    ]
    return wallets


def export_wallets_to_csv(wallets: List[Wallet], file_path: str) -> None:
    """Export wallets to a csv file."""
    wallets_df = pd.DataFrame(
        [
            {
            'name': wallet.name,
            'address': wallet.address,
            'private_key': wallet.private_key,
            'balance': wallet.balance
            }
        for wallet in wallets
        ]
    )
    wallets_df.to_csv(file_path, index=False)
