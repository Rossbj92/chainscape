import unittest

import pandas as pd

from tests.test_constants import VITALIK_ADDRESS, ETH_DEV, TEST_WALLET_CSV_PATH
from wallet_manager import WalletManager


class TestWalletManager(unittest.TestCase):
    def setUp(self):
        self.wallet_manager = WalletManager(wallets_csv_path=TEST_WALLET_CSV_PATH)
        self.wallets = None

    def test_add_wallet(self):
        wallet_manager = WalletManager()
        wallet_manager.add_wallet(wallet_name='ethdev',
                                  address=ETH_DEV,
                                  private_key='private_key')
        self.assertEqual(len(wallet_manager.wallets), 1)

    def test_remove_wallet(self):
        wallet_manager = WalletManager(wallets_csv_path=TEST_WALLET_CSV_PATH)
        wallet_manager.remove_wallet(VITALIK_ADDRESS)
        self.assertEqual(len(wallet_manager.wallets), 1)

    def test_load_wallets_from_csv(self):
        wallet_manager = WalletManager()
        wallet_manager.wallets = wallet_manager.load_wallets_from_csv(wallets_csv_path=TEST_WALLET_CSV_PATH)
        expected_output = pd.read_csv(TEST_WALLET_CSV_PATH)
        self.assertEqual(expected_output['address'].to_list(), [w.address for w in wallet_manager.wallets])
        self.assertEqual(expected_output['name'].to_list(), [w.name for w in wallet_manager.wallets])
        self.assertEqual(expected_output['private_key'].to_list(), [w.private_key for w in wallet_manager.wallets])


    def test_get_wallet_dataframe(self):
        expected_output = pd.read_csv(TEST_WALLET_CSV_PATH)
        actual_output = self.wallet_manager.get_wallet_dataframe()
        self.assertEqual(expected_output['address'].to_dict(), actual_output['address'].to_dict())
        self.assertEqual(expected_output['name'].to_dict(), actual_output['name'].to_dict())
        self.assertEqual(expected_output['private_key'].to_dict(), actual_output['private_key'].to_dict())

    def test_get_wallets(self):
        wallet_manager = WalletManager(wallets_csv_path=TEST_WALLET_CSV_PATH)
        test_wallet_df = pd.read_csv(TEST_WALLET_CSV_PATH)
        all_wallets = wallet_manager.get_wallets()
        rm_vitalik = wallet_manager.get_wallets(excluded_address=VITALIK_ADDRESS)
        get_one = wallet_manager.get_wallets(num_needed=1)
        self.assertEqual(len(all_wallets), test_wallet_df.shape[0])
        self.assertNotIn(VITALIK_ADDRESS, rm_vitalik)
        self.assertEqual(1, len(get_one))

if __name__ == '__main__':
    unittest.main()