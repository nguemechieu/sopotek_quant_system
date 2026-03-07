import json
import keyring
import traceback


class CredentialManager:

    SERVICE_NAME = "SopotekTradingApp"
    ACCOUNT_INDEX = "accounts_index"

    # =====================================================
    # SAVE ACCOUNT
    # =====================================================

    @staticmethod
    def save_account(account_name: str, config: dict):

        try:

            data = json.dumps(config)

            keyring.set_password(
                CredentialManager.SERVICE_NAME,
                account_name,
                data
            )

            accounts = CredentialManager.list_accounts()

            if account_name not in accounts:

                accounts.append(account_name)

                keyring.set_password(
                    CredentialManager.SERVICE_NAME,
                    CredentialManager.ACCOUNT_INDEX,
                    json.dumps(accounts)
                )

        except Exception:
            traceback.print_exc()

    # =====================================================
    # LOAD ACCOUNT
    # =====================================================

    @staticmethod
    def load_account(account_name: str):

        try:

            data = keyring.get_password(
                CredentialManager.SERVICE_NAME,
                account_name
            )

            if not data:
                return None

            return json.loads(data)

        except Exception:
            traceback.print_exc()

            return None

    # =====================================================
    # LIST ACCOUNTS
    # =====================================================

    @staticmethod
    def list_accounts():

        try:

            data = keyring.get_password(
                CredentialManager.SERVICE_NAME,
                CredentialManager.ACCOUNT_INDEX
            )

            if not data:
                return []

            return json.loads(data)

        except Exception:
            traceback.print_exc()

            return []

    # =====================================================
    # DELETE ACCOUNT
    # =====================================================

    @staticmethod
    def delete_account(account_name):

        try:

            keyring.delete_password(
                CredentialManager.SERVICE_NAME,
                account_name
            )

            accounts = CredentialManager.list_accounts()

            if account_name in accounts:

                accounts.remove(account_name)

                keyring.set_password(
                    CredentialManager.SERVICE_NAME,
                    CredentialManager.ACCOUNT_INDEX,
                    json.dumps(accounts)
                )

        except Exception:
            traceback.print_exc()

    # =====================================================
    # LEGACY SUPPORT
    # =====================================================

    @staticmethod
    def save_credentials(exchange, api_key, secret):

        config = {
            "broker": {
                "exchange": exchange,
                "api_key": api_key,
                "secret": secret
            }
        }

        CredentialManager.save_account(exchange, config)

    @staticmethod
    def load_credentials(exchange):

        config = CredentialManager.load_account(exchange)

        if not config:
            return None, None

        broker = config.get("broker", {})

        return broker.get("api_key"), broker.get("secret")

    @staticmethod
    def delete_credentials(exchange):

        CredentialManager.delete_account(exchange)