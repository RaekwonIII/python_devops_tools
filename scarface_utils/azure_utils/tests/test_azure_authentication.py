from io import StringIO
from msrest.exceptions import AuthenticationError

from unittest import main, TestCase
from unittest.mock import patch, MagicMock, mock_open

from scarface_utils.azure_utils.azure_authentication import AzureAuthentication, AzureAuthenticationException

CONFIG_FILE = u"file.cfg"

CONFIG_TEMPLATE = u'[KeyVault]\n' \
                  u'client_id={}\n' \
                  u'client_secret={}\n' \
                  u'tenant_id={}\n' \
                  u'key_vault_uri={}'

KV_CLIENT_ID = 'test-client-id'
KV_SECRET_ID = 'test-secret-id'
KV_TENANT_ID = 'test-tenant-id'
KV_URI = 'https://testkv.vault.azure.net:443'

DEFAULT_RESOURCE = 'https://management.core.windows.net/'
BATCH_RESOURCE = 'https://batch.core.windows.net/'

CONFIG_TEMPLATE_BATCH = u'[Batch]\n' \
                        u'client_id={}\n' \
                        u'client_secret={}\n' \
                        u'tenant_id={}\n' \
                        u'key_vault_uri={}\n' \
                        u'resource={}'


class AzureAuthenticationTest(TestCase):

    @classmethod
    @patch('scarface_utils.azure_utils.azure_authentication.ServicePrincipalCredentials')
    @patch('scarface_utils.azure_utils.azure_authentication.KeyVaultClient')
    @patch('scarface_utils.azure_utils.azure_authentication.KeyVaultAuthentication')
    def setUpClass(cls, mocked_kv_auth, mocked_kv_client, mocked_sp_credentials):
        cls.test_secret = 'test-secret-value'
        test_secret_1 = MagicMock()
        test_secret_1.value = cls.test_secret

        mocked_kv_client.configure_mock(**{'get_secret.return_value': test_secret_1})

        cls.azure_authentication = AzureAuthentication(
            kv_client=mocked_kv_client,
            vault_uri=KV_TENANT_ID,
            credentials=mocked_sp_credentials
        )

    def setUp(self):
        pass

    @patch('scarface_utils.azure_utils.azure_authentication.ServicePrincipalCredentials')
    @patch('scarface_utils.azure_utils.azure_authentication.KeyVaultClient')
    @patch('scarface_utils.azure_utils.azure_authentication.KeyVaultAuthentication')
    @patch('builtins.open', new_callable=mock_open)
    def test_constructor(self, mocked_open, mocked_kv_auth, mocked_kv_client, mocked_sp_credentials):
        file_mock = StringIO(
            CONFIG_TEMPLATE.format(KV_CLIENT_ID, KV_SECRET_ID, KV_TENANT_ID, KV_URI)
        )
        mocked_open.return_value = file_mock

        sp_credentials_mock = MagicMock()
        mocked_sp_credentials.return_value = sp_credentials_mock

        kv_auth_mock = MagicMock()
        mocked_kv_auth.return_value = kv_auth_mock

        kv_client_mock = MagicMock()
        mocked_kv_client.return_value = kv_client_mock

        az_authentication = AzureAuthentication.from_config_file(CONFIG_FILE)

        mocked_sp_credentials.assert_called_with(client_id=KV_CLIENT_ID,
                                                 secret=KV_SECRET_ID,
                                                 tenant=KV_TENANT_ID,
                                                 resource=DEFAULT_RESOURCE)
        mocked_kv_auth.assert_called_with(credentials=sp_credentials_mock)
        mocked_kv_client.assert_called_with(kv_auth_mock)

        self.assertEqual(az_authentication.vault_uri, KV_URI)
        self.assertEqual(az_authentication.kv_client, kv_client_mock)

        mocked_kv_client.side_effect = AuthenticationError("Get Token request returned http error: 400")
        file_mock = StringIO(
            CONFIG_TEMPLATE.format(KV_CLIENT_ID, KV_SECRET_ID, KV_TENANT_ID, KV_URI, )
        )
        mocked_open.return_value = file_mock

        with self.assertRaises(AzureAuthenticationException):
            AzureAuthentication.from_config_file(CONFIG_FILE)

    def test_get_secret(self):
        secret_value = self.azure_authentication.get_secret('test-secret')

        self.assertEqual(secret_value, self.test_secret)


if __name__ == '__main__':
    main()
