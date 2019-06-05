import logging
import os
from io import StringIO
from unittest import main, TestCase

from azure.common import AzureHttpError
from unittest.mock import patch, MagicMock, mock_open

from utils.azure_utils.azure_storage import AzureStorage

CONFIG_TEMPLATE = '[KeyVault]\n' \
                  'client_id={}\n' \
                  'client_secret={}\n' \
                  'tenant_id={}\n' \
                  'key_vault_uri={}\n' \
                  'storage_secret_name={}\n' \
                  '[Storage]\n' \
                  'storageaccountname={}\n' \
                  'storageaccountkey={}\n' \
                  'containername={}\n' \
                  'sastoken={}\n' \
                  '[Files]\n' \
                  'allowed={}'

KV_CLIENT_ID = 'test-client-id'
KV_SECRET_ID = 'test-secret-id'
KV_TENANT_ID = 'test-tenant-id'
KV_URI = 'https://testkv.vault.azure.net:443'
STORAGE_SECRET_NAME = 'test-secret-name'

BERGAMOT_STORAGE_ACCOUNT_NAME = 'storageaccountname'
BERGAMOT_STORAGE_ACCOUNT_KEY = 'storageaccountkey'
BERGAMOT_CONTAINER_NAME = 'container_test'
SAS_TOKEN = 'sastoken'

ALLOWED_FILES = ["txt", "zip"]
DEFAULT_MAX_CONNECTIONS = 8

CONFIG_FILE = 'file.cfg'

BLOB_NAME = 'blob_test'
BLOB_URL = 'https://storageaccountname.blob.core.windows.net/{}/{}'

FILE_A = {'file_name': 'a.txt', 'file_content': 'hello'}

CONFIG_DICT = {
    'account_name': BERGAMOT_STORAGE_ACCOUNT_NAME,
    'account_key': BERGAMOT_STORAGE_ACCOUNT_KEY,
    'container_name': BERGAMOT_CONTAINER_NAME
}

STORAGE_SECRET = SAS_TOKEN


class AzureStorageTest(TestCase):

    @classmethod
    @patch('utils.azure_utils.azure_storage.BlockBlobService')
    def setUpClass(cls, mocked_block_blob):
        logging.basicConfig(level=logging.ERROR)
        obj_a = MagicMock()
        obj_a.name = 'a'
        obj_b = MagicMock()
        obj_b.name = 'b'

        mocked_block_blob.configure_mock(
            **{
                'list_blobs.return_value': [obj_a, obj_b],
                'list_containers.return_value': [obj_a, obj_b],
                'create_container.return_value': True,
                'get_blob_to_path.return_value': MagicMock(),
            }
        )

        cls.azure_storage = AzureStorage(
            block_blob_service=mocked_block_blob,
            container_name=BERGAMOT_CONTAINER_NAME,
            allowed_files=ALLOWED_FILES,
            config_file=CONFIG_FILE,
            max_connections=DEFAULT_MAX_CONNECTIONS,
            logger=logging.getLogger(__name__),
        )

    def setUp(self):
        pass

    @patch('utils.azure_utils.azure_storage.BlockBlobService')
    @patch('builtins.open', new_callable=mock_open)
    def test_constructor(
            self,
            mocked_open,
            mock_blockblobservice,
    ):
        file_mock = StringIO(
            CONFIG_TEMPLATE.format(KV_CLIENT_ID, KV_SECRET_ID, KV_TENANT_ID, KV_URI, STORAGE_SECRET_NAME,
                                   BERGAMOT_STORAGE_ACCOUNT_NAME, BERGAMOT_STORAGE_ACCOUNT_KEY, BERGAMOT_CONTAINER_NAME,
                                   SAS_TOKEN, '["txt", "zip"]')
        )

        mocked_open.return_value = file_mock
        blob_service_mock = MagicMock()
        mock_blockblobservice.return_value = blob_service_mock

        az_storage = AzureStorage.from_config_file_path(CONFIG_FILE)
        mock_blockblobservice.assert_called_with(account_name=BERGAMOT_STORAGE_ACCOUNT_NAME,
                                                 account_key=BERGAMOT_STORAGE_ACCOUNT_KEY,
                                                 sas_token=SAS_TOKEN)

        self.assertEqual(az_storage.container_name, BERGAMOT_CONTAINER_NAME)
        self.assertEqual(az_storage.allowed_files, ALLOWED_FILES)
        self.assertEqual(az_storage.MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS)
        self.assertEqual(az_storage.config_file, CONFIG_FILE)

    @patch('utils.azure_utils.azure_storage.BlockBlobService')
    def test_constructor_from_config(
            self,
            mock_blockblobservice,
    ):
        blob_service_mock = MagicMock()
        mock_blockblobservice.return_value = blob_service_mock

        az_storage = AzureStorage.from_config(**CONFIG_DICT)
        mock_blockblobservice.assert_called_with(
            account_name=BERGAMOT_STORAGE_ACCOUNT_NAME,
            account_key=BERGAMOT_STORAGE_ACCOUNT_KEY,
            sas_token=None,
        )

        self.assertEqual(az_storage.container_name, BERGAMOT_CONTAINER_NAME)
        self.assertEqual(az_storage.MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS)

    @patch('utils.azure_utils.azure_storage.AzureAuthentication')
    @patch('utils.azure_utils.azure_storage.BlockBlobService')
    @patch('builtins.open', new_callable=mock_open)
    def test_constructor_from_key_vault(self, mocked_open, mocked_blockblobservice, mocked_azure_auth):
        file_mock = StringIO(
            CONFIG_TEMPLATE.format(KV_CLIENT_ID, KV_SECRET_ID, KV_TENANT_ID, KV_URI, STORAGE_SECRET_NAME,
                                   BERGAMOT_STORAGE_ACCOUNT_NAME, BERGAMOT_STORAGE_ACCOUNT_KEY, BERGAMOT_CONTAINER_NAME,
                                   SAS_TOKEN, '["txt", "zip"]')
        )
        mocked_open.return_value = file_mock

        blob_service_mock = MagicMock()
        mocked_blockblobservice.return_value = blob_service_mock

        azure_auth_mock = MagicMock()
        attrs = {'get_secret.return_value': STORAGE_SECRET}
        azure_auth_mock.configure_mock(**attrs)
        mocked_azure_auth.configure_mock(**{'from_config_file.return_value': azure_auth_mock})

        az_storage = AzureStorage.from_key_vault_config(CONFIG_FILE)

        mocked_azure_auth.from_config_file.assert_called_with(CONFIG_FILE)
        mocked_azure_auth.from_config_file().get_secret.assert_called_with(STORAGE_SECRET_NAME)
        mocked_blockblobservice.assert_called_with(account_name=BERGAMOT_STORAGE_ACCOUNT_NAME,sas_token=SAS_TOKEN)

        self.assertEqual(az_storage.container_name, BERGAMOT_CONTAINER_NAME)
        self.assertEqual(az_storage.allowed_files, ALLOWED_FILES)
        self.assertEqual(az_storage.MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS)
        self.assertEqual(az_storage.config_file, CONFIG_FILE)

    @patch('builtins.open', new_callable=mock_open)
    def test_get_blob_url(self, mocked_open):
        file_mock = StringIO(
            CONFIG_TEMPLATE.format(KV_CLIENT_ID, KV_SECRET_ID, KV_TENANT_ID, KV_URI, STORAGE_SECRET_NAME,
                                   BERGAMOT_STORAGE_ACCOUNT_NAME, "", "", "", "")
        )
        mocked_open.return_value = file_mock
        blob_url = self.azure_storage.get_blob_url(BLOB_NAME)
        expected_blob_url = BLOB_URL.format(BERGAMOT_CONTAINER_NAME, BLOB_NAME)
        self.assertEqual(blob_url, expected_blob_url)

    def test_create_container(self):
        self.assertEqual(self.azure_storage.create_container(), True)

    def test_list_blobs(self):
        list_blobs = self.azure_storage.list_blobs()
        expected_list_blobs = ["a", "b"]
        self.assertEqual(list_blobs, expected_list_blobs)

    def test_list_containers(self):
        list_containers = self.azure_storage.list_containers()
        expected_list_blobs = ["a", "b"]
        self.assertEqual(list_containers, expected_list_blobs)

    def test_container_exists(self):
        self.azure_storage.block_blob_service.configure_mock(
            **{
                'exists.return_value': True,
                'exists.side_effect': None,
            }
        )
        exists = self.azure_storage.container_exists(BERGAMOT_CONTAINER_NAME)
        self.assertEqual(exists, True)

    def test_container_does_not_exists(self):
        self.azure_storage.block_blob_service.configure_mock(
            **{
                'exists.side_effect': AzureHttpError("not found", 404),
            }
        )
        exists = self.azure_storage.container_exists(BERGAMOT_CONTAINER_NAME)
        self.assertEqual(exists, False)

    def test_blob_exists(self):
        mocked_properties = MagicMock(content_length=100)
        mock_blob = MagicMock(properties=mocked_properties)
        self.azure_storage.block_blob_service.configure_mock(
            **{
                'get_blob_properties.return_value': mock_blob,
            }
        )
        file_size = 100
        exists = self.azure_storage.blob_exists(BERGAMOT_CONTAINER_NAME, BLOB_NAME, file_size)
        self.assertEqual(exists, True)

    def test_blob_exists_no_file_size(self):
        mocked_properties = MagicMock(content_length=100)
        mock_blob = MagicMock(properties=mocked_properties)
        self.azure_storage.block_blob_service.configure_mock(
            **{
                'get_blob_properties.return_value': mock_blob,
            }
        )
        exists = self.azure_storage.blob_exists(BERGAMOT_CONTAINER_NAME, BLOB_NAME)
        self.assertEqual(exists, True)

    def test_blob_does_not_exists(self):
        self.azure_storage.block_blob_service.configure_mock(
            **{
                'get_container_properties.side_effect': AzureHttpError("not found", 404),
            }
        )
        file_size = 100
        exists = self.azure_storage.blob_exists(BERGAMOT_CONTAINER_NAME, BLOB_NAME, file_size)
        self.assertEqual(exists, False)

    def test_blob_is_not_equal(self):
        mocked_properties = MagicMock(content_length=200)
        mock_blob = MagicMock(properties=mocked_properties)
        self.azure_storage.block_blob_service.configure_mock(
            **{
                'get_blob_properties.return_value': mock_blob,
            }
        )
        file_size = 100
        exists = self.azure_storage.blob_exists(BERGAMOT_CONTAINER_NAME, BLOB_NAME, file_size)
        self.assertEqual(exists, False)

    @patch('os.path.exists')
    @patch('os.path.getsize')
    @patch('os.walk')
    @patch('builtins.open', new_callable=mock_open)
    def test_upload_directory(self, mocked_open, mock_walk, mock_size, mock_exists):
        mock_walk.return_value = [('directory', [], ['a.txt', 'b.txt', 'c.csv'])]
        mock_size.return_value = 10
        mock_exists.return_value = True
        file_mock = StringIO(FILE_A["file_content"])
        mocked_open.return_value = file_mock
        files_uploaded = self.azure_storage.upload_directory("dir")
        self.assertEqual(files_uploaded, 2)

    @patch('utils.azure_utils.azure_storage.AzureStorage.blob_exists')
    def test_get_new_blob_name(self, mock_blob_exists):
        mock_blob_exists.side_effect = [True, True, False]
        file_size = 100
        new_name = self.azure_storage.get_new_blob_name(BLOB_NAME, file_size)
        filename, file_extension = os.path.splitext(BLOB_NAME)
        self.assertEqual(new_name, "{}-{}{}".format(filename, 3, file_extension))


if __name__ == '__main__':
    main()
