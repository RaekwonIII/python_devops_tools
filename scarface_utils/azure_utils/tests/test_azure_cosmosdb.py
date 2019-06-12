from collections import OrderedDict
from io import StringIO
from unittest import main, TestCase
from unittest.mock import patch, MagicMock, mock_open

from scarface_utils.azure_utils.azure_cosmosdb import CosmosDBService

COLLECTION_LINK_ID = 'dbs/PMYQAA==/colls/PMYQAI7cTsM=/'

BERGAMOT_COSMOS_COLLECTION_WITH_PARTITION_NAME = 'collection_with_partition_name'

DOCUMENT_NAME = 'document_id'

PARTITION_KEY = '/partition_key'

CONFIG_FILE = 'file.cfg'

CONFIG_TEMPLATE = '[KeyVault]\n' \
                  'client_id={}\n' \
                  'client_secret={}\n' \
                  'tenant_id={}\n' \
                  'key_vault_uri={}\n' \
                  'cosmos_secret_name={}\n' \
                  '[Cosmos]\n' \
                  'cosmosendpoint={}\n' \
                  'cosmosmasterkey={}\n' \
                  'db_name={}\n' \
                  'collection_name={}'

KV_CLIENT_ID = 'test-client-id'
KV_SECRET_ID = 'test-secret-id'
KV_TENANT_ID = 'test-tenant-id'
KV_URI = 'https://testkv.vault.azure.net:443'
COSMOS_SECRET_NAME = 'test-secret-name'

COSMOS_END_POINT = 'endpoint'
COSMOS_MASTER_KEY = 'masterkey'
BERGAMOT_COSMOS_DB_NAME = 'db_name'
BERGAMOT_COSMOS_COLLECTION_NAME = 'collection_name'

DB_OBJECT = {'_self': 'dbs/PMYQAA==/', 'id': BERGAMOT_COSMOS_DB_NAME}

COLLECTION_OBJECT = {'_self': COLLECTION_LINK_ID, "id": BERGAMOT_COSMOS_COLLECTION_NAME}

COLLECTION_OBJECT_WITH_PARTITION = {
    '_self': COLLECTION_LINK_ID,
    'id': BERGAMOT_COSMOS_COLLECTION_WITH_PARTITION_NAME,
    'partitionKey': {'paths': [PARTITION_KEY]}
}

DOCUMENT_OBJECT = {'_self': 'self', 'id': DOCUMENT_NAME}

DOCUMENT_OBJECT_WITH_PARTITION = {
    '_self': 'self',
    'id': DOCUMENT_NAME,
    'partitionKey': {'paths': [PARTITION_KEY]}
}

DOC_FIELD = 'field'

DOC_VALUE = 'value'

COSMOS_SECRET = COSMOS_MASTER_KEY


class CosmosDBServiceTest(TestCase):

    @classmethod
    @patch('pydocumentdb.document_client.DocumentClient')
    def setUpClass(cls, mocked_client):
        mocked_client.configure_mock(
            **{
                'ReadDatabases.return_value': [DB_OBJECT],
                'ReadCollections.return_value': [COLLECTION_OBJECT, COLLECTION_OBJECT_WITH_PARTITION],
                'QueryDocuments.return_value': [DOCUMENT_OBJECT],
                'ReadDocuments.return_value': [DOCUMENT_OBJECT, DOCUMENT_OBJECT],
            }
        )

        cls.cosmos_db_service = CosmosDBService(
            client=mocked_client,
            config_file=CONFIG_FILE,
            db=DB_OBJECT,
            collection=COLLECTION_OBJECT,
            logger=None,
        )

        cls._db = BERGAMOT_COSMOS_DB_NAME
        cls._collection = BERGAMOT_COSMOS_COLLECTION_NAME

    def setUp(self):
        pass

    @patch('pydocumentdb.document_client.DocumentClient')
    @patch('builtins.open', new_callable=mock_open)
    def test_constructor(
            self,
            mocked_open,
            mocked_client,
    ):
        file_mock = StringIO(
            CONFIG_TEMPLATE.format(KV_CLIENT_ID, KV_SECRET_ID, KV_TENANT_ID, KV_URI, COSMOS_SECRET_NAME,
                                   COSMOS_END_POINT, COSMOS_MASTER_KEY, BERGAMOT_COSMOS_DB_NAME,
                                   BERGAMOT_COSMOS_COLLECTION_NAME)
        )
        mocked_open.return_value = file_mock

        client_mock = MagicMock()
        client_mock.configure_mock(
            **{
                'ReadDatabases.return_value': [DB_OBJECT],
                'ReadCollections.return_value': [COLLECTION_OBJECT, COLLECTION_OBJECT_WITH_PARTITION],
                'QueryDocuments.return_value': [DOCUMENT_OBJECT],
                'ReadDocuments.return_value': [DOCUMENT_OBJECT, DOCUMENT_OBJECT],
            }
        )
        mocked_client.return_value = client_mock

        cosmos_db_service = CosmosDBService.from_config_file_path(CONFIG_FILE)
        mocked_client.assert_called_with(COSMOS_END_POINT, {'masterKey': COSMOS_MASTER_KEY})
        self.assertEqual(cosmos_db_service.config_file, CONFIG_FILE)

    @patch('scarface_utils.azure_utils.azure_cosmosdb.AzureAuthentication')
    @patch('pydocumentdb.document_client.DocumentClient')
    @patch('builtins.open', new_callable=mock_open)
    def test_constructor_from_key_vault(self, mocked_open, mocked_client, mocked_azure_auth):
        file_mock = StringIO(
            CONFIG_TEMPLATE.format(KV_CLIENT_ID, KV_SECRET_ID, KV_TENANT_ID, KV_URI, COSMOS_SECRET_NAME,
                                   COSMOS_END_POINT, COSMOS_MASTER_KEY, BERGAMOT_COSMOS_DB_NAME,
                                   BERGAMOT_COSMOS_COLLECTION_NAME)
        )
        mocked_open.return_value = file_mock

        client_mock = MagicMock()
        client_mock.configure_mock(
            **{
                'ReadDatabases.return_value': [DB_OBJECT],
                'ReadCollections.return_value': [COLLECTION_OBJECT, COLLECTION_OBJECT_WITH_PARTITION],
                'QueryDocuments.return_value': [DOCUMENT_OBJECT],
                'ReadDocuments.return_value': [DOCUMENT_OBJECT, DOCUMENT_OBJECT],
            }
        )
        mocked_client.return_value = client_mock

        azure_auth_mock = MagicMock()
        attrs = {'get_secret.return_value': COSMOS_SECRET}
        azure_auth_mock.configure_mock(**attrs)
        mocked_azure_auth.configure_mock(**{'from_config_file.return_value': azure_auth_mock})

        cosmos_db_service = CosmosDBService.from_key_vault_config(CONFIG_FILE)

        mocked_azure_auth.from_config_file.assert_called_with(CONFIG_FILE)
        mocked_azure_auth.from_config_file().get_secret.assert_called_with(COSMOS_SECRET_NAME)
        mocked_client.assert_called_with(COSMOS_END_POINT, {'masterKey': COSMOS_MASTER_KEY})
        self.assertEqual(cosmos_db_service.config_file, CONFIG_FILE)

    def test_get_db(self):
        self.cosmos_db_service.set_db_from_name(BERGAMOT_COSMOS_DB_NAME)
        db = self.cosmos_db_service.get_db()
        self.assertEqual(db, DB_OBJECT)

    @patch('builtins.open', new_callable=mock_open)
    def test_set_db_from_config(self, mocked_open):
        file_mock = StringIO(
            CONFIG_TEMPLATE.format(KV_CLIENT_ID, KV_SECRET_ID, KV_TENANT_ID, KV_URI, COSMOS_SECRET_NAME,
                                   COSMOS_END_POINT, COSMOS_MASTER_KEY, BERGAMOT_COSMOS_DB_NAME,
                                   BERGAMOT_COSMOS_COLLECTION_NAME)
        )
        mocked_open.return_value = file_mock
        db = self.cosmos_db_service.set_db_from_config(CONFIG_FILE)
        self.assertEqual(db, DB_OBJECT)

    def test_get_collection_from_name(self):
        self.cosmos_db_service.set_db_from_name(BERGAMOT_COSMOS_DB_NAME)
        collection = self.cosmos_db_service.get_collection_from_name(BERGAMOT_COSMOS_COLLECTION_NAME)
        self.assertEqual(collection, COLLECTION_OBJECT)

    @patch('builtins.open', new_callable=mock_open)
    def test_collection_from_config(self, mocked_open):
        self.cosmos_db_service.set_db_from_name(BERGAMOT_COSMOS_DB_NAME)
        file_mock = StringIO(
            CONFIG_TEMPLATE.format(KV_CLIENT_ID, KV_SECRET_ID, KV_TENANT_ID, KV_URI, COSMOS_SECRET_NAME,
                                   COSMOS_END_POINT, COSMOS_MASTER_KEY, BERGAMOT_COSMOS_DB_NAME,
                                   BERGAMOT_COSMOS_COLLECTION_NAME)
        )
        mocked_open.return_value = file_mock
        collection = self.cosmos_db_service.get_collection_from_config(CONFIG_FILE)
        self.assertEqual(collection, COLLECTION_OBJECT)

    def test_get_doc_link_by_id(self):
        doc_id = "id_001"
        doc_link = self.cosmos_db_service.get_doc_link_by_id(doc_id)
        expected_doc_link = "dbs/{}/colls/{}/docs/{}".format(
            BERGAMOT_COSMOS_DB_NAME,
            BERGAMOT_COSMOS_COLLECTION_NAME,
            doc_id
        )
        self.assertEqual(doc_link, expected_doc_link)

    def test_get_query_obj(self):
        query = self.cosmos_db_service.get_query_obj(DOC_FIELD, DOC_VALUE)
        expected_query = "SELECT * FROM c WHERE c.{} = \"{}\"".format(DOC_FIELD, DOC_VALUE)
        self.assertEqual(query["query"], expected_query)

    def test_get_doc_by_field(self):
        doc = self.cosmos_db_service.get_doc_by_field(DOC_FIELD, DOC_VALUE)
        self.assertEqual(doc, DOCUMENT_OBJECT)

    def test_get_doc_by_field_multiple_docs(self):
        self.cosmos_db_service.client.configure_mock(
            **{
                'QueryDocuments.return_value': [DOCUMENT_OBJECT, DOCUMENT_OBJECT],
            }
        )
        with self.assertRaises(Exception) as exc:
            self.cosmos_db_service.get_doc_by_field(DOC_FIELD, DOC_VALUE)
        self.assertTrue('Too many documents found with field' in str(exc.exception))

    def test_get_doc_by_field_no_docs(self):
        self.cosmos_db_service.client.configure_mock(
            **{
                'QueryDocuments.return_value': [],
            }
        )
        doc = self.cosmos_db_service.get_doc_by_field(DOC_FIELD, DOC_VALUE)
        self.assertEqual(doc, {})

    def test_get_docs_by_fields_with_partitionKey(self):
        # necessary class setup
        self.cosmos_db_service.set_db_from_name(BERGAMOT_COSMOS_DB_NAME)
        self.cosmos_db_service.get_collection_from_name(BERGAMOT_COSMOS_COLLECTION_WITH_PARTITION_NAME)

        # system under test
        doc_filter = OrderedDict({'fake': 'test', 'name': 'test123'})
        self.cosmos_db_service.get_docs_by_fields(doc_filter)

        # assert system has performed as expected
        self.cosmos_db_service.client.QueryDocuments.assert_called_with(
            COLLECTION_LINK_ID,
            {'query': 'SELECT c._self, c.{} FROM c WHERE {}'.format(
                PARTITION_KEY.replace("/", ""),
                " and ".join(['c.{} = "{}"'.format(key, val) for key, val in doc_filter.items()]),
            )},
            {
                'enableCrossPartitionQuery': True,
                'maxItemCount': 1000
            }
        )

    def test_get_docs_by_fields_without_partitionKey(self):
        # necessary class setup
        self.cosmos_db_service.set_db_from_name(BERGAMOT_COSMOS_DB_NAME)
        self.cosmos_db_service.get_collection_from_name(BERGAMOT_COSMOS_COLLECTION_NAME)

        # system under test
        doc_filter = OrderedDict({'name': 'test456', 'version': 'v1'})
        doc = self.cosmos_db_service.get_docs_by_fields(doc_filter)

        # assert system has performed as expected
        self.cosmos_db_service.client.QueryDocuments.assert_called_with(
            COLLECTION_LINK_ID,
            {'query': 'SELECT c._self FROM c WHERE {}'.format(
                " and ".join(['c.{} = "{}"'.format(key, val) for key, val in doc_filter.items()])
            )},
            {
                'enableCrossPartitionQuery': True,
                'maxItemCount': 1000
            }
        )

    def test_get_doc_by_id(self):
        self.cosmos_db_service.client.configure_mock(
            **{
                'ReadDocument.return_value': DOCUMENT_OBJECT,
            }
        )
        doc = self.cosmos_db_service.get_doc_by_id_and_partition(DOCUMENT_NAME)
        self.assertEqual(doc, DOCUMENT_OBJECT)

    def test_get_all_documents(self):
        docs = self.cosmos_db_service.get_all_documents()
        self.assertEqual(docs, [DOCUMENT_OBJECT, DOCUMENT_OBJECT])

    def test_create_doc(self):
        self.cosmos_db_service.client.configure_mock(
            **{
                'CreateDocument.return_value': DOCUMENT_OBJECT,
            }
        )
        doc = self.cosmos_db_service.create_doc(DOCUMENT_OBJECT)
        self.assertEqual(doc, DOCUMENT_OBJECT)

    def test_create_or_update_doc(self):
        new_document = DOCUMENT_OBJECT
        new_document["new_field"] = "new_field"
        self.cosmos_db_service.client.configure_mock(
            **{
                'ReplaceDocument.return_value': new_document,
            }
        )
        doc = self.cosmos_db_service.create_or_update_doc(new_document)
        self.assertEqual(doc, new_document)

    def test_create_or_update_no_doc(self):
        new_document = {"id": "new id", "new_field": "new_field"}
        self.cosmos_db_service.client.configure_mock(
            **{
                'ReadDocument.return_value': None,
                'CreateDocument.return_value': new_document,
            }
        )
        doc = self.cosmos_db_service.create_or_update_doc(new_document)
        self.assertEqual(doc, new_document)


if __name__ == '__main__':
    main()
