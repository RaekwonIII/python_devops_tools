import logging
from io import StringIO
import datetime

from unittest import main, TestCase
from unittest.mock import patch, MagicMock, mock_open
import azure.batch.models as batchmodels


from scarface_utils.azure_utils.azure_batch import AzureBatch

CONFIG_FILE = u"file.cfg"

CONFIG_TEMPLATE_BATCH = u'[Batch]\n' \
                        u'batchserviceurl={}\n' \
                        u'client_id={}\n' \
                        u'client_secret={}\n' \
                        u'tenant_id={}\n' \
                        u'key_vault_uri={}\n' \
                        u'resource={}'

BATCH_SERVICE_URL = 'https://test.batch.azure.com'
BATCH_CLIENT_ID = 'test-client-id'
BATCH_SECRET_ID = 'test-secret-id'
BATCH_TENANT_ID = 'test-tenant-id'
BATCH_KV_URI = 'https://testkv.vault.azure.net:443'
BATCH_RESOURCE = 'https://batch.core.windows.net/'

POOL_ID = "pool_id"
JOB_ID = "job_id"
TASK_ID = "task_id"
VM_SIZE = "vm_size"
VM_COUNT = 2
MAX_TASKS = 2


class AzureBatchTest(TestCase):

    @classmethod
    @patch('azure.batch.batch_service_client.BatchServiceClient')
    def setUpClass(cls, mocked_batch_client):
        mocked_batch_client.configure_mock(
            **{
                'credentials.return_value': MagicMock(),
                'batch_url.return_value': BATCH_SERVICE_URL,
                'pool.exists.return_value': False
            }
        )

        cls.azure_batch = AzureBatch(batch_client=mocked_batch_client,
                                     config_file=CONFIG_FILE,
                                     logger=logging.getLogger(__name__),
                                     )

    def setUp(self):
        pass

    @patch('scarface_utils.azure_utils.azure_authentication.AzureAuthentication.from_config')
    @patch('azure.batch.batch_service_client.BatchServiceClient')
    @patch('builtins.open', new_callable=mock_open)
    def test_constructor(self, mocked_open, mocked_batch_service_client, mocked_azure_authentication):
        file_mock = StringIO(
            CONFIG_TEMPLATE_BATCH.format(BATCH_SERVICE_URL,
                                         BATCH_CLIENT_ID,
                                         BATCH_SECRET_ID,
                                         BATCH_TENANT_ID,
                                         BATCH_KV_URI,
                                         BATCH_RESOURCE)
        )
        mocked_open.return_value = file_mock

        batch_service_mock = MagicMock()
        mocked_azure_authentication.return_value = batch_service_mock

        mocked_credentials = MagicMock()
        mocked_azure_authentication.configure_mock(
            **{
                'credentials.return_value': mocked_credentials,
            }
        )

        batch_service = AzureBatch.from_config_file(CONFIG_FILE)

        mocked_batch_service_client.assert_called_with(mocked_azure_authentication().credentials,
                                                       batch_url=BATCH_SERVICE_URL)
        self.assertEqual(batch_service.config_file, CONFIG_FILE)

    def test_create_pool(self):
        self.azure_batch.create_pool(POOL_ID, VM_SIZE, VM_COUNT, MAX_TASKS)
        self.assertTrue(self.azure_batch.batch_client.pool.add.called)
        self.assertEqual(self.azure_batch.config_file, CONFIG_FILE)

    def test_create_pool_already_exists(self):

        mocked_batch_client = MagicMock()
        mocked_batch_client.configure_mock(
            **{
                'credentials.return_value': MagicMock(),
                'batch_url.return_value': BATCH_SERVICE_URL,
                'pool.exists.return_value': True,
            }
        )
        ab = AzureBatch(batch_client=mocked_batch_client,
                        config_file=CONFIG_FILE,
                        logger=logging.getLogger(__name__),
                        )

        ab.batch_client.configure_mock(
            **{
                'pool.exists.return_value': True,
            }
        )
        ab.create_pool(POOL_ID, VM_SIZE, VM_COUNT, MAX_TASKS)
        self.assertFalse(ab.batch_client.pool.add.called)
        self.assertEqual(ab.config_file, CONFIG_FILE)

    def test_create_job_already_exists(self):
        self.azure_batch.batch_client.configure_mock(
            **{
                'job.get.return_value': MagicMock(),
            }
        )

        self.azure_batch.create_job(POOL_ID, JOB_ID)
        self.assertFalse(self.azure_batch.batch_client.job.add.called)
        self.assertEqual(self.azure_batch.config_file, CONFIG_FILE)

    def test_create_job(self):
        mocked_batch_client = MagicMock()
        mocked_batch_client.configure_mock(
            **{
                'credentials.return_value': MagicMock(),
                'batch_url.return_value': BATCH_SERVICE_URL,
            }
        )
        ab = AzureBatch(batch_client=mocked_batch_client,
                        config_file=CONFIG_FILE,
                        logger=logging.getLogger(__name__),
                        )

        ab.batch_client.configure_mock(
            **{
                'job.get.side_effect': batchmodels.BatchErrorException(MagicMock(), "response"),
            }
        )

        ab.create_job(POOL_ID, JOB_ID)
        self.assertTrue(ab.batch_client.job.add.called)
        self.assertEqual(ab.config_file, CONFIG_FILE)

    def test_create_task(self):
        task_1 = MagicMock()
        task_2 = MagicMock
        task = [task_1, task_2]
        self.azure_batch.create_tasks(JOB_ID, task)
        self.assertTrue(self.azure_batch.batch_client.task.add_collection.called)
        self.azure_batch.batch_client.task.add_collection.assert_called_with(JOB_ID, task)
        self.assertEqual(self.azure_batch.config_file, CONFIG_FILE)

    def test_generate_unique_resource_name(self):
        prefix = "test"
        generated_name = self.azure_batch.generate_unique_resource_name(prefix)
        date_str = datetime.datetime.utcnow().strftime("%Y%m%d")
        self.assertRegex(generated_name, "{}-{}-{}".format(prefix, date_str, "\d{12,12}$"))

    def test_build_task(self):
        commands = ["command_1", "command_2"]
        resource_files = [MagicMock()]
        output_files = [MagicMock()]
        task = self.azure_batch.build_task(TASK_ID, commands, resource_files, output_files)
        self.assertEqual(task.id, TASK_ID)
        self.assertEqual(task.command_line, "/bin/bash -c 'set -e; set -o pipefail; command_1;command_2; wait'")

    def test_build_resource_file(self):
        storage_container = "test_container"
        blob_prefix = "test_prefix"
        resource_file = self.azure_batch.build_resource_file(storage_container, blob_prefix)
        self.assertEqual(resource_file.auto_storage_container_name, storage_container)
        self.assertEqual(resource_file.blob_prefix, blob_prefix)

    def test_build_output_file(self):
        file_pattern = "test_pattern"
        container_url = "http://test_container"
        file_path = "test_path"
        output_file = self.azure_batch.build_output_file(file_pattern, container_url, file_path)
        self.assertEqual(output_file.file_pattern, file_pattern)
        self.assertEqual(output_file.destination.container.container_url, container_url)
        self.assertEqual(output_file.destination.container.path, file_path)

    def test_submit_job_and_add_task(self):
        tasks = [MagicMock]
        ab = AzureBatch(batch_client=MagicMock(),
                        config_file=CONFIG_FILE,
                        logger=logging.getLogger(__name__),
                        )

        ab.batch_client.configure_mock(
            **{
                'pool.exists.return_value': False,
                'job.get.side_effect': batchmodels.BatchErrorException(MagicMock(), "response"),
            }
        )

        ab.submit_job_and_add_task(POOL_ID, JOB_ID, tasks, VM_SIZE, VM_COUNT, MAX_TASKS)
        self.assertTrue(ab.batch_client.pool.add.called)
        self.assertTrue(ab.batch_client.job.add.called)
        self.assertTrue(ab.batch_client.task.add_collection.called)

    def test_wait_for_tasks_to_complete(self):
        self.azure_batch.wait_for_tasks_to_complete(JOB_ID, datetime.timedelta(seconds=10))
        self.azure_batch.batch_client.task.list.assert_called_with(JOB_ID)
        self.assertTrue(self.azure_batch.batch_client.task.list.called)

    def test_wait_for_tasks_to_complete_timeout(self):
        ab = AzureBatch(batch_client=MagicMock(),
                        config_file=CONFIG_FILE,
                        logger=logging.getLogger(__name__),
                        )
        ab.batch_client.configure_mock(
            **{
                'task.list.return_value': [MagicMock()],
            }
        )

        try:
            ab.wait_for_tasks_to_complete(JOB_ID, datetime.timedelta(seconds=5))
        except:
            self.azure_batch.batch_client.task.list.assert_called_with(JOB_ID)
            self.assertTrue(self.azure_batch.batch_client.task.list.called)


if __name__ == '__main__':
    main()
