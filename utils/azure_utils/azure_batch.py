import configparser
import logging.config
import datetime
import time
import io

import azure.batch.batch_service_client as batch
import azure.batch.models as batchmodels
from utils.azure_utils.azure_authentication import AzureAuthentication

UBUNTU_IMAGE_CONFIG = batchmodels.VirtualMachineConfiguration(
    image_reference=batchmodels.ImageReference(
        publisher="Canonical",
        offer="UbuntuServer",
        sku="18.04-LTS",
        version="latest"),
    node_agent_sku_id="batch.node.ubuntu 18.04"
)


class AzureBatch(object):
    """
        This class exposes utility methods to interact with AzureBatch Service
    """

    def __init__(self,
                 batch_client=None,
                 config_file=None,
                 logger=None):

        self.logger = logger or logging.getLogger(__name__)
        self.config_file = config_file
        self.batch_client = batch_client

    @classmethod
    def from_config_file(cls, config_file):
        # type: (str) -> AzureBatch
        """
        Method builds an instance of this class, by reading essential configuration from a config file.
        :param config_file: The path to the config file to be used as configuration for this class' instance
        :return: an instance of AzureBatch class
        """
        cfp = configparser.RawConfigParser()
        with open(config_file) as cfg:
            cfp.read_file(cfg)
        batch_service_url = cfp.get('Batch', 'batchserviceurl')
        client_id = cfp.get('Batch', 'client_id')
        client_secret = cfp.get('Batch', 'client_secret')
        tenant_id = cfp.get('Batch', 'tenant_id')
        vault_uri = cfp.get('Batch', 'key_vault_uri')
        resource = cfp.get('Batch', 'resource')

        az_auth = AzureAuthentication.from_config(client_id,
                                                  client_secret,
                                                  tenant_id,
                                                  vault_uri,
                                                  resource)

        batch_client = batch.BatchServiceClient(
            az_auth.credentials,
            batch_url=batch_service_url
        )

        return cls(
            batch_client=batch_client,
            config_file=config_file
        )

    def create_pool(self, pool_id, vm_size, vm_count, max_tasks_per_node, vm_config=None):
        # type: (str, str, int, batchmodels.VirtualMachineConfiguration) -> None
        """
        Method that creates an Azure batch pool (collection of nodes)
        :param pool_id: pool id
        :param vm_size: The size name of virtual machines in the pool.
        (https://azure.microsoft.com/documentation/articles/cloud-services-sizes-specs/)
        :param int vm_count: The desired number of dedicated compute nodes in the pool
        :param int max_tasks_per_node:  maximun task to run in parallel in the same node
        :param batchmodels.VirtualMachineConfiguration vm_config: VM configuration
        :return: None
        """
        if not self.batch_client.pool.exists(pool_id):
            if not vm_config:
                vm_config = UBUNTU_IMAGE_CONFIG
            pool = batch.models.PoolAddParameter(
                id=pool_id,
                virtual_machine_configuration=vm_config,
                vm_size=vm_size,
                target_dedicated_nodes=vm_count,
                max_tasks_per_node=max_tasks_per_node,
                resize_timeout=datetime.timedelta(minutes=20),
            )
            self.logger.info("Creating pool {}.".format(pool_id))
            self.batch_client.pool.add(pool)
        else:
            self.logger.info("Pool {} already exists".format(pool_id))

    def create_job(self, pool_id, job_id):
        # type: (str, str) -> None
        """
        Method that creates an Azure batch job (collection of tasks)
        :param pool_id: pool id
        :param job_id: job id
        :return: None
        """
        try:
            self.batch_client.job.get(job_id)
            self.logger.info("Job {} already exists".format(job_id))
        except batchmodels.BatchErrorException:
            job = batch.models.JobAddParameter(
                id=job_id,
                pool_info=batch.models.PoolInformation(pool_id=pool_id)
            )
            self.logger.info("Creating job {}.".format(job_id))
            self.batch_client.job.add(job)

    def create_tasks(self, job_id, tasks):
        # type: (str, list[batchmodels.TaskAddParameter]) -> None
        """
        Method that creates an Azure batch task (unit of computation that is associated with a job)
        :param job_id: job id
        :param tasks: list of tasks to execute
        :return: None
        """
        self.batch_client.task.add_collection(job_id, tasks)

    def submit_job_and_add_task(self,
                                pool_id,
                                job_id,
                                tasks,
                                vm_size="STANDARD_A1_V2",
                                vm_count=1,
                                max_tasks_per_node=2,
                                vm_config=None):

        self.create_pool(pool_id, vm_size, vm_count, max_tasks_per_node, vm_config)
        self.create_job(pool_id, job_id)
        self.create_tasks(job_id, tasks)

    def wait_for_tasks_to_complete(self, job_id, timeout):
        """Waits for all the tasks in a particular job to complete.
        :param str job_id: The id of the job to monitor.
        :param timeout: The maximum amount of time to wait.
        :type timeout: `datetime.timedelta`
        """
        time_to_timeout_at = datetime.datetime.now() + timeout

        while datetime.datetime.now() < time_to_timeout_at:
            print("Checking if all tasks are complete...")
            tasks = self.batch_client.task.list(job_id)
            incomplete_tasks = [task for task in tasks if task.state != batchmodels.TaskState.completed]
            if not incomplete_tasks:
                return
            time.sleep(5)
        raise TimeoutError("Timed out waiting for tasks to complete")

    @staticmethod
    def generate_unique_resource_name(resource_prefix):
        """Generates a unique resource name by appending a time
        string after the specified prefix.
        :param str resource_prefix: The resource prefix to use.
        :param str suffix: The resource suffix to use.
        :rtype: str
        """
        return resource_prefix + "-" + datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S%f")

    @staticmethod
    def build_task(task_id, commands, resource_files, output_files):
        """Generates a task object
          :param str task_id: task id
          :param list[str] commands: list of command to execute on the nodes.
          :param list[batchmodels.ResourceFile] resource_files: list of resources files
          :param listbatchmodels.OutputFile] output_files: list of output files
          :rtype: batchmodels.TaskAddParameter
        """

        task = batchmodels.TaskAddParameter(
            id=task_id,
            command_line='/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(';'.join(commands)),
            application_package_references=None,
            user_identity=batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    elevation_level=batchmodels.ElevationLevel.admin,
                    scope=batchmodels.AutoUserScope.task)),
            resource_files=resource_files,
            output_files=output_files
        )

        return task

    @staticmethod
    def build_resource_file(storage_container, blob_prefix):
        """Generates a resource file object
          :param str storage_container: name of storage container  attached to the batch account
          :param str blob_prefix: blob prefix to get resources files from.
          :rtype: batchmodels.ResourceFile
        """
        return batchmodels.ResourceFile(auto_storage_container_name=storage_container, blob_prefix=blob_prefix)

    @staticmethod
    def build_output_file(
            file_pattern,
            container_url,
            file_path,
            upload_condition=batchmodels.OutputFileUploadCondition.task_completion):

        """Generates an output file object
           :param str file_pattern: A pattern indicating which file(s) to upload.
           :param str container_url: azure contaiuner URL with sas token.
           :param str file_path: The destination for the output file(s)
           :param str upload_condition: The conditions under which the task output file or set of files should be uploaded.
           The default is taskcompletion. Possible values include: 'taskSuccess', 'taskFailure', 'taskCompletion'
           :rtype: batchmodels.OutputFile
        """

        output_file = batchmodels.OutputFile(
            file_pattern=file_pattern,
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    path=file_path,
                    container_url=container_url)),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=upload_condition))

        return output_file

    @staticmethod
    def read_stream_as_string(stream, encoding):
        """Read stream as string
        :param stream: input stream generator
        :param str encoding: The encoding of the file. The default is utf-8.
        :return: The file content.
        :rtype: str
        """
        output = io.BytesIO()
        try:
            for data in stream:
                output.write(data)
            if encoding is None:
                encoding = 'utf-8'
            return output.getvalue().decode(encoding)
        finally:
            output.close()
        raise RuntimeError('could not write data to stream or decode bytes')
