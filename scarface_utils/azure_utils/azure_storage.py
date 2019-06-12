import os
from azure.common import AzureHttpError
from azure.storage.blob import BlockBlobService
import configparser
import logging.config
import timeit
import json

from scarface_utils.azure_utils.exceptions import BlobExistsException
from scarface_utils.azure_utils.azure_authentication import AzureAuthentication


class AzureStorage(object):
    """
        This class exposes utility methods to interact with Azure Storage
    """

    def __init__(self,
                 block_blob_service=None,
                 container_name=None,
                 allowed_files=None,
                 config_file=None,
                 max_connections=None,
                 logger=None):

        self.logger = logger or logging.getLogger(__name__)
        self.config_file = config_file
        self.container_name = container_name
        self.allowed_files = allowed_files
        self.block_blob_service = block_blob_service
        self.MAX_CONNECTIONS = max_connections

    @classmethod
    def from_config_file_path(cls, config_file):
        # type: (str) -> AzureStorage
        """
        Method builds an instance of this class, by reading essential configuration from a config file.
        :param config_file: The path to the config file to be used as configuration for this class' instance
        :return: an instance of AzureStorage class
        """
        cfp = configparser.RawConfigParser()
        with open(config_file) as cfg:
            cfp.read_file(cfg)
        account_name = cfp.get('Storage', 'storageaccountname')
        account_key = cfp.get('Storage', 'storageaccountkey')
        sas_token = cfp.get('Storage', 'sastoken')
        container_name = cfp.get('Storage', 'containername')
        allowed_files = json.loads(cfp.get('Files', 'allowed'))
        if cfp.has_option("Storage", "maxconnections"):
            max_connections = cfp.getint('Storage', 'maxconnections')
        else:
            max_connections = 8

        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key, sas_token=sas_token)

        return cls(
            block_blob_service=block_blob_service,
            container_name=container_name,
            allowed_files=allowed_files,
            max_connections=max_connections,
            config_file=config_file
        )

    @classmethod
    def from_config(
            cls,
            account_name=None,
            account_key=None,
            sas_token=None,
            container_name=None,
            max_connections=8,
            allowed_files=None,
    ):
        # type: (str, str, str, str, int, list) -> AzureStorage
        """
        Method builds an instance of this class, by reading essential configuration from a dictionary.
        :param account_name: The name of the Storage account to use
        :param account_key: eventual account key for authenticating to this account (mutually exclusive with sas_token)
        :param sas_token: eventual temporary token to use for authenticating to this account (see account_key)
        :param container_name: name of the container to use to upload/download blobs
        :param max_connections: maximum number of simultaneous connections to the Storage client
        :param allowed_files: extensions of files allowed to be uploaded
        :return: an instance of AzureStorage class
        """

        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key, sas_token=sas_token)

        return cls(
            block_blob_service=block_blob_service,
            container_name=container_name,
            allowed_files=allowed_files,
            max_connections=max_connections,
        )

    @classmethod
    def from_key_vault_config(cls, config_file):
        # type: (str) -> AzureStorage
        """
        Method builds an instance of this class, by getting secret from Azure Key Vault
        and reading other configuration parameters from a config file.
        :param config_file: The path to the config file to be used as configuration for this class' instance
        :return: an instance of AzureStorage class
        """

        cfp = configparser.RawConfigParser()
        with open(config_file) as cfg:
            cfp.read_file(cfg)
        account_name = cfp.get('Storage', 'storageaccountname')
        container_name = cfp.get('Storage', 'containername')
        allowed_files = json.loads(cfp.get('Files', 'allowed'))
        if cfp.has_option("Storage", "maxconnections"):
            max_connections = cfp.getint('Storage', 'maxconnections')
        else:
            max_connections = 8

        az_auth = AzureAuthentication.from_config_file(config_file)
        storage_secret_name = cfp.get('KeyVault', 'storage_secret_name')
        sas_token = az_auth.get_secret(storage_secret_name)

        block_blob_service = BlockBlobService(account_name=account_name,sas_token=sas_token)

        return cls(
            block_blob_service=block_blob_service,
            container_name=container_name,
            allowed_files=allowed_files,
            max_connections=max_connections,
            config_file=config_file,
        )

    def get_storage_account_name(self, config_file=None):
        # type: (str) -> str
        """
        Method that returns the Azure Storage account name
        :param config_file: the path to a configuration file containing the Azure Storage properties
        :return: a str, storage account name
        """
        if not config_file:
            config_file = self.config_file
        cfp = configparser.ConfigParser()
        with open(config_file) as cfg:
            cfp.read_file(cfg)
        account_name = cfp.get('Storage', 'storageaccountname')
        return account_name

    def get_blob_url(self, blob_name):
        # type: (str) -> str
        """
        Returns the Blob url for the given blob name
        :param blob_name: name of blob
        :return: blob url
        """
        account_name = self.get_storage_account_name()
        blob_url = "https://{0}.blob.core.windows.net/{1}/{2}".format(account_name, self.container_name, blob_name)
        return blob_url

    def create_container(self):
        """
            Creates a new container in Azure under the specified account with private access
        """
        return self.block_blob_service.create_container(self.container_name)

    def list_blobs(self):
        """
            Method that returns a list containing the blob names in the container associated with the class
            :return: a list object
        """
        generator = self.block_blob_service.list_blobs(self.container_name)
        blob_names = []
        for blob in generator:
            blob_names.append(blob.name)
        return blob_names

    def list_containers(self):
        """
           Method that returns a list containing the containers names in the storage account associated with the class
           :return: a list object
        """
        generator = self.block_blob_service.list_containers()
        containers_names = []
        for container in generator:
            containers_names.append(container.name)
        return containers_names

    def download_from_blob(self, blob_name, local_path):
        # type: (str, str) -> None
        """
        Method that downloads a blob file from Azure to a local file system
        :param blob_name: name of blob file in Azure
        :param local_path: path location where to download the file.
                Must be the full path of the destination file, not only the directory
        :return: None
        """
        self.logger.info('Downloading blob: %s from blob: %s to local path: %s', blob_name, blob_name, local_path)
        return self.block_blob_service.get_blob_to_path(self.container_name, blob_name, local_path)

    def upload_to_blob_with_rename(self, blob_name, local_path, file_size):
        # type: (str, str, int) -> str
        """
        Method that uploads a file as a block blob to Azure. If the file already exists in the container, this method
        will try and find a new name, by adding a counter suffix to it. The check is done by taking into account both
        the file name and the size of the file.
        :param blob_name: name of blob file
        :param local_path: path where the file is located.
                Must be the full path of the source file, not only the directory
        :param file_size of file to be uploaded in Bytes
        :return: name of the uploaded blob
        """
        try:
            self.upload_to_blob(blob_name=blob_name, local_path=local_path, file_size=file_size)
        except BlobExistsException:
            blob_name = self.get_new_blob_name(blob_name=blob_name, file_size=file_size)
            self.upload_to_blob(blob_name=blob_name, local_path=local_path, file_size=file_size)

        return blob_name

    def get_new_blob_name(self, blob_name, file_size):
        # type: (str, int) -> str
        """
        Method checks for existence of a blob in storage and generates a new filename with a suffix that represents
        a counter of files with the same name.
        :param blob_name: name of the blob to check
        :param file_size: size of the blob to check
        :return: a new filename
        """
        filename, file_extension = os.path.splitext(blob_name)
        i = 1
        new_fname = "{}-{}{}".format(filename, i, file_extension)
        while self.blob_exists(self.container_name, new_fname, file_size):
            i += 1
            new_fname = "{}-{}{}".format(filename, i, file_extension)
        self.logger.info("New blob name: %s", new_fname)
        return new_fname

    def upload_to_blob(self, blob_name=None, local_path=None, file_size=0):
        # type: (str, str, int) -> None
        """
        Method that uploads a file as a block blob to Azure. It will only upload a file if it does not exist already
        in the container. The check is done by taking into account both the file name and the size of the file.
        :param blob_name: name of blob file
        :param local_path: path where the file is located.
                Must be the full path of the source file, not only the directory
        :param file_size of file to be uploaded in Bytes
        """

        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            if not self.container_exists(self.container_name):
                self.create_container()

            if self.blob_exists(self.container_name, blob_name, file_size):
                self.logger.warning("Blob %s already exists.", blob_name)
                raise BlobExistsException

            start = timeit.default_timer()
            self.block_blob_service.create_blob_from_path(self.container_name, blob_name, local_path,
                                                          progress_callback=None,
                                                          max_connections=self.MAX_CONNECTIONS)
            end = timeit.default_timer()
            self.logger.info(
                "Blob has been uploaded successfully. Container Name: %s. Blob Name: %s. Size : %s B. Transfer Time: "
                "%s s. From Local Path: %s.",
                self.container_name,
                blob_name,
                file_size,
                round(end - start, 2),
                local_path
            )
        else:
            self.logger.error("The file located in provided path {} could not be uploaded because "
                              "either the file does not exist or it is empty.".format(local_path))

    def container_exists(self, container_name):
        # type: (str) -> bool
        """
        Method that checks if a container exists in the Azure Storage account associated with the class
        :param container_name: name of container
        :return: boolean, True if container exists, False otherwise
        """
        try:
            return self.block_blob_service.exists(container_name)
        except AzureHttpError:
            return False

    def blob_exists(self, container_name, blob_name, file_size=None):
        # type: (str, str, int) -> bool
        """
        Method that checks if a blob exists in  a given container. A blob is considered to already exists
        when both the name of the file and the size of the file are the same.
        :param container_name: name of container
        :param blob_name: name of blob
        :param file_size: size of file in Bytes
        :return: boolean, True if blob exists, False otherwise
        """
        try:
            if self.block_blob_service.exists(container_name, blob_name):
                blob_properties = self.block_blob_service.get_blob_properties(container_name, blob_name)
                if not file_size:
                    return True
                elif blob_properties.properties.content_length == file_size:
                    return True
                else:
                    return False
            else:
                return False
        except AzureHttpError:
            return False

    def upload_directory(self, directory, callback=None):
        # type: (str, function) -> int
        """
        Method that uploads allowed_files extensions recursively  from the given directory into block blobs in Azure,
        keeping track of the uploading progress
        :param directory: name of directory
        :param callback: function to progress upload callback
        :return: int, number of files uploaded successfully
        """

        if callback is not None:
            next(callback)

        total = 0
        for root, subdirs, files in os.walk(directory):
            for f in files:
                if f.endswith(tuple(self.allowed_files)):
                    total = total + os.path.getsize(os.path.join(root, f))

        uploaded_files_number = 0
        if total != 0:
            uploaded_files_size = 0
            for root, subdirs, files in os.walk(directory):
                for f in files:
                    if f.endswith(tuple(self.allowed_files)):
                        previous_size = uploaded_files_size * 100 / total
                        file_size = os.path.getsize(os.path.join(root, f))
                        self.upload_to_blob(f, os.path.join(root, f), file_size)
                        uploaded_files_number += 1
                        uploaded_files_size = uploaded_files_size + file_size
                        current_size = uploaded_files_size * 100 / total
                        if callback is not None:
                            callback.send(current_size - previous_size - 0.000001)

        if callback is not None:
            callback.close()

        return uploaded_files_number
