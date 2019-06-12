from azure.keyvault import KeyVaultClient, KeyVaultAuthentication
from azure.common.credentials import ServicePrincipalCredentials
from msrest.exceptions import AuthenticationError
from scarface_utils.azure_utils.constants import DEFAULT_AUTHENTICATION_RESOURCE

import configparser


class AzureAuthenticationException(Exception):
    pass


class AzureAuthentication(object):
    """
    This class exposes methods in order to authenticate and retrieve secrets from Azure Key Vault
    """

    def __init__(self, kv_client, vault_uri, credentials):
        self.vault_uri = vault_uri
        self.kv_client = kv_client
        self.credentials = credentials

    @classmethod
    def from_config_file(cls, config_file):
        # type: (str) -> AzureAuthentication
        """
        Class method that creates an instance of this class, using the information from a configuration file.
        :param config_file: path to the file containing the Azure Key Vault configuration
        :return: an instance of this class
        """
        cfp = configparser.ConfigParser()
        with open(config_file) as cfg:
            cfp.read_file(cfg)
            client_id = cfp.get('KeyVault', 'client_id')
            client_secret = cfp.get('KeyVault', 'client_secret')
            tenant_id = cfp.get('KeyVault', 'tenant_id')
            vault_uri = cfp.get('KeyVault', 'key_vault_uri')

        return cls.from_config(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            vault_uri=vault_uri,
        )

    @classmethod
    def from_config(cls, client_id, client_secret, tenant_id, vault_uri, resource=None):
        # type: (str, str, str, str) -> AzureAuthentication
        """
        Class method that creates an instance of this class, using the passed arguments
        :param client_id: id of the KeyVault client
        :param client_secret: authentication key for the KeyVault client
        :param tenant_id: directory ID of the Azure subscription where the KeyVault is defined
        :param vault_uri: URL of the KeyVault account
        :param resource: azure uri resource. default https://management.core.windows.net/
        :return: an instance of this class
        """
        try:
            credentials = ServicePrincipalCredentials(
                client_id=client_id,
                secret=client_secret,
                tenant=tenant_id,
                resource=resource or DEFAULT_AUTHENTICATION_RESOURCE
            )
            kv_client = KeyVaultClient(KeyVaultAuthentication(credentials=credentials))
        except AuthenticationError:
            raise AzureAuthenticationException("The credentials provided to access KeyVault are not correct.")

        return cls(
            kv_client=kv_client,
            vault_uri=vault_uri,
            credentials=credentials
        )

    def get_secret(self, secret_name, secret_version=''):
        # type: (str, str) -> str
        """
        Method that retrieves a specific secret from the Key Vault.
        :param secret_name: the name of the secret
        :param secret_version: the version of the secret
        :return: the secret value
        """
        return self.kv_client.get_secret(self.vault_uri, secret_name, secret_version).value
