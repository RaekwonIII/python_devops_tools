import uuid

import configparser
import logging

import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors
from requests.exceptions import ProxyError

from scarface_utils.azure_utils.exceptions import CosmosDBConnectionException
from scarface_utils.azure_utils.constants import DOCUMENT_QUERY
from scarface_utils.azure_utils.azure_authentication import AzureAuthentication


class CosmosDBService(object):
    """
    This class is a Service, interfacing with Cosmos DB using pydocumentdb external library.
    """

    def __init__(self, client, config_file, db=None, collection=None, logger=None):
        # type: ('ConfigParser', str, dict, dict, 'Logger') -> None
        self.client = client
        self.config_file = config_file
        self._db = db
        self._collection = collection
        self.logger = logger or logging.getLogger(__name__)

    @classmethod
    def from_config_file_path(cls, config_file):
        # type: (str) -> CosmosDBService
        """
        Method builds an instance of this class, by reading essential configuration from a config file.
        :param config_file: The path to the config file to be used as configuration for this class' instance
        :return: an instance of CosmosDBService class
        """
        cfp = configparser.ConfigParser()
        with open(config_file) as cfg:
            cfp.read_file(cfg)
        cosmos_endpoint = cfp.get('Cosmos', 'cosmosendpoint')
        cosmos_masterkey = cfp.get('Cosmos', 'cosmosmasterkey')
        db_name = cfp.get('Cosmos', 'db_name') if cfp.has_option('Cosmos', 'db_name') else None
        collection_name = cfp.get('Cosmos', 'collection_name') if cfp.has_option('Cosmos', 'collection_name') else None

        return cls.from_config(
            cosmos_endpoint=cosmos_endpoint,
            cosmos_masterkey=cosmos_masterkey,
            config_file=config_file,
            db_name=db_name,
            collection_name=collection_name,
        )

    @classmethod
    def from_key_vault_config(cls, config_file):
        # type: (str) -> CosmosDBService
        """
        Method builds an instance of this class, by reading essential configuration from a config file and fetching
        authentication keys from Azure KeyVault
        :param config_file: The path to the config file to be used as configuration for this class' instance
        :return: an instance of CosmosDBService class
        """

        cfp = configparser.RawConfigParser()
        with open(config_file) as cfg:
            cfp.read_file(cfg)
            cosmos_secret_name = cfp.get('KeyVault', 'cosmos_secret_name')
            cosmos_endpoint = cfp.get('Cosmos', 'cosmosendpoint')
            db_name = cfp.get('Cosmos', 'db_name') if cfp.has_option('Cosmos', 'db_name') else None
            collection_name = cfp.get('Cosmos', 'collection_name') if cfp.has_option('Cosmos',
                                                                                     'collection_name') else None

        az_auth = AzureAuthentication.from_config_file(config_file)
        cosmos_masterkey = az_auth.get_secret(cosmos_secret_name)

        return cls.from_config(
            cosmos_endpoint=cosmos_endpoint,
            cosmos_masterkey=cosmos_masterkey,
            config_file=config_file,
            db_name=db_name,
            collection_name=collection_name,
        )

    @classmethod
    def from_config(cls, cosmos_endpoint, cosmos_masterkey, config_file=None, db_name=None, collection_name=None):
        # type: (str, str, str, str, str) -> CosmosDBService
        """
        Method builds an instance of this class when provided essential configuration for it
        :param cosmos_endpoint: the endpoint of the CosmosDB to connect to
        :param cosmos_masterkey: the masterkey to use to authenticate to CosmosDB
        :param config_file: (optional) configuration file containing further configuration
        :param db_name: (optional) name of the database to use
        :param collection_name: (optional) name of the collection to use
        :return: an instance of CosmosDBService class
        """
        cdbs = cls(
            client=document_client.DocumentClient(cosmos_endpoint, {"masterKey": cosmos_masterkey}),
            config_file=config_file,
        )

        cdbs.set_db_from_name(db_name=db_name)
        cdbs.get_collection_from_name(collection_name=collection_name)

        return cdbs

    @staticmethod
    def get_link_from_object(obj):
        # type: (dict) -> str
        """
        Method is a utility to extract link component from a cosmosDB document object (be it a DB or a collection)
        :param obj: a dictionary, representing a document object in Cosmos DB
        :return: a string, representing the link to such object
        """
        return obj['_self']

    def set_db_from_name(self, db_name):
        # type: (str) -> dict
        """
        Method returns a DB object, given its name, using this class' cosmos DB client. The DB is cached in a class
        member variable and created only if it doesn't exist or if it is different to the one being requested.
        :param db_name: the name of the DB to fetch
        :return: a dictionary object, representing the requested DB
        """
        if not self._db or self._db['id'] != db_name:
            self._db = next((data for data in self.client.ReadDatabases() if data['id'] == db_name))
        return self._db

    def set_db_from_config(self, config_file=None):
        # type: (str) -> dict
        """
        Method sets this class' DB object, given a config file, using this class' helper method. It defaults to the
        config file used to instantiate this class
        :param config_file: the path to a configuration file containing the database name
        :return: a dictionary object, representing the requested DB
        """
        if not config_file:
            config_file = self.config_file

        cfp = configparser.ConfigParser()
        with open(config_file) as cfg:
            cfp.read_file(cfg)
        db_name = cfp.get('Cosmos', 'db_name')
        return self.set_db_from_name(db_name=db_name)

    def get_db(self):
        # type: () -> dict
        """
        Method gets this instance's DB object, if not available, it creates it, using this class' helper method
        :return: a dictionary object, representing the requested DB
        """
        if not self._db:
            self._db = self.set_db_from_config()
        return self._db

    def get_collection_from_name(self, collection_name):
        # type: (str) -> dict
        """
        Method returns a collection object, given its name, using this class' cosmos collection client. The collection
        is cached in a class member variable and created only if it doesn't exist or if it is different to the one
        being requested.
        :param collection_name: the name of the collection to fetch
        :return: a dictionary object, representing the requested collection
        """
        if not self._collection or self._collection['id'] != collection_name:
            db_link = self.get_link_from_object(self.get_db())
            self._collection = next(
                (coll for coll in self.client.ReadCollections(db_link) if coll['id'] == collection_name)
            )
        return self._collection

    def get_collection_from_config(self, config_file=None):
        # type: (str) -> dict
        """
        Method sets this class' collection object, given a config file, using this class' helper method. It defaults to
        the config file used to instantiate this class
        :param config_file: the path to a configuration file containing the database name
        :return: a dictionary object, representing the requested collection
        """
        if not config_file:
            config_file = self.config_file
        cfp = configparser.ConfigParser()
        with open(config_file) as cfg:
            cfp.read_file(cfg)
        collection_name = cfp.get('Cosmos', 'collection_name')
        return self.get_collection_from_name(collection_name=collection_name)

    def get_collection(self):
        # type: () -> dict
        """
        Method gets this instance's collection object, if not available, it creates it, using this class' helper method
        :return: a dictionary object, representing the requested collection
        """
        if not self._collection:
            self._collection = self.get_collection_from_config()
        return self._collection

    @staticmethod
    def _with_error_catching(func, *args, **kwargs):
        """
        Helper method to wrap function execution in a try/catch and log errors
        :param func: reference to the function to be executed
        :param args: arguments of the function to be executed
        :param kwargs: keyword arguments of the function to be executed
        :return: the result of function call, if executed correctly
        """
        try:
            return func(*args, **kwargs)
        except errors.HTTPFailure as e:
            raise CosmosDBConnectionException("CosmosDBService encountered a connection failure: %s" % str(e))
        except ProxyError as e:
            raise CosmosDBConnectionException("CosmosDBService encountered a connection error: %s" % str(e))

    def get_request_options(self, document):
        # type: (dict) -> dict
        if self.get_collection().get('partitionKey'):
            # find partitionKey name
            partition_key_name = self.get_collection().get('partitionKey')['paths'][0].split('/', 1)[1]
            return {'partitionKey': document[partition_key_name]}
        return {}

    def get_doc_link_by_id(self, doc_id, db=None, collection=None):
        # type: (str, dict, dict) -> str
        """
        Method formats a link to a document, given its id
        :param doc_id: the id of the object
        :param db: optional parameter pointing at the db containing the document. Defaults to this
            class' member variable
        :param collection: optional parameter pointing at the collection containing the document. Defaults to this
            class' member variable
        :return: a string, representing the link to the provided document id
        """
        if not db:
            db = self.get_db()
        if not collection:
            collection = self.get_collection()
        return "dbs/{}/colls/{}/docs/{}".format(db['id'], collection['id'], doc_id)

    @staticmethod
    def get_query_obj(doc_field, doc_value):
        # type: (str, str) -> dict
        """
        Method prepares a query object, given the name of the document that the user is looking for
        :param doc_field: name of the document field to be used in the query
        :param doc_value: value of the document field to be used in the query
        :return: a query in the form of a dictionary
        """
        return {'query': DOCUMENT_QUERY.format(doc_field, doc_value)}

    def get_docs_by_query(self, query, max_items=1000):
        # type: (dict, int) -> list
        """
        Method queries for a document with the provided name. It returns it if only one found, raises exception if
        more than one found. Returns empty object otherwise.
        :param query: query to be used by this method
        :param max_items: maximum number of items to return, if user does not wants to limit the request, this
        parameter should be -1
        :return: a list of documents found in the DB
        """
        if not query['query']:
            raise Exception('Badly formatted query')

        docs = list(
            self._with_error_catching(
                self.client.QueryDocuments,
                self.get_link_from_object(self.get_collection()),
                query,
                {
                    'enableCrossPartitionQuery': True,  # mandatory
                    'maxItemCount': max_items,
                },
            )
        )
        doc_num = len(docs)
        if doc_num > 1000:
            self.logger.warning(
                "Too many documents found with provided query\n %s\n. Please narrow down the results",
                query['query']
            )
        elif doc_num > 0:
            return docs
        else:
            return []

    def get_docs_by_fields(self, doc_filter, max_items=1000):
        # type: (dict, int) -> list
        """
        Method queries for a document with the provided name. It returns it if only one found, raises exception if more
        than one found. Returns empty object otherwise.
        :param doc_filter: name(s) and value(s) of the document fields to be used in the query
        :param max_items: maximum number of items to return
        :return: a list of documents found in the DB
        """
        where = []
        for key, val in doc_filter.items():
            where.append('c.{} = "{}"'.format(key, val))

        if self.get_collection().get('partitionKey'):
            partition_key_name = self.get_collection().get('partitionKey')['paths'][0].replace("/", "", 1)
            query_template = 'SELECT c._self, c.{} FROM c WHERE {{}}'.format(partition_key_name)
        else:
            query_template = 'SELECT c._self FROM c WHERE {}'
        query = {'query': query_template.format(" and ".join(where))}
        return self.get_docs_by_query(query, max_items=max_items)

    def get_docs_by_field(self, doc_field, doc_value, max_items=1000):
        # type: (str, str, int) -> list
        """
        Method queries for a document with the provided name. It returns it if only one found, raises exception if more
        than one found. Returns empty object otherwise.
        :param doc_field: name of the document field to be used in the query
        :param doc_value: value of the document field to be used in the query
        :param max_items: maximum number of items to return
        :return: a list of documents found in the DB
        """
        return self.get_docs_by_query(self.get_query_obj(doc_field=doc_field, doc_value=doc_value), max_items=max_items)

    def get_doc_by_field(self, doc_field, doc_value):
        # type: (str, str) -> dict
        """
        Method queries for a document with the provided name. It returns it if only one found, raises exception if more
        than one found. Returns empty object otherwise.
        :param doc_field: name of the document field to be used in the query
        :param doc_value: value of the document field to be used in the query
        :return: a dictionary, representing a document in the DB
        """
        docs = self.get_docs_by_field(doc_field, doc_value)
        doc_num = len(docs)
        if doc_num > 1:
            raise Exception("Too many documents found with field %s equal to %s", doc_field, doc_value)
        elif doc_num > 0:
            return docs[0]
        else:
            return {}

    def get_doc_by_id_and_partition(self, doc_id, partition_key=None):
        # type: (str, str) -> dict
        """
         Method directly fetches a document with the provided id.
        :param doc_id: name of the document that the user is looking for
        :param partition_key: value of the partition key used in the request
        :return: a dictionary, representing a document in the DB
        """
        partition_key = partition_key or doc_id
        return self._with_error_catching(
            self.client.ReadDocument,
            self.get_doc_link_by_id(doc_id=doc_id),
            {'partitionKey': partition_key},
        )

    def create_doc(self, document):
        # type: (dict) -> dict
        """
        Method creates a new document in the DB with the content of the one provided.
        :param document: the content of the new document to be created in the DB.
        :return: the created document
        """
        return self._with_error_catching(
            self.client.CreateDocument,
            self.get_link_from_object(self.get_collection()),
            document,
            # support partition key different than id
            self.get_request_options(document)
        )

    def create_or_update_doc(self, document):
        # type: (dict) -> dict
        """
        Method attempts to fetch and update an existing document in the DB. If not found, it creates it with the content
        of the one provided.
        :param document: the content of the new document to be created in the DB.
        :return: the created or updated document
        """
        retrieve_trip = None
        try:
            retrieve_trip = self.get_doc_by_id_and_partition(document["id"])
        except KeyError:
            unique_id = uuid.uuid4().hex
            document['id'] = unique_id
            self.logger.warning(
                "Provided document %s has no 'id' field, it is going to be created "
                "and a new ID %s is going to be assigned to it",
                document,
                unique_id
            )
        if not retrieve_trip:
            new_or_updated = self.create_doc(document)
        else:
            for key in document:
                if not key.startswith("_"):
                    retrieve_trip[key] = document[key]
            new_or_updated = self._with_error_catching(
                self.client.ReplaceDocument,
                retrieve_trip["_self"],
                retrieve_trip,
                self.get_request_options(document)
            )
        return new_or_updated

    def get_all_documents(self):
        # type: () -> list
        """
        Method fetches all document in a given collection. The collection used is the one represented in this class'
        member variable.
        :return: a list of all documents in a collection
        """
        return list(
            self._with_error_catching(self.client.ReadDocuments, self.get_link_from_object(self.get_collection()))
        )

    def remove_all_documents(self):
        # type: () -> bool
        """
        Method removes all document in a given collection. The collection used is the one represented in this class'
        member variable.
        :return: True or False
        """
        return self.remove_documents(documents=self.get_all_documents())

    def remove_documents(self, documents):
        # type: (list) -> bool
        """
        Method removes all docs . The collection used is the one represented in this class'  member variable.
        :param documents: the documents to be deleted in the DB.
        :return: True if successful, False if not
        """
        ret_code = False
        for document in documents:
            try:
                self._with_error_catching(
                    self.client.DeleteDocument, document["_self"],
                    self.get_request_options(document)
                )
            except CosmosDBConnectionException:
                ret_code = True
                self.logger.error("An error occurred while deleting document document %s", document['id'])
        return ret_code
