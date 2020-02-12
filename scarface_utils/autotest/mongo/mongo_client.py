from pymongo import MongoClient


class MongoConnectionManager(object):
    """
    Initializes an instance of MongoClient, given basic configuration
    """
    def __init__(self, client, host, db, collection):
        self.client = client
        self.host = host
        self.db = db
        self.collection = collection

    @classmethod
    def from_config(cls, host, db_name=None, collection_name=None):
        client = MongoClient(host=host)

        db = client.get_database(db_name) if db_name else None
        collection = db.get_collection(collection_name) if db_name and collection_name else None
        return cls(client=client, host=host, db=db, collection=collection)

    def get_document_by_key_value(self, key, value, db_name=None, collection_name=None):
        db = self.client.get_database(db_name) if db_name and collection_name else self.db
        collection = db.get_collection(collection_name) if collection_name else self.collection
        return collection.find({key: value})


