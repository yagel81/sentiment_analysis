
"""
Elasticsearch client class for connecting to an Elasticsearch server, storing records, and retrieving data.
"""

from elasticsearch import Elasticsearch
import json
import datetime


class ElasticsearchClient:
    def __init__(self):
        """
        Initializes an Elasticsearch client connecting to 'http://localhost:9200/'.
        Prints a message indicating successful connection.
        """
        self.client = Elasticsearch(['http://localhost:9200/'], verify_certs=True)
        print("Connected to Elasticsearch: {}".format(self.client.ping()))

    def __del__(self):
        """
        Closes the Elasticsearch client when the object is deleted.
        """
        if self.client is not None:
            self.client.close()

    def store_record(self, id, record, index_name='sentiment_twits'):
        """
        Stores a record in the specified Elasticsearch index.
        :param id: The ID of the record.
        :param record: The record data to be stored.
        :param index_name: The name of the Elasticsearch index.
        """
        try:
            outcome = self.client.create(id=id, index=index_name, body=json.dumps(record).encode('utf_8'))
            print(outcome)
        except Exception as ex:
            print('Error in storing a new record')
            print(str(ex))

    def get_data(self):
        """
        Prints information about all indices in the Elasticsearch cluster.
        """
        try:
            outcome = self.client.indices.get_alias()
            print(outcome)
        except Exception as ex:
            print('Error in getting data from indices')
            print(str(ex))

    def get_index_data(self, index_name='sentiment_news'):
        """
        Retrieves and prints data from the specified Elasticsearch index.
        :param index_name: The name of the Elasticsearch index.
        """
        try:
            result = self.client.search(
                index=index_name,
                body={
                    "query": {
                        "match_all": {}
                    }
                }
            )
            print(result)
        except Exception as ex:
            print('Error in getting data from the index')
            print(str(ex))
