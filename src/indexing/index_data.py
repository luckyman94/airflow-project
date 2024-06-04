import json
import logging
import os
import sys

import pandas as pd
from elasticsearch import Elasticsearch, helpers




class ElasticsearchIndexer:
    def __init__(self):
        self.host = "http://localhost:9200"
        self.index_name = "recommendations"
        self.es = Elasticsearch(
            hosts=self.host,
            verify_certs=False,
        )


    def get_client(self):
        return self.es
    def test_connection(self):
        try:
            info = self.es.info()
            print("Connected to Elasticsearch: ", info)
        except Exception as e:
            print("Error connecting to Elasticsearch:", e)

    def read_parquet(self, file_path):
        try:
            print("Chemin du fichier:", os.path.abspath(file_path))
            df = pd.read_parquet(file_path)
            return df.to_dict(orient='records')
        except Exception as e:
            logging.error("Error reading Parquet file:", exc_info=True)
            return []

    def index_data(self, records):
        try:
            actions = [
                {"_index": self.index_name, "_source": record}
                for record in records
            ]
            helpers.bulk(self.es, actions)
            logging.info(f"Indexed {len(records)} records to index {self.index_name}.")
        except Exception as e:
            logging.error("Error during indexing:", exc_info=True)

    def index_from_parquet(self, file_path):
        records = self.read_parquet(file_path)
        if records:
            self.index_data(records)

    def search_data(self, query, size=10):
        try:
            response = self.es.search(
                index=self.index_name,
                body={"query": query, "size": size}
            )
            return response['hits']['hits']
        except Exception as e:
            logging.error("Error searching for data:", exc_info=True)
            return []

    def save_index_to_json(self, output_file):
        try:
            all_records = []
            query = {"match_all": {}}
            response = self.es.search(index=self.index_name, body={"query": query, "size": 1000})
            all_records.extend(response['hits']['hits'])
            with open(output_file, 'w') as f:
                json.dump(all_records, f)
            print(f"Index saved to {output_file} successfully.")
        except Exception as e:
            logging.error("Error saving index to JSON:", exc_info=True)

if __name__ == '__main__':
    #mapping = {
    #
    #}
    ##indexer = ElasticsearchIndexer()
    ##client = indexer.get_client()
    #indexer.test_connection()
    #indexer.index_from_parquet("recommendations.parquet")
    #indexer.save_index_to_json("recommendations_index.json")
    #client.indices.create(index="recommendations")


    #Convert to ndjson

    with open('recommendations_index.json', 'r') as f:
        data = json.load(f)

    with open('recommendations_index.ndjson', 'w') as f:
        for item in data:
            # Écrire l'opération d'indexation
            index_line = json.dumps({"index": {"_index": item["_index"], "_id": item["_id"]}})
            f.write(index_line + "\n")
            # Écrire le document source
            source_line = json.dumps(item["_source"])
            f.write(source_line + "\n")


    #To export the file we have to do this : curl -H "Content-Type: application/x-ndjson" -XPOST "localhost:9200/recommendations/_bulk" --data-binary @/Users/ilan/airflow-docker/src/indexing/recommendations_index.ndjson
    # Verifier importation curl -X GET "localhost:9200/recommendations/_search?pretty"