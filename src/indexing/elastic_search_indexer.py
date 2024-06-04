import pandas as pd
from elasticsearch import Elasticsearch, helpers


HOME = "/opt/airflow"
DATALAKE_ROOT_FOLDER = HOME + "/cert/http_ca.crt"
class ElasticsearchIndexer:
    def __init__(self):
        self.es_hosts = ["https://localhost:9200"]
        self.auth = ('elastic', ' yy34aLwTBnEMX*seQjlL')
        self.index_name =  "recommendations"
        self.es = Elasticsearch(
            hosts=self.es_hosts,
            basic_auth=self.auth,
            verify_certs=True,
            ca_certs=DATALAKE_ROOT_FOLDER

        )

    def test_connection(self):
        try:
            info = self.es.info()
            print("Connected to Elasticsearch: ", info)
        except Exception as e:
            print("Error connecting to Elasticsearch:", e)

    def read_parquet(self, file_path):
        df = pd.read_parquet(file_path)
        return df.to_dict(orient='records')

    def index_data(self, records):
        actions = [
            {
                "_index": self.index_name,
                "_source": record
            }
            for record in records
        ]
        helpers.bulk(self.es, actions)
        print(f"Indexed {len(records)} records to index {self.index_name}.")

    def index_from_parquet(self, file_path):
        records = self.read_parquet(file_path)
        self.index_data(records)



if __name__ == '__main__':
    ElasticsearchIndexer().test_connection()