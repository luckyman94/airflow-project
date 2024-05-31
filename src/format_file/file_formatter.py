from pyspark.sql import SparkSession
from pyspark.sql.functions import to_utc_timestamp, col


class FileFormatter:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("File Formatter") \
            .getOrCreate()

    def normalize_dates_to_utc(self, df):
        for column in df.columns:
            if dict(df.dtypes)[column] == 'timestamp':
                df = df.withColumn(column, to_utc_timestamp(col(column), "UTC"))
        return df


    def read_csv(self, input_path, header=True, infer_schema=True):
        return self.spark.read.csv(input_path, header=header, inferSchema=infer_schema)

    def convert_to_parquet(self, df, output_path):
        df.write.parquet(output_path)

    def stop(self):
        self.spark.stop()