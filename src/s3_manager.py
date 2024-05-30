import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3Manager:
    def __init__(self, aws_conn_id='s3_conn'):
        self.s3 = S3Hook(aws_conn_id=aws_conn_id)
        self.bucket_name = 'datalake-isep'
    def list_csv_files_in_bucket(self):
        keys = self.s3.list_keys(bucket_name=self.bucket_name)
        csv_files = [key for key in keys if key.endswith('.csv')]
        return csv_files

    def list_parquet_files_in_bucket(self):
        keys = self.s3.list_keys(bucket_name=self.bucket_name)
        parquet_files = [key for key in keys if key.endswith('.parquet')]
        return parquet_files

    def download_file(self, key, local_path):
        self.s3.get_key(key, self.bucket_name).download_file(local_path)

    def upload_file(self, key, local_path):
        self.s3.load_file(local_path, key, self.bucket_name, replace=True)

    def upload_directory(self, local_directory, s3_directory=None, remove_files=False, extension='.parquet'):
        local_directory = os.path.abspath(local_directory)
        if not local_directory.endswith('/'):
            local_directory += '/'

        for root, dirs, files in os.walk(local_directory, topdown=False):
            for file in files:
                if file.endswith(extension):
                    filepath = os.path.join(root, file)
                    if s3_directory is None:
                        key = os.path.relpath(filepath, local_directory).replace(os.sep, '/')
                    else :
                        key = os.path.join(s3_directory, os.path.relpath(filepath, local_directory).replace(os.sep, '/'))
                    if not self.s3.check_for_key(key, bucket_name=self.bucket_name):
                        self.upload_file(key, filepath)
                        print(f"Uploaded {filepath} to {self.bucket_name}/{key}")
                    else:
                        print(f"File {file} already exists in the bucket {self.bucket_name}, skipping.")

                    if remove_files:
                        os.remove(filepath)
                        print(f"Removed {filepath}")

            if remove_files:
                try:
                    os.rmdir(root)
                    print(f"Removed directory {root}")
                except OSError as e:
                    print(f"Failed to remove directory {root}: {e}")

        if remove_files:
            try:
                if not os.listdir(local_directory):
                    os.rmdir(local_directory)
                    print(f"Removed directory {local_directory}")
            except OSError as e:
                print(f"Failed to remove directory {local_directory}: {e}")

    def delete_file(self, key):
        self.s3.delete_objects(bucket=self.bucket_name,keys=key)

