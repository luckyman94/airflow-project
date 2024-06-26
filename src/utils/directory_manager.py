import os
import shutil

from src.utils.s3_manager import S3Manager
class DirectoryManager:
    def __init__(self):
        self.s3 = S3Manager()
        self.bucket_name = 'datalake-isep'

    def remove_files_with_extension(self, directory, extension):
        directory = os.path.abspath(directory)
        for root, dirs, files in os.walk(directory, topdown=False):
            for file in files:
                if file.endswith(extension):
                    filepath = os.path.join(root, file)
                    os.remove(filepath)
                    print(f"Removed {filepath}")

    def clean_empty_subdirectories(self, directory):
        directory = os.path.abspath(directory)
        for root, dirs, files in os.walk(directory, topdown=False):
            if not dirs and not files:
                os.rmdir(root)
                print(f"Removed empty directory {root}")

    def clean_directory_with_wrong_name(self,directory, name):
        try:
            for root, dirs, files in os.walk(directory, topdown=False):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    if dir_name != name:
                        shutil.rmtree(dir_path)
                        print(f"Directory has been deleted : {dir_path}")

        except Exception as e:
            print(f"Error while deleting files : {e}")
