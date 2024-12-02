import logging
logging.basicConfig(level=logging.DEBUG)

from hdfs import InsecureClient
import os

# Connect to HDFS
client = InsecureClient('http://localhost:9870', user='hadoop')

# Define local directory and HDFS directory
local_dir = './data'
hdfs_dir = '/data'

# Upload files
def upload_files_to_hdfs():
    client.makedirs(hdfs_dir)  # Create the directory in HDFS
    
    for file_name in os.listdir(local_dir):
        local_path = os.path.join(local_dir, file_name)
        hdfs_path = f'/{file_name}'
        client.upload(hdfs_path, local_path)
        print(f'Uploaded {file_name} to {hdfs_path}')