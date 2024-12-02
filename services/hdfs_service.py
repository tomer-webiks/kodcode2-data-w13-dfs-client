from hdfs import InsecureClient
import os

# Connect to HDFS
client = InsecureClient('http://localhost:9870', user='hadoop')