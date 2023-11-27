#Task 8
import boto3
import json
import pandas as pd
import db_connector

# Download the JSON file from S3
s3_client = boto3.client('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
bucket_name = 'data-handling-public'
object_key = 'date_details.json'
s3_client.download_file(bucket_name, object_key, 'date_details.json')

# Load the JSON data into a pandas DataFrame
with open('date_details.json') as f:
    data = json.load(f)

df = pd.DataFrame(data)

# Data cleaning
# ... Perform any necessary data cleaning tasks here

# Upload the cleaned data to the database
db_connector = db_connector.DatabaseConnector()
db_connector.upload_to_db(df, 'dim_date_times')
