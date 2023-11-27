#Task 5
#Step 1
import requests

class DataExtractor:

    def list_number_of_stores(self, endpoint, headers):
        """Retrieve the number of stores from the API."""
        response = requests.get(endpoint, headers=headers)

        if response.status_code == 200:
            # Extract the number of stores from the JSON response
            number_of_stores = response.json()['number_of_stores']
            return number_of_stores
        else:
            raise Exception('Error retrieving number of stores: {}'.format(response.status_code))

#step 3

import requests
import pandas as pd

class DataExtractor:

    def retrieve_stores_data(self, endpoint, headers):
        """Retrieve all stores data from the API."""
        number_of_stores = self.list_number_of_stores(endpoint, headers)

        # Extract all stores data from the API
        store_data = []
        for i in range(1, number_of_stores + 1):
            store_endpoint = endpoint.format(i)
            response = requests.get(store_endpoint, headers=headers)

            if response.status_code == 200:
                store_data.append(response.json())
            else:
                raise Exception('Error retrieving store data: {}'.format(store_endpoint))

        # Convert the extracted store data to a pandas DataFrame
        df = pd.DataFrame(store_data)
        return df

#step 4

import pandas as pd

class DataCleaning:

    def clean_store_data(self, store_data_df):
        """Clean store data from the DataFrame."""

        # Handle NULL values
        store_data_df.dropna(axis=0, inplace=True)

        # Strip leading and trailing whitespace from all columns
        store_data_df = store_data_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Convert numeric columns to the appropriate data type
        for col in store_data_df.columns:
            if pd.api.types.is_numeric_dtype(store_data_df[col]):
                store_data_df[col] = pd.to_numeric(store_data_df[col], errors='coerce')

        # Check for invalid store numbers
        # ... Implement specific checks for store number validity

        # Check for missing or invalid store addresses
        # ... Implement specific checks for store address validity

        return store_data_df

#step 5

import requests
import pandas as pd

# API endpoint URLs
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
number_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

# API key header information
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

# Data extraction
data_extractor = DataExtractor()
store_data_df = data_extractor.retrieve_stores_data(store_endpoint, headers)

# Data cleaning
data_cleaner = DataCleaning()
cleaned_store_data_df = data_cleaner.clean_store_data(store_data_df)

# Data upload
db_connector = DatabaseConnector()
db_connector.upload_to_db(cleaned_store_data_df, 'dim_store_details')


