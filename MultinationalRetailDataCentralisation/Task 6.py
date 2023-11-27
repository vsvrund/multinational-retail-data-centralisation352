#Task 6
#Step 1
import boto3
import pandas as pd

class DataExtractor:

    def extract_from_s3(self, s3://data-handling-public/products.csv): 
        """Extract product information from S3 and return a pandas DataFrame."""
        # Create an S3 client
        s3_client = boto3.client('s3')

        # Download the CSV file from S3 to a temporary file
        with open('products.csv', 'wb') as tmpfile:
            s3_client.download_fileobj('data-handling-public', 'products.csv', tmpfile)

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv('products.csv')

        # Remove the temporary file
        import os
        os.remove('products.csv')

        return df


#step 2

import pandas as pd

class DataCleaning:

    def convert_product_weights(self, products_df):
        """Convert product weights to kilograms."""

        # Handle NULL values in the weight column
        products_df.weight.dropna(inplace=True)

        # Extract the weight unit from the weight column
        products_df['weight_unit'] = products_df['weight'].str.extract('(\D+)$', expand=False)

        # Convert weight values to kilograms
        products_df['weight'] = products_df['weight'].str.extract('(\d+)', expand=False).astype(float)

        # Convert ml weights to grams using a 1:1 ratio
        products_df.loc[products_df['weight_unit'] == 'ml', 'weight'] = products_df.loc[products_df['weight_unit'] == 'ml', 'weight'] / 1000

        # Remove all excess characters from the weight column
        products_df['weight'] = products_df['weight'].str.replace(',', '', regex=True)

        # Convert weight values to float
        products_df['weight'] = products_df['weight'].astype(float)

        # Drop the weight_unit column
        products_df.drop('weight_unit', axis=1, inplace=True)

        return products_df


#step 3
import pandas as pd

class DataCleaning:

    def clean_products_data(self, products_df):
        """Clean product data from the DataFrame."""

        # Handle NULL values
        products_df.dropna(axis=0, inplace=True)

        # Strip leading and trailing whitespace from all columns
        products_df = products_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Convert numeric columns to the appropriate data type
        for col in products_df.columns:
            if pd.api.types.is_numeric_dtype(products_df[col]):
                products_df[col] = pd.to_numeric(products_df[col], errors='coerce')

        # Check for invalid product IDs
        # ... Implement specific checks for product ID validity

        # Check for invalid product names
        # ... Implement specific checks for product name validity

        # Check for invalid product descriptions
        # ... Implement specific checks for product description validity

        return products_df


#step 4
import pandas as pd
import boto3

# Data extraction
data_extractor = DataExtractor()
products_df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')

# Data cleaning
data_cleaner = DataCleaning()
cleaned_products_df = data_cleaner.convert_product_weights(products_df)
cleaned_products_df = data_cleaner.clean_products_data(cleaned_products_df)

# Data upload
db_connector = DatabaseConnector()
db_connector.upload_to_db(cleaned_products_df, 'dim_products')

