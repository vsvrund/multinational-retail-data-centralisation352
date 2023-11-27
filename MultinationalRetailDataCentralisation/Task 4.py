#Task 4
#step 2
import tabula

class DataExtractor:

    def retrieve_pdf_data(self, https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf):
        """Extract data from a PDF document and return it as a pandas DataFrame."""
        # Use tabula-py to extract tables from the PDF document
        tables = tabula.read_pdf(https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf, pages='all')

        # Combine all tables into a single pandas DataFrame
        data = []
        for table in tables:
            data.extend(table.to_records(index=False))

        df = pd.DataFrame(data)
        return df




#step 3


import pandas as pd

class DataCleaning:

    def clean_card_data(self, card_data_df):
        """Clean card data from the DataFrame."""

        # Handle NULL values
        card_data_df.dropna(axis=0, inplace=True)

        # Strip leading and trailing whitespace from all columns
        card_data_df = card_data_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Convert numeric columns to the appropriate data type
        for col in card_data_df.columns:
            if pd.api.types.is_numeric_dtype(card_data_df[col]):
                card_data_df[col] = pd.to_numeric(card_data_df[col], errors='coerce')

        # Check for invalid card numbers
        # ... Implement specific checks for card number validity

        # Check for invalid expiration dates
        # ... Implement specific checks for expiration date validity

        return card_data_df
    
#step 4

import pandas as pd
import tabula

# Extract card data from the PDF document
data_extractor = DataExtractor()
card_data_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

# Clean the card data
data_cleaner = DataCleaning()
cleaned_card_data_df = data_cleaner.clean_card_data(card_data_df)

# Upload the cleaned card data to the sales_data database
db_connector = DatabaseConnector()
db_connector.upload_to_db(cleaned_card_data_df, 'dim_card_details')

