#task 7
#step 1

import db_connector

# Create a database connector instance
db_connector = db_connector.DatabaseConnector()

# List all tables in the database
tables = db_connector.list_db_tables()

# Print the table names
for table in tables:
    print(table)

#step 2
import pandas as pd
import db_connector

# Create a database connector instance
db_connector = db_connector.DatabaseConnector()

# Extract the orders data from the RDS table
order_table_name = 'orders'  # Replace with the actual table name
orders_df = db_connector.read_rds_table(order_table_name)

# Print the number of orders
print(f"Number of orders: {len(orders_df)}")

#step 3
import pandas as pd

class DataCleaning:

    def clean_orders_data(self, orders_df):
        """Clean orders data from the DataFrame."""

        # Remove unnecessary columns
        orders_df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)

        # Handle NULL values
        orders_df.dropna(axis=0, inplace=True)

        # Convert numeric columns to the appropriate data type
        for col in orders_df.columns:
            if pd.api.types.is_numeric_dtype(orders_df[col]):
                orders_df[col] = pd.to_numeric(orders_df[col], errors='coerce')

        return orders_df


#step 4
import pandas as pd
import db_connector

# Data cleaning
data_cleaner = DataCleaning()
cleaned_orders_df = data_cleaner.clean_orders_data(orders_df)

# Data upload
db_connector = db_connector.DatabaseConnector()
db_connector.upload_to_db(cleaned_orders_df, 'orders_table')

