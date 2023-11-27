#Task 3
#step 2

import yaml

def read_db_creds():
    """Read database credentials from the db_creds.yaml file."""
    with open('db_creds.yaml') as f:
        db_creds = yaml.safe_load(f)

    return db_creds

#step 3

import sqlalchemy
from sqlalchemy import create_engine

def init_db_engine():
    """Create and initialize a SQLAlchemy database engine."""
    db_creds = read_db_creds()

    # Construct the connection string
    connection_string = f"postgresql://{db_creds['aicore_admin']}:{db_creds['AiCore2022']}@{db_creds['data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com']}" \
                        f":5432/{db_creds['postgres']}"

    # Create the SQLAlchemy engine
    engine = create_engine(connection_string)

    return engine

#step 4

import sqlalchemy

def list_db_tables(engine):
    """List all tables in the database."""
    inspector = sqlalchemy.inspect(engine)
    table_names = inspector.get_table_names()
    return table_names


#step 5

import pandas as pd

class DataExtractor:

    def extract_rds_data(self, table_name):
        """Extract data from a specific table in the RDS database."""
        engine = init_db_engine()
        connection = engine.connect()

        table = sqlalchemy.Table(table_name, engine.metadata, autoload=True)
        select_stmt = sqlalchemy.select(table)
        results = connection.execute(select_stmt)

        data = []
        for row in results:
            data.append(row)

        connection.close()
        engine.dispose()

        return pd.DataFrame(data)

    def read_rds_table(self, db_connector, table_name):
        """Extract the specified table from the RDS database to a pandas DataFrame."""
        data = db_connector.extract_rds_data(table_name)
        return pd.DataFrame(data)


#step 6

import pandas as pd

class DataCleaning:

    def clean_user_data(self, user_data_df):
        """Clean user data from the DataFrame."""

        # Handle NULL values
        user_data_df.dropna(axis=0, inplace=True)

        # Clean date columns
        for col in user_data_df.columns:
            if pd.api.types.is_datetime_dtype(user_data_df[col]):
                # Convert date strings to datetime objects
                user_data_df[col] = pd.to_datetime(user_data_df[col])

        # Clean number columns
        for col in user_data_df.columns:
            if pd.api.types.is_numeric_dtype(user_data_df[col]):
                # Convert non-numeric values to NaN
                user_data_df[col] = pd.to_numeric(user_data_df[col], errors='coerce')

        # Handle incorrectly typed values
        for col in user_data_df.columns:
            if pd.api.types.is_string_dtype(user_data_df[col]):
                # Strip leading and trailing whitespace
                user_data_df[col] = user_data_df[col].str.strip()

        # Check for rows with incorrect information
        # ... Implement specific checks for user data

        return user_data_df

#step 7

import sqlalchemy as sa

class DatabaseConnector:

    def upload_to_db(self, df, table_name):
        """Upload a pandas DataFrame to the specified table in the database."""
        engine = init_db_engine()

        # Convert pandas DataFrame to SQLAlchemy Table object
        table = sa.Table(table_name, engine.metadata, autoload=True)

        # Insert DataFrame values into the database table
        df.to_sql(table_name, engine, if_exists='append', index=False)

#step 8

import pandas as pd

# Extract user data from the RDS database
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
user_data_df = data_extractor.read_rds_table(db_connector, 'users')

# Clean the user data
data_cleaner = DataCleaning()
cleaned_user_data_df = data_cleaner.clean_user_data(user_data_df)

# Upload the cleaned user data to the sales_data database
db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')