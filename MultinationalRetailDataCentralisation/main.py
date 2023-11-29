# main.py
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

if __name__ == "__main__":
    # Initialize DataExtractor and DatabaseConnector once
    data_extractor = DataExtractor()
    db_conn = DatabaseConnector()
    data_cleaner = DataCleaning()  # Initialize once

    # Initialize SQLAlchemy engine
    engine = db_conn.init_db_engine()
    print("Initialized SQLAlchemy engine:", engine)

    # List all tables
    all_tables = db_conn.list_db_tables()
    print(f"All tables: {all_tables}")

    # Extract orders data
    orders_table_name = 'orders_table'  
    orders_data = data_extractor.read_rds_table(db_conn, orders_table_name)

    # Get the number of stores
    num_stores = data_extractor.list_number_of_stores()
    print(f"Number of stores: {num_stores}")

    # Extract and clean stores data
    raw_data = data_extractor.retrieve_stores_data()
    cleaned_data = data_cleaner.clean_store_data(raw_data)
    if cleaned_data is not None:
        db_conn.upload_to_db(cleaned_data, 'dim_store_details')
        print("Uploaded cleaned data to dim_store_details table.")

    # Clean and upload orders data
    cleaned_orders_data = data_cleaner.clean_orders_data(orders_data)  # Use the same data_cleaner instance
    db_conn.upload_to_db(cleaned_orders_data, 'orders_table')  # Use the same db_conn instance