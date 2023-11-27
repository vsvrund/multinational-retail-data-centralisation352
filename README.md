Project Title: Data Extraction and Cleaning for Sales Analytics

 Table of Contents

- Introduction
- Project Overview
- Data Extraction and Cleaning Steps
- Data Uploading to Database
- GitHub Repository Updates

Introduction:

This project aims to develop a data pipeline for extracting and cleaning sales data from various sources, including CSV files stored in AWS S3, JSON files stored in AWS S3, and PostgreSQL databases. The cleaned data will then be uploaded to a PostgreSQL database for further analysis.

Project Overview:

The project involves the following steps:

1. Data Extraction:

- Extract product data from CSV files stored in AWS S3 using the boto3 library.
- Extract store data from the API using the requests library.
- Extract order data from a PostgreSQL database using the psycopg2 library.

2. Data Cleaning:

- Clean product data by converting weights to kilograms, handling NULL values, and removing unnecessary columns.
- Clean store data by handling NULL values, stripping leading and trailing whitespace, and converting numeric columns to appropriate data types.
- Clean order data by removing unnecessary columns, handling NULL values, and converting numeric columns to appropriate data types.

3. Data Uploading to Database:

- Upload cleaned product data to a PostgreSQL database table named dim_products.
- Upload cleaned store data to a PostgreSQL database table named dim_store_details.
- Upload cleaned order data to a PostgreSQL database table named orders_table.



Data Extraction and Cleaning Steps

Product Data
- Extract product data from CSV files stored in AWS S3 using the boto3 library.
- Convert product weights to kilograms.
- Handle NULL values in product data.
- Remove unnecessary columns from product data.

Store Data
- Extract store data from the API using the requests library.
- Handle NULL values in store data.
- Strip leading and trailing whitespace from store data.
- Convert numeric columns in store data to appropriate data types.

Order Data
- Extract order data from a PostgreSQL database using the psycopg2 library.
- Remove unnecessary columns from order data.
- Handle NULL values in order data.
- Convert numeric columns in order data to appropriate data types.

Data Uploading to Database
- Upload cleaned product data to a PostgreSQL database table named dim_products.
- Upload cleaned store data to a PostgreSQL database table named dim_store_details.
- Upload cleaned order data to a PostgreSQL database table named orders_table.

GitHub Repository Updates
- Update README file to reflect project progress.
- Add code comments to improve code readability.
- Commit code changes to GitHub repository.
- Push committed changes to GitHub remote repository.


License

 This project is licensed under the MIT License.
