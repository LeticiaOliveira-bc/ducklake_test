import duckdb
from dotenv import load_dotenv
import os
import time

DUCKLAKE_PARQUET_FILES_PATH = 's3://bc569494975354-dwh-data-integration/staging/test_ducklake'
S3_CSV_PATH = 's3://bc569494975354-dwh-data-integration/staging/dms_test/scraper_staging/brand_assets/LOAD00000001.csv'
duck_lake_name = 'ducklake_s3'
table_name = 'brand_assets'

def setup_s3_connection(con):
    print('Setup S3 connection.')
    key_id = os.getenv('S3_KEY_ID') 
    secret = os.getenv('S3_SECRET')
    ## Install all 
    con.execute("""
            INSTALL aws;
            LOAD aws;
            INSTALL httpfs;
            LOAD httpfs;
            INSTALL parquet;
            LOAD parquet;
            INSTALL ducklake;
            LOAD ducklake;"""
            )

    ## Create S3 connection
    con.execute(
    f"""CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{key_id}',
            SECRET '{secret}',
            REGION 'eu-west-1',
            ENDPOINT 's3.eu-west-1.amazonaws.com'
        );""")

## Read s3 data
def query_s3_data(con):
    print('Query data from S3')
    start_time = time.time() 

    result = con.sql(f"""
        CREATE TABLE {duck_lake_name}.scraper_staging.{table_name} AS
        SELECT * FROM read_csv_auto('{S3_CSV_PATH}');
    """) 
    result.show()
    end_time = time.time()   
    execution_time = (end_time - start_time)/60
    print(f"Execution time: {execution_time} minutes")

def create_s3_duck_lake(con):
    con.execute = f"""
    ATTACH 'ducklake:{duck_lake_name}/metadata.ducklake' AS {duck_lake_name}  (
    DATA_PATH '{DUCKLAKE_PARQUET_FILES_PATH}'
    );
    """

def define_schema(con):
    con.sql("""
    CREATE SCHEMA IF NOT EXISTS scraper_staging;
    """) 

def query_data(con):
    print('Select data')
    result = con.sql(f"""
    SELECT * FROM {duck_lake_name}.scraper_staging.{table_name};
    """)
    result.show() 

# def create_table(con):
#     con.sql("""CREATE TABLE IF NOT EXISTS metadata.customers (
#         customer_id INTEGER,
#         first_name STRING,
#         last_name STRING,
#         email STRING,
#         city STRING,
#         created_at TIMESTAMP
#     );""")

#     con.sql("""
#         INSERT INTO metadata.customers VALUES
#         (1, 'Alice', 'Smith', 'alice@example.com', 'New York', CURRENT_TIMESTAMP),
#         (2, 'Bob', 'Johnson', 'bob@example.com', 'San Francisco', CURRENT_TIMESTAMP);
#             """)


# def delete_data(con):
#     print('Delete data')
#     con.sql("""
#     DELETE FROM customer WHERE customer_id = 1;
#     """)

# def query_data_snapshot(con):
#     print('Select data before delete')
#     result = con.sql(f"""
#     SELECT * FROM customer AT (VERSION => 3);
#     """)
#     result.show()

load_dotenv()
con = duckdb.connect(read_only = False)
setup_s3_connection(con)
create_s3_duck_lake(con)
define_schema(con)
query_s3_data(con)
query_data(con)

con.close()