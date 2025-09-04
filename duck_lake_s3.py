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
def create_table_from_s3(con):
    print(f'Create table {duck_lake_name}.scraper_staging.{table_name} from S3 data\n')
    start_time = time.time() 

    con.sql(f"""
        CREATE OR REPLACE TABLE scraper_staging.{table_name} AS
        SELECT * FROM read_csv_auto('{S3_CSV_PATH}');
    """) 
    end_time = time.time()   
    execution_time = (end_time - start_time)/60
    print(f"Execution time: {execution_time} minute \n")

def create_s3_duck_lake(con):
    print(f'Create/ATTACH ducklake {duck_lake_name}.\n')
    con.execute(f"""
    ATTACH 'ducklake:{duck_lake_name}/metadata.ducklake' AS {duck_lake_name}  (
    DATA_PATH '{DUCKLAKE_PARQUET_FILES_PATH}'
    );
    USE {duck_lake_name};
    """)

def define_schema(con):
    con.sql("""
    CREATE SCHEMA IF NOT EXISTS scraper_staging;
    """) 

def query_data(con):
    print(f'Select data :\n SELECT * FROM {duck_lake_name}.scraper_staging.{table_name}; \n')
    result = con.sql(f"""
    SELECT * FROM {duck_lake_name}.scraper_staging.{table_name} limit 5;
    """)
    result.show() 

def check_snapshots(con):
    result = con.sql(f"""
        SELECT * FROM ducklake_snapshots('{duck_lake_name}');
        """)
    result.show()

def query_data_snapshot(con):
    print('query snapshot data')
    result = con.sql(f"""
    SELECT * FROM {duck_lake_name}.scraper_staging.{table_name} AT (VERSION => 2) limit 5;
    """)
    result.show()

def check_rows_count(con):
    result = con.sql(f"""
    SELECT count(*) FROM {duck_lake_name}.scraper_staging.{table_name};
    """).fetchone()
    print(f"Number of rows: {result[0]} \n")

def append_data(con):
    print(f"append data \n COPY {duck_lake_name}.scraper_staging.{table_name} FROM '{S3_CSV_PATH}' \n")
    con.execute(f"COPY {duck_lake_name}.scraper_staging.{table_name} FROM '{S3_CSV_PATH}'")

def delete_rows_and_rollback(con):
    print("Start transaction - Delete table \n")
    result = con.sql(f"""
        BEGIN TRANSACTION;
        DELETE FROM {duck_lake_name}.scraper_staging.{table_name};
        select count (*) FROM {duck_lake_name}.scraper_staging.{table_name};""")
    result.show()

    print("Rollback table before commit \n")
    result = con.sql(f"""
        ROLLBACK;
        select count(8) FROM {duck_lake_name}.scraper_staging.{table_name};""")
    result.show()

    print("Start transaction - Delete part of the table and commit \n")
    result = con.sql(f"""
        BEGIN TRANSACTION;
        DELETE FROM {duck_lake_name}.scraper_staging.{table_name} WHERE column11 is not null;
        COMMIT;
        select count (*) FROM {duck_lake_name}.scraper_staging.{table_name};
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



load_dotenv()
con = duckdb.connect(read_only = False)
print('### Setup ducklake\n')
setup_s3_connection(con)
create_s3_duck_lake(con)
define_schema(con)
print('### Creating table from S3 \n')
create_table_from_s3(con)
query_data(con)
print('### Test append data on the table\n')
check_rows_count(con)
append_data(con)
check_rows_count(con)
print('### Check snapshots \n')
check_snapshots(con)
query_data_snapshot(con)
print('### Test rollback \n')
delete_rows_and_rollback(con)
con.close()