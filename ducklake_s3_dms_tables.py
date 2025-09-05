import duckdb
import boto3
from dotenv import load_dotenv
import os
import time

S3_BUCKET = 'bc569494975354-dwh-data-integration'
ROOT_S3_PREFIX = 'staging/dms_test/scraper_staging/' 
DUCKLAKE_PARQUET_FILES_PATH = 's3://bc569494975354-dwh-data-integration/staging/test_ducklake'
duck_lake_name = 'ducklake_s3'

# Initialize the S3 client
def get_s3_client():
    session = boto3.Session(
        aws_access_key_id=os.getenv('S3_KEY_ID'),
        aws_secret_access_key=os.getenv('S3_SECRET'),
        region_name='eu-west-1'
    )
    return session.client('s3')

# Extract unique folder names
def extract_unique_folders():
    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=ROOT_S3_PREFIX, Delimiter='/')

    folder_names = set()

    for page in page_iterator:
        for content in page.get('CommonPrefixes', []):
            folder_path = content.get('Prefix') 
            if folder_path:
                folder_name = folder_path.strip('/').split('/')[-1]
                folder_names.add(folder_name)

    return folder_names


def setup_s3_connection(con):
    print('Setup S3 connection.')
    con.execute("""
        INSTALL aws;
        LOAD aws;
        INSTALL httpfs;
        LOAD httpfs;
        INSTALL parquet;
        LOAD parquet;
        INSTALL ducklake;
        LOAD ducklake;
    """)
    con.execute(f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{os.getenv('S3_KEY_ID')}',
            SECRET '{os.getenv('S3_SECRET')}',
            REGION 'eu-west-1',
            ENDPOINT 's3.eu-west-1.amazonaws.com'
        );
    """)


def create_tables_from_folders(con, folders):
    for folder_name in folders:
        print(f'Creating table scraper_staging.{folder_name} from folder: {folder_name}\n')

       
        csv_path = f"s3://{S3_BUCKET}/{ROOT_S3_PREFIX}{folder_name}/*.csv"
        start_time = time.time()

        con.sql(f"""
            CREATE OR REPLACE TABLE scraper_staging.{folder_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}');
        """)
        
        end_time = time.time()
        execution_time = (end_time - start_time) / 60
        print(f"Execution time for {folder_name}: {execution_time} minute \n")

def create_s3_duck_lake(con):
    print(f'Create/ATTACH ducklake {duck_lake_name}.\n')
    con.execute(f"""
        ATTACH 'ducklake:{duck_lake_name}/metadata.ducklake' AS {duck_lake_name} (
        DATA_PATH '{DUCKLAKE_PARQUET_FILES_PATH}'
        );
        USE {duck_lake_name};
    """)

def define_schema(con):
    con.sql("CREATE SCHEMA IF NOT EXISTS scraper_staging;")

def query_data(con, table_name):
    print(f'Select data from {table_name}:\n')
    result = con.sql(f"""
    SELECT * FROM {duck_lake_name}.scraper_staging.{table_name} LIMIT 5;
    """)
    result.show()

load_dotenv()
con = duckdb.connect(read_only=False)
setup_s3_connection(con)
create_s3_duck_lake(con)
define_schema(con)

# Extract folder names and create tables
folders = extract_unique_folders()
create_tables_from_folders(con, folders)

for folder_name in folders:
    print(f'Querying data from table: {folder_name}')
    query_data(con, folder_name)

if folders:
    first_folder_name = next(iter(folders))
    query_data(con, first_folder_name)

con.close()
