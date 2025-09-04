# ducklake_test

### How to run:

Create a virtualenv:

    python3.12 -m venv env
    source env/bin/activate

install requirements:

    pip install - requirements.txt

source .env with S3 keys:

    source .env

run notebooks :

To run tables in s3:

    python3 duck_lake_s3.py

To teste creating local files(data_files path):

    python3 duck_lake_local

