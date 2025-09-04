import duckdb
import os

def setup_ducklake(con):
    ## install ducklake
    con.execute("""INSTALL ducklake;
                    LOAD ducklake;""")
    
    ## Create the metabase using duckdb as database
    con.execute("""ATTACH 'ducklake:ducklake_metadata/metadata.ducklake' AS my_ducklake 
                    (DATA_PATH 'data_files');
                    USE my_ducklake;""")

    ## Or use PostgreSQL or other sql database for Metadata, ex:
    # con.execute("""INSTALL ducklake;
    #             INSTALL postgres;
    #             ATTACH 'ducklake:postgres:dbname=ducklake user=username password=xyz host=localhost port=5432' AS my_ducklake (DATA_PATH 'data_files');
    #             USE my_ducklake;""")

def define_schema(con):
    con.sql("""
    CREATE SCHEMA IF NOT EXISTS test_schema;
    USE test_schema;
    """) 

def create_duckdb_table(con):
    con.sql("""
    CREATE TABLE IF NOT EXISTS customer (
        customer_id INTEGER NOT NULL,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        date_joined DATE NOT NULL
    );
    """)

    con.sql("""
        INSERT INTO customer (customer_id, first_name, last_name, date_joined) VALUES
        (1, 'Jane', 'Dunbar', '2023-01-11'),
        (2, 'Jimmy', 'Smith', '2024-08-26'),
        (3, 'Alice', 'Johnston', '2023-05-05');
    """)

def alter_table_schema(con):
    print('alter data')
    con.sql("""
        ALTER TABLE customer
        ADD COLUMN test INTEGER DEFAULT 10;
        """)

def check_snapshots(con):
    result = con.sql("""
        SELECT * FROM ducklake_snapshots('my_ducklake');
        """)
    result.show()

def delete_data(con):
    print('Delete data')
    con.sql("""
    DELETE FROM customer WHERE customer_id = 2;
    """)

def query_data_snapshot(con):
    print('Select data before delete')
    result = con.sql(f"""
    SELECT * FROM customer AT (VERSION => 3);
    """)
    result.show()

def query_data(con):
    print('Select data')
    result = con.sql(f"""
    SELECT * FROM customer;
    """)
    result.show()

con = duckdb.connect('my_database.duckdb')
setup_ducklake(con)
create_duckdb_table(con)
alter_table_schema(con)
check_snapshots(con)
delete_data(con)
query_data(con)
query_data_snapshot(con)
con.close()