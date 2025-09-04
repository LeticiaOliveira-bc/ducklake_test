import duckdb 
con = duckdb.connect()

con.execute("""
    CREATE TABLE my_data (
    id INTEGER,
    name VARCHAR,
    value DOUBLE
);""")

con.execute("""
    INSERT INTO my_data VALUES
    (1, 'Alpha', 10.5),
    (2, 'Beta', 20.1),
    (3, 'Gamma', 30.7);""")

con.execute("""
    COPY my_data TO 'existing_parquet/existing_parquet_file.parquet' (FORMAT PARQUET); 
    """)