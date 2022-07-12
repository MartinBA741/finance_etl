import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Execute the sql-queries that load the "raw" data to the staging tables."""
    for query in copy_table_queries:
        # print(query) # uncomment to print the current sql query executed 
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Execute the sql-queries that insert data from the staging tables to the star schema."""
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """Extract raw data to staging tables. Transform and load data to fact and dimensions tables."""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('config read! - now connecting...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('connected! - now loading staging tables...')
    load_staging_tables(cur, conn)

    print('staging tables loaded! - now inserting tables...')
    insert_tables(cur, conn)

    print('tables inserted! - now closing connection...')
    conn.close()
    print('ETL done!')


if __name__ == "__main__":
    main()