import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Execute the sql-queries that drop existing tables."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Execute the sql-queries that create the tables (without inserting data)."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Drop existing tables and creates new tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('config file read! - now connecting...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('connection established! - now dropping tables...')
    drop_tables(cur, conn)

    print('tables dropped! - now creating tables...')
    create_tables(cur, conn)

    print('tables are now created! - closing connection...')
    conn.close()

    print('done!')

if __name__ == "__main__":
    main()