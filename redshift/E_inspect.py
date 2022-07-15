import boto3
import psycopg2
import configparser
import pandas as pd
import warnings
import sql_queries

warnings.filterwarnings('ignore')

config = configparser.ConfigParser()
config.read_file(open(r'config.ini'))

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()

# List of all tables:
query_show_tables = """ 
    SELECT table_name 
    FROM information_schema.tables
    WHERE table_schema = 'public'
    """
cur.execute(query_show_tables)
lst_tables = [table[0] for table in cur.fetchall()]


def query_top(N=5):
    for tbl in lst_tables:
        topN = f""" SELECT * FROM {tbl} LIMIT {N}  """
        df = pd.read_sql_query(topN, con=conn)
        print('\n Table: ', tbl, '\n',df)


def count_tables():
    for _table in lst_tables:
        cur.execute(f"""SELECT COUNT(1) FROM {_table}""")
        print(_table, cur.fetchall()[0])


def main():
    print('\n No. obs in tables: ')
    count_tables()
    query_top(N=5)

    print('\n \n ------ Tables for analysis ------ \n \n')
    df = pd.read_sql_query(sql_queries.overview_query, con=conn)
    print('\n Overview table | join fact and dim: \n', df.head())

    df = pd.read_sql_query(sql_queries.pct_query, con=conn)
    print('\n Daily return table in % | calculate fact: \n', df.head())

    cur.close()


if __name__ == "__main__":
    main()