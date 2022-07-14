import boto3
import psycopg2
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read_file(open(r'config.ini'))

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()


query_show_tables = """ 
    SELECT table_name 
    FROM information_schema.tables
    WHERE table_schema = 'public'
    """


cur.execute(query_show_tables)
lst_tables = []
for table in cur.fetchall():
    lst_tables.append(table[0])
    print(table)


def query_top(N=5, table='FactHist'):
    topN = f""" SELECT * FROM {table} LIMIT {N}"""
    #cur.execute(topN)
    df = pd.read_sql_query(topN, con=conn)
    print(df)


def count_tables():
    lst_count = []

    for _table in lst_tables:
        lst_count.append(cur.execute(f"""
            SELECT COUNT(1) 
            FROM {_table}
            """))

    df_count = pd.DataFrame() 

    for _table in lst_tables:
        df_count = df_count.append(pd.read_sql_query(f"""
            SELECT COUNT(1) 
            FROM {_table}
            """, con=conn))
    print(df_count)


def main():
    query_top(N=5, table='FactHist')
    #count_tables()
    cur.close()


if __name__ == "__main__":
    main()