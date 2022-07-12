#%%
import pandas as pd
import configparser
import psycopg2

#%%
config = configparser.ConfigParser()
config.read('dwh.cfg')
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()

#%%
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

#%%
query = """
        SELECT * 
        FROM users
        LIMIT 10
        """

cur.execute(query)
df = pd.read_sql_query(query, con=conn)
print(df)
#%%
lst_count = []

for _table in lst_tables:
    lst_count.append(cur.execute(f"""
        SELECT COUNT(1) 
        FROM {_table}
        """))

#%%
df_count = pd.DataFrame() 

for _table in lst_tables:
    df_count = df_count.append(pd.read_sql_query(f"""
        SELECT COUNT(1) 
        FROM {_table}
        """, con=conn))
print(df_count)


# %%
# Close connection...
cur.close()

# %%
