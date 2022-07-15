import boto3
import psycopg2
import configparser
import pandas as pd
import warnings
import sql_queries

warnings.filterwarnings('ignore')

config = configparser.ConfigParser()
config.read_file(open(r'config.ini'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()


# List all tables:
#query_show_tables = """ 
#    SELECT table_name 
#    FROM information_schema.tables
#    WHERE table_schema = 'public'
#    """
#cur.execute(query_show_tables)
#lst_tables = [table[0] for table in cur.fetchall()]

########################################################################
##################### TEST FACT & DIM ##################################
########################################################################


def EmptyTableCheck():   
    print('\n*** Data quality check for empty tables ***')
    for table in ['FactHist', 'DimCorp']:
        records = pd.read_sql_query(f"Select count(*) from {table}", con=conn)
        if len(records) < 1:
            raise ValueError(f"Data quality check failed. '{table}' retuned no results")
        else:
            print(f'{table} passed')

def CrucialNullCheck():  
    # list of dicts for check
    dq_checks=[
        {'table': 'FactHist',
         'column': 'Ticker',
         'check_sql': "SELECT COUNT(*) FROM FactHist WHERE Ticker is null",
         'expected_result': 0},
        {'table': 'FactHist',
         'column': 'Date',
         'check_sql': "SELECT COUNT(*) FROM FactHist WHERE Date is null",
         'expected_result': 0},
        {'table': 'FactHist',
         'column': 'AdjClose',
         'check_sql': "SELECT COUNT(*) FROM FactHist WHERE AdjClose is null",
         'expected_result': 0},
         {'table': 'DimCorp',
         'column': 'Ticker',
         'check_sql': "SELECT COUNT(*) FROM DimCorp WHERE Ticker is null",
         'expected_result': 0}
        ]
    
    print('\n*** Data quality check for Null values ***')
    for check in dq_checks:
        cur.execute(check['check_sql'])
        records = cur.fetchall()[0]
        if records[0] != check['expected_result']:
            raise ValueError(f"Data quality check failed. '{check['table']}' contains null in the '{check['column']}' column, got ({records[0]}) instead")
        else:
            print(f'Column {check["column"]} in table {check["table"]} passed' )
        

def main():
    print('\n ------ Test Fact and DIM tables ------')
    EmptyTableCheck()
    CrucialNullCheck()
    cur.close()

if __name__ == "__main__":
    main()