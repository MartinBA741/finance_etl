import configparser
import psycopg2
import boto3
from sql_queries import copy_table_queries, insert_table_queries
import redshift.test as test


#def connect_to_s3(KEY, SECRET):
#    s3 = boto3.resource('s3',
#        region_name="us-east-1",
#        aws_access_key_id=KEY,
#        aws_secret_access_key=SECRET
#        )
#    return s3
#
#def print_s3_bucket(s3):
#    sampleDbBucket =  s3.Bucket("s3stocks")
#    #for obj in sampleDbBucket.objects.filter(Prefix="stocks"):
#    #    print(obj)
#    for obj in sampleDbBucket.objects.all():
#        print(obj)

def load_staging_tables(cur, conn):
    """Execute the sql-queries that load the "raw" data to the staging tables."""
    for query in copy_table_queries:
        #print(query) # uncomment to print the current sql query executed 
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Execute the sql-queries that insert data from the staging tables to the star schema."""
    for query in insert_table_queries:
        #print(query) # uncomment to print the current sql query executed 
        cur.execute(query)
        conn.commit()


def main():
    """Extract raw data to staging tables. Transform and load data to fact and dimensions tables."""
    config = configparser.ConfigParser()
    config.read('config.ini') #dwh.cfg
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')

    print('config read! - now connecting to db...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
   
    # s3 = connect_to_s3(KEY, SECRET)
    # print('print s3 bucket: ', print_s3_bucket(s3))
    
    print(f'connected - now loading staging tables...')
    load_staging_tables(cur, conn)

    print('staging tables loaded! - now inserting tables...')
    insert_tables(cur, conn)
    print('tables inserted!')

    print('\n ------ Test Fact and DIM tables ------')
    test.EmptyTableCheck()
    test.CrucialNullCheck()

    print('\nAll test passed - now closing connection...')
    conn.close()
    print('ETL done!')


if __name__ == "__main__":
    main()