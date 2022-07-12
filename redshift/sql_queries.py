import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#S3_LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
#S3_SONG_DATA = config.get('S3','SONG_DATA')
S3_DimCorp_DATA = config.get('S3','DimCorp_DATA') 
S3_HIST_DATA = config.get('S3','LOG_DATA')
IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')

#######################################################
################ DROP TABLES ##########################
#######################################################
staging_hist_drop = "DROP TABLE IF EXISTS staging_hist"
FactHist_drop = "DROP TABLE IF EXISTS FactHist"
DimCorp_drop = "DROP TABLE IF EXISTS DimCorp"



#######################################################
################ CREATE TABLES ########################
#######################################################

## Create Staging Tables: 

create_staging_hist = ("""
    CREATE TABLE IF NOT EXISTS staging_hist 
    (
        Ticker varchar(256) NOT NULL,
        Date timestamp,
        Open numeric(18,0),
        High numeric(18,0),
        Low numeric(18,0),
        Close numeric(18,0),
        AdjClose numeric(18,0),
        Volume numeric(18,0),
        PRIMARY KEY (Ticker, Date)
    );
    """)

## Create Fact Table
create_FactHist = ("""
    CREATE TABLE IF NOT EXISTS FactHist 
    (
    	Ticker varchar(256) NOT NULL,
        Date timestamp,
        AdjClose numeric(18,0),
        PRIMARY KEY (Ticker, Date)
    );
    """)


## Create Dimension Tables
create_DimCorp = ("""
    CREATE TABLE IF NOT EXISTS DimCorp 
    (
    	Ticker varchar(256) NOT NULL,
        Company varchar(256),
        CEO varchar(256),
        Founded timestamp,
        PRIMARY KEY (Ticker, Date)
    );
    """)

#######################################################
################## INSERT DATA ########################
#######################################################

# Insert data to staging hist
staging_hist_copy = (f"""
        COPY staging_hist FROM '{S3_HIST_DATA}'
        CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
        REGION 'us-east-1' 
        COMPUPDATE OFF
        JSON AS 'auto'
    """)   #JSON AS '{S3_LOG_JSONPATH}'


# Insert data to DimCorp
staging_DimCorp_copy = (f"""
        COPY DimCorp FROM '{S3_DimCorp_DATA}'
        CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
        REGION 'us-east-1' 
        COMPUPDATE OFF
        JSON AS 'auto'
    """)   #JSON AS '{S3_LOG_JSONPATH}'


# FINAL TABLES

FactHist_insert = ("""
    INSERT INTO FactHist (
        Ticker,
        Date,
        AdjClose
    )
    SELECT 
        Ticker,
        Date,
        AdjClose
    FROM staging_hist
    WHERE Ticker IS NOT NULL
""") 

#######################################################
################## QUERY LISTS ########################
#######################################################
create_table_queries = [create_staging_hist, create_FactHist, create_DimCorp]
drop_table_queries = [staging_hist_drop, FactHist_drop, DimCorp_drop]
copy_table_queries = [staging_hist_copy, staging_DimCorp_copy]
insert_table_queries = [FactHist_insert]