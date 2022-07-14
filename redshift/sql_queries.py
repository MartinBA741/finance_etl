import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('config.ini') #dwh.cfg

S3_HIST_DATA = config.get('S3','HIST_DATA')
S3_DimCorp_DATA = config.get('S3','CorpDim_PATH') 
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
        Ticker      VARCHAR(256) NOT NULL,
        "Date"      TIMESTAMP,
        "Open"      NUMERIC,
        High        NUMERIC,
        Low         NUMERIC,
        Close       NUMERIC,
        AdjClose    NUMERIC,
        Volume      NUMERIC
    );
    """) #         PRIMARY KEY (Ticker, "Date")

## Create Fact Table
create_FactHist = ("""
    CREATE TABLE IF NOT EXISTS FactHist 
    (
    	Ticker      VARCHAR(256) NOT NULL,
        "Date"      TIMESTAMP,
        AdjClose    NUMERIC
    );
    """) #         PRIMARY KEY (Ticker, "Date")


## Create Dimension Tables
create_DimCorp = ("""
    CREATE TABLE IF NOT EXISTS DimCorp 
    (
    	Ticker  VARCHAR(256) NOT NULL,
        Company VARCHAR(256),
        CEO     VARCHAR(256),
        Founded TIMESTAMP
    );
    """) # PRIMARY KEY (Ticker, "Date")

#######################################################
################## INSERT DATA ########################
#######################################################

# Insert data to staging hist
staging_hist_copy = (f"""
        COPY staging_hist FROM '{S3_HIST_DATA}'
        CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
        REGION 'us-east-1' 
        COMPUPDATE OFF
        csv
        IGNOREHEADER 1
    """)


# Insert data to DimCorp
staging_DimCorp_copy = (f"""
        COPY DimCorp FROM '{S3_DimCorp_DATA}'
        CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
        REGION 'us-east-1' 
        COMPUPDATE OFF
        csv
        IGNOREHEADER 1
    """)


# FINAL TABLES
FactHist_insert = ("""
    INSERT INTO FactHist (
        Ticker,
        "Date",
        AdjClose
    )
    SELECT 
        Ticker,
        "Date",
        AVG(AdjClose) as AdjClose
    FROM staging_hist
    GROUP BY Ticker, "Date"
""") 

#######################################################
################## QUERY LISTS ########################
#######################################################
create_table_queries = [create_staging_hist, create_FactHist, create_DimCorp]
drop_table_queries = [staging_hist_drop, FactHist_drop, DimCorp_drop]
copy_table_queries = [staging_DimCorp_copy, staging_hist_copy]
insert_table_queries = [FactHist_insert]