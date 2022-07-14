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
staging_DimCorp_drop = "DROP TABLE IF EXISTS StagingDimCorp"
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
    """)

create_staging_DimCorp = ("""
    CREATE TABLE IF NOT EXISTS StagingDimCorp 
    (
    	Ticker   VARCHAR(256) NOT NULL,
        CorpName VARCHAR(256),
        Ceo      VARCHAR(256),
        Founded  TIMESTAMP
    );
    """) 


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
    	Ticker   VARCHAR(256) NOT NULL,
        CorpName VARCHAR(256),
        Ceo      VARCHAR(256),
        Founded  TIMESTAMP
    );
    """) # PRIMARY KEY (Ticker, "Date")

#######################################################
################## INSERT DATA ########################
#######################################################

# Insert data to staging hist
staging_hist_copy = (f"""
        COPY public.staging_hist FROM '{S3_HIST_DATA}'
        CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
        REGION      'us-east-1' 
        DELIMITER   ','
        EMPTYASNULL
        csv
        IGNOREHEADER 1
    """)
    #COMPUPDATE OFF


# Insert data to DimCorp
staging_DimCorp_copy = (f"""
        COPY public.StagingDimCorp FROM '{S3_DimCorp_DATA}'
        CREDENTIALS 'aws_iam_role={IAM_ROLE_ARN}'
        REGION      'us-east-1' 
        DELIMITER   ','
        EMPTYASNULL
        csv
        IGNOREHEADER 1
    """)


# FINAL FACT AND DIMENSION TABLES
DimCorp_insert = ("""
    INSERT INTO DimCorp (
        Ticker, 
        CorpName,
        Ceo,
        Founded
    )
    SELECT 
        Ticker, 
        CorpName,
        Ceo,
        Founded 
    FROM StagingDimCorp 
    WHERE Ticker
        NOT IN (SELECT DISTINCT Ticker
                FROM DimCorp)
""") 

FactHist_insert = ("""
    INSERT INTO FactHist (
        Ticker,
        "Date",
        AdjClose
    )
    SELECT 
        Ticker, 
        "Date", 
        AVG(AdjClose) AS AdjClose 
    FROM staging_hist 
    WHERE (Ticker, "Date") 
        NOT IN (SELECT DISTINCT Ticker, "Date"
                FROM FactHist)
    GROUP BY Ticker, "Date" 
    ORDER BY Ticker, "Date"
""") 



#######################################################
############### ANALYSIS QUERIES ######################
#######################################################

overview_query = ("""
    SELECT 
        his.Ticker, 
        his."Date", 
        his.AdjClose,
        dc.CorpName,
        dc.Ceo,
        dc.Founded
    
    FROM (
        SELECT 
            Ticker, 
            "Date", 
            AVG(AdjClose) AS AdjClose 
        FROM FactHist 
        GROUP BY Ticker, "Date" 
        ) AS his

    LEFT JOIN DimCorp AS dc
        ON his.Ticker=dc.Ticker

    ORDER BY his.Ticker, his."Date"
    """)

pct_query = ("""
    SELECT 
        Ticker, 
        "Date", 
        AdjClose,
        ((AdjClose / lag(AdjClose, 1) OVER (PARTITION BY Ticker ORDER BY ["Date"])) - 1)*100 AS daily_return
    FROM FactHist
    ORDER BY Ticker, Date;
    """)


#######################################################
################## QUERY LISTS ########################
#######################################################
create_table_queries = [create_staging_hist, create_staging_DimCorp, create_FactHist, create_DimCorp]
drop_table_queries = [staging_hist_drop, staging_DimCorp_drop, FactHist_drop, DimCorp_drop]
copy_table_queries = [staging_hist_copy, staging_DimCorp_copy]
insert_table_queries = [FactHist_insert, DimCorp_insert]