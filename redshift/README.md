# AWS_S3_redshift
This project creates a data warehouse hosted on AWS, running an ETL pipeline for a database hosted on Redshift. The ETL pipeline extracts data from AWS S3, stages them in Redshift, and transforms data into a set of dimensional tables analytical purposes. 

## Ropository contains
1) sql_queries.py is a collection of sql queries. It includes statements on creating, staging and dropping tables.
2) create_table.py is a python script connecting to redshift and call the sql queries from sql_queries.py that drop old tables and create the frame of new tables
3) etl.py is an etl pipeline connecting to redshift and call the sql queries from sql_queries.py that load data from the staging tables and insert data to the tables in the STAR schema
4) IaC_create_IAM_Cluster is a notebook that create an IAM role and a redshift cluster using Infrastructure as Code (IaC)
5) test_run is a python script made to inspect the various tables

Note that the files are refering to a private dwh.cfg (not on github), which direct them to the AWS S3 data and AWS redshift. The XX_dwh.cfg is almost identical to the original dwh config file, except sensitive information is sensored...

Cheers!
