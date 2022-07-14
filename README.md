# Finance_etl
ETL process of financial data from yahoo finance --> AWS S3 --> AWS redshift --> Ready for analysis

Outline:
- Data is downloaded from yahoo finance and uploaded to a data lake on AWS S3
- Create: IAM Role and Redshifter cluster
- Create PostgreSQL tables
- *E*xtract data from S3 to staging tables on Redshift
- *T*ransform data from staging tables to Fact and Dimension tables 
- Test tables are correct
- *L*oad data for analysis

How to:
- Scripts are run from A-G before analysis can begin.
- Run Z_kill_cluster.py when you are done with the cluster to avoid surprising AWS bills.

[A-C]: Create redshift stuff

[D-E]: ETL

[F-G]: inspect and test data

[Z]: shut down cluster 
