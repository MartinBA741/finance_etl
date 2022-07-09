# Finance_etl
ETL process of financial data from yahoo finance --> AWS S3 --> AWS redshift --> (O)

Outline:
- Data is downloaded from yahoo finance and uploaded to a data lake on AWS S3 (frequencey: whenever... Alternatively set-up API)
- 


How to:
- Start airflow: airflow webserver -p 8080 -D
- Start scheduler: airflow scheduler