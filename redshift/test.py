import psycopg2
import configparser
import redshift_connector

config = configparser.ConfigParser()
config.read_file(open(r'config.ini'))

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get('DWH','DWH_IAM_ROLE_NAME')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_HOST               = config.get("CLUSTER","HOST")
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")


conn = redshift_connector.connect(
    host=DWH_HOST,
    database=DWH_DB,
    user=DWH_DB_USER,
    password=DWH_DB_PASSWORD
    )

