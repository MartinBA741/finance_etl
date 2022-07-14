import boto3
import configparser
import pandas as pd
import psycopg2
import json
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open(r'config.ini'))

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get('DWH','DWH_IAM_ROLE_NAME')
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

ec2 = boto3.resource('ec2',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
)

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

DWH_ENDPOINT = endpoint = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = roleArn = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", endpoint)
print("DWH_ROLE_ARN :: ", roleArn)

print('Open an incoming  TCP port to access the cluster endpoint')

try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0', #"10.0.0.1/32"
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

print('Connecting...')
try:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Succesfully connected')
except Exception as e:
    print(e)