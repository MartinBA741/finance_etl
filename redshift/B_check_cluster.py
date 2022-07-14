import boto3
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read_file(open(r'config.ini'))

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# Describe cluster
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None) #-1
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

print('Take note of the cluster endpoint')
print(prettyRedshiftProps(myClusterProps))
print('Change Redshift properties --> VPC security group --> MySecurityGroup')