import configparser
import boto3
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open(r'config.ini'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_IAM_ROLE_NAME      = config.get('DWH','DWH_IAM_ROLE_NAME')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

iam = boto3.client('iam',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-east-1'
                  )

print('Delete cluster:')
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

print('Delete IAM Role')
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftFullAccess")
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AWSCloudFormationFullAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)