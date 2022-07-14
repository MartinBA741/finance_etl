import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError

print('packages installed')

config = configparser.ConfigParser()
config.read_file(open(r'config.ini'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })
ec2 = boto3.resource('ec2',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-east-1'
                  )

redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# CREATE IAM ROLE
#1.1 Create the role, 
try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
            'Effect': 'Allow',
            'Principal': {'Service': 'redshift.amazonaws.com'}}],
            'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
    
print("1.2 Attaching Policy")
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")['ResponseMetadata']['HTTPStatusCode']
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftFullAccess")['ResponseMetadata']['HTTPStatusCode']
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AWSCloudFormationFullAccess")['ResponseMetadata']['HTTPStatusCode']


print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print('roleArn: ', roleArn)
print('DWH_IAM_ROLE_NAME: ', DWH_IAM_ROLE_NAME)     


## Create Redshift Cluster
try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
    # Change security group
    #response = boto3.client.authorize_cluster_security_group_ingress(
    #    ClusterSecurityGroupName='MySecurityGroup',
    #    CIDRIP='sg-0594827b8e17446de',
    #    EC2SecurityGroupName='vpc-03527cb119a739754',
    #    EC2SecurityGroupOwnerId='sg-0faf1a9f04a0da66e'
    #)
except Exception as e:
    print(e)