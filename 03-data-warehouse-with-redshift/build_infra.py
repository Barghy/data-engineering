import pandas as pd
import boto3
import json
import configparser
import time
from botocore.exceptions import ClientError

def init_config():
    global KEY, SECRET, REGION, DWH_IAM_ROLE_NAME, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_DB_USER, DWH_CLUSTER_IDENTIFIER, DWH_DB_PASSWORD
    print('-- INITIALIZING CONFIG --')

    try:
        print('1/2: gathering configurations')
        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))
        print('2/2: assinging configurations')
        KEY                    = config.get('AWS','KEY')
        SECRET                 = config.get('AWS','SECRET')
        DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
        DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
        DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
        DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        DWH_DB                 = config.get("DWH","DWH_DB")
        DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
        DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
        DWH_PORT               = config.get("DWH","DWH_PORT")
        DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
        REGION                 = config.get("CLUSTER", "REGION")
        print('-- CONFIG INITIALIZED --')
    except Exception as e:
        print(e)

def create_clients():
    global ec2, s3, iam, redshift
    print('-- CREATING CLIENTS --')

    try:
        ec2 = boto3.resource('ec2',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
        print('1/4: ec2 created')
    except Exception as e:
        print(e)

    try:
        s3 = boto3.resource('s3',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                        )
        print('2/4: s3 created')
    except Exception as e:
        print(e)

    try:
        iam = boto3.client('iam',aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET,
                            region_name=REGION
                        )
        print('3/4: iam created')
    except Exception as e:
        print(e)

    try:
        redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
        print('4/4: redshift created')
    except Exception as e:
        print(e)
        
    print('-- CLIENTS CREATED --')

def create_role():
    global roleArn
    print("-- CREATING ROLE --") 
    
    try:
        print("1/3: creating role") 
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
    
    try:
        print("2/3: attaching policy")
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
            print(e)

    try:
        print("3/3: saving role arn")
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        print("-- ROLE CREATED --")
    except Exception as e:
            print(e)

def create_cluster():
    global ClusterIdentifier

    try:
        response = redshift.create_cluster(        

            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            IamRoles=[roleArn]  
        )
        print('-- CREATING CLUSTER --')
    except Exception as e:
        print(e)

def check_cluster():
    
    global DWH_ENDPOINT
    global DWH_ROLE_ARN
    
    cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print('-- CHECKING CLUSTER --')
    
    while cluster['ClusterStatus'] != 'available':
        print('Status: ', cluster['ClusterStatus'])
        time.sleep(10)
        cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    else:
        DWH_ENDPOINT = cluster['Endpoint']['Address']
        DWH_ROLE_ARN = cluster['IamRoles'][0]['IamRoleArn']
        print('-- CLUSTER CREATED --')

def main():
    
    print('-- BUILDING INFRASTRUCTURE --')
    init_config()

    create_clients()

    create_role()

    create_cluster()

    check_cluster()
    print('-- INFRASTRUCTURE BUILT --')


if __name__ == "__main__":
    main()