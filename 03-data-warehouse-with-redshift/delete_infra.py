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
    global iam, redshift
    print('-- CREATING CLIENTS --')

    try:
        iam = boto3.client('iam',aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET,
                            region_name=REGION
                        )
        print('1/2: iam created')
    except Exception as e:
        print(e)

    try:
        redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
        print('2/2: redshift created')
    except Exception as e:
        print(e)
        
    print('-- CLIENTS CREATED --')

def delete_cluster():

    cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    print('-- DELETING CLUSTER --')

    while cluster['ClusterStatus'] == 'deleting':
        try:
            print('Status: ', cluster['ClusterStatus'])
            time.sleep(10)
            cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        except:
            print('-- CLUSTER DELETED --')
            iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
            iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
            print('-- ROLES DELETED --')

def main():
    print('-- DELETING INFRASTRUCTURE --')
    init_config()

    create_clients()

    delete_cluster()
    print('-- INFRASTRUCTURE DELETED --')

if __name__ == "__main__":
    main()