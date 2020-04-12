import argparse
import configparser
import json
import logging
import os
import shlex
import subprocess
import time

import boto3
from botocore.exceptions import ClientError
import psycopg2


# CONFIG
CONFIG_FILE = 'redshift.cfg'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET = os.environ['AWS_SECRET_ACCESS_KEY']
DWH_IAM_ROLE_NAME = config['REDSHIFT']['IAM_ROLE_NAME']
DWH_CLUSTER_ID = config['REDSHIFT']['CLUSTER_IDENTIFIER']
REGION = config['REDSHIFT']['REGION']
DB_PORT = config['DB']['PORT']
S3_READ_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"


def create_resources():
    """ Create required AWS resources """
    options = dict(region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    ec2 = boto3.resource('ec2', **options)
    s3 = boto3.resource('s3', **options)
    iam = boto3.client('iam', **options)
    redshift = boto3.client('redshift', **options)
    return ec2, s3, iam, redshift


def create_iam_role(iam):
    """ Create IAM role for Redshift cluster """
    try:
        dwh_role = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                }],
                'Version': '2012-10-17'
            })
        )
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn=S3_READ_ARN
        )
    except ClientError as e:
        logging.warning(e)

    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    logging.info('Role {} with arn {}'.format(DWH_IAM_ROLE_NAME, role_arn))
    return role_arn


def create_redshift_cluster(redshift, role_arn):
    """ Create Redshift cluster """
    try:
        redshift.create_cluster(
            ClusterType=config['REDSHIFT']['CLUSTER_TYPE'],
            NodeType=config['REDSHIFT']['NODE_TYPE'],
            NumberOfNodes=int(config['REDSHIFT']['NUM_NODES']),
            DBName=config['DB']['NAME'],
            ClusterIdentifier=DWH_CLUSTER_ID,
            MasterUsername=config['DB']['USER'],
            MasterUserPassword=config['DB']['PASSWORD'],
            IamRoles=[role_arn],
        )
        logging.info('Creating cluster {}...'.format(DWH_CLUSTER_ID))
    except ClientError as e:
        logging.warning(e)


def delete_iam_role(iam):
    """ Delete IAM role """
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=S3_READ_ARN)
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    logging.info('Deleted role {} with {}'.format(DWH_IAM_ROLE_NAME, role_arn))


def delete_redshift_cluster(redshift):
    """ Delete Redshift cluster """
    try:
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_ID,
            SkipFinalClusterSnapshot=True,
        )
        logging.info('Deleted cluster {}'.format(DWH_CLUSTER_ID))
    except Exception as e:
        logging.error(e)


def execute_query_from_file(host, query_file):
    """ Execute query defined in sql file

    Arguments:
        host: Database host address
        query_file: Path to file with sql queries

    Returns:
        None
    """
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            host,
            config['DB']['NAME'],
            config['DB']['USER'],
            config['DB']['PASSWORD'],
            config['DB']['PORT']
        )
    )
    with open(args.query_file, 'r') as file:
        query = file.read()
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()
        conn.close()
        logging.info(f'Executed query {query}')
    except psycopg2.errors.DuplicateTable as e:
        logging.error(e)


def get_public_ip():
    """ Get public IP of this machine to enable increased security """
    command = 'dig +short myip.opendns.com @resolver1.opendns.com'
    proc = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    out, err = proc.communicate()
    return out.strip().decode('ascii')


def open_tcp(ec2, vpc_id):
    """ Open TCP connection from outside VPC """
    ip = get_public_ip()
    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='{}/32'.format(ip),
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT),
        )
        logging.info('Allow TCP connections from {}'.format(ip))
    except ClientError as e:
        logging.warning(e)


def main(args):
    """ Main function """
    ec2, s3, iam, redshift = create_resources()

    if args.delete:
        delete_redshift_cluster(redshift)
        delete_iam_role(iam)
        return

    role_arn = create_iam_role(iam)
    create_redshift_cluster(redshift, role_arn)
    config['REDSHIFT']['IAM_ROLE_ARN'] = role_arn

    # Poll the Redshift cluster after creation until available
    timestep = 15
    for _ in range(int(600/timestep)):
        cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_ID)['Clusters'][0]
        if cluster['ClusterStatus'] == 'available':
            break
        logging.info('Cluster status is "{}". Retrying in {} seconds.'.format(cluster['ClusterStatus'], timestep))
        time.sleep(timestep)

    # Open TCP connection upon successful cluster creation
    if cluster:
        logging.info('Cluster created at {}'.format(cluster['Endpoint']))
        open_tcp(ec2, cluster['VpcId'])
    else:
        logging.error('Could not connect to cluster')

    host = cluster['Endpoint']['Address']
    config['DB']['HOST'] = host
    # Execute SQL command upon cluster creation
    if args.query_file:
        execute_query_from_file(host=host, query_file=args.query_file)

    # Update config file
    with open(CONFIG_FILE, 'w') as cfg_file:
        config.write(cfg_file)


if __name__ == '__main__':
    """ Set logging level and cli arguments """
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', dest='delete', default=False, action='store_true')
    parser.add_argument('--query_file', dest='query_file', default=None)
    args = parser.parse_args()
    main(args)
