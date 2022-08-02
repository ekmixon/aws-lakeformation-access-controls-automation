# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import logging
import re
import time
import os
from datetime import datetime
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class Error(Exception):
    """Base class for other exceptions"""
    pass

class LFAttributeError(Error):
    """Raised when one or more mandatory Lake Formation Permission Perameters are Missing"""
    pass

def parse_s3_event(s3_event):
    """ Parses the S3 event 
        Arguments:
            s3 event -- dict 
        Returns:
            dict -- metadata dictionary with buckename, key 
    """
    return {
        'bucket': s3_event['s3']['bucket']['name'],
        'key': unquote_plus(s3_event['s3']['object']['key']),
        'size': s3_event['s3']['object']['size'],
        'last_modified_date': s3_event['eventTime'].split('.')[0]+'+00:00',
        'timestamp': int(round(datetime.utcnow().timestamp()*1000, 0))
    }

def read_s3_content(bucket, key):
    """ Reads the contents of s3 object
        Arguments:
            bucket {str} -- Name of the bucket
            key {str} -- object key
        Returns:
            contents of s3 object
    """
    try:
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, key)
        s3_content = obj.get()['Body'].read().decode('utf-8')
        s3_content = json.loads(obj.get()['Body'].read().decode('utf-8'))
        return s3_content
    except Exception as e:
        logger.error(f'Exception while reading data from s3::/{bucket}/{key}')
        raise e

def generate_db_perm(perm_record):

    """ Creates a db perm json for granting discribe DB to cross account
        Arguments:
            perm_record {dict} -- a single perm records from incoming manifest file
        Returns:
            db_perm record -- {dict}
        Sample db_perm record:
        {
            'AccountID': 'centralCatalogAccount #',
            'Principal': 'consumptionAccount #',
            'Table': {
                'DatabaseName': 'dbname',
                'TableWildcard': {}
            },
            'Permissions': ['SELECT', 'DESCRIBE'],
            'PermissionsWithGrantOption': ['SELECT', 'DESCRIBE'],
            'AccessType': 'grant'
        }
    """

    logger.info(f'Generating DB_Perm record for {perm_record}')
    arn_pattern = '^arn:(?P<Partition>[^:\n]*):(?P<Service>[^:\n]*):(?P<Region>[^:\n]*):(?P<AccountID>[^:\n]*):(?P<Ignore>(?P<ResourceType>[^:\/\n]*)[:\/])?(?P<Resource>.*)$'
    arn_regex = re.compile(arn_pattern)
    if regex_obj := arn_regex.match(perm_record['Principal']):
        table_json = {}
        table_wild_Card = {}
        db_perm = {'AccountID': os.environ['ACCOUNT_ID'], 'Principal': regex_obj[4]}
        if (
            'Table' in perm_record
            and 'DatabaseName' not in perm_record['Table']
            or 'Table' not in perm_record
            and 'TableWithColumns' not in perm_record
        ):
            raise LFAttributeError
        elif 'Table' in perm_record:
            table_json['DatabaseName'] = perm_record['Table']['DatabaseName']
        elif 'DatabaseName' in perm_record['TableWithColumns']:
            table_json['DatabaseName'] = perm_record['TableWithColumns']['DatabaseName']
        else:
            raise LFAttributeError
        table_json['TableWildcard'] = table_wild_Card
        db_perm['Table'] = table_json
        db_perm['Permissions'] =  ["SELECT", "DESCRIBE"]
        db_perm['PermissionsWithGrantOption'] = ["SELECT", "DESCRIBE"]
        db_perm['AccessType'] = "grant"
        return db_perm
    else:
        logger.error('Permissions Principal is not valid raising LFAttributeError')
        raise LFAttributeError


def publish_sns(record):

    """ Publishes the message to central perm SNS Topic
        Arguments:
            perm_record {dict} -- perm record to publish
        Returns:
            SNS Response {dict}
    """

    sns_client = boto3.client('sns')
    response_to_sns = {
    "perms_to_set" : record
    }
    logger.info(f'record  --->  {record} ')
    logger.info(f'sending event to sns --->  {response_to_sns} ')
    response = sns_client.publish(
        TopicArn=f"arn:aws:sns:{os.environ['REGION']}:{os.environ['ACCOUNT_ID']}:lakeformation-automation",
        Message=json.dumps(response_to_sns),
        MessageStructure='string',
        MessageAttributes={
            'account_id': {
                'DataType': 'String',
                'StringValue': str(record['AccountID']),
            }
        },
    )

    logger.info(f'response from sns --->  {response} ')
    return response
    
    
def lambda_handler(event, context):
    app = os.environ['PREFIX']
    env = os.environ['ENV']
    acc_id = os.environ['ACCOUNT_ID']
    region = os.environ['REGION']

    arn_pattern = '^arn:(?P<Partition>[^:\n]*):(?P<Service>[^:\n]*):(?P<Region>[^:\n]*):(?P<AccountID>[^:\n]*):(?P<Ignore>(?P<ResourceType>[^:\/\n]*)[:\/])?(?P<Resource>.*)$'
    arn_regex = re.compile(arn_pattern)

    try:
        logger.info(f"Received {len(event['Records'])} messages")
        logger.info(f'messages {event}')
        for record in event['Records']:
            event_body = json.loads(record['body'])['Records'][0]
            message = parse_s3_event(event_body)
            s3_content = read_s3_content(message['bucket'], message['key'])
            for perm_record in s3_content['Records']:
                regex_obj = arn_regex.match(perm_record['Principal'])
                if (
                    perm_record['AccessType'] == 'grant'
                    and regex_obj[4] != acc_id
                ):
                    response = publish_sns(generate_db_perm(perm_record))
                    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                        logger.info(f'DB Perm Record Published to sns {s3_content}')
                        time.sleep(3)
                response = publish_sns(perm_record)
                logger.info(f'response of actual perm block -- {response}')
            logger.info(f'Processing Permissions for perm json started --> {s3_content} ')
    except Exception as e:
        raise e