from functools import cache

import boto3


@cache
def session():
    return boto3.Session()

@cache
def get_account():
    return session().client('sts').get_caller_identity()['Account']

def get_region():
    return session().region_name