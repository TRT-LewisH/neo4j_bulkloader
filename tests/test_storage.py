import pytest
import boto3
from modules.storage import AWSStorage
import os

AWS_REGION = 'us-east-1'
AWS_PROFILE = 'localstack'
ENDPOINT_URL = os.environ.get('LOCALSTACK_ENDPOINT_URL')

boto3.setup_default_session(profile_name=AWS_PROFILE)
s3_client = boto3.client("s3", region_name=AWS_REGION,
                         endpoint_url=ENDPOINT_URL)

def create_bucket(bucket_name):
    """
    Creates a S3 bucket.
    """
    try:
        response = s3_client.create_bucket(
            Bucket=bucket_name)
    except ClientError:
        pass
    else:
        return response


def test_aws_download_file():

    bucket_name = "hands-on-cloud-localstack-bucket"
    s3 = create_bucket(bucket_name)

    aws_storage = AWSStorage(bucket_name)
    aws_storage.pull(save_to=TEMP_STORAGE, limit=limit)
    assert True == True
   