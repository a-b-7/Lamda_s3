'''
author : arpit
Boto3 access  api : using client
Boto3 alternative api : Resource
This is first lambda which is creating folder


'''
import boto3
import sys
import os
import pandas  as pd 
import csv
import io
import urllib.parse
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError

'''
making a  function for making bucket

'''
def create_bucket(bucket_name,region = None):
    #pass
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    # create bucket 
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket = bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True





# def make_read_transfer(s3_bucket_name):

#     s3_client = boto3.client('s3')

    
    # bucket object 
    # my_bucket = s3_client.Bucket(s3_bucket_name)
    # for object in my_bucket.objects.all():
    #     creation_date = object.last_modified
    #     break
    '''
    check
      # print('creatrion_date')

    '''
    
    '''
    copying all files from one bucket to another using boto3
    '''
    # define a source bucket name
    # dest_bucket = s3_client.Bucket(new_bucket_key)
    # loop to iterate
    # over all the file 
    #present in the bucket
    # for file in my_bucket.objects.all():
        # create a source dict
        # copy_source = {
        #     'Bucket' : my_bucket,
        #     'key': file.key
        # }
        # dest_bucket.copy(copy_source,file.key)



  

    



def lambda_handler(event, context):
    print(event)

    bucket = event['Records'][0]['s3']['bucket']['name'] # let bucket is creeated
    key = event['Records'][0]['s3']['object']['key']
    date_mod = key.last_modified
    new_date = datetime.strftime(date_mod,"%Y%m%d")
    new_bucket_key = bucket+"/"+new_date
    create_bucket(new_bucket_key)
    return True