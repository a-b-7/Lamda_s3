'''
author : arpit
Boto3 access  api : using client
Boto3 alternative api : Resource


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





def make_read_transfer(s3_bucket_name):
    # client 
    s3_client = boto3.client('s3')

    
    # bucket object 
    my_bucket = s3_client.Bucket(s3_bucket_name)
    for object in my_bucket.objects.all():
        creation_date = object.last_modified
        break
    '''
    check
      # print('creatrion_date')

    '''
    # formatting time as yyyymmdd
    new_date = datetime.strftime(creation_date,"%Y%m%d")
    # making  a new function to make a new folder with this time
    new_bucket_key = "*"+"/"+new_date
    # calling the create bucket function 
    create_bucket(new_bucket_key) # it can be return

    '''
    copying all files from one bucket to another using boto3
    '''
    # define a source bucket name
    dest_bucket = s3_client.Bucket(new_bucket_key)
    # loop to iterate
    # over all the file 
    #present in the bucket
    for file in my_bucket.objects.all():
        # create a source dict
        copy_source = {
            'Bucket' : my_bucket,
            'key': file.key
        }
        dest_bucket.copy(copy_source,file.key)



  

    



def lambda_handler(event, context):
    print('in lambda')
    bucket = event['Records'][0]['s3']['bucket']['name'] # let bucket is created first
    # calling function with new created bucket as an event 
    make_read_transfer(bucket) 

    # key = event['Records'][0]['s3']['object']['key']

    # defining json
    # json_dictionary = json.loads(event)
    # defining list 
    # list_of_bucket = []
    # making a list of bucket keys
    # for i in json_dictionary:
    #     list_of_bucket.append(json_dictionary[i])
    return True

