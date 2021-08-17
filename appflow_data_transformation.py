
import json
import logging
import os
import sys
import urllib.parse
#from datetime import datetime

import boto3  # pylint: disable=import-error

import athena_util as athena_helper
import audit_util as audit_helper

def read_s3_file(bucket, key):
    """
    This function is used to read a file from S3.
    Args:
        Bucket - name of the bucket where file is placed.
        Key - The complete file path in S3 bucket (with extension like .json/.csv)
    Returns:
        string : file data
    """
    try:
        s_3 = boto3.client('s3')
        file = s_3.get_object(Bucket=bucket, Key=key)
        file_body = file['Body'].read().decode("utf-8")
        return file_body

    except (FileNotFoundError, IndexError, SystemExit, OSError, IOError) as err:
        print("Error in reading file", str(err))
        return ""

# def list_files(client, bucket_name, prefix):
#     """List files in specific S3 URL"""
#     #print("entering into list_files")
#     # _bucket_name = bucket_name
#     # _prefix = prefix
#     prefix_file_list = []
#     #print("bucket_name: ",bucket_name)
#     #print("prefix: ",prefix)
#     response = client.list_objects(Bucket=bucket_name, Prefix=prefix)
#     #print(response)
#     for content in response.get('Contents',[]):
#         prefix_file_list.append(content.get('Key'))
#         return prefix_file_list

def create_complete_payload(data,query_result_record_id):  # pylint: disable=too-many-locals
    """
    Function to create a complete payload
    """
    output_json = {}
    if len(query_result_record_id['ResultSet']['Rows']) > 1:
        print("inside if")
        columns_list_dict = query_result_record_id['ResultSet']['Rows'][0]
        values_list_dict = query_result_record_id['ResultSet']['Rows'][1]
        column_list = columns_list_dict["Data"]
        values_list = values_list_dict["Data"]
        actual_dict={}
        for index,list_value in enumerate(zip(column_list[1:-1], values_list[1:-1])):#pylint: disable=unused-variable
            key = list_value[0]["VarCharValue"]
            value=""
            if len(list_value[1]) != 0:
                value = list_value[1]["VarCharValue"]
            else:
                continue
            temp_dict = {key:value}
            actual_dict.update(temp_dict)
        #print(actual_dict)
        new_dict = {k.lower(): v for k, v in data.items()}
        print(new_dict)
        for data_keys in actual_dict:
            if data_keys.lower() not in new_dict:
                #print(data_keys.lower())
                temp = {data_keys:actual_dict[data_keys]}
                new_dict.update(temp)
        #print(data)
        #output_json = json.dumps(new_dict)
        output_json = new_dict
    else:
        print("inside else")
        output_json = {}
    return output_json

def lambda_handler(event, context):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """ the main lambda function to initialize all the required
        objects and to call all the required function """
    try:  # pylint: disable=too-many-nested-blocks
        print("Execution started!")
        #print("Event: ",event)
        # Bucket name and Full path for file - where file will be uploded
        source_bucket_name = event["detail"]["requestParameters"]["bucketName"]
        source_key = urllib.parse.unquote_plus(
            event["detail"]["requestParameters"]["key"], encoding='utf-8')
        
        print("file_path: ",source_key)
        #Loading master config
        print("Loading master_config")
        audit_config = {}
        config_path = "./config/" + \
            os.environ['CCM_ENV'] + "/master_config.json"
        config_content = open(config_path).read()
        config_json = json.loads(config_content)
        audit_config = config_json["audit_config"]
        snow_params = config_json["ERROR_NOTIFICATION_SNOW_PARAMS"]
        athena_query_param = config_json["ATHENA_QUERY_PARAMS"]
        athena_table_params = config_json["ATHENA_TABLE_PARAMS"]

        # Audit Parameters Based on the Invoking lambda and its operation involved
        audit_config["component_type_code"] = "ETL"
        audit_config["component_name"] = "PCP Appflow"
        audit_config["source_name"] = "Patient Connections Platform"
        audit_config["target_name"] = "Consumer Consent Management"
        audit_config["full_file_path"] = "s3://" + \
            source_bucket_name + "/" + source_key
        audit_config["file_version_id"] = ""

        # Creates Job Entry in ABC Framework
        print("audit config::", audit_config)
        process_execution_id = audit_helper.\
            invoke_edb_abc_log_process_status_event_job_entry(audit_config)
        audit_config["process_execution_id"] = process_execution_id
        print("process_execution_id ::", process_execution_id)
        #print("source_key: ",source_key)
        s3_write = boto3.client('s3')
        record_dict = {}
        file_name = ""
        final_json = ""
        # prefix = ""
        # file_list = []
        # client = boto3.client("s3")
        # result = client.list_objects(Bucket=source_bucket_name, Prefix=source_key, Delimiter='/')
        # #print(result)
        # for obj in result.get('CommonPrefixes'):
        #     prefix = obj.get('Prefix')
        #     #print(prefix)
        #     file_list = list_files(client,source_bucket_name,prefix)
        #     for file in file_list:
        #         #print(file)
        json_read = read_s3_file(source_bucket_name, source_key)
        data = json.loads(json_read)
        #print(data)
        if data != '':
            record_dict = {k.lower(): v for k, v in data.items()}
            print("Record_Dict::",record_dict)
            event_type_param = {}
            event_type_list = athena_table_params.keys()
            print("event_type_list",event_type_list)
            for key in event_type_list:
                print("key",key)
                if key in source_key:
                    print("key",key)
                    event_type_param = athena_table_params[key]
                    print(event_type_param)
            if "changeeventheader" in record_dict:
                if record_dict["changeeventheader"]["changeType"] == "CREATE":
                    #and record_dict["dtpc_affiliate__c"] == 'US':
                    recordid_create = record_dict["changeeventheader"]["recordIds"][0]
                    print(recordid_create)
                    if recordid_create != '':
                        last_modified_date = record_dict["lastmodifieddate"].replace(":",".")
                        create_json = json.dumps(record_dict)
                        final_json = create_json
                        file_name = recordid_create + "-create-" + str(last_modified_date)
                        print("file_name: ",file_name)
                        outbound_path = event_type_param["folder_path"]
                        final_source_key = outbound_path + '/' + file_name+".json"
                        print("final_source_key :", final_source_key)
                        s3_write.put_object(
                            Body=final_json, Bucket=source_bucket_name, Key=final_source_key)
                    else:
                        raise Exception("RecordId is missing: ", record_dict)
                elif record_dict["changeeventheader"]["changeType"] == "UPDATE":
                    record_ids_list = record_dict["changeeventheader"]["recordIds"]
                    if len(record_ids_list) != 0:
                        for ele in record_ids_list:
                            print(ele)
                            element = "'" + ele + "'"
                            payload_condition = event_type_param["recordid_condition"]
                            query = 'SELECT * FROM '+event_type_param["athena_create_table"]+\
                                ' WHERE lastmodifieddate IN(SELECT max(lastmodifieddate) from '\
                                +event_type_param["athena_create_table"]+\
                                ', UNNEST("'+payload_condition[0]+'"."'+payload_condition[1]+\
                                '") AS ln(jsondata) WHERE jsondata IN ('+element+'));'
                            print(query)
                            athena_query_param['athena_query'] = query
                            query_result_record_id = athena_helper.perform_athena_search\
                                (athena_query_param)
                            print("Athena Query Result for Create Path:::", query_result_record_id)
                            update_json = create_complete_payload(data,query_result_record_id)
                            print("update_json: ",update_json)
                            if len(update_json) != 0:
                                last_modified_date = record_dict["lastmodifieddate"].replace\
                                    (":",".")
                                final_json = json.dumps(update_json)
                                file_name = ele + "-update-" + str(last_modified_date)
                                print("file_name: ",file_name)
                                outbound_path = event_type_param["folder_path"]
                                final_source_key = outbound_path + '/' + file_name+".json"
                                print("final_source_key :", final_source_key)
                                s3_write.put_object(
                                    Body=final_json, Bucket=source_bucket_name, \
                                        Key=final_source_key)
                            else:
                                print(ele," does not have a create payload")
                    else:
                        raise Exception("RecordId is missing: ", record_dict)
            else:
                raise Exception("ChangeEventHeader is missing: ", record_dict)
        else:
            raise Exception("Invalid Payload: ", record_dict)

    except (Exception) as err:  # pylint: disable=line-too-long,broad-except
        print("Error occured: {0}".format(str(err)))
        audit_type = "error"
        error_msg = sys.exc_info()
        exc_type = error_msg
        exc_obj = error_msg
        snow_params["flag"] = "FAIL"
        snow_params["error_message"] = str(exc_obj)
        snow_params["error_type"] = str(exc_type)
        audit_config["exception_message"] = str(exc_obj)
        if audit_config != {}:
            logging.exception(sys.exc_info())
            audit_helper.invoke_edb_abc_log_process_status_event(
                audit_type, audit_config)  # pylint: disable=line-too-long
        audit_helper.raise_snow_incident(snow_params)
