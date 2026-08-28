import logging
import time
import os
import boto3
from botocore.exceptions import ClientError

#S3Worker is a helper class used to stash files in an S3 object.
class S3Worker:

    #create an instance of the S3Worker, requred initialization paramerters are:
    #  theAWS_ACCESS_KEY_ID- the id of an AWS access id/key pair with access to write to the S3 bucket
    #  theAWS_SECRET_ACCESS_KEY- the secret/key side of the AWS access id/key pair with access to write to the S3 Bucket
    #  theAWS_REGION_NAME- the AWS Region where the S3 Bucket exists
    #  theAWS_BUCKET_NAME- the name of the AWS S3 bucket where the results/obect will be stashed
    #  theAWS_OBJECT_URL_EXPIRATION_IN_SECS- the number of seconds that URLs created for S3 Object download from
    #                                        the S3 Bucket are valid for access.
    def __init__(self, theAWS_ACCESS_KEY_ID, theAWS_SECRET_ACCESS_KEY, theAWS_REGION_NAME, theAWS_S3_BUCKET_NAME,
                 theAWS_OBJECT_URL_EXPIRATION_IN_SECS):
        try:
            self.logger = logging.getLogger(__name__)
            self.logger.info(f"S3Worker initialized logger with logging level {self.logger.level}")
        except Exception as e:
            print(f"Error opening logger for S3Worker: {str(e)}")

        try:
            self.aws_access_key_id = theAWS_ACCESS_KEY_ID
            self.aws_secret_access_key = theAWS_SECRET_ACCESS_KEY
            self.aws_region_name= theAWS_REGION_NAME
            self.aws_region = theAWS_REGION_NAME
            self.aws_s3_bucket_name = theAWS_S3_BUCKET_NAME
            self.aws_object_url_expiration_in_secs = theAWS_OBJECT_URL_EXPIRATION_IN_SECS
        except KeyError as ke:
            raise Exception(f"Expected configuration failed to load {ke} from constructor parameters.")

        try:
            self.s3resource = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name= self.aws_region_name
            ).resource('s3')
            # try an operation just to verify the Resource works so that the except clause
            # is entered here rather than another method if configuration is not correct.
            self.s3resource.meta.client.head_bucket(Bucket=self.aws_s3_bucket_name)
        except ClientError as ce:
            raise Exception(f"Unable to access S3 Resource. ce={ce} with aws_access_key_id={self.aws_access_key_id}.")

        try:
            self.s3client = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region
            ).client('s3')
            # try an operation just to verify the Resource works so that the except clause
            # is entered here rather than another method if configuration is not correct.
            self.s3resource.meta.client.head_bucket(Bucket=self.aws_s3_bucket_name)
        except ClientError as ce:
            raise Exception(f"Unable to create/access S3 Client. ce={ce} with aws_access_key_id={self.aws_access_key_id}.")

    # Write an object to the configured bucket
    # INPUTS:
    #  theText- the text that will be written to the object
    #  aUUID- A unique id to idenify the object
    # RETURNS:
    #  object_key- A unique key to identify the object (used in create_URL_for_object
    def stash_file_as_object(self, filename, folder_name_without_delim='' ,delimiter=''):
        connector_bucket = self.s3resource.Bucket(self.aws_s3_bucket_name)
        prefix = folder_name_without_delim + delimiter
        object_name = prefix + os.path.basename(filename)

        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/bucket/upload_file.html
            response = connector_bucket.upload_file(filename, object_name)
            return response
        except ClientError as ce:
            raise Exception(f"Unable to access S3 Resource. ce={ce}.")

    # Write an object to the configured bucket
    # INPUTS:
    #  theText- the text that will be written to the object
    #  aUUID- A unique id to idenify the object
    # RETURNS:
    #  object_key- A unique key to identify the object (used in create_URL_for_object
    def stash_text_as_object(self, theText, aUUID):
        connector_bucket = self.s3resource.Bucket(self.aws_s3_bucket_name)
        object_key = f"{aUUID}_{len(theText)}_{str(time.time())}"
        obj = connector_bucket.Object(object_key)
        try:
            obj.put(Body=theText)
            return object_key
        except ClientError as ce:
            raise Exception(f"Unable to access S3 Resource. ce={ce} with app_config={app_config}.")

    # Read names of all S3 Objects in the configured S3 Bucket folder
    # INPUTS:
    #  folder_name_without_delim - the name of the "folder" within the bucket, if any.
    #  delimiter - the delimiter used when referring to S3 Objects when a "folder" is used.
    # RETURNS:
    #  obj_name_list - a list containing the name of each S3 Object in the S3 Bucket.
    def list_bucket_folder_objects(self, folder_name_without_delim, delimiter):

        obj_name_list=[]
        paginator = self.s3client.get_paginator('list_objects_v2')
        prefix = folder_name_without_delim + delimiter
        pages = paginator.paginate(Bucket=self.aws_s3_bucket_name
                                   ,Prefix=prefix
                                   ,Delimiter=delimiter)

        for page in pages:
            for obj in page['Contents']:
                name_in_subfolder=obj['Key'][len(prefix):]
                if obj['Key'][:len(prefix)] == prefix and name_in_subfolder:
                    obj_name_list.append(name_in_subfolder)
        return obj_name_list

    # Read all S3 Objects in the configured S3 Bucket folder
    # INPUTS:
    #  folder_name_without_delim - the name of the "folder" within the bucket, if any.
    #  delimiter - the delimiter used when referring to S3 Objects when a "folder" is used.
    #  obj_type - a string mask to limit the S3 Objects return to those with a certain suffix
    # RETURNS:
    #  obj_list - a list containing the S3 Objects in the S3 Bucket folder which have the
    #             specified suffix.
    def get_bucket_folder_objects(self, folder_name_without_delim, delimiter, obj_type='.json'):

        self.logger.debug(f"In get_bucket_folder_objects() with"
                          f" self.aws_s3_bucket_name={self.aws_s3_bucket_name}"
                          f" folder_name_without_delim={folder_name_without_delim}")
        obj_list=[]
        paginator = self.s3client.get_paginator('list_objects_v2')
        prefix = folder_name_without_delim + delimiter
        suffix = obj_type
        self.logger.debug(f"prefix={prefix} and suffix={suffix}")
        pages = paginator.paginate(Bucket=self.aws_s3_bucket_name
                                   ,Prefix=prefix
                                   ,Delimiter=delimiter)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    name_in_subfolder=obj['Key'][len(prefix):]
                    if (obj['Key'][:len(prefix)] == prefix and \
                        name_in_subfolder and \
                        name_in_subfolder[-len(suffix):] == suffix):
                        obj_list.append(obj)
            else:
                raise Exception(f"Unexpected configuration - failed to find 'Contents' in"
                                f"page keys: {page.keys()}")
        return obj_list

    # Retrieve the whole object body, assuming it won't be so large that we
    # should read incrementally from the stream.
    def get_object_body(self, object_key):
        try:
            obj = self.s3client.get_object(Bucket=self.aws_s3_bucket_name
                                           ,Key=object_key)
        except Exception as e:
            raise e
        return obj['Body'].read()

