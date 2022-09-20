import tempfile
import datetime
import io
import zipfile
import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv
import tqdm

class aws_interaction():
    __logging__ = None
    __aws_rainbow_table__ = None
    
    aws_access_key = os.getenv('aws_s3_accessKey')
    aws_secret_key = os.getenv('aws_s3_secretKey')
    aws_bucket = os.getenv('aws_s3_bucket')
    signature_version = os.getenv('aws_s3_signature_version')
    region_name = os.getenv('aws_s3_region')
    aws_prefix = os.getenv('aws_s3_prefix')


    def __init__(self, loggingMain, rainbow_table_startdate = datetime.datetime(2022, 9, 1, tzinfo = None)):
        self.__logging__ = loggingMain
        self.__aws_rainbow_table__ = self.list_s3_files(rainbow_table_startdate)

    def download_aws(aws_filename, local_filename, access_key=aws_access_key, secret_key=aws_secret_key, bucket_name=aws_bucket):
        s3 = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key= secret_key)
        
        s3.Bucket(bucket_name).download_file(local_filename, aws_filename)
        print("Download Successful!")
        return True


    '''
    def aws_getFileURL(aws_filename, access_key=aws_access_key, secret_key=aws_secret_key, bucket_name=aws_bucket):
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key= secret_key,
                        config=Config(signature_version=signature_version), region_name=region_name)

        # Generate the URL to get 'key-name' from 'bucket-name'
        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket_name,
                'Key': aws_filename
            }
        )
        return url
    '''

    def list_s3_files(self, start_datetime):
        # creates a list of all the files in S3 bucket
        s3 = boto3.resource('s3', aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key)
        
        fileList = []
        s3_bucket = s3.Bucket(self.aws_bucket)
        for s3_bucket_object in s3_bucket.objects.filter(Prefix=self.aws_prefix):
        #for s3_bucket_object in s3_bucket.objects.all():
            if (s3_bucket_object.last_modified).replace(tzinfo = None) >= start_datetime:
                fileList.append(s3_bucket_object.key)
                self.__logging__.info(s3_bucket_object.key)
            
        return fileList


    def download_payload_from_aws_s3_and_unzip(self, aws_file_url):
        s3_client = boto3.client('s3', aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key)
        temp_file = tempfile.NamedTemporaryFile(prefix=aws_file_url.replace('/', '_').replace('\\', '_'))
        s3_client.download_fileobj(self.aws_bucket, aws_file_url, temp_file)
        temp_file.seek(0)
        
        z = zipfile.ZipFile(temp_file)
    
        payload_files = {i: z.read(i) for i in z.namelist()}
        
        return payload_files


    def find_aws_file_url(self, input_definition_group_id):
        aws_file_url = ''

        for item in self.__aws_rainbow_table__:
            if item.__contains__(input_definition_group_id):
                aws_file_url = item
                break

        return aws_file_url

def main():
    load_dotenv()
    aws_interaction_client = aws_interaction()
    #aws_interaction_client.test()
    #aws_interaction_client.test2()
    #aws_interaction_client.list_s3_files()
    aws_interaction_client.download_payload_from_aws_s3_and_unzip('any_dmfzip_amq/2022-09/08/ARVATO/0af7ca19-36b1-4029-8205-a11775a62b0c-ANY_DMFZIP_AMQ-ARVATO-adea1089-9d5b-4909-93dc-c048c092b2ce.zip')

#main()