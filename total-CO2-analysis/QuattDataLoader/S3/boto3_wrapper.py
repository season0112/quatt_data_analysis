import logging
import boto3
from botocore.exceptions import ClientError


class Boto3:

    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')

    def Create_bucket(self, bucket_name):
        try:
            self.s3_client.create_bucket(Bucket=bucket_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def Print_excited_buckets_from_client(self):
        response = self.s3_client.list_buckets()
        print('Existing buckets:')
        for bucket in response['Buckets']:
            print(f'{bucket["Name"]}')

    def Print_buckets_from_resource(self):
        for bucket in self.s3_resource.buckets.all():
            print(bucket.name)

    def Print_objects_in_bucket(self, bucket, folder=''):
        for obj in bucket.objects.filter(Prefix=folder):
            print(obj.key)

    def file_exists_in_bucket(self, bucket_name, object_key):
        try:
            self.s3_resource.Object(bucket_name, object_key).load()
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise

    def UploadObjectToBucket(self, bucket_name, filepath, filename):
        if not self.file_exists_in_bucket(bucket_name, filename):
            try:
                print("File doesn\'t exist, try to upload it.")
                with open( filepath + filename, 'rb') as data:
                    self.s3_resource.Bucket(bucket_name).put_object(Key=filename, Body=data)
                print('Successfully uploading the file!')
            except FileNotFoundError:
                print(f"File not found.")
            except Exception as e:
                print(f"An error occurred: {e}")
        else:
            print("File already exsited.")

    def DeleteObjectFromBucket(self, bucket_name, filename):
        if self.file_exists_in_bucket(bucket_name, filename):
            try:
                print("File exist, continue to delete it.")
                obj = self.s3_resource.Object(bucket_name, filename)
                response = obj.delete()
            except Exception as e:
                print(f"An error occured: {e}")
            else:
                print("Deleting done.")

    def DownloadObjectFromBucket(self, bucket_name, object_key, ObjectPathInBucket, download_path):
        try:
            fullPathInBucket = os.path.join(ObjectPathInBucket, object_key)
            self.s3_client.download_file(bucket_name, fullPathInBucket, download_path)
            print(f'Successfully downloaded {object_key} to {download_path}')
        except ClientError as e:
            logging.error(e)
            print(f"An error occurred: {e}")

    def LoadObjectFromBucket(self, bucket_name, object_key, ObjectPathInBucket):
        try:
            fullPathInBucket = os.path.join(ObjectPathInBucket, object_key)
            response = self.s3_client.get_object(Bucket=bucket_name, Key=fullPathInBucket
            )
            return response
        except ClientError as e:
            logging.error(e)

