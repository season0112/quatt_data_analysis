
import logging
import boto3
from botocore.exceptions import ClientError


def main():

    s3 = Boto3()

    # Print buckets
    s3.Print_excited_buckets_from_client()
    s3.Print_buckets_from_resource()

    # Print objects in bucket
    # development environment
    print_bucket_name = 'demo-s3-static-website'
    print_bucket_name = 'quatt-iot-data-dev/dt/CIC-13b8aa43-3ceb-5700-8223-ac14ba28476f/'
    print_bucket_name = 'quatt-iot-data-dev'
    # production environment
    print_bucket_name = 'quatt-iot-stats-production'
    #bucket = s3.s3_resource.Bucket(print_bucket_name)
    #folder_name = 'dt/CIC-027921f5-6d43-52b0-ad3c-98ff6811212b/2024/03/25/' 
    #s3.Print_objects_in_bucket(bucket, folder_name)
    #s3.Print_objects_in_bucket(bucket)

    # Load object in bucket
    #load_bucket_name = 'quatt-systems-control'
    #load_object_key = 'clickhouse_test.csv'
    #load_object_pathInBucket = 'data/test/'
    #load_object = s3.LoadObjectFromBucket(load_bucket_name, load_object_key, load_object_pathInBucket)
    #df = pd.read_csv(load_object['Body'])
    #print( df.head() )

    '''
    # Download the object from Bucket
    bucket_name_download = 'quatt-iot-data-dev'
    object_key = 'firehose_to_s3-5-2024-03-25-22-10-53-0b2f9ee6-a3c4-322a-9be6-6964a408cfd6.gz'
    current_directory = os.getcwd()
    ObjectPathInBucket = 'dt/CIC-027921f5-6d43-52b0-ad3c-98ff6811212b/2024/03/25/'
    download_path = os.path.join(current_directory, object_key)
    s3.DownloadObjectFromBucket(bucket_name_download, object_key, ObjectPathInBucket, download_path)
    if object_key[-3:] == '.gz':
        un_gz(object_key)
    '''

    '''
    # Convert from JSON to Pandas Frame
    file_path = 'firehose_to_s3-5-2024-03-25-22-10-53-0b2f9ee6-a3c4-322a-9be6-6964a408cfd6.json'
    test = read_json_to_pandasFrame(file_path)
    print(test)
    print(test.columns)
    '''

    '''
    # Create a new bucket
    new_bucket_name = 'test-bucket-24-06-2024'
    s3.Create_bucket(new_bucket_name)

    # Upload file to an exsited bucket
    filepath = '/Users/sichenli_quatt/Desktop/'
    filename = 'NoutAnalysis.txt'
    upload_target_bucket_name = 'demo-s3-static-website'
    s3.UploadObjectToBucket(upload_target_bucket_name, filepath, filename)
    s3.Print_objects_in_bucket(bucket)

    # Delete file in a bucket
    delete_target_bucket_name = 'demo-s3-static-website'
    filename = 'NoutAnalysis.txt'
    s3.DeleteObjectFromBucket(delete_target_bucket_name, filename)    
    s3.Print_objects_in_bucket(bucket)
    '''

if __name__=='__main__':
    main()


