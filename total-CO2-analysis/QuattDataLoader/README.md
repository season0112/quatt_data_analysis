Quatt Data Loader tool.

This includes Quatt data from AWS S3, Clickhouse, and AWS RDS Mysql.

Before using this data loader tool, please check if your credentials are properly set.
For mysql, it load AWS Mysql credential named ".mysql_env" file in your $HOME directoriy.
For S3, it load AWS S3 credential named ".aws" folder in your $HOME directoriy, which includes default config and credentials.
For clickhouse, it load Clickhouse credential named ".clickhouse_env" in your $HOME directoriy.  

Example usage:
python demo.py mysql # TODO: subparsers to be added 
python demo.py clickhouse --extractVariables qc_minimumCOP --table cic_stats --clientid CIC-87845a22-3d04-5f8c-8d4e-c1e735a9e472 --startTime "2024-01-02 00:00:00" --endTime "2024-01-02 20:00:00" # TODO: default extractQuery. 
python demo.py s3 # TODO: subparsers to be added

If you if no subparser is given, the tool will seek for "extractQuery" by default.


For S3: you can also perform actions with S3 instance like:
Create_bucket, Print_excited_buckets_from_client, Print_objects_in_bucket, UploadObjectToBucket, DeleteObjectFromBucket, DownloadObjectFromBucket, LoadObjectFromBucket etc.  


