import argparse
import QuattDataLoader.Mysql as mysql_loader
import QuattDataLoader.Clickhouse as clickhouse_loader
import QuattDataLoader.S3 as S3_loader
import time

def main():


    # Parser Argument
    parser = argparse.ArgumentParser(description='QuattDataLoader main parser')
    subparsers = parser.add_subparsers(dest='subpackage', help='Subpackage to use')

    clickhouse_parser = subparsers.add_parser('clickhouse', help='Options for Clickhouse')
    clickhouse_args = clickhouse_loader.clickhouse_argparse.parse_clickhouse_args()
    for action in clickhouse_args._actions:
        clickhouse_parser._add_action(action)

    mysql_parser = subparsers.add_parser('mysql', help='Options for MySQL')
    mysql_args = mysql_loader.mysql_argparse.parse_mysql_args()
    for action in mysql_args._actions:
        mysql_parser._add_action(action)

    s3_parser = subparsers.add_parser('s3', help='Options for S3')
    s3_args =  S3_loader.boto3_argparse.parse_boto3_args()  
    for action in s3_args._actions:
        s3_parser._add_action(action)
    args = parser.parse_args()


    # Load Data
    if args.subpackage == 's3':
        s3_instance = S3_loader.Boto3()
        # Example action:
        bucket = s3_instance.s3_resource.Bucket('quatt-iot-stats-production')
        folder_name = 'dt/CIC-2107ad0d-8e9a-517c-8d37-7604ae9cd93f/2024/03/25/'
        s3_instance.Print_objects_in_bucket(bucket, folder_name)
    elif args.subpackage == 'clickhouse':
        clickhouse_loader.connect_clickhouse(args)
    elif args.subpackage == 'mysql':
        extractedData = mysql_loader.connect_mysql(args)

    # Analysis
    print(extractedData)

    # Save
    extractedData.to_csv("extractedData_" + str(args.subpackage) + ".csv", index=False )


if __name__ == "__main__":
    time_start=time.time()
    main()
    time_end=time.time()
    print('Running Time: ', (time_end-time_start)/60,'mins')


