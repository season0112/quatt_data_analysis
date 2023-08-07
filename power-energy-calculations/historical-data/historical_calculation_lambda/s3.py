import boto3
import pandas as pd
import json
import gzip
from collections import defaultdict
from typing import Union, Optional
from datetime import datetime, timedelta
import logging

def get_cic_data(self, cic_ids: Union[list, str], 
                 bucket_name:str = 'quatt-iot-stats-production',
                 cloud_type: str = 'development',
                 start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, 
                 extract_date: Optional[datetime] = None,
                 filter_objects: Optional[list[str]] = None,
                 filter_properties: Optional[dict] = None) -> Optional[pd.DataFrame]:
    """Get data pertaining to one or more CiCs from Quatt's AWS S3 storage as a pandas dataframe

    Args:
        cic_ids (Union[list, str]): One or list of CiCs
        bucket_name (str, optional): Name of the S3 bucket. Defaults to 'quatt-iot-stats-production'.
        cloud_type (str, optional): cloud-type to connect to. Possible values are ['development', 'production']. Defaults to 'development'.
        start_date (Optional[datetime], optional): Starting date for collecting data. When start_date is specified and end_date is not specified, end_date is assumed to be the current date. Defaults to None.
        end_date (Optional[datetime], optional): Ending date for collecting data. When end_date is specified and start_date is not specified, start_date is assumed to be the same as the end_date. Defaults to None.
        extract_date (Optional[datetime], optional): Specify a single date for which the data should be extracted. This is an alternative to specifying the start_date and end_date. Defaults to None.
        filter_objects (Optional[list[str]], optional): A list of cloud connector object names that should be extracted from the data. Defaults to None.
        filter_properties (Optional[dict], optional): A dictionary specifying the cloud connector object and corresponding list of properties (as key-value pairs) that should be extracted from the data. Defaults to None.

    Returns:
        Optional[pd.DataFrame]: A pandas dataframe containing the extracted data
    """    
    logger = logging.getLogger("quatt_aws_utils")
    logger.setLevel(logging.INFO)
    
    # Input validation
    if not((start_date and end_date) or extract_date):
        logger.info("No timerange was provided for data extraction. Data will only be extracted for the current date")
        start_date = self.start_of_day(datetime.now())
        end_date = start_date
    elif start_date and end_date and extract_date:
        logger.info("Ignoring extract date and using the provided date range")
    elif start_date and not end_date:
        logger.info("Provide start date but not end date for data extraction. Assuming today's date as end date")
        end_date = datetime.now()
    elif end_date and not start_date:
        logger.info("Provide end date but not start date for data extraction. Assuming today's date as start date")
        start_date = end_date
    elif extract_date:
        start_date = extract_date
        end_date = extract_date
    
    if cloud_type not in ['development', 'production']:
        raise ValueError(f"Value of argument cloud_type should be one of ['development', 'production'], received {cloud_type}")

    if not isinstance(cic_ids, list):
        cic_ids = [cic_ids]
    
    # Map to different value for production vs development
    if cloud_type == 'production':
        bucket_name = 'quatt-iot-stats-production'
        s3_uri_parent = 'dt/'
    elif cloud_type == 'development':
        bucket_name = 'quatt-iot-data-dev'
        s3_uri_parent = '/dt/'

    
    s3_objects_list = self.list_s3_objects(bucket=bucket_name, cic_ids=cic_ids, start_date=start_date, end_date=end_date,
                                           s3_uri_parent=s3_uri_parent)
    
    # s3_objects_list = self.list_s3_objects(bucket=bucket_name, cic_id=cic_id, start_date=start_date, end_date=end_date)
    if not s3_objects_list:
        logger.info("No s3 objects found for given cic-id in the given date-range")
        return None

    json_data = []
    for s3_objects in s3_objects_list:
        json_data = json_data + self.load_data_as_dict(bucket_name=bucket_name, s3_objects=s3_objects, 
                                                       filter_objects=filter_objects,
                                                       filter_properties=filter_properties,
                                                       add_cic_id=True)

    df = pd.json_normalize(json_data, max_level=1)

    # restore de-duplicated data if it exists
    if 'msg_time' in df.columns:
        df['msg_time'].fillna(method='ffill', inplace=True)
        df = df.groupby('msg_time').ffill()

    return df

def start_of_day(date_in: datetime):
    return(datetime(year=date_in.year, month=date_in.month, day=date_in.day))

def list_s3_objects(self, bucket: str, cic_ids: str, start_date: datetime, end_date: datetime, s3_uri_parent: str) -> list:
    s3_objects = []

    for cic_id in cic_ids:
        date_iter = start_date
        while date_iter <=end_date:
            s3_uri_prefix = self.make_s3_uri_prefix(cic_id, date_iter, s3_uri_parent)
            object_list = self.list_objects(Bucket=bucket, Prefix=s3_uri_prefix)

            # filter out objects that are before start time
            if (date_iter.date() == start_date.date()) \
                & ('Contents' in object_list):
                object_list['Contents'] = [obj 
                                           for obj in object_list['Contents'] 
                                           if datetime.strptime(
                                                '-'.join(obj['Key']
                                                         .split('/')[-1].split('-')[2:8]),
                                                    '%Y-%m-%d-%H-%M-%S')
                                                      >= start_date]
            # filter out objects that are after end time
            elif (date_iter.date() == end_date.date()) \
                & ('Contents' in object_list):
                object_list['Contents'] = [obj 
                                           for obj in object_list['Contents'] 
                                           if datetime.strptime(
                                                '-'.join(obj['Key']
                                                         .split('/')[-1].split('-')[2:8]),
                                                    '%Y-%m-%d-%H-%M-%S')
                                                      <= end_date]
            s3_objects.append(object_list)
            
            date_iter = date_iter + timedelta(days=1)

    return s3_objects

def make_s3_uri_prefix(self, cic_id: str, search_date: datetime, s3_uri_parent: str):
    date_str = search_date.strftime("%Y/%m/%d")
    return f"{s3_uri_parent}{cic_id}/{date_str}/"

def load_data_as_dict(self, bucket_name, s3_objects,
                      filter_objects: Optional[list[str]] = None,
                      filter_properties: Optional[dict] = None,
                      add_cic_id=True) -> dict:
    data = []
    if 'Contents' not in s3_objects:
        return data

    for s3_object in s3_objects['Contents']:
        obj_data = self.get_object(Bucket=bucket_name, Key=s3_object['Key'])['Body'].read()

        # Decompress the data (assuming it's in gzip format)
        decompressed_data = gzip.decompress(obj_data)

        # Decode the data using the appropriate codec (e.g. utf-8)
        decoded_data = decompressed_data.decode('utf-8')

        for msg in decoded_data.split('\n'):
            json_msg = json.loads(msg)
            stats = json_msg['payload']['stats']
            
            if not stats: # if stats is empty
                continue

            if add_cic_id:
                cic_id = json_msg['payload']['ctx']['hostName']

            if filter_objects or filter_properties:
                stats = self.filter_stats(stats, filter_objects, filter_properties, add_cic_id, cic_id)
            
            # check if stats is not an empty object
            stats[0]['msg_time'] = stats[0]['time']['ts']
            data = data + stats
    
    return data

def filter_stats(self, stats, filter_objects: Optional[list[str]] = None, 
                 filter_properties: Optional[dict] = None,
                 add_cic_id=True, cic_id=None):
    if add_cic_id and not cic_id:
        raise ValueError("Should have received a cic id to add to the data, received None")
    
    filtered_stats = []
    for stat in stats:
        if filter_objects:
            filtered_stat = {obj: obj_data for obj, obj_data in stat.items() if obj in filter_objects}
        else:
            filtered_stat = dict()
        if filter_properties:
            for filter_obj, filter_props in filter_properties.items():
                if filter_obj not in stat or stat[filter_obj] is None:
                    filtered_stat[filter_obj] = {filter_prop: None for filter_prop in filter_props}
                else:
                    for filter_prop in filter_props:
                        if filter_obj not in filtered_stat:
                            filtered_stat[filter_obj] = dict()
                        try:
                            filtered_stat[filter_obj][filter_prop] = stat[filter_obj][filter_prop]
                        except KeyError:
                            filtered_stat[filter_obj][filter_prop] = None
        if add_cic_id:
            filtered_stat['cic_id'] = cic_id

        filtered_stats.append(filtered_stat)
    
    return filtered_stats

def add_custom_methods(class_attributes, **kwargs):
    class_attributes['get_cic_data'] = get_cic_data
    class_attributes['start_of_day'] = start_of_day
    class_attributes['list_s3_objects'] = list_s3_objects
    class_attributes['make_s3_uri_prefix'] = make_s3_uri_prefix
    class_attributes['load_data_as_dict'] = load_data_as_dict
    class_attributes['filter_stats'] = filter_stats

def create_s3_client():

    # Replace this with the name of your AWS profile
    # AWS_PROFILE = 'production'

    # Create a session using the named profile
    session = boto3.Session()
    session.events.register('creating-client-class.s3', add_custom_methods)

    # Create a client for interacting with the S3 service
    quatt_s3_client = session.client('s3')

    return quatt_s3_client

if __name__ == '__main__':
    test_cloud_type = 'production'
    # test_cloud_type = 'development'
    
    if test_cloud_type == 'production':
        cic_ids = 'CIC-00149d9a-da31-5e61-844d-3e818b8a2ded'
        start_date = datetime(2023,3,18)
        end_date = datetime(2023,3,19)
        extract_date = datetime(2023,5,31)
        props = {'time': ['ts'],
                'flowMeter': ['flowRate', 'waterSupplyTemperature']}
        
        S3_PROPERTIES = {
         'time': ['ts'],
         'system': ['quattId'],
         'qc': ['hp1PowerInput',
                'hp1PowerOutput',
                'hp1ElectricalEnergyCounter',
                'hp2ElectricalEnergyCounter',
                'hp1ThermalEnergyCounter',
                'hp2ThermalEnergyCounter',
                'cvEnergyCounter',
                'cvPowerOutput',
                'supervisoryControlMode'],
         'hp1': ['acInputVoltage',
                 'acInputCurrent',
                 'getFanSpeed',
                 'bottomPlateHeaterEnable',
                 'compressorCrankcaseHeaterEnable',
                 'circulatingPumpDutyCycle',
                 'getCirculatingPumpRelay',
                 'powerInput',
                 'powerOutput',
                 'temperatureOutside',
                 'power',
                 'defrostFlag'],
         'hp2': ['acInputVoltage',
                 'acInputCurrent',
                 'getFanSpeed',
                 'bottomPlateHeaterEnable',
                 'compressorCrankcaseHeaterEnable',
                 'circulatingPumpDutyCycle',
                 'getCirculatingPumpRelay',
                 'powerInput',
                 'powerOuput',
                 'temperatureOutside',
                 'power']}

        quatt_s3_client = create_s3_client(aws_profile='nout_prod')
        df_new = quatt_s3_client.get_cic_data(cic_ids=cic_ids, 
                                            #   bucket_name='quatt-iot-data-dev',
                                            start_date=start_date, end_date=end_date, 
                                            cloud_type=test_cloud_type,
                                            # extract_date=extract_date,
                                            # filter_objects=['time'],
                                            filter_properties=S3_PROPERTIES
                                            )
    
    elif test_cloud_type == 'development':
        # Connect to development cloud
        cic_ids = ['CIC-f42058e3-44c5-5d70-809d-f2ee78b2abf9']
        extract_date = datetime(2023, 2, 28)
        quatt_s3_client = create_s3_client(aws_profile='default')
        df_new = quatt_s3_client.get_cic_data(cic_ids=cic_ids, 
                                            cloud_type='development', 
                                            extract_date=extract_date,
                                            filter_objects=['time', 'qc'], 
                                            filter_properties={'hp1': ['temperatureWaterIn', 'temperatureWaterOut']}
                                            )
    
    # print(df_new.head())
    # print(df_new.describe())
    pass