'''
This script aggregates second level data from S3 to hourly data.
- Extracts data from S3 for a certain cic and date range.
- Calculates energy consumption en production.
- Aggregate data to hourly data.
- Load data to MySQL database.

Input: cic, start date, end date
'''
import s3
import boto3
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pickle
import os
import re
import mysql.connector
from mysql.connector import Error
from urllib.parse import urlparse
import os
import sys
from pathlib import Path
import logging

# set up logging
logging.basicConfig(filename="std.log",
                    format='%(asctime)s %(levelname)s %(name)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='a')

logger=logging.getLogger()
logger.setLevel(logging.INFO)

try:
    MYSQL_URL = os.environ['MYSQLDEV']
except Exception as e:
    logger.critical(f'Could not load .env or MYSQLDEV url not in .env file, {e}')

# SETTINGS
MAX_INTEGRATION_INTERVAL =  900# max interval in seconds for which integration is calculated
RECALCULATE_HP_HEAT_PRODUCED = False # if True, hp1.powerOuput calculated manually
DENSITY_WATER = 997 # kg/m3
SPECIFIC_HEAT_WATER = 4182 # J/kg/K

# INPUTS FOR CALCULATION
AGGREGATIONS =[
    # directly available from cic stats
    {'sql':'cic_id', 'src':'cic_id', 'func':'last'},
    {'sql':'quattBuild', 'src':'system.quattBuild', 'func':'first'},
    {'sql':'OLD_hp1_electrical_energy_counter',
     'src':'qc.hp1ElectricalEnergyCounter',
     'func':'last'},
    {'sql':'OLD_hp2_electrical_energy_counter',
     'src':'qc.hp2ElectricalEnergyCounter',
     'func':'last'},
    {'sql':'OLD_hp1_thermal_energy_counter',
     'src':'qc.hp1ThermalEnergyCounter',
     'func':'last'},
    {'sql':'OLD_hp2_thermal_energy_counter',
     'src':'qc.hp2ThermalEnergyCounter',
     'func':'last'},
    {'sql':'OLD_cv_energy_counter',
     'src':'qc.cvEnergyCounter',
     'func':'last'},
    {'sql':'hp1_defrost', 'src':'hp1.defrostFlag', 'func':'mean'},
    # extra
    {'sql':'water_supply_temperature',
     'src':'flowMeter.waterSupplyTemperature',
     'func':'mean'},
    {'sql':'water_return_temperature',
     'src':'hp1.temperatureWaterIn',
     'func':'mean'},
    {'sql':'room_set_temperature',
     'src':'thermostat.otFtRoomSetpoint',
     'func':'mean'},
    {'sql':'room_temperature',
     'src':'thermostat.otFtRoomTemperature',
     'func':'mean'},
    {'sql':'outside_temperature',
     'src':'hp1.temperatureOutside',
     'func':'mean'},

    # preprocessed from cic stats
    {'sql':'house_energy_demand', 'src':'estimatedEnergyDemand', 'func':'sum'},

    {'sql':'hp1_energy_consumed', 'src':'hp1.energyConsumption', 'func':'sum'},
    {'sql':'hp2_energy_consumed', 'src':'hp2.energyConsumption', 'func':'sum'},
    {'sql':'hp1_data_availability', 'src':'hp1_data_availability', 'func':'min'},
    {'sql':'hp2_data_availability', 'src':'hp2_data_availability', 'func':'min'},
    {'sql':'hp1_heat_generated', 'src':'hp1.heatGenerated', 'func':'sum'},
    {'sql':'hp2_heat_generated', 'src':'hp2.heatGenerated', 'func':'sum'},
    {'sql':'boiler_heat_generated', 'src':'cvHeatGenerated', 'func':'sum'},
    {'sql':'hp1_active', 'src':'hp1.active', 'func':'mean'},
    {'sql':'hp2_active', 'src':'hp2.active', 'func':'mean'},
    {'sql':'boiler_active', 'src':'cvActive', 'func':'mean'},
    {'sql':'anti_freeze_protection', 'src':'antiFreeze', 'func':'mean'}, # extra
    {'sql':'flow_rate_oos', 'src':'flowRateOos', 'func':'mean'},# extra
    {'sql':'inlet_temperature_oos', 'src':'inletTemperatureOos', 'func':'mean'} # extra
]

# properties to download from S3
S3_PROPERTIES = {
         'time': ['ts'],
         'system': ['quattId',
                    'quattBuild',
                    'hp1Connected',
                    'hp2Connected',
                    'modbusConnected'],
         'qc': ['hp1PowerOutput',
                'hp1ElectricalEnergyCounter',
                'hp2ElectricalEnergyCounter',
                'hp1ThermalEnergyCounter',
                'hp2ThermalEnergyCounter',
                'cvEnergyCounter',
                'cvPowerOutput',
                'supervisoryControlMode',
                'watchdogState',
                'watchdogSubcode',
                'systemWatchdogCode',
                'estimatedPowerDemand'],
        'flowMeter' : ['waterSupplyTemperature',
                       'flowRate'],
        'thermostat' : ['otFtRoomSetpoint',
                        'otFtRoomTemperature'],
         'hp1': ['acInputVoltage',
                 'acInputCurrent',
                 'getFanSpeed',
                 'bottomPlateHeaterEnable',
                 'compressorCrankcaseHeaterEnable',
                 'circulatingPumpDutyCycle',
                 'getCirculatingPumpRelay',
                 'powerOutput',
                 'temperatureOutside',
                 'defrostFlag',
                 'electricalEnergyCounter',
                 'thermalEnergyCounter',
                 'temperatureWaterIn',
                 'watchdogCode',
                 'temperatureOutside',
                 'power'],
         'hp2': ['acInputVoltage',
                 'acInputCurrent',
                 'getFanSpeed',
                 'bottomPlateHeaterEnable',
                 'compressorCrankcaseHeaterEnable',
                 'circulatingPumpDutyCycle',
                 'getCirculatingPumpRelay',
                 'powerOutput',
                 'temperatureOutside',
                 'electricalEnergyCounter',
                 'thermalEnergyCounter',
                 'power']}

# Integration keys
INTEGRATION_KEYS = {'hp1.energyConsumption':'hp1.powerConsumption',
                    'hp2.energyConsumption':'hp2.powerConsumption',
                    'hp1.heatGenerated':'hp1.powerOutput',
                    'hp2.heatGenerated':'hp2.powerOutput',
                    'cvHeatGenerated':'cv_power_output',
                    'estimatedEnergyDemand':'qc.estimatedPowerDemand'}


# calculate heat produced
def hp_heat_produced(flowrate, water_in, water_out):
    return (flowrate / 3600 / 1000 * DENSITY_WATER * 
            SPECIFIC_HEAT_WATER * (water_out - water_in))

# function to estimate bphprobability
def bottom_plate_heater_probability(temperature_outside):
    A = 0.31530397864623305
    B = 4.416233732189494
    C = 3.796331812113364

    ans = np.max(
        # if temperature is below 4 degrees always on
        [(temperature_outside<=4).astype(float),
        # between 4 and 7 use probability
        np.all([temperature_outside>4, 
                temperature_outside<=7],
                axis=0)
                .astype(float) \
            *(1 / (A + B * (temperature_outside-C)))
        ], 
        axis=0
    ).tolist()
    return pd.Series(ans)

# replace hp1.powerInput
def prepare_data_for_calculation(df, hp):

    # calculate power input
    df[f'{hp}.powerInput'] = df[f'{hp}.acInputVoltage'] * df[f'{hp}.acInputCurrent']
    df.drop([f'{hp}.acInputVoltage', f'{hp}.acInputCurrent'], axis=1, inplace=True)
        
    # fill missing old counters
    df[f'qc.{hp}ElectricalEnergyCounter'] = (
        df[f'qc.{hp}ElectricalEnergyCounter'].fillna(df[f'{hp}.electricalEnergyCounter']))
    df[f'qc.{hp}ThermalEnergyCounter'] = (
        df[f'qc.{hp}ThermalEnergyCounter'].fillna(df[f'{hp}.thermalEnergyCounter']))
    df.drop([f'{hp}.electricalEnergyCounter',
             f'{hp}.thermalEnergyCounter'], axis=1, inplace=True)
    
    # set data availability
    df[f'{hp}_data_availability_2'] = (
        df[[f'{hp}.powerInput',
            f'{hp}.getFanSpeed',
            f'{hp}.bottomPlateHeaterEnable',
            f'{hp}.compressorCrankcaseHeaterEnable',
            f'{hp}.circulatingPumpDutyCycle',
            f'{hp}.getCirculatingPumpRelay']]
            .notna()
            .all(axis=1)
            .astype(int) * 2)
    df[f'{hp}_data_availability_1'] = (
        df[[f'{hp}.powerInput',
        f'{hp}.getFanSpeed',
        f'{hp}.temperatureOutside',
        f'{hp}.circulatingPumpDutyCycle',
        f'{hp}.getCirculatingPumpRelay']]
        .notna()
        .all(axis=1)
        .astype(int))

    df[f'{hp}_data_availability'] = (
        np.max(df[[f'{hp}_data_availability_1',
                   f'{hp}_data_availability_2']], axis=1)
    )
    df.drop([f'{hp}_data_availability_1',
                f'{hp}_data_availability_2'], axis=1, inplace=True) # TODO

    # set bottomplateheaterenable zero in case {hp}.powerInput is zero
    # bottom_plate_heaterprobability(temp_outside) [0-1]
    # when powerInput ==0 -> (1 - 1) * -1 = 0, so will overrule bphprobability
    # when powerInput > 0 -> (0 - 1) * -1 = 1, so bphprobability is always lower
    df[f'{hp}.bottomPlateHeaterEnable'] = (
        df[f'{hp}.bottomPlateHeaterEnable'].fillna(np.minimum(
            ((df[f'{hp}.powerInput'] == 0).astype(float)-1)*-1,
            bottom_plate_heater_probability(
                df[f'{hp}.temperatureOutside'].values))  
        )
    )

    # set compressorCrankcaseHeaterEnable zero in case...
    df[f'{hp}.compressorCrankcaseHeaterEnable'] = (
        df[f'{hp}.compressorCrankcaseHeaterEnable'].fillna(
            ((df[f'{hp}.temperatureOutside'] > -4) &
             (df[f'{hp}.powerInput'] > 0)).astype(float)
        )
    )
    return df

def estimate_energy_consumption(powerInput, 
                                getFanSpeed,
                                bottomPlateHeaterEnable,
                                circulatingPumpDutyCycle, 
                                circulatingPumpRelay, 
                                crankcaseHeater):
    energy_consumption = (5.150232354845286 
                          + (1.1240096401010435 * powerInput)
                          - (0.04858859969715763 * getFanSpeed)
                          + (150.06430841218332 * bottomPlateHeaterEnable)
                          + (circulatingPumpDutyCycle * circulatingPumpRelay)
                          + (40 * crankcaseHeater))
    return energy_consumption

def integrate_data(df, keys):
    df['timediff[S]'] = (df.sort_values(['cic_id','time.ts'])
                         .groupby('cic_id')['time.ts']
                         .diff()/1000
    )
    df['timediff[S]'] = np.minimum(df['timediff[S]'], MAX_INTEGRATION_INTERVAL)

    for key, value in zip(keys.keys(), keys.values()):
        try:
            df[key] = df[value] * df['timediff[S]'] / 3600
        except KeyError:
            df[key] = np.nan
    
    return df

# aggregate data per hour
def aggregate_data_hourly(df, aggregations):
    '''Aggregate dataframe cic and hour creating a new dataframe.'''

    # check input frame
    if ('time.ts' not in df.columns) | ('cic_id' not in df.columns):
        logger.error('time.ts or cic_id not in dataframe')
        raise ValueError('time.ts or cic_id not in dataframe')
    
    # create time stamps
    df['timestamp_of_data'] = pd.to_datetime(df['time.ts'],
                                             unit='ms').dt.ceil('H')
    time_stamps = df.groupby('timestamp_of_data')['timestamp_of_data'].unique().index.values

    df_aggregated = pd.DataFrame(index=time_stamps)
    # add number of rows for each timestamp to df_aggregated
    df_aggregated['number_of_rows'] = (
        df.groupby('timestamp_of_data', sort='time.ts')['time.ts'].count())

    # aggregate data
    for agg in aggregations:
        try:
            if agg['func']=='last':
                df_aggregated[agg['sql']] = (
                    df.groupby('timestamp_of_data',
                               sort='time.ts')[agg['src']]
                               .agg(lambda x: x.iloc[-1])
                )
            elif agg['func']=='first':
                df_aggregated[agg['sql']] = (
                    df.fillna(method='bfill').groupby('timestamp_of_data',
                                                      sort='time.ts')[agg['src']]
                                                      .agg(lambda x: x.iloc[0])
                )
            elif agg['func']=='sum':
                df_aggregated[agg['sql']] = (
                    df.groupby('timestamp_of_data',
                               sort='time.ts')[agg['src']]
                               .agg(agg['func'], min_count=1)
                )
            else:
                df_aggregated[agg['sql']] = (
                    df.groupby('timestamp_of_data',
                               sort='time.ts')[agg['src']]
                               .agg(agg['func'])
                )
        except KeyError:
            logger.info(f'Could not aggregate {agg["src"]} with {agg["func"]}')
            df_aggregated[agg['sql']] = np.nan
        except ValueError:
            raise ValueError(f'Could not aggregate {agg["src"]} with {agg["func"]}')
    
    return df_aggregated

def calculate_and_aggregate(df):
    # check for second heat pump
    if (df['hp2.acInputCurrent'].notna().any()):
        heat_pumps = ['hp1', 'hp2'] 
    else:
        heat_pumps = ['hp1']

    # merge hp1 disconnected data
    df['system.hp1Connected'] = df['system.hp1Connected'].fillna(df['system.modbusConnected'])

    for hp in heat_pumps:
        # get data availability and parameters for power calculation
        df = prepare_data_for_calculation(df, hp)

        # calculate powerConsumption for all rows
        df[f'{hp}.powerConsumption'] = (
            estimate_energy_consumption(
                df[f'{hp}.powerInput'].fillna(0).values,
                df[f'{hp}.getFanSpeed'].fillna(0).values,
                df[f'{hp}.bottomPlateHeaterEnable'].fillna(0).values,
                df[f'{hp}.circulatingPumpDutyCycle'].values,
                df[f'{hp}.getCirculatingPumpRelay'].values,
                df[f'{hp}.compressorCrankcaseHeaterEnable'].values)
        )
        # set all values of powerconsumption to zero where supervisoryControlMode is NaN
        df.loc[df['qc.supervisoryControlMode'].isna(), 
                    f'{hp}.powerConsumption'] = 0
        
        # get activity of heatpump
        df[f'{hp}.active'] = df['qc.supervisoryControlMode'].isin([2,3]).astype(float)

        # get power output
        df[f'{hp}.powerOutput'] = (
            df[f'{hp}.powerOutput'].fillna(df[f'{hp}.power']))
        
        # set power consumption and production to zero where heat pump is disconnected
        df.loc[df[f'system.{hp}Connected'].fillna(True)==False,
               [f'{hp}.powerConsumption', f'{hp}.powerOutput']] = 0

    # get hp1 power output
    if RECALCULATE_HP_HEAT_PRODUCED:
        df['hp1.powerOutput'] = (
            hp_heat_produced(df['flowMeter.flowRate'].fillna(0).values,
                             df['hp1.waterTemperatureIn'].fillna(0).values,
                             df['flowMeter.waterSupplyTemperature'].fillna(0).values)
        )
    else:
        df['hp1.powerOutput'] = (
            df['hp1.powerOutput'].fillna(df['qc.hp1PowerOutput'])
        )
    
    # get anti-freeze and oos states
    df['antiFreeze'] = df['qc.supervisoryControlMode'].isin([96,97,98,99]).astype(float)
    df['flowRateOos'] = np.maximum(
        np.all([df['qc.watchdogState']==8,
                df['qc.watchdogSubcode']==2],
                axis=0).astype(float),
        df['qc.systemWatchdogCode']==2)
    df['inletTemperatureOos'] = np.maximum(
        np.all([df['qc.watchdogState']==2,
                df['qc.watchdogSubcode']==10],
                axis=0).astype(float),
        df['qc.systemWatchdogCode']==10)

    # get cv power output
    df['cv_power_output'] = df['qc.cvPowerOutput']
    df.loc[df['cv_power_output'] < 0, 'cv_power_output'] = 0
    df.loc[df['system.hp2Connected'].fillna(df['system.hp1Connected'])==False,
           'cv_power_output'] = 0 # see END-283
    df['cvActive'] = df['qc.supervisoryControlMode'].isin([3,4]).astype(float)

    # integrate data
    df = integrate_data(df, INTEGRATION_KEYS)

    aggregated_data = aggregate_data_hourly(df, AGGREGATIONS)

    return aggregated_data, df

def make_insert_row_query(index, row):
    query_start = "INSERT INTO cic_data (`time`,"
    query_end = f") VALUES ('{index}',"
    
    # drop nan values
    row = row.dropna()

    for column in row.index:
        query_start += f"{column},"
        query_end += f"'{row[column]}',"
    query = query_start[:-1] + query_end[:-1] + ")"
    return query

# create connection to mysql
def push_data_to_mysql(agg_df: pd.DataFrame):
    parsed_mysql_url = urlparse(MYSQL_URL)
    connection = mysql.connector.connect(host=parsed_mysql_url.hostname,
                                            user=parsed_mysql_url.username,
                                            password=parsed_mysql_url.password,
                                            database=parsed_mysql_url.path[1:],
                                            port=parsed_mysql_url.port)

    # create cursor
    cursor = connection.cursor()

    for index, row in agg_df.iterrows():
        if row.isna()[1]:
            print(row)
            logger.debug(f"skipping row {index} because it contains NaN values")
            continue
        query = make_insert_row_query(index, row)
        logger.debug(f"Executing query: {query}")
        cursor.execute(query)
        connection.commit()

    # close connection
    cursor.close()
    connection.close()

def extract_data_from_s3(cic_id, start_date, end_date, aws_profile):
    # create s3 clients
    quatt_s3_client_pull = s3.create_s3_client(aws_profile)
    
    # add 20 min to end_date
    extract_df = quatt_s3_client_pull.get_cic_data(cic_ids=cic_id, 
                                                    start_date=start_date,
                                                    end_date=end_date, 
                                                    filter_properties=S3_PROPERTIES,
                                                    cloud_type='production' #production is hardcoded for now
                                                    )
    return extract_df

def main(cic_id, start_date, end_date, aws_profile=""):
    # log input parameters
    logger.info(f'''Input parameters: cic_id: {cic_id}, start_date: {start_date}, end_date: {end_date}''')
    
    # input validation
    pattern = re.compile(r'CIC-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
    if not pattern.match(cic_id):
        logger.error(f'Invalid cic_id: {cic_id}')
        raise
    if not start_date:
        logger.error(f'Invalid start_date: {start_date}')
        raise
    if not end_date:
        logger.error(f'Invalid end_date: {end_date}')
        raise
    
    # Extract data
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # add 20 min to end_date to capture last minutes of the day
        end_date_extract = end_date + timedelta(minutes=20)

        # extract data from s3
        extract_df = extract_data_from_s3(cic_id, start_date, end_date_extract, aws_profile)
    except Exception as e:
        logger.exception(f'Could not extract data from s3: {e}')
        raise

    # check if data is empty
    if extract_df.empty:
        logger.info(f'No data found for cic_id: {cic_id}')
        return
    else:
        # de-duplicate dataframe
        extract_df = extract_df.drop_duplicates(subset=['time.ts'], keep='first')

    # calculate and aggregate data
    try:
        aggregated_data, df = calculate_and_aggregate(extract_df)
    except Exception as e:
        logger.exception(f'Could not calculate and aggregate data: {e}')
        raise

    # keep only rows of within data range
    aggregated_data = aggregated_data.loc[start_date+timedelta(hours=1):end_date]

    # push data to mysql
    try:
        push_data_to_mysql(aggregated_data)
    except Exception as e:
        logger.exception(f'Could not push data to mysql: {e}')
        raise
    
    logger.info(f'Finished successfully for cic_id: {cic_id}')

def lambda_handler(event, context):
    if event:
        batch_item_failures = []
        sqs_batch_response = {}
        logger.debug(f"Received event: {event}")
        for record in event["Records"]:
            try:
                logger.debug(f"Processing record: {record}")
                attributes = record["messageAttributes"]

                cic_id = attributes['cic_id']['stringValue']
                start_date = attributes['start_date']['stringValue']
                end_date = attributes['end_date']['stringValue']

                main(cic_id, start_date, end_date)

            except Exception as e:
                batch_item_failures.append({"itemIdentifier": record['messageId']})
        
        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response

if __name__ == "__main__":

    # test data
    cic_id = "CIC-6e3d2c85-f792-5a06-afc0-a7525487fa4f"
    start_date = "2023-03-24"
    end_date = "2023-03-25"

    aws_profile = 'nout_prod'

    main(cic_id, start_date, end_date, aws_profile)