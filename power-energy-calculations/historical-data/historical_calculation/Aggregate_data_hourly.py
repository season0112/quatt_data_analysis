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
from dotenv import load_dotenv
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

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

# # load .env file
try:
    env_path = os.path.join(__location__, '.env')
    load_dotenv(env_path)
    # MySQL connection
    MYSQL_URL = os.getenv('MYSQLDEV')
except Exception as e:
    logger.critical(f'Could not load .env or MYSQLDEV url not in .env file, {e}')

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

    # preprocessed from cic stats
    {'sql':'hp1_energy_consumed', 'src':'hp1.energyConsumption', 'func':'sum'},
    {'sql':'hp2_energy_consumed', 'src':'hp2.energyConsumption', 'func':'sum'},
    {'sql':'hp1_data_availability', 'src':'hp1_data_availability', 'func':'max'},
    {'sql':'hp2_data_availability', 'src':'hp2_data_availability', 'func':'max'},
    {'sql':'hp1_heat_generated', 'src':'hp1.heatGenerated', 'func':'sum'},
    {'sql':'hp2_heat_generated', 'src':'hp2.heatGenerated', 'func':'sum'},
    {'sql':'boiler_heat_generated', 'src':'cvHeatGenerated', 'func':'sum'},
    {'sql':'hp1_active', 'src':'hp1.active', 'func':'mean'},
    {'sql':'hp2_active', 'src':'hp2.active', 'func':'mean'},
    {'sql':'boiler_active', 'src':'cvActive', 'func':'mean'}
]

# properties to download from S3
S3_PROPERTIES = {
         'time': ['ts'],
         'system': ['quattId',
                    'quattBuild'],
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
                 'defrostFlag',
                 'electricalEnergyCounter',
                 'thermalEnergyCounter'],
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
                 'power',
                 'electricalEnergyCounter',
                 'thermalEnergyCounter']}

# Integration keys
INTEGRATION_KEYS = {'hp1.energyConsumption':'hp1.powerConsumption',
                    'hp2.energyConsumption':'hp2.powerConsumption',
                    'hp1.heatGenerated':'hp1.powerOutput',
                    'hp2.heatGenerated':'hp2.powerOutput',
                    'cvHeatGenerated':'cv_power_output'}


# load linear model with pickle
try:
    filename = os.path.join(__location__, 'energy-power-standard-model.pkl')
    with open(filename, 'rb') as f:
        LINEAR_MODEL = pickle.load(f)
except Exception as e:
    logger.critical(f'Could not load linear model {e}')
    sys.exit()


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

    # replace power input
    if hp=='hp1':
        df['hp1.powerInput'] = (
            df['hp1.powerInput'].fillna(df['qc.hp1PowerInput']))
        
    # fill missing old counters
    df['qc.hp1ElectricalEnergyCounter'] = (
        df['qc.hp1ElectricalEnergyCounter'].fillna(df['hp1.electricalEnergyCounter']))
    df['qc.hp2ElectricalEnergyCounter'] = (
        df['qc.hp2ElectricalEnergyCounter'].fillna(df['hp2.electricalEnergyCounter']))
    df['qc.hp1ThermalEnergyCounter'] = (
        df['qc.hp1ThermalEnergyCounter'].fillna(df['hp1.thermalEnergyCounter']))
    df['qc.hp2ThermalEnergyCounter'] = (
        df['qc.hp2ThermalEnergyCounter'].fillna(df['hp2.thermalEnergyCounter']))
    
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

def estimate_energy_consumption(modelInput, 
                                circulatingPumpDutyCycle, 
                                circulatingPumpRelay, 
                                crankcaseHeater):
    return (LINEAR_MODEL.predict(modelInput)
            + (circulatingPumpDutyCycle * circulatingPumpRelay)
            + (crankcaseHeater * 40))

def integrate_data(df, keys):
    df['timediff[S]'] = df.groupby('system.quattId',
                                   sort='time.ts')['time.ts'].diff()/1000

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
    if ('time.ts' not in df.columns) | ('system.quattId' not in df.columns):
        logger.error('time.ts or system.quattId not in dataframe')
        raise ValueError('time.ts or system.quattId not in dataframe')
    
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
    if (df['hp2.powerInput'].notna().any()):
        heat_pumps = ['hp1', 'hp2'] 
    else:
        heat_pumps = ['hp1']

    for hp in heat_pumps:
        # get data availability and parameters for power calculation
        df = prepare_data_for_calculation(df, hp)

        # calculate powerConsumption for all rows
        df[f'{hp}.powerConsumption'] = (
            estimate_energy_consumption(
                df[[f'{hp}.powerInput',
                    f'{hp}.getFanSpeed',
                    f'{hp}.bottomPlateHeaterEnable']].fillna(0).values,
                df[f'{hp}.circulatingPumpDutyCycle'].values,
                df[f'{hp}.getCirculatingPumpRelay'].values,
                df[f'{hp}.compressorCrankcaseHeaterEnable'].values)
        )

        # set all values of powerconsumption to zero where supervisoryControlMode is NaN
        df.loc[df['qc.supervisoryControlMode'].isna(), 
                    f'{hp}.powerConsumption'] = 0
        
        # get activity of heatpump
        df[f'{hp}.active'] = df['qc.supervisoryControlMode'].isin([2,3]).astype(float)

    # get hp1 power output
    df['hp1.powerOutput'] = (
        df['hp1.powerOutput'].fillna(df['qc.hp1PowerOutput'])
    )

    # get cv power output
    df['cv_power_output'] = df['qc.cvPowerOutput']
    df.loc[df['cv_power_output'] < 0, 'cv_power_output'] = 0
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
            logger.info(f"skipping row {index} because it contains NaN values")
            continue
        query = make_insert_row_query(index, row)
        cursor.execute(query)
        connection.commit()

    # close connection
    cursor.close()
    connection.close()

def extract_data_from_s3(cic_id, start_date, end_date):
    # create s3 clients
    quatt_s3_client_pull = s3.create_s3_client(aws_profile="nout_prod")
    
    # add 20 min to end_date
    extract_df = quatt_s3_client_pull.get_cic_data(cic_ids=cic_id, 
                                                    start_date=start_date,
                                                    end_date=end_date, 
                                                    filter_properties=S3_PROPERTIES,
                                                    cloud_type='production' #production is hardcoded for now
                                                    )
    return extract_df

def main(cic_id, start_date, end_date):
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
        extract_df = extract_data_from_s3(cic_id, start_date, end_date_extract)
    except Exception as e:
        logger.error(f'Could not extract data from s3: {e}')
        raise
    
    # check if data is empty
    if extract_df.empty:
        logger.info(f'No data found for cic_id: {cic_id}')
        return
    else:
        # de-duplicate dataframe
        extract_df = extract_df[~extract_df.index.duplicated(keep='first')]

    # calculate and aggregate data
    try:
        aggregated_data, df = calculate_and_aggregate(extract_df)
    except Exception as e:
        logger.error(f'Could not calculate and aggregate data: {e}')
        raise

    # keep only rows of within data range
    aggregated_data = aggregated_data.loc[start_date+timedelta(hours=1):end_date]

    # push data to mysql
    try:
        push_data_to_mysql(aggregated_data)
    except Exception as e:
        logger.error(f'Could not push data to mysql: {e}')
        raise
    
    logger.info(f'Finished successfully for cic_id: {cic_id}')

if __name__ == "__main__":
    # cic_id = sys.argv[1]
    # start_date = sys.argv[2]
    # end_date = sys.argv[3]

    cic_id = "CIC-691989e5-24b7-59b9-9807-537c89035f25"
    start_date = "2023-03-08"
    end_date = "2023-03-09"

    main(cic_id, start_date, end_date)