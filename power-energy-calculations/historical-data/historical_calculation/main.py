# Calculate historical data for set of cic dates

import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd

from slack_sdk import WebClient

import Aggregate_data_hourly

logger=logging.getLogger()

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

# # load .env file
try:
    env_path = os.path.join(__location__, '.env')
    load_dotenv(env_path)
except Exception as e:
    logger.critical(f'Could not load .env or MYSQLDEV url not in .env file, {e}')

# set variables
SLACK_TOKEN = os.getenv('SLACK_TOKEN')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL')
FINAL_DATE = datetime(2023,7,10)
finished = 0
i = 0

# load csv file in pandas dataframe
df = pd.read_csv(os.path.join(__location__, 'cic_start_dates.csv'), header=0)

total_days = (FINAL_DATE -pd.to_datetime(df['first_date'])).dt.days.sum()

while not finished:

    # read cic and start_date from csv
    cic_id = df.iloc[i, 0]
    start_date = df.iloc[i, 1]

    # run script Aggregate_data_hourly.py with cic_id and start_date as input
    sdate = datetime.strptime(start_date, '%Y-%m-%d')
    edate = sdate + timedelta(days=1)

    while edate <= FINAL_DATE:
        start_date = sdate.strftime('%Y-%m-%d')
        end_date = edate.strftime('%Y-%m-%d')
        try:
            Aggregate_data_hourly.main(cic_id, start_date, end_date)
        except Exception as e:
            logger.error(f'Error in Aggregate_data_hourly.py, {e}')
            # read last 10 lines of std.log
            with open(os.path.join(__location__, 'std.log'), 'r') as f:
                lines = f.readlines()
                last_lines = lines[-10:]
                last_lines = ''.join(last_lines)
                # send last 10 lines of std.log to slack
                client = WebClient(token=SLACK_TOKEN)
                slack_msg = f'Error in Aggregate_data_hourly.py, {e}\n{last_lines}'
                client.chat_postMessage(channel=SLACK_CHANNEL, text=slack_msg)
            # exit script
            exit()
        
        sdate = edate
        edate += timedelta(days=1)

    # calculate % of calculations done:
    days_done = (FINAL_DATE - pd.to_datetime(df.iloc[:i,1])).dt.days.sum()
    percent_done = round(days_done/total_days*100, 2)

    # send slack message with % done
    client = WebClient(token=SLACK_TOKEN)
    slack_msg = f'Finished calculation for {cic_id}. \n {percent_done}% done of total.'
    client.chat_postMessage(channel=SLACK_CHANNEL, text=slack_msg)

    i += 1