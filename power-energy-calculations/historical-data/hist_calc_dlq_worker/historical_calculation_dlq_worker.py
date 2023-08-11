'''
Open each incoming message in DLQ.
Set data_missing flag to true for that cic_id in _cicsWithSoftwareVersion208 table.
Sent Slack message with contents of DLQ message, to track failed cics and dates.
'''

import logging
from urllib.parse import urlparse
import os
import pymysql.cursors

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# set up logging
logging.basicConfig(filename="log.log",
                    format='%(asctime)s %(levelname)s %(name)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='a')

logger=logging.getLogger()
logger.setLevel(logging.INFO)

# Settings
TESTING = True

# load environment variables
MYSQL_URL = os.environ["MYSQL_URL"]
SLACK_TOKEN = os.environ["SLACK_TOKEN"]
SLACK_CHANNEL = os.environ["SLACK_CHANNEL"]


def lambda_handler(event, context):
    if event:
        batch_item_failures = []
        sqs_batch_response = {}
        logger.debug(f"Received event: {event}")
        for record in event["Records"]:
            try:
                logger.debug(f"Processing record: {record}")
                attributes = record["messageAttributes"]

                cic_id = attributes['cic_id']['StringValue']
                start_date = attributes['start_date']['StringValue']
                end_date = attributes['end_date']['StringValue']

                main(cic_id, start_date, end_date)

            except Exception as e:
                batch_item_failures.append({"itemIdentifier": record['messageId']})
        
        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response
    

def main(cic_id, start_date, end_date):

    logger.info("Processing DLQ message...")

    # set up slack client
    try:
        client = WebClient(token=SLACK_TOKEN)
        report_missing_data_to_slack(client, cic_id, start_date, end_date)
    except SlackApiError as e:
        logger.error(f"Error while connecting to Slack: {e.response['error']}")
    except:
        logger.error("Error while connecting to Slack", exc_info=True)
    
    # set data_missing flag to true in MySQL
    try:
        set_data_missing_flag_in_mysql(cic_id)
    except:
        logger.error("Error while setting data_missing flag in MySQL", exc_info=True)

    logger.info("Finished processing DLQ message.")


def set_data_missing_flag_in_mysql(cic_id):
    parsed_mysql_url = urlparse(MYSQL_URL)
    try:
        connection = pymysql.connect(host=parsed_mysql_url.hostname,
                                    user=parsed_mysql_url.username,
                                    password=parsed_mysql_url.password,
                                    database=parsed_mysql_url.path[1:],
                                    port=parsed_mysql_url.port,
                                    autocommit=True)
        cursor = connection.cursor()
        if cursor.connection:
            try:
                sql_statement = f"UPDATE _cicsWithSoftwareVersion208 SET data_missing = 1 WHERE cic_id = '{cic_id}';"
                cursor.execute(sql_statement)
            except pymysql.Error as err:
                logger.error("Error while executing script {err}", exc_info=True)
    except:
        logger.error("Error while connecting to MySQL", exc_info=True)
    finally:
        cursor.close()
        connection.close()

def report_missing_data_to_slack(client, cic_id, start_date, end_date):
    slack_msg = f"Historical data calculation failed for: {cic_id} {start_date} - {end_date}."
    if TESTING:
        slack_msg = "[TEST] " + slack_msg
    client.chat_postMessage(channel=SLACK_CHANNEL, text=slack_msg)