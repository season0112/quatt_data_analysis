'''
This file will get a list of cic_ids from production redis and then send this
list to SQS for processing by Lambda.
'''

from urllib.parse import urlparse
import os
import json
import logging
import datetime

import boto3
import redis

# set up logging
logging.basicConfig(filename="log.log",
                    format='%(asctime)s %(levelname)s %(name)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='a')

logger=logging.getLogger()
logger.setLevel(logging.INFO)

# get environment variables
REDIS_URL = os.environ["REDISPROD"]

# to get list of cics from redis
def get_cic_stats_from_redis(redis_url):
    parsed_url = urlparse(redis_url)
    r = redis.Redis(host=parsed_url.hostname,
                    port=parsed_url.port,
                    db=0,
                    password=parsed_url.password,
                    username=parsed_url.username)
    
    # get objects from redis
    redis_objects = r.mget(r.keys(pattern="cic:*CIC*lastStat*"))
    results = []
    for obj, key in zip(redis_objects, r.keys(pattern="cic:*CIC*lastStat*")):
        try:
            data = json.loads(obj.decode())
            data['cic'] = key.decode().split(':')[1]
            results.append(data)
        except:
            pass
    return results

# QUEUE messsages for SQS
def sent_message_to_sqs(sqs, cic_id, date):
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        DelaySeconds=0,
        MessageAttributes={
            'cic_id': {
                'DataType': 'String',
                'StringValue': cic_id},
            'start_date': {
                'DataType': 'String',
                'StringValue': date.strftime('%Y-%m-%d')},
            'end_date': {
                'DataType': 'String',
                'StringValue': (date+datetime.timedelta(days=1)).strftime('%Y-%m-%d')}
        },
        MessageBody=(
            f'Message input: {cic_id}, {date.strftime("%Y-%m-%d")} - {(date+datetime.timedelta(days=1)).strftime("%Y-%m-%d")}'
        )
    )

    logger.debug(response.get('MessageId'))
    logger.debug(response.get('MD5OfMessageBody'))
    return response

# main function
def main():
    # set-up boto3 session
    session = boto3.Session(profile_name=PROFILE_NAME)
    sqs = session.client('sqs')

    # get dates
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

    # initialise counters
    message_count = 0
    error_count = 0
    
    
    redis_data = get_cic_stats_from_redis(REDIS_URL)

    for cic in redis_data:
        while message_count < 10:
            try:
                cic_id = cic['cic']

                response = sent_message_to_sqs(sqs, cic_id, yesterday)

                if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    logger.error(f"Error in sending message to SQS for {cic_id}, {yesterday}", 
                                exc_info=True)
                    error_count += 1
                else:
                    logger.debug(f"Message sent to SQS for {cic_id}, {yesterday}")
                    message_count += 1
            except:
                logger.warning("No cic_id found in redis_data", exc_info=True)
                error_count += 1

    logger.info(f"Finished sending {message_count} messages to SQS. {error_count} errors")


if __name__ == "__main__":
    QUEUE_URL = 'https://sqs.eu-west-1.amazonaws.com/621303523838/sqs_historic_calculation'
    PROFILE_NAME = "nout_prod"

    main()
