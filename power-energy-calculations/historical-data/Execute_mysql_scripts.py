'''
This script executes a set of sql queries to keep energy_data database up to date.
'''

import os
import sys
import logging
from urllib.parse import urlparse
import pymysql.cursors

# set up logging
logging.basicConfig(filename="std.log",
                    format='%(asctime)s %(levelname)s %(name)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filemode='a')

logger=logging.getLogger()
logger.setLevel(logging.INFO)

# read mysql url from environment variable
MYSQL_URL = os.environ['MYSQLDEV']

def parse_sql(sql_file_path):
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        data = f.read().splitlines()
    stmt = ''
    stmts = []
    for line in data:
        if line:
            if line.startswith('--'):
                continue
            stmt += line.strip() + ' '
            if ';' in stmt:
                stmts.append(stmt.strip())
                stmt = ''
    return stmts


def execute_sql_script(mysql_url, script_name):
    parsed_mysql_url = urlparse(mysql_url)
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
                sql_statements = parse_sql(script_name)
                for sql_statement in sql_statements:
                    cursor.execute(sql_statement)
                logger.info(f"MySQL script: {script_name} executed successfully.")
            except pymysql.Error as err:
                logger.error("Error while executing script {err}", exc_info=True)
                sys.exit(1)
    except:
        logger.error("Error while connecting to MySQL", exc_info=True)
        sys.exit(1)
    finally:
        cursor.close()
        connection.close()

def main(sql_scripts):
    
    for script in sql_scripts:
        logger.info(f"Executing script: {script}")
        execute_sql_script(MYSQL_URL, script)
    
    logger.info("All scripts executed successfully.")
    

if __name__ == '__main__':
    # testing scripts
    testing = True
    if testing:  
        sql_scripts = ['./power-energy-calculations/historical-data/sql/calculate_counters_offset_test.sql',
                    './power-energy-calculations/historical-data/sql/calculate_new_counters_test.sql']
    else:
        sql_scripts = ['./sql/calculate_counters_offset.sql',
                    './sql/calculate_new_counters.sql']
        
    main(sql_scripts)