import mysql.connector
from mysql.connector import Error
from .loadMysqlCredentials import mysql_host, mysql_user, mysql_password, mysql_port, mysql_database
from . import CommonQuery
import pandas as pd

def connect_mysql(arguments):

    try:
        connection = mysql.connector.connect(host     = mysql_host,
                                             user     = mysql_user,
                                             password = mysql_password,
                                             database = mysql_database,
                                             port     = mysql_port)

        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()

            #CommonQuery.ListDataBases(cursor)
            #CommonQuery.ListTables(cursor)

            #CommonQuery.MartijnCO2Calculation(cursor) 
            CommonQuery.MartijnCO2Calculation_Revised(cursor)

            #CommonQuery.check(cursor) 
            #CommonQuery.check2(cursor)
            #CommonQuery.check4(cursor)

            #CommonQuery.testQuery(cursor)
            #CommonQuery.test2Query(cursor)
            #CommonQuery.test3Query(cursor)

            fields = [field_md[0] for field_md in cursor.description]
            result = [dict(zip(fields,row)) for row in cursor.fetchall()]
            df = pd.DataFrame(result)

            #print(fields)
            #print("\n")
            #pd.set_option('display.max_colwidth', None)
            #print(df)            

            return df

    except Error as e:
            print("error out.")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


