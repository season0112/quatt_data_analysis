import clickhouse_connect
from .loadClickHouseCredentials import clickhouse_host, clickhouse_user, clickhouse_password, clickhouse_port
from . import Utility_ClickHouse
from . import NoutKPIQuery, CommonQuery, CO2Query
import pandas as pd

def connect_clickhouse(arguments):

    client = clickhouse_connect.get_client(
        host     = clickhouse_host,
        user     = clickhouse_user,
        password = clickhouse_password,
        port     = clickhouse_port,
        secure   = True
    )

    # Execute Query
    #start_date = '2024-06-01 00:00:00'
    start_date = '2023-04-01 00:00:00'
    #start_date = '2022-11-01 00:00:00'
    #end_date   = '2023-04-08 00:00:00'
    end_date   = '2023-04-04 00:00:00'

    data_range = pd.date_range(start=start_date, end=end_date, freq='D')

    df_all = pd.DataFrame()

    for index in range(len(data_range)-1):
        print(data_range[index])
  
        if arguments.query == None:
            query, params = CommonQuery.query_define(arguments.extractVariables,
                                        arguments.table,
                                        arguments.clientid,
                                        arguments.startTime,
                                        arguments.endTime)
            df = client.query_df(query, params)
        else:
            #df = client.query_df(getattr(CommonQuery, arguments.query)())
            #df = client.query_df(getattr(NoutKPIQuery, arguments.query)(StartTime='2024-01-01 00:00:00', EndTime='2024-01-01 23:59:59', Q_min=100))
            df = client.query_df(getattr(CO2Query, arguments.query)(StartTime=data_range[index], EndTime=data_range[index+1], Q_min=0))

        #print(df)
        df_all = pd.concat([df_all, df], axis=0)    

    #print(df_all)        

    return df_all, start_date, end_date 


