#! /usr/bin/env python3

# Example Useage: python LoadClickHouseData.py --extractVariables qc_minimumCOP --table cic_stats --clientid CIC-87845a22-3d04-5f8c-8d4e-c1e735a9e472 --startTime "2024-01-02 00:00:00" --endTime "2024-01-02 20:00:00"

import argparse
from clickhouse_driver import Client
from datetime import datetime
import pandas as pd
from termcolor import colored
import time
import Utility_LoadClickHouse


if __name__ == '__main__':

    # Save the query result to CSV
    Utility_LoadClickHouse.SaveQueryResult(df_sorted, arguments.extractVariables, arguments.startTime, arguments.endTime, arguments.clientid, params)


