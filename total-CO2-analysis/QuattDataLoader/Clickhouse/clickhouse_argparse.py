import argparse

def parse_clickhouse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--extractVariables', nargs='+', help='The variables you want to extract from the database, used in "SELECT    ".', default=['*'])
    parser.add_argument('--table', help='The table you want to extract variables.', default='cic_stats')
    parser.add_argument('--clientid', help='Clientid is cic-UUID', default=None)
    parser.add_argument('--startTime', help='The start of time period for the data taking.', default=None)
    parser.add_argument('--endTime', help='The end of time period for the data taking.', default=None)

    parser.add_argument('--query', help='Another option to extract data, directly from written query', default=None)

    return parser





