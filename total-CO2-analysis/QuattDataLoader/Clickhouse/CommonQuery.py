def query_define(extractVariables, table, clientid, startTime, endTime):

    columns = ', '.join(extractVariables)
    base_query = f"""
        SELECT
            {columns}
        FROM
            {table}
        WHERE
            clientid = %(clientid)s
    """

    if startTime is not None and endTime is not None:
        base_query += """
            AND time_ts BETWEEN %(startTime)s AND %(endTime)s
        """
 
    params = {
        'clientid'        : clientid,
        'startTime'       : startTime,
        'endTime'         : endTime
    }
 
    return base_query, params


def test_query():
    return f'''
    SELECT
        DISTINCT (qc_supervisoryControlMode)
    FROM
        cic_stats
    '''


    """
    return f'''
    SELECT
        COUNT(*)
    FROM
        cic_stats   
    '''
    """

def ListDataBases():
    # DataBases: "default" and "system" 
    return f'''
    SHOW DATABASES
    '''

def ListTables():
    # Tables: 
    # 24 tables in "default"
    # "cic_connection_status", "cic_counters_temporary", "cic_energy_values_temporary"
    # "cic_installation", "cic_stats", "external_meter_data", "external_meter_data_complete"
    # "house_stats", "installation_counters", "installation_counters_interpolated"
    # "installation_daily_summary", "installation_energy_consumption", "mysql_cic"
    # "mysql_cic_commissioning", "mysql_cic_prod", "mysql_cic_state"
    # "mysql_energy_consumption", "mysql_installation", "s3_cic_installation_connections"
    # "s3_hubspot_deal_data", "temp_cic_stats_data_coverage", "temp_commissioning_dates"
    # "temperatures_backfill_dump", "test_energy_consumption"

    # 'cic_stats' has shape of (11351327320 × 360), namely 11351327320 records on 360 properties of CIC collected data. (Updated on 22.07.2024)

    return f'''
    SHOW TABLES
    '''


