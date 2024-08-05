import pandas as pd


def test3Query(cursor):
    cursor.execute("""
    WITH CTE AS
    (
        SELECT
            timestamp,
            installationId,
            hpHeat,
            hpElectric,
            boilerHeat,
            LAG(hpHeat) OVER (PARTITION BY installationId ORDER BY timestamp) AS hpHeat_prev_day,
            LAG(hpElectric) OVER (PARTITION BY installationId ORDER BY timestamp) AS hpElectric_prev_day,
            LAG(boilerHeat) OVER (PARTITION BY installationId ORDER BY timestamp) AS boilerHeat_prev_day 
        FROM
            energyConsumption
        WHERE
            timestamp BETWEEN '2024-07-01 00:00:00' AND '2024-07-01 23:59:59'
    ),
    CTE2 AS
    (
    SELECT
        installationId AS distinctInstallationID,
        timestamp,
        hpHeat,
        hpElectric,
        boilerHeat,
        hpHeat_prev_day,
        hpElectric_prev_day,
        boilerHeat_prev_day
    FROM
        CTE
    WHERE
        hpHeat > hpHeat_prev_day
    GROUP BY
        installationId
    )
    SELECT
        CTE2.distinctInstallationID,
        cic.id,
        CTE2.hpHeat,
        CTE2.hpHeat_prev_day,
        CTE2.hpHeat - CTE2.hpHeat_prev_day AS hpHeat_diff,
        CTE2.hpElectric,
        CTE2.hpElectric_prev_day,
        CTE2.hpElectric - CTE2.hpElectric_prev_day AS hpElectric_diff,
        CTE2.boilerHeat,
        CTE2.boilerHeat_prev_day,
        CTE2.boilerHeat - CTE2.boilerHeat_prev_day AS boilerHeat_diff
    FROM 
        CTE2
    JOIN
        cic
    ON
        CTE2.distinctInstallationID=cic.installationId
    """)


def test2Query(cursor):
    cursor.execute("""
    SELECT
        timestamp,
        installationId,
        hpHeat,
        hpElectric,
        boilerHeat,
        LAG(hpHeat) OVER (PARTITION BY installationId ORDER BY timestamp) AS hpHeat_prev_day,
        LAG(hpElectric) OVER (PARTITION BY installationId ORDER BY timestamp) AS hpElectric_prev_day,
        LAG(boilerHeat) OVER (PARTITION BY installationId ORDER BY timestamp) AS boilerHeat_prev_day 
    FROM
        energyConsumption
    WHERE
        timestamp BETWEEN '2024-07-01 00:00:00' AND '2024-07-01 23:59:59'
    """)


def check(cursor):
    cursor.execute("""
    SELECT
        id, 
        installationId
    FROM 
        cic
    WHERE
        id='CIC-f8a7b152-ab05-5a0f-a2c6-3dcef31be463'

    """)

  
def testQuery(cursor):

    ## cic Table: 9625 rows x 9 columns (Update on 16.07.2024)
    # 9 columns: ['id', 'createdAt', 'updatedAt', 'availableWifiNetworks', 'isScanningForWifi', 'lastScannedForWifi', 'wifiConnectionStatus', 'installationId', 'menderId']
    # "id": CIC id. Distinct count: 9625
    # "createdAt": record created date time 
    # "updatedAt": record updated date time
    # "availableWifiNetworks": show all the available Wifi Networks for this CIC id device.
    # "isScanningForWifi": Bool
    # "lastScannedForWifi": last date time for scanning the WIFI
    # "wifiConnectionStatus": A str variable, "connected" or "disconnected"
    # "installationId": continuous 1-8352, then 10041-48326 with many gaps. Distinct count: 9574   
    # "menderId": Most are "None", 19 out of 9625 have menderId. A example, CIC id:CIC-1106aaaa-a7ca-54cb-8da4-16eca3794e68, menderId:4d38b6fb-5663-4eff-982a-ef0472640180, installationId:4049 

    cursor.execute("SELECT * FROM cic;") # [9625 rows x 9 columns], Runing Time: 1.3 s
    #cursor.execute("SELECT COUNT(DISTINCT installationId) FROM cic;")
    #cursor.execute("SELECT COUNT(DISTINCT installationId) FROM energyConsumption")

    ## energyConsumption Table: 47484777 rows x 10 columns (Update on 16.07.2024)
    # 10 columns: ['id', 'installationId', 'hpElectric', 'hpHeat', 'boilerHeat', 'timestamp', 'roomSetpoint', 'roomTemperature', 'temperatureOutside', 'waterTemperature']
    # 'id': Distinct count: 47484777 
    # 'installationId': Distinct count:7954
    # 'hpElectric':  Distinct count:4304303  
    # 'hpHeat':
    # 'boilerHeat':
    # 'timestamp': datetime format: year-month-day hour:minutes:second
    # 'roomSetpoint':
    # 'roomTemperature':
    # 'temperatureOutside':
    # 'waterTemperature':
   
    #cursor.execute("SELECT COUNT(*) FROM energyConsumption") # Count: 47484777, Runing Time: 2 mins
    #cursor.execute("SELECT COUNT(DISTINCT installationId) FROM energyConsumption") # COUNT:7954, Runing Time: 8 s
    #cursor.execute("SELECT COUNT(DISTINCT id) FROM energyConsumption") # COUNT:47484777, Runing Time: 100 s
    #cursor.execute("SELECT * FROM energyConsumption LIMIT 200;")


def ListDataBases(cursor):
    # DataBases: 'information_schema', 'mysql', 'performance_schema', 'quatt_production', 'sys' 
    cursor.execute("SHOW DATABASES")
    print("SHOW DATABASES:")
    for x in cursor:
        print(x)
    print("\n")


def ListTables(cursor):
    # 18 Tables in 'quatt_production': 
    # '_prisma_migrations','cic','cicCommissioning','cicRegistration','cicState',
    # 'energyConsumption','heatPump','heatPumpCommissioning','installation',
    # 'installationNote','installationTariff','installer','settingsUpdate','user',
    # 'userCic','userCicPairRequest','userClient','zipCodesWithEarlyNightPricing',
    cursor.execute("SHOW TABLES")
    print("SHOW TABLES:")
    for x in cursor:
        print(x)
    print("\n")


def MartijnCO2Calculation(cursor):
    cursor.execute("""
    SELECT 
        DATE(timestamp) as Date,
        COUNT(DISTINCT installationId) as ActiveInstallations,
        SUM(hpHeat - hpHeat_prev_day) AS Total_hpHeat_diff, 
        SUM(hpElectric - hpElectric_prev_day) AS Total_hpElectric_diff,
        SUM(boilerHeat - boilerHeat_prev_day) AS Total_boilerHeat_diff, -- Added total boilerHeat difference
        (SUM(hpHeat - hpHeat_prev_day) / NULLIF(SUM(hpElectric - hpElectric_prev_day), 0)) AS Total_COP,
        (SUM(hpHeat - hpHeat_prev_day) / 8792.5) AS Savings_Gas,    -- (m^3)
        ((SUM(hpHeat - hpHeat_prev_day) / 8792.5) * 1.78) AS CO2_Gas_Saved,  -- (kg)
        ((SUM(hpElectric - hpElectric_prev_day) / 1000) * 0.22) AS CO2_Electricity,  -- (kg)
        (((SUM(hpHeat - hpHeat_prev_day) / 8792.5) * 1.78) - ((SUM(hpElectric - hpElectric_prev_day) / 1000) * 0.22)) AS Savings_CO2, -- (kg)
        (((SUM(hpHeat - hpHeat_prev_day) / 8792.5) * 1.78) - ((SUM(hpElectric - hpElectric_prev_day) / 1000) * 0.22)) / COUNT(DISTINCT installationId) AS Savings_CO2_perCIC -- (kg)
    FROM (
        SELECT 
            timestamp,
            installationId,
            hpHeat,
            hpElectric,
            boilerHeat,
            LAG(hpHeat) OVER (PARTITION BY installationId ORDER BY timestamp) AS hpHeat_prev_day,
            LAG(hpElectric) OVER (PARTITION BY installationId ORDER BY timestamp) AS hpElectric_prev_day,
            LAG(boilerHeat) OVER (PARTITION BY installationId ORDER BY timestamp) AS boilerHeat_prev_day -- Added previous day's boilerHeat
        FROM 
            energyConsumption
        WHERE
            timestamp BETWEEN '2024-07-01' AND '2024-07-03'
            -- AND installationId = 8260
            -- AND DAY(timestamp) = 01
            -- AND MONTH(timestamp) >= 01
            -- AND MONTH(timestamp) <= 04 
            -- AND YEAR(timestamp)=2024
    ) AS subquery
    WHERE 
        hpHeat > hpHeat_prev_day
    GROUP BY
        Date
    ORDER BY Date;
    """) # Running time: 7.3 mins

def check2(cursor):
    cursor.execute("""
    SELECT
        installationId
    FROM
        cic
    GROUP BY 
        installationId
    HAVING 
        COUNT(DISTINCT id) > 1
    """)

def check3(cursor):
    cursor.execute("""
    SELECT
        COUNT( DISTINCT installationId )
    FROM
        cic
    """)

def check4(cursor):
    cursor.execute("""
    SELECT
        timestamp,
        installationId,
        hpHeat
    FROM
        energyConsumption
    WHERE
         installationId='1228'
    """)

def MartijnCO2Calculation_Revised(cursor):
    cursor.execute("""

    SELECT 
        DATE(timestamp) as Date,
        COUNT(DISTINCT installationId) as ActiveInstallations,
        SUM(hpHeat - hpHeat_prev_day) AS Total_hpHeat_diff, 
        SUM(hpElectric - hpElectric_prev_day) AS Total_hpElectric_diff,
        SUM(boilerHeat - boilerHeat_prev_day) AS Total_boilerHeat_diff, -- Added total boilerHeat difference
        (SUM(hpHeat - hpHeat_prev_day) / NULLIF(SUM(hpElectric - hpElectric_prev_day), 0)) AS Total_COP,
        (SUM(hpHeat - hpHeat_prev_day) / 8792.5) AS Savings_Gas,    -- (m^3)
        ((SUM(hpHeat - hpHeat_prev_day) / 8792.5) * 1.78) AS CO2_Gas_Saved,  -- (kg)
        ((SUM(hpElectric - hpElectric_prev_day) / 1000) * 0.22) AS CO2_Electricity,  -- (kg)
        (((SUM(hpHeat - hpHeat_prev_day) / 8792.5) * 1.78) - ((SUM(hpElectric - hpElectric_prev_day) / 1000) * 0.22)) AS Savings_CO2, -- (kg)
        (((SUM(hpHeat - hpHeat_prev_day) / 8792.5) * 1.78) - ((SUM(hpElectric - hpElectric_prev_day) / 1000) * 0.22)) / COUNT(DISTINCT installationId) AS Savings_CO2_perCIC -- (kg)
    FROM (

        WITH MultipleCicIds AS (
            SELECT installationId
            FROM cic
            GROUP BY installationId
            HAVING COUNT(DISTINCT id) > 1
        )

        SELECT 
            timestamp,
            installationId,
            hpHeat,
            hpElectric,
            boilerHeat,
            LAG(hpHeat) OVER (PARTITION BY installationId ORDER BY timestamp) AS hpHeat_prev_day,
            LAG(hpElectric) OVER (PARTITION BY installationId ORDER BY timestamp) AS hpElectric_prev_day,
            LAG(boilerHeat) OVER (PARTITION BY installationId ORDER BY timestamp) AS boilerHeat_prev_day -- Added previous day's boilerHeat
        FROM 
            energyConsumption
        WHERE
            timestamp BETWEEN '2023-04-04' AND '2023-04-10'
            AND installationId NOT IN (SELECT installationId FROM MultipleCicIds)
    ) AS subquery
    WHERE 
        hpHeat > hpHeat_prev_day
    GROUP BY
        Date
    ORDER BY Date;
    """) # Running time: 7.3 mins



