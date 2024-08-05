def NoutQuery_OverviewDashboard(**kwargs):

    return f'''
    WITH CTE AS
    (
      SELECT  
        clientid,
        max(hp1_thermalEnergyCounter) - min(hp1_thermalEnergyCounter) AS Q_hp1,
        max(hp1_electricalEnergyCounter) - min(hp1_electricalEnergyCounter) AS E_hp1,
        ifNull(max(hp2_thermalEnergyCounter) - min(hp2_thermalEnergyCounter), 0) AS Q_hp2,
        ifNull(max(hp2_electricalEnergyCounter) - min(hp2_electricalEnergyCounter), 0) AS E_hp2,
        max(qc_cvEnergyCounter) - min(qc_cvEnergyCounter) AS Q_cv
      FROM 
        "cic_stats"
      WHERE
        time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}' AND
        qc_supervisoryControlMode in (2,3,4)
      GROUP BY
        clientid
      HAVING
        (Q_hp1 + Q_cv) >=  {kwargs.get('Q_min')}*1000
    )
    SELECT
      COUNT(clientid) AS numberOfHeatpumps,
      SUM(Q_hp1)/SUM(E_hp1) AS fleetCOP,
      SUM(Q_hp1)/SUM(Q_hp1 + Q_cv)*100 AS avgPercentagHeatByHeatpump,
      ((SUM(Q_hp1) / 8.8 * 1.788) - (SUM(E_hp1) * 0.272)) / 1000 / COUNT(clientid) AS avgCo2Savings,
      ((SUM(Q_hp1) / 8.8 * 1.788) - (SUM(E_hp1) * 0.272)) / 1000 / 1000 / 0.949    AS totalCo2Savings,
      ((SUM(Q_hp1) / 8.8 * 1.46)  - (SUM(E_hp1) * 0.36))  / 1000 / COUNT(clientid) AS avgEuroSavings
    FROM
      CTE
    '''

def NoutQuery_AverageTrackingErrorDashboard(**kwargs):
    return f'''
    WITH CTE AS
    (
        SELECT
            clientid,
            avg(thermostat_otFtRoomSetpoint - thermostat_otFtRoomTemperature) AS roomError,
            (max(hp1_thermalEnergyCounter) - min(hp1_thermalEnergyCounter)) AS Q_hp1,
            (max(hp2_thermalEnergyCounter) - min(hp2_thermalEnergyCounter)) AS Q_hp2,
            (max(qc_cvEnergyCounter) - min(qc_cvEnergyCounter)) AS Q_cv,
            (max(qc_houseEnergyCounter) - min(qc_houseEnergyCounter)) AS Q_house
        FROM 
        "cic_stats"
        WHERE
            time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}' AND
            has([2, 3, 4], qc_supervisoryControlMode)
        GROUP BY clientid
        HAVING roomError BETWEEN -2 AND 5
            AND (Q_house) >= {kwargs.get('Q_min')}*1000
    )
    SELECT
        roomError
    FROM CTE    
    '''

def NoutQuery_PowerDeliveredDashboard(**kwargs):
    return f'''
    WITH CTE AS 
    (
        SELECT 
            clientid, 
            toStartOfHour(time_ts) AS time_start,
            avg(hp1_temperatureOutside) AS meanAmbientTemperature, 
            avg(flowMeter_waterSupplyTemperature) AS meanSupplyTemperature, 
            max(hp1_thermalEnergyCounter) - min(hp1_thermalEnergyCounter) AS Q_hp1,
            max(hp1_electricalEnergyCounter) - min(hp1_electricalEnergyCounter) AS E_hp1,
            max(qc_cvEnergyCounter) - min(qc_cvEnergyCounter) AS Q_cv,
            avg(qc_housePowerConsumed) as P_house
        FROM 
            "cic_stats"
        WHERE
            time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}' 
        GROUP BY
            clientid, time_start
        HAVING
            (Q_hp1 + Q_cv) >= 1000
            AND meanAmbientTemperature IS NOT NULL
            AND meanSupplyTemperature IS NOT NULL
            AND P_house > 0
            AND min(qc_supervisoryControlMode) >= 2 
            AND max(qc_supervisoryControlMode) <= 4
            AND max(flowMeter_waterSupplyTemperature) - min(flowMeter_waterSupplyTemperature) < 1
    ),
    CTE2 AS 
    (
        SELECT 
            clientid, 
            max(hp1_thermalEnergyCounter) - min(hp1_thermalEnergyCounter) AS Q_hp1,
            max(hp1_electricalEnergyCounter) - min(hp1_electricalEnergyCounter) AS E_hp1,
            Q_hp1 / E_hp1 AS periodCop,
            max(qc_houseEnergyCounter) - min(qc_houseEnergyCounter) AS Q_house
        FROM 
            "cic_stats"
        WHERE
            time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}'
        GROUP BY
            clientid
        HAVING
            Q_house >= 1000 * {kwargs.get('Q_min')}
    )
    SELECT
        CTE.clientid,
        CTE2.periodCop,
        toInt16(round(meanSupplyTemperature)) AS supplyTemperature,
        avg(P_house) AS powerDemandPerDegree
    FROM
        CTE inner join CTE2
        on CTE.clientid = CTE2.clientid
    WHERE 
        CTE.E_hp1 > 0
        AND CTE.Q_hp1 > 1000
        AND periodCop > 0
        AND periodCop < 10
    GROUP BY CTE.clientid, CTE2.periodCop, supplyTemperature
    HAVING
        supplyTemperature > 10
    ORDER BY
        CTE.clientid, supplyTemperature

    '''

def NoutQuery_HeatPercentbyHeatpump(**kwargs):
    return f'''WITH CTE as
    (
        SELECT
            clientid,
            MAX(hp1_thermalEnergyCounter) - MIN(hp1_thermalEnergyCounter) AS Q_hp1,
            MAX(hp1_electricalEnergyCounter) - MIN(hp1_electricalEnergyCounter) AS E_hp1,
            MAX(qc_cvEnergyCounter) - MIN(qc_cvEnergyCounter) AS Q_boiler,
            (MAX(qc_houseEnergyCounter) - MIN(qc_houseEnergyCounter))/1000 AS Q_house
        FROM
            cic_stats
        WHERE
            time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}'            
        GROUP BY
            clientid
        HAVING
            Q_hp1 IS NOT NULL
            AND E_hp1 IS NOT NULL
            AND Q_boiler IS NOT NULL
            AND (Q_hp1 + Q_boiler) >= {kwargs.get('Q_min')} * 1000
    )
    SELECT
        clientid,
        Q_hp1 / E_hp1 AS COP,
        Q_hp1 / (Q_hp1 + Q_boiler) * 100 AS heatPercentByHeatpump,
        Q_house,
    FROM
        CTE
    WHERE
        COP > 0
        AND COP < 20
    '''


def NoutQuery_COPvsSupplyTemperature(**kwargs):
    return f'''
    WITH CTE2 AS
    (
        WITH CTE AS 
        (
            SELECT 
                clientid, 
                toStartOfHour(time_ts) AS time_start,
                avg(hp1_temperatureOutside) AS meanAmbientTemperature, 
                avg(flowMeter_waterSupplyTemperature) AS meanSupplyTemperature, 
                max(hp1_thermalEnergyCounter) - min(hp1_thermalEnergyCounter) AS Q_hp1,
                max(hp1_electricalEnergyCounter) - min(hp1_electricalEnergyCounter) AS E_hp1,
                max(qc_cvEnergyCounter) - min(qc_cvEnergyCounter) as Q_cv 
            FROM 
                "cic_stats"
            WHERE
                time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}'
                AND has([2], qc_supervisoryControlMode)
            GROUP BY
                clientid, time_start
            HAVING
                (Q_hp1 + Q_cv) >= 1000
                AND meanAmbientTemperature IS NOT NULL
                AND meanSupplyTemperature IS NOT NULL
        )
        SELECT
            clientid,
            toInt16(round(meanSupplyTemperature)) as supplyTemperature,
            sum(Q_hp1)/sum(E_hp1) as copPerDegree
        FROM
            CTE
        WHERE 
            E_hp1 > 0
            AND Q_hp1 > 100
        GROUP BY clientid, supplyTemperature
        HAVING
            copPerDegree > 0
            AND copPerDegree < 20
    )
    SELECT
        supplyTemperature,
        avg(copPerDegree) AS avgCopPerDegree,
        avgCopPerDegree + 3*stddevSamp(copPerDegree) as maxCopPerDegree,
        avgCopPerDegree - 3*stddevSamp(copPerDegree) as minCopPerDegree,
        COUNT(copPerDegree) AS numberOfDataPoints
    FROM
        CTE2
    GROUP BY supplyTemperature
    HAVING 
        isFinite(avgCopPerDegree)
        AND isFinite(minCopPerDegree)
        AND isFinite(maxCopPerDegree)
        AND supplyTemperature >= 10
    '''


def NoutQuery_COPvsAmbientTemperature(**kwargs):
    return f'''
    WITH CTE2 AS
    (
        WITH CTE AS 
        (
            SELECT 
                clientid, 
                toStartOfHour(time_ts) AS time_start,
                avg(hp1_temperatureOutside) AS meanAmbientTemperature, 
                avg(flowMeter_waterSupplyTemperature) AS meanSupplyTemperature, 
                max(hp1_thermalEnergyCounter) - min(hp1_thermalEnergyCounter) AS Q_hp1,
                max(hp1_electricalEnergyCounter) - min(hp1_electricalEnergyCounter) AS E_hp1,
                max(qc_cvEnergyCounter) - min(qc_cvEnergyCounter) as Q_cv 
            FROM 
                "cic_stats"
            WHERE
                time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}' 
                AND has([2, 3, 4], qc_supervisoryControlMode)
            GROUP BY
                clientid, time_start
            HAVING
                (Q_hp1 + Q_cv) >= 1000
                AND meanAmbientTemperature IS NOT NULL
                AND meanSupplyTemperature IS NOT NULL
        )
        SELECT
            clientid,
            toInt16(round(meanAmbientTemperature)) as ambientTemperature,
            sum(Q_hp1)/sum(E_hp1) as copPerDegree
        FROM
            CTE
        WHERE 
            E_hp1 > 0
            AND Q_hp1 > 100
        GROUP BY clientid, ambientTemperature
        HAVING
            copPerDegree > 0
            AND copPerDegree < 20
    )
    SELECT
        ambientTemperature,
        avg(copPerDegree) AS avgCopPerDegree,
        avgCopPerDegree + 3*stddevSamp(copPerDegree) as maxCopPerDegree,
        avgCopPerDegree - 3*stddevSamp(copPerDegree) as minCopPerDegree,
        COUNT(copPerDegree) AS numberOfDataPoints
    FROM
        CTE2
    GROUP BY ambientTemperature
    HAVING 
        isFinite(avgCopPerDegree)
        AND isFinite(minCopPerDegree)
        AND isFinite(maxCopPerDegree)
    ORDER BY ambientTemperature
    '''

