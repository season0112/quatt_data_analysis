
def checkCIC(**kwargs):
    return f'''
    SELECT
        time_ts,
        hp1_thermalEnergyCounter
    FROM
        "cic_stats"
    WHERE
        time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}'
    '''


def NoutCO2Query(**kwargs):

    return f'''
    WITH CTE AS
    (
      SELECT  
        '{kwargs.get('StartTime')}' AS startTime,
        clientid,
        max(hp1_thermalEnergyCounter) - min(hp1_thermalEnergyCounter) AS Q_hp1,
        max(hp1_electricalEnergyCounter) - min(hp1_electricalEnergyCounter) AS E_hp1,
        ifNull(max(hp2_thermalEnergyCounter) - min(hp2_thermalEnergyCounter), 0) AS Q_hp2,
        ifNull(max(hp2_electricalEnergyCounter) - min(hp2_electricalEnergyCounter), 0) AS E_hp2,
        max(qc_cvEnergyCounter) - min(qc_cvEnergyCounter) AS Q_cv
      FROM 
        "cic_stats"
      WHERE
        time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}' 
        AND qc_supervisoryControlMode in (2,3,4)
        -- AND clientid = 'CIC-17fcd27d-dbd7-561c-887e-faf59bb9ebeb'
        -- AND clientid = 'CIC-f8a7b152-ab05-5a0f-a2c6-3dcef31be463'
      GROUP BY
        clientid
      HAVING
        (Q_hp1 + Q_hp2 + Q_cv) >  {kwargs.get('Q_min')}*1000
    )
    SELECT
      startTime AS Date,
      COUNT(clientid) AS ActiveClientID,
      SUM(Q_hp1 + Q_hp2) AS Total_hpHeat_diff, -- (Wh)
      SUM(E_hp1 + E_hp2) AS Total_hpElectric_diff, -- (Wh)
      SUM(Q_cv) AS Total_boilerHeat_diff, -- (Wh)
      SUM(Q_hp1 + Q_hp2) / SUM(E_hp1 + E_hp2) AS Total_COP,
      SUM(Q_hp1 + Q_hp2) / 1000 / 8.7925 AS Savings_Gas,  -- (m^3)
      SUM(Q_hp1 + Q_hp2) / 1000 / 8.7925 * 1.788 AS CO2_Gas_Saved, -- (kg)
      SUM(E_hp1 + E_hp2) * 0.22 / 1000 AS CO2_Electricity, -- (kg)
      ((SUM(Q_hp1 + Q_hp2) / 8.7925 * 1.788) - (SUM(E_hp1 + E_hp2) * 0.22)) / 1000 AS Savings_CO2, -- (kg)
      ((SUM(Q_hp1 + Q_hp2) / 8.7925 * 1.788) - (SUM(E_hp1 + E_hp2) * 0.22)) / 1000 / COUNT(clientid) AS Savings_CO2_perCIC -- (kg)
    FROM
      CTE
    GROUP BY 
      Date
    '''

def testCO2Query(**kwargs):

    return f'''
      SELECT  
        '{kwargs.get('StartTime')}' AS startTime,
        clientid,
        max(hp1_thermalEnergyCounter) - min(hp1_thermalEnergyCounter) AS Q_hp1,
        max(hp1_electricalEnergyCounter) - min(hp1_electricalEnergyCounter) AS E_hp1,
        ifNull(max(hp2_thermalEnergyCounter) - min(hp2_thermalEnergyCounter), 0) AS Q_hp2,
        ifNull(max(hp2_electricalEnergyCounter) - min(hp2_electricalEnergyCounter), 0) AS E_hp2,
        max(qc_cvEnergyCounter) - min(qc_cvEnergyCounter) AS Q_cv
      FROM 
        "cic_stats"
      WHERE
        time_ts BETWEEN '{kwargs.get('StartTime')}' AND '{kwargs.get('EndTime')}'
        AND qc_supervisoryControlMode in (2,3,4) 
      GROUP BY
        clientid
      HAVING
        (Q_hp1 + Q_hp2 + Q_cv) >  {kwargs.get('Q_min')}*1000
    '''


