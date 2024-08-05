from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import time
from termcolor import colored


def CalculateAvgTemperatureOutside():
    if any(loaddata['hp2_temperatureOutside'].isna()) == True:
        return loaddata['hp1_temperatureOutside']
    else:
        return (loaddata['hp1_temperatureOutside'] + loaddata['hp2_temperatureOutside'])/2    


def CalculateCOP(loaddata):
    if all(loaddata['hp1_thermalEnergyCounter'] > 0) and all(loaddata['hp1_electricalEnergyCounter'] > 0) and all(loaddata['qc_cvEnergyCounter']): 
        if any(loaddata['hp2_thermalEnergyCounter'].isna()) == True and any(loaddata['hp2_electricalEnergyCounter'].isna()==True):
            return loaddata['hp1_thermalEnergyCounter'] / loaddata['hp1_electricalEnergyCounter']
        else:
            return (loaddata['hp1_thermalEnergyCounter'] + loaddata['hp2_thermalEnergyCounter']) / (loaddata['hp1_electricalEnergyCounter'] + loaddata['hp2_electricalEnergyCounter'])


def SaveQueryResult(df, extractVariables, startTime, endTime, clientid, params):
    try:

        if extractVariables != ['*']:
            variables_name = extractVariables
        else:
            variables_name = "AllVariables"

        if startTime != None and endTime != None:
            dt_start = datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")
            unixtime_start = int(time.mktime(dt_start.timetuple()))
            dt_end = datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S")
            unixtime_end = int(time.mktime(dt_end.timetuple()))
            time_name = f"_{unixtime_start}_{unixtime_end}"
        else:
            time_name = str("")

        Name = f"{variables_name}_{clientid}{time_name}.csv"

        df.to_csv(Name, index=False)
        print(colored("Succesfully saving the file: ", "red") + Name)
        print(colored("Parameters are:", "red"))
        for key, value in params.items():
            value = "None" if value is None else str(value)
            print(key + str(' = ') + value)
    except Exception as e:
        print(f"An error occurred: {e}")


def PlotMatrix(times, loaddata, start_index, end_index):

    for index in range(start_index, end_index):

        plt.figure( figure=(30,18) )
        ax=plt.gca()

        plt.plot(times, loaddata.iloc[:,index])

        ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m'))
        ax.tick_params(axis='x', labelsize=30, size=10)

        y_value_name = loaddata.columns[index]
        plt.ylabel(y_value_name, fontsize=60)

        plt.savefig( 'Plots/' + y_value_name + "_" + str(index) + '.png')
        plt.close()


def Plot(x, y, plotname, filename):

    #Variables, CIC_id, starttime, endtime = filename.split('_')
    #endtime = endtime.replace('.csv', '')

    plt.figure( figure=(30,18) )
    ax=plt.gca()

    plt.scatter(x, y)

    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m'))
    ax.tick_params(axis='x', labelsize=30, size=10)
    
    plt.ylabel(plotname, fontsize=60)

    #plt.savefig( 'Plots/' + plotname + '_' + CIC_id + '_' + starttime + '_' + endtime + '.png')
    plt.savefig( 'Plots/' + filename + '.png') 
    plt.close()

