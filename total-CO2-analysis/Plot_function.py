import pandas as pd
import matplotlib.pyplot as plt
import QuattDataLoader.Utility.PythonPlotDefaultParameters
import datetime


def Plot_CompareTwoResult(loaddata):

    if loaddata == 'old':
        loaddata_mysql = pd.read_csv('Data/OldComparasion/extracteddata_Martijn.csv', sep=';')
        loaddata_nout  = pd.read_csv('Data/OldComparasion/extracteddata_noutco2query.csv')
    elif loaddata == 'RecoveredComparasion':
        loaddata_mysql = pd.read_csv('Data/RecoveredComparasion/extracteddata_Martijn.csv')
        loaddata_nout  = pd.read_csv('Data/RecoveredComparasion/extracteddata_noutco2query.csv')

    y_labels = ['Date', 'Number of active cics', 'Heatpump deliverd heat (wh)', "Electricity consumed (wh)", "Boiler heat (wh)", "COP", "Saving gas (m^3)", "Saving gas equivalent to CO2 (kg)", "Electricity consumed equivalent to CO2 (kg)", "Total CO2 saving (kg)", "Savings_co2_percic"]

    for index in range(1,11):

        plt.figure( figure=(30,18) )
        ax=plt.gca()

        if loaddata == 'old':
            plt.plot(pd.to_datetime(loaddata_mysql['Date'], utc=True), loaddata_mysql.iloc[:,index], color='blue', marker='*', label='Martijn Google Sheet')
        elif loaddata == 'RecoveredComparasion':
            plt.plot(pd.to_datetime(loaddata_mysql['Date'], utc=True), loaddata_mysql.iloc[:,index], color='blue', marker='*', label='Martijn Google Sheet (With clean data)')
        plt.plot(pd.to_datetime(loaddata_nout['Date'] , utc=True), loaddata_nout.iloc[:,index], color='red', marker='o', label='Fleet Dashboard')

        ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%y-%m-%d'))
        ax.tick_params(axis='x', labelsize=30, size=10)

        if index == 1 or index == 5:
            ax.set_yscale('linear')
        else:
            ax.set_yscale('linear')

        y_value_name = loaddata_nout.columns[index]
        #plt.ylabel(y_value_name, fontsize=60)
        plt.ylabel(y_labels[index], fontsize=60)

        plt.legend(loc='best')
        #plt.legend(loc='lower right')

        if loaddata == 'old':
            plt.savefig( 'Plots/Plots_old/' + y_value_name + "_" + str(index) + '.png')
        elif loaddata == 'RecoveredComparasion':
            plt.savefig( 'Plots/Plots_Comparision/' + y_value_name + "_" + str(index) + '.png')
        plt.close()


def PlotCO2_OneDay_AllCIC(): 
    loaddata_mysql = pd.read_csv('Data/OneDay_AllCIC_mysql.csv')

    plt.figure( figure=(30,18) )
    ax=plt.gca()

    plt.bar(loaddata_mysql['id'], loaddata_mysql['hpHeat_diff'])

    ax.set_xlabel("144 CIC_IDs")
    ax.set_ylabel("Pump Delivered Heat on 01.07.2024 (Wh)")
    ax.set_xticklabels(loaddata_mysql['id'], fontsize=0)

    ax.set_yscale('log')

    plt.savefig('Plots/AllCIC_OneDay.png')
    plt.close()


def Plot_ProblematicCIC_FromClickhouse():

    #start = 0
    #end   = 5000

    loaddata_clickhouse = pd.read_csv('Data/Clickhouse_checkProblematicCIC.csv')

    plt.figure( figure=(30,18) )
    ax=plt.gca()

    plt.plot(pd.to_datetime(loaddata_clickhouse['time_ts'], utc=True), loaddata_clickhouse['hp1_thermalEnergyCounter'], color='blue', label='')

    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d'))
    ax.tick_params(axis='x', labelsize=30, size=10)
    #ax.set_yscale('linear')

    #plt.ylabel(y_labels[index], fontsize=60)

    #plt.savefig('checkPlot/test_' + str(start) + '_' + str(end) + '.png')
    plt.savefig('checkPlot/test' + '.png')

    ax.set_yscale('log')
    #plt.savefig('checkPlot/test_' + str(start) + '_' + str(end) + '_log.png')
    plt.savefig('checkPlot/test' + '_log.png')

    plt.close()


def Compare_CIC_Number():
    loaddata_clickhouse = pd.read_csv('cic_clickhouse.csv')
    loaddata_mysql      = pd.read_csv('cic_mysql.csv')

    print(loaddata_clickhouse.shape)
    print(loaddata_mysql.shape)

    loaddata_clickhouse['clientid'] = loaddata_clickhouse['clientid'].astype(str)
    loaddata_mysql['id']            = loaddata_mysql['id'].astype(str)

    count = 0
    for i in loaddata_mysql['id']:
        if i not in loaddata_clickhouse['clientid'].values:
            count += 1
            print(i)
    print(count)


def Plot_Installation1228():
    loaddata_mysql1228 = pd.read_csv('Data/Installation1228_details.csv')
    
    plt.figure( figure=(30,18) )
    ax=plt.gca()

    #plt.plot(pd.to_datetime(loaddata_mysql1228['timestamp'], utc=True), loaddata_mysql1228['hpHeat'], color='blue', marker='*', label='Installation 1228')

    plt.plot(pd.to_datetime(loaddata_mysql1228['timestamp'], utc=True), loaddata_mysql1228['hpHeat'])

    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%y-%m'))
    ax.tick_params(axis='x', labelsize=30, size=10)

    # ax.set_yscale('linear')

    plt.ylabel('hpHeat (Wh)', fontsize=60)

    plt.savefig( 'Plots/Installation1228_details.png')
    plt.close()


def DailyCO2Savings(path):


    loaddata_all = pd.read_csv(path)

    print(loaddata_all['Savings_CO2'])
    print(sum(loaddata_all['Savings_CO2']))


    loaddata_Martijn = pd.read_csv('Data/Overall heatpump performance - overview_new_2023_nr.csv')
    loaddata_Martijn['Savings_CO2']         = loaddata_Martijn['Savings_CO2'].str.replace(',', '')
    loaddata_Martijn['ActiveInstallations'] = loaddata_Martijn['ActiveInstallations'].str.replace(',', '')

    loaddata_Martijn['Savings_CO2']         = pd.to_numeric(loaddata_Martijn['Savings_CO2'], errors='coerce')
    loaddata_Martijn['ActiveInstallations'] = pd.to_numeric(loaddata_Martijn['ActiveInstallations'], errors='coerce')

    # Plot Daily CO2
    plt.figure( figure=(30,50) )
    ax=plt.gca()

    plt.plot(pd.to_datetime(loaddata_all['Date'], utc=True)    , loaddata_all['Savings_CO2'], 'bs', markersize=10, label='Fleet Performance Savings CO2')
    plt.plot(pd.to_datetime(loaddata_Martijn['Date'], utc=True), loaddata_Martijn['Savings_CO2'], 'rs', markersize=10, label='Martijn Savings_CO2 (Without correction)')

    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%y-%m'))
    ax.tick_params(axis='x', labelsize=50, size=5)

    ax.set_xlabel("Time")
    ax.set_ylabel("Daily CO2 Savings (kg)")
    plt.legend(loc='best')

    ax.set_yscale('log')
    #plt.ylim(bottom=0)

    plt.tight_layout() 

    plt.savefig('Plots/Daily_log.png')
    ax.set_yscale('linear')
    #plt.ylim(top=70000)
    plt.tight_layout()
    plt.savefig('Plots/Daily_linear.png')

    plt.close()


    # Plot Daily CO2 per CIC
    plt.figure( figure=(30,50) )
    ax=plt.gca()

    plt.plot(pd.to_datetime(loaddata_all['Date'], utc=True), loaddata_all['Savings_CO2']/loaddata_all['ActiveClientID'], 'bs', markersize=10, label='Fleet Performance Savings CO2 per CIC')
    plt.plot(pd.to_datetime(loaddata_Martijn['Date'], utc=True), loaddata_Martijn['Savings_CO2']/loaddata_Martijn['ActiveInstallations'], 'rs', markersize=10, label='Martijn Savings_CO2 per CIC (Without correction)')

    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%y-%m'))
    ax.tick_params(axis='x', labelsize=50, size=5)

    ax.set_xlabel("Time")
    ax.set_ylabel("Daily CO2 Savings per CIC (kg)")
    plt.legend(loc='best')

    ax.set_yscale('log')
    #plt.ylim(bottom=0)

    plt.tight_layout() 

    plt.savefig('Plots/DailyPerCIC_log.png')
    ax.set_yscale('linear')
    #plt.ylim(bottom=-50)
    plt.tight_layout()
    plt.savefig('Plots/Daily_linearPerCIC.png')

    plt.close()

