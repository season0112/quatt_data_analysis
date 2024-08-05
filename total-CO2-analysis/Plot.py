#! /usr/bin/env python3
import argparse
import Plot_function
import Utility

def main():
    #Plot_function.Plot_CompareTwoResult('RecoveredComparasion')
    #Plot_function.PlotCO2_OneDay_AllCIC()

    Plot_function.Plot_ProblematicCIC_FromClickhouse()

    #Plot_function.Compare_CIC_Number()
    #Plot_function.Plot_Installation1228()
    #Plot_function.DailyCO2Savings('Data/TotalCO2Data_Clickhouse_GreenHouseGasEmission0.22/TotalCO2_withGreenHouseGasEmission0.22_Exclude7WrongDate.csv')


    #Utility.MergeCSVFile('Data/TotalCO2Data_Clickhouse_GreenHouseGasEmission0.22/')

if __name__ == '__main__':
    main()


