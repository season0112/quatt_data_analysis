'''
This Python file is made to perfrom a data collection experiment on the Wilo pump and the AWMT pump 
for the purpose of estimating a model that can be used for failure detection and prediction
'''

from QuattRedis import QuattRedis
import time
import csv
import os
import sys

# define redis adresses
CONTROLLER_FLAG_ADRESS = '9008'
EXTERNAL_FLOW_METER_ADRESS = '31'
INTEGRATED_FLOW_METER_ADRESS = '191'
PUMP_1_RELAY_ADRESS = '103'
SET_PUMP_1_DUTY_CYCLE_ADRESS = '104'
GET_PUMP_1_DUTY_CYCLE_ADRESS = '186'
HP1_HAS_FLOW_SENSOR = '440'
HP2_HAS_FLOW_SENSOR = '471'

# ADDRESS_MAP = {
#     "AWMT": {
#         'name': 'AWMT',
#         'raw_flow_rate': EXTERNAL_FLOW_METER_ADRESS,
#     },
#     "WILO": {
#         'name': 'WILO', 
#         'raw_flow_rate': INTEGRATED_FLOW_METER_ADRESS,
#     }
# }

# Experiment parameters
SAMPLING_TIME = 1  # The time interval between samples in seconds

def main():
    # Get user input for pump name
    pump_select = input("Select pump type: \n1: AWMT \n2: Wilo \nInput (1 or 2): ")
    assert pump_select in ['1', '2'], "Invalid selection, please select 1 or 2"
    pump_select = "AWMT" if pump_select == '1' else "Wilo"

    # # Get user input for flowmeter type
    # flowmeter_select = input("Select flowmeter type: \n1: External flowmeter \n2: Integrated flowmeter \nInput (1 or 2): ")
    # assert flowmeter_select in ['1', '2'], "Invalid selection, please select 1 or 2"
    # flowmeter_select = "External" if flowmeter_select == '1' else "Integrated"

    print('Starting pump data collection experiment')

    # set pump duty cycles
    results = []
    field_names = ['time', 'idx', 'pump_name', 'set_pump_duty_cycle', 'pump_feedback', 'measured_flow_external', 'measured_flow_integrated']
    # Load signal from csv file
    with open(os.path.join(os.path.dirname(sys.argv[0]), 'data', 'pump_modelling_experiment.csv'), 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        time_vec = [float(row['time']) for row in reader]
        csvfile.seek(0)
        next(reader)  # Skip the header row
        pump_inputs = [int(row['signal']) for row in reader]

    redis = QuattRedis()

    # Initialize hardware
    redis.mset(
        {
            CONTROLLER_FLAG_ADRESS:1  # set controller flag alive

        }
    )

    
    # Perfrom test for each pump
    print(f"Performing experiment with {pump_select} pump")

    # Set pump relay on
    redis.mset({PUMP_1_RELAY_ADRESS: 1})

    # Cycle through pump speeds
    for t, duty_cycle in zip(time_vec, pump_inputs):
        # Start timer 
        start = time.time()

        # set pump duty cycle
        redis.mset({SET_PUMP_1_DUTY_CYCLE_ADRESS: duty_cycle})

        # take measurements
        get_pump_duty_cycle = redis.get(GET_PUMP_1_DUTY_CYCLE_ADRESS) # get pump duty cycle from redis
        flow_external = redis.get(EXTERNAL_FLOW_METER_ADRESS)
        flow_internal = redis.get(INTEGRATED_FLOW_METER_ADRESS)

        #add measurements to dictionary
        measurements = {
            'time':time.time(),
            'idx': t,
            'pump_name': pump_select,
            'set_pump_duty_cycle':duty_cycle,
            'pump_feedback':get_pump_duty_cycle,
            'measured_flow_external':flow_external,
            'measured_flow_integrated':flow_internal
            }
        results.append(measurements)

        # stop timer
        end = time.time()

        # wait for the remaining time
        time.sleep(SAMPLING_TIME - (end - start))
    
    # Switch off pump relay
    redis.mset({PUMP_1_RELAY_ADRESS: 0})
    time.sleep(5)
    
    # save dataframe to csv
    print(f'Saving results to data/flow_meter_test_{int(time.time())}.csv')
    file_path = os.path.join(os.path.dirname(sys.argv[0]), 'data', f'{pump_select}_flow_experiment_{int(time.time())}.csv')
    with open(file_path, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(results)
    print('Done')


if __name__=='__main__':
    # run script
    main()
