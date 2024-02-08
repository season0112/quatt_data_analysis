'''
This Python file is made to perfrom a data collection experiment on the Wilo pump and the AWMT pump 
for the purpose of estimating a model that can be used for failure detection and prediction
'''

from QuattRedis import QuattRedis
import time
import csv
import os
import numpy as np
from scipy.signal import butter, lfilter

# define redis adresses
CONTROLLER_FLAG_ADRESS = '9008'
EXTERNAL_FLOW_METER_ADRESS = '31'
INTEGRATED_FLOW_METER_ADRESS = '191'
FLOW_METER_FILTERED_ADRESS = '434'
PUMP_1_RELAY_ADRESS = '103'
SET_PUMP_1_DUTY_CYCLE_ADRESS = '104'
GET_PUMP_1_DUTY_CYCLE_ADRESS = '186'
HP1_HAS_FLOW_SENSOR = '440'
HP2_HAS_FLOW_SENSOR = '471'

ADDRESS_MAP = {
    "AWMT": {
        'name': 'AWMT',
        'raw_flow_rate': EXTERNAL_FLOW_METER_ADRESS,
    },
    "WILO": {
        'name': 'WILO', 
        'raw_flow_rate': INTEGRATED_FLOW_METER_ADRESS,
    }
}

# Experiment parameters
SAMPLING_TIME = 1  # The time interval between samples in seconds
DURATION = 120  # The duration of the experiment in seconds
CUTOFF_FREQUENCY = 0.3  # The cutoff frequency for the low-pass filter
MIN_VALUE = 100  # The minimum value for the random pump input signal
MAX_VALUE = 850  # The maximum value for the random pump input signal

def experiment_design(sampling_time: int = 1, 
                      duration: int = 120, 
                      cutoff_frequency: float = 0.3, 
                      min_value: int = 100, 
                      max_value: int = 850):
    """
    Design an experiment for modelling the pump.

    Parameters:
    - sampling_time (int): The time interval between samples in seconds. Default is 1.
    - duration (int): The duration of the experiment in seconds. Default is 120.
    - cutoff_frequency (float): The cutoff frequency for the low-pass filter. Default is 0.3.
    - min_value (int): The minimum value for the random signal. Default is 100.
    - max_value (int): The maximum value for the random signal. Default is 850.

    Returns:
    - time (ndarray): The time vector.
    - filtered_signal (ndarray): The filtered signal.
    """
    
    # Calculate the number of samples
    num_samples = int(duration * sampling_time)

    # Generate the time vector
    time = np.arange(num_samples) / sampling_time

    # Generate the random signal
    signal = np.random.uniform(min_value, max_value, num_samples)

    # Apply a low-pass filter to the signal
    normalized_cutoff = cutoff_frequency / (sampling_time / 2)
    b, a = butter(1, normalized_cutoff, btype='low', analog=False)
    filtered_signal = lfilter(b, a, signal)

    return time, filtered_signal

def main():
    # Get user input for pump name
    pump_select = input("Select pump type: \n1: AWMT \n2: Wilo \nInput (1 or 2): ")
    assert pump_select in ['1', '2'], "Invalid selection, please select 1 or 2"
    pump_select = "AWMT" if pump_select == '1' else "Wilo"

    # Get user input for flowmeter type
    flowmeter_select = input("Select flowmeter type: \n1: External flowmeter \n2: Integrated flowmeter \nInput (1 or 2): ")
    assert flowmeter_select in ['1', '2'], "Invalid selection, please select 1 or 2"
    flowmeter_select = "External" if flowmeter_select == '1' else "Integrated"

    print('Starting pump data collection experiment')

    # set pump duty cycles
    results = []
    field_names = ['time', 'idx', 'pump_name', 'flowmeter_type', 'set_pump_duty_cycle', 'pump_feedback', 'measured_flow_raw', 'measured_flow_filtered']
    time_vec, pump_inputs = experiment_design(sampling_time=SAMPLING_TIME, 
                                              duration=DURATION, 
                                              cutoff_frequency=CUTOFF_FREQUENCY, 
                                              min_value=MIN_VALUE, 
                                              max_value=MAX_VALUE)
    
    # Round pump inputs to nearest integer
    pump_inputs = np.round(pump_inputs).astype(int)

    redis = QuattRedis()

    # Initialize hardware
    redis.mset(
        {
            CONTROLLER_FLAG_ADRESS:1  # set controller flag alive

        }
    )

    
    # Perfrom test for each pump
    print(f"Testing {pump_select} pump with {flowmeter_select} flowmeter")

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
        flow_raw = redis.get(ADDRESS_MAP[pump_select]['raw_flow_rate'])
        flow_filtered = redis.get(FLOW_METER_FILTERED_ADRESS)

        #add measurements to dictionary
        measurements = {
            'time':time.time(),
            'idx': t,
            'pump_name': pump_select,
            'set_pump_duty_cycle':duty_cycle,
            'pump_feedback':get_pump_duty_cycle,
            'measured_flow_raw':flow_raw,
            'measured_flow_filtered':flow_filtered
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
    file_path = os.path.join(os.getcwd(), 'data', f'{pump_select}_{flowmeter_select}_flow_experiment_{int(time.time())}.csv')
    with open(file_path, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(results)
    print('Done')


if __name__=='__main__':
    # run script
    main()
