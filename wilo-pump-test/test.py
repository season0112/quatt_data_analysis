'''
This Python file is made to compare the performance of the Wilo pump with that of the AWMT pump
'''

from QuattRedis import QuattRedis
import time
import csv
import os

# define redis adresses
CONTROLLER_FLAG_ADRESS = '9008'
FLOW_METER_ADRESS = '31'
PUMP_1_RELAY_ADRESS = '103'
SET_PUMP_1_DUTY_CYCLE_ADRESS = '104'
GET_PUMP_1_DUTY_CYCLE_ADRESS = '186'
PUMP_2_RELAY_ADRESS = '203'
SET_PUMP_2_DUTY_CYCLE_ADRESS = '204'
GET_PUMP_2_DUTY_CYCLE_ADRESS = '286'

CONFIG = [{'name': 'AWMT', 'relay-address': PUMP_1_RELAY_ADRESS, 'set-duty-cycle': SET_PUMP_1_DUTY_CYCLE_ADRESS, 'power-feedback': GET_PUMP_1_DUTY_CYCLE_ADRESS},
          {'name': 'Wilo', 'relay-address': PUMP_2_RELAY_ADRESS, 'set-duty-cycle': SET_PUMP_2_DUTY_CYCLE_ADRESS, 'power-feedback': GET_PUMP_2_DUTY_CYCLE_ADRESS}]


def main():
    print('Starting flow meter test')
    # set pump duty cycles
    results = []
    field_names = ['time', 'pump_name', 'set_pump_duty_cycle', 'pump_feedback', 'measured_flow']
    set_pump_duty_cycles = [100, 250, 400, 550, 700, 850]

    redis = QuattRedis()
    
    # set controller flag alive
    redis.mset({CONTROLLER_FLAG_ADRESS:1})

    # switch off both pump relays
    for pump in CONFIG:
        redis.mset({pump['relay-address']: 0})
    
    # Perfrom test for each pump
    for pump in CONFIG:
        print(f"Testing {pump['name']} pump")

        # Set pump relay on
        redis.mset({pump['relay-address']: 1})

        # Cycle through pump speeds
        for duty_cycle in set_pump_duty_cycles:
            print('Setting pump duty cycle to: ', duty_cycle)
            # set pump duty cycle
            redis.mset({pump['set-duty-cycle']: duty_cycle})
            #time.sleep(60) # Taking out waiting time, and analyzing the data afterwards

            # take measurements for 30*1 seconds = 30 seconds
            for _ in range(30):
                # take measurements
                get_pump_duty_cycle = redis.get(pump['power-feedback']) # get pump duty cycle from redis
                flow = redis.get(FLOW_METER_ADRESS)

                #add measurements to pandas frame
                measurements = {
                    'time':time.time(),
                    'pump_name': pump['name'],
                    'set_pump_duty_cycle':duty_cycle,
                    'pump_feedback':get_pump_duty_cycle,
                    'measured_flow':flow
                    }
                results.append(measurements)
                time.sleep(1) # wait 5 seconds between measurements
        
        # Switch off pump relay
        redis.mset({pump['relay-address']: 0})
        time.sleep(10)
    
    # save dataframe to csv
    print(f'Saving results to data/flow_meter_test_{int(time.time())}.csv')
    file_path = os.path.join(os.getcwd(), 'data', f'wilo_test_{int(time.time())}.csv')
    with open(file_path, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(results)
    print('Done')


if __name__=='__main__':
    # run script
    main()
