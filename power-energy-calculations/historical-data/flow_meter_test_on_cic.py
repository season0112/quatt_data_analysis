'''
This Python file is made to the difference between measurements of internal and external flow meters.
It will run the pump at different powers for a certain amount of time.
'''

from QuattRedis import QuattRedis
import time
import csv

# define redis adresses
CONTROLLER_FLAG_ADRESS = '9008'
INTERNAL_FLOW_METER_ADRESS = '31'
EXTERNAL_FLOW_METER_ADRESS = '30012'
PUMP_RELAY_ADRESS = '103'
SET_PUMP_DUTY_CYCLE_ADRESS = '104'
GET_PUMP_DUTY_CYCLE_ADRESS = '186'


def main():
    print('Starting flow meter test')
    # set pump duty cycles
    results = []
    field_names = ['time', 'set_pump_duty_cycle', 'get_pump_duty_cycle', 'internal_flow', 'external_flow']
    set_pump_duty_cycles = [100, 250, 400, 550, 700, 850]

    redis = QuattRedis()
    
    # set controller flag alive
    redis.mset({CONTROLLER_FLAG_ADRESS:1})

    # switch on pump relay
    redis.mset({PUMP_RELAY_ADRESS:1})

    for set_pump_duty_cycle in set_pump_duty_cycles:
        print('Setting pump duty cycle to: ', set_pump_duty_cycle)
        # set pump duty cycle
        redis.mset({SET_PUMP_DUTY_CYCLE_ADRESS:set_pump_duty_cycle})
        #time.sleep(60) # Taking out waiting time, and analyzing the data afterwards

        # take measurements for 36*5 seconds = 3 minutes
        for _ in range(36):
            # take measurements
            get_pump_duty_cycle = redis.get(GET_PUMP_DUTY_CYCLE_ADRESS) # get pump duty cycle from redis
            internal_measurement = redis.get(INTERNAL_FLOW_METER_ADRESS)
            external_measurement = redis.get(EXTERNAL_FLOW_METER_ADRESS) # still needs to be set up

            #add measurements to pandas frame
            measurements = {
                'time':time.time(),
                'set_pump_duty_cycle':set_pump_duty_cycle,
                'get_pump_duty_cycle':get_pump_duty_cycle,
                'internal_flow':internal_measurement,
                'external_flow':external_measurement
                }
            results.append(measurements)
            time.sleep(5) # wait 5 seconds between measurements
    
    # switch off pump & pump relay & controller adress
    redis.mset({SET_PUMP_DUTY_CYCLE_ADRESS:0})
    redis.mset({PUMP_RELAY_ADRESS:0})
    
    # save dataframe to csv
    print(f'Saving results to data/flow_meter_test_{int(time.time())}.csv')
    with open(f'data/flow_meter_test_{int(time.time())}.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(results)
    print('Done')


if __name__=='__main__':
    # run script
    main()