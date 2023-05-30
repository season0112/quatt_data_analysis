'''
This Python file is made to the difference between measurements of internal and external flow meters.
It will run the pump at different powers for a certain amount of time.
'''

from QuattRedis import QuattRedis, datatype
import numpy as np
import time
import pandas as pd

def main():

    df = pd.DataFrame(columns=['pump_duty_cycle', 'internal_flow', 'external_flow'])


    redis = QuattRedis()
    redis.connect()
    
    # set controller flag alive
    redis.mset({'9008':1})

    # switch on pump relay
    redis.mset({'103':1})

    # set pump duty cycles
    pump_duty_cycles = [100, 250, 400, 550, 700, 850]

    for pump_duty_cycle in pump_duty_cycles:
        
        # set pump duty cycle
        redis.mset({'104':pump_duty_cycle})
        time.sleep(30) # wait 30 seconds to reach steady state

        # take measurements
        for _ in range(10):
            # take measurements
            internal_measurement = redis.get('31')
            external_measurement = redis.get('32') # still needs to be set up

            #add measurements to pandas frame
            df = df.append({'pump_duty_cycle':pump_duty_cycle, 
                            'internal_flow':internal_measurement, 
                            'external_flow':external_measurement}, ignore_index=True)
            
            time.sleep(5) # wait 5 seconds between measurements
    
    # save dataframe to csv
    df.to_csv(f'flow_meter_test_{int(time.time())}.csv', index=False)


if __name__=='__main__':
    # run script
    main()