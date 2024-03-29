import csv
import os
import numpy as np
import sys
import matplotlib.pyplot as plt
from scipy.signal import butter, lfilter

# Experiment parameters
SAMPLING_TIME = 1  # The time interval between samples in seconds
DURATION = 120  # The duration of the experiment in seconds
CUTOFF_FREQUENCY = 0.1  # The cutoff frequency for the low-pass filter
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

    # Round to integer
    filtered_signal = np.round(filtered_signal).astype(int)

    return time, filtered_signal

def main():
    # Generate experiment
    time, signal = experiment_design(SAMPLING_TIME, DURATION, CUTOFF_FREQUENCY, MIN_VALUE, MAX_VALUE)

    # Plot the signal
    plt.plot(time, signal)
    plt.xlabel('Time (s)')
    plt.ylabel('Pump duty cycle')
    plt.title('Pump Modelling Experiment')
    plt.show()

    # Save to CSV
    with open(os.path.join(os.path.dirname(sys.argv[0]), 'data', 'pump_modelling_experiment.csv'), 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["time", "signal"])
        writer.writerows(zip(time, signal))

if __name__ == '__main__':
    main() # Call the main function
