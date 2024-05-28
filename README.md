# quatt_data_analysis
This repository contains several data analytics scripts and results, all in Python / Jupyter Notebook.  
A short description of each project is given below.

## Contents
1) all-e-market-research
1) customer-issues
1) guaranteed-savings
1) offline-cics
1) Other
1) power-energy-calculations
1) refurb-issues
1) silent-mode-mismatch
1) wilo-pump-test

## Summary of each project

### all-e-market-research
Jupyter notebook answering the question: what percentage of our customers can convert to all-e? Based on data from clickhouse.
Delivered: 2024-01-08.

### customer-issues
Data analysis related to various customer issues, either reported via customer service or detected from the various dashboards.

### guaranteed-savings
Explorative notebooks, containing research into building a savings estimation model.  
The target of the model is to estimate the savings of new customers, by looking at data of their house (like gas usage, and size).  
The model contains 2 parts: first predicting savings of existing customers, based on measurements (data in Clickhouse). 
Then the second model (not build yet) uses the values predicted and tries to predict these values based on data available in Hubspot.

### offline-cics
Contains an analysis of the connectivity statuses of CiC's in the installed base with respect to Quatt AWS cloud and Mender cloud.

### Other
Contains short examples for getting data from redis / mysql / cloudwatch.

### power-energy-calculations
Goal: Setting up a model to recalculate historical counters and measuring the accuracy of this model.
Detailed documentation on Slite: 
- Final model for historical energy consumption: [Final Model](https://quatt.slite.com/app/docs/JEG8lRg5Jlkz_T/Calculation-model-or-energy-consumption-production)
- Documentation about accuracy of metrics: [Metric Accuracy](https://quatt.slite.com/app/docs/-MfmJZxR1-IB_J/Accuracy-of-data-metrics)

#### data
Data files that were saved in used in the analysis.

#### live-data
- Gets data from 4 cics (6 heat pumps) which are equipped with external meters that measure electricity consumption and heat production of the heat pump.
- After some exploratory analysis there was some indication of a potential inaccuray of our flow-meter.
- flow_meter_test_data contains the script and data used in a test setup to measure this inaccuracy of the flow meter. Concluded was that the error was not large enough to take into account when estimating a model.
- The final model estimates the electricity consumption of the heat pump based on: `powerInput`, `fanSpeed`, `pumpDutyCycle`, `bottomPlateHeaterEnable`, `compressorCrankCaseHeaterEnable`

#### historical-data
- Extends on the live-data model with a model to replace the `bottomPlateHeaterEnable` parameter when this is not available.
- Extends on the live-data model with a model to replace the model when non of the above inputs are available (LTE mode).

### refurb issues
Analysis of CiCs that may have been incorrectly refurbished in the cloud (i.e., data was accidently deleted).

### silent-mode-mismatch
Analysis of installations for which the silent mode setting in the CiC does not match the silent mode setting in the cloud due to incorrect default setting during migration of silent mode setting to production

### wilo-pump-test
Test scripts used to test the Wilo pump and the associated test data


## Test data in production
Before each analysis make sure to check [this sheet](https://docs.google.com/spreadsheets/d/170q_-Qxdcddj69vHCYtmeCo2Gb-KSEnt7WJJAc5BEgw/edit#gid=0) to see what testing data in production might interfere with your analysis.