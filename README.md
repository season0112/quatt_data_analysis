# quatt_data_analysis
This repository contains several data analytics scripts and results, all in Python / Jupyter Notebook.

## customer-issues
Data analysis related to various customer issues, either reported via customer service or detected from the various dashboards.

## Other
Contains short examples for getting data from redis / mysql / cloudwatch.

## offline-cics
Contains an analysis of the connectivity statuses of CiC's in the installed base with respect to Quatt AWS cloud and Mender cloud.

## power-energy-calculations
Goal: Setting up a model to recalculate historical counters and measuring the accuracy of this model.

### data
Data files that were saved in used in the analysis.

### live-data
- Gets data from 4 cics (6 heat pumps) which are equipped with external meters that measure electricity consumption and heat production of the heat pump.
- After some exploratory analysis there was some indication of a potential inaccuray of our flow-meter.
- flow_meter_test_data contains the script and data used in a test setup to measure this inaccuracy of the flow meter. Concluded was that the error was not large enough to take into account when estimating a model.
- The final model estimates the electricity consumption of the heat pump based on: `powerInput`, `fanSpeed`, `pumpDutyCycle`, `bottomPlateHeaterEnable`, `compressorCrankCaseHeaterEnable`

### historical-data
- Extends on the live-data model with a model to replace the `bottomPlateHeaterEnable` parameter when this is not available.
- Extends on the live-data model with a model to replace the model when non of the above inputs are available (LTE mode).
- TBD: place rest of scripts in other repository

## refurb issues
Analysis of CiCs that may have been incorrectly refurbished in the cloud (i.e., data was accidently deleted).

## silent-mode-mismatch
Analysis of installations for which the silent mode setting in the CiC does not match the silent mode setting in the cloud due to incorrect default setting during migration of silent mode setting to production

## wilo-pump-test
Test scripts used to test the Wilo pump and the associated test data