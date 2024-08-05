cicDic = ['boiler_oTtbTurnOnOffBoilerOn', 
    'boiler_otFbCh2Mode', 
    'boiler_otFbChModeActive', 
    'boiler_otFbCoolingStatus', 
    'boiler_otFbDhwActive', 
    'boiler_otFbDiagnosticServiceEvent', 
    'boiler_otFbFaultFlags', 
    'boiler_otFbFaultIndication', 
    'boiler_otFbFlameOn', 
    'boiler_otFbMaxChWaterSetpoint', 
    'boiler_otFbOemErrorCode', 
    'boiler_otFbSupplyInletTemperature', 
    'boiler_otFbSupplyOutletTemperature', 
    'boiler_otFbWaterPressure', 
    'boiler_otTbCH', 
    'boiler_otTbCh2Enable', 
    'boiler_otTbControlSetpoint', 
    'boiler_otTbCoolingEnable', 
    'boiler_otTbDHW', 
    'boiler_otTbDhwBlocked', 
    'boiler_otTbOtcActive', 
    'boiler_otTbSummerModeEnable', 
    'clientid', 
    'createdAt', 
    'flowMeter_flowRate', 
    'flowMeter_waterSupplyTemperature', 
    'hp1_acCurrentFailure', 
    'hp1_acInputCurrent', 
    'hp1_acInputVoltage', 
    'hp1_acVoltageProtection', 
    'hp1_ambientTempSensorFailure', 
    'hp1_ambientTemperatureFiltered', 
    'hp1_ambientTemperatureFrequencyLimitProtection', 
    'hp1_bottomPlateHeaterEnable', 
    'hp1_circulatingPumpDutyCycle', 
    'hp1_circulatingPumpModelError', 
    'hp1_circulatingPumpPwmFeedBack', 
    'hp1_circulatingPumpPwmFeedForward', 
    'hp1_commissioningExpectedPower', 
    'hp1_compressorCrankcaseHeaterEnable', 
    'hp1_compressorDriverFailure', 
    'hp1_compressorFrequency', 
    'hp1_compressorFrequencyDemand', 
    'hp1_compressorFrequencyMax', 
    'hp1_compressorOilReturnProtection', 
    'hp1_compressorPhaseCurrentFailure', 
    'hp1_compressorPhaseCurrentOverload', 
    'hp1_compressorPhaseCurrentProtection', 
    'hp1_condenserPressure', 
    'hp1_condenserPressureLockProtection', 
    'hp1_condenserPressureSensorFailure', 
    'hp1_condensingTemperature', 
    'hp1_controlPcbModuleCommunicationFailure', 
    'hp1_dcWaterPumpFailure', 
    'hp1_defrostFlag', 
    'hp1_eepromFailure', 
    'hp1_electricalCounterDegradedFlag', 
    'hp1_electricalEnergyCounter', 
    'hp1_evaporatingPressureLockProtection', 
    'hp1_evaporatingTemperature', 
    'hp1_evaporatorCoilTempSensorFailure', 
    'hp1_evaporatorCoilTempSensorProtection', 
    'hp1_evaporatorCoilTemperature', 
    'hp1_evaporatorPressure', 
    'hp1_evaporatorPressureSensorFailure', 
    'hp1_eviInletTempSensorFailure', 
    'hp1_eviOutletTempSensorFailure', 
    'hp1_eviPressureSensorFailure', 
    'hp1_failureAmbientTemperature', 
    'hp1_fanDrivePcbFailure', 
    'hp1_fanFailure', 
    'hp1_firstStartPreHeatProtection', 
    'hp1_flowRate', 
    'hp1_gasDischargeTempSensorFailure', 
    'hp1_gasDischargeTempSensorProtection', 
    'hp1_gasDischargeTemperature', 
    'hp1_gasReturnTempSensorFailure', 
    'hp1_getCirculatingPumpRelay', 
    'hp1_getFanSpeed', 
    'hp1_getFanSpeedMax', 
    'hp1_getMainWorkingMode', 
    'hp1_highPressureCompressorSpeedDown', 
    'hp1_highPressureSwitch', 
    'hp1_highPressureSwitchLockProtection', 
    'hp1_highPressureSwitchProtection', 
    'hp1_inletTemperatureFiltered', 
    'hp1_inletWaterTempSensorFailure', 
    'hp1_innerCoilTempSensorFailure', 
    'hp1_ipmModuleProtection', 
    'hp1_limitedByCop', 
    'hp1_lowPressureCompressorSpeedDown', 
    'hp1_lowPressureSwitch', 
    'hp1_lowPressureSwitchLockProtection', 
    'hp1_lowPressureSwitchProtection', 
    'hp1_mainControlBoardItemNumber', 
    'hp1_mainLineCurrentProtection', 
    'hp1_masterSlaveCommunicationFailure', 
    'hp1_maximumCompressorFrequency', 
    'hp1_minimumCompressorFrequency', 
    'hp1_moduleVDCVoltageFailure', 
    'hp1_outletTemperatureFiltered', 
    'hp1_outletWaterTempSensorFailure', 
    'hp1_pcbFirmwareVersion', 
    'hp1_power', 
    'hp1_powerInput', 
    'hp1_ratedPower', 
    'hp1_setCirculatingPumpDutyCycle', 
    'hp1_setCirculatingPumpRelay', 
    'hp1_setCompressorFrequencyLevel', 
    'hp1_setMainWorkingMode', 
    'hp1_silentModeStatus', 
    'hp1_temperatureOutside', 
    'hp1_temperatureWaterIn', 
    'hp1_temperatureWaterOut', 
    'hp1_thermalCounterDegradedFlag', 
    'hp1_thermalEnergyCounter', 
    'hp1_watchdogCode', 
    'hp1_waterFlowSwitch', 
    'hp1_waterPumpLevel', 
    'hp2_acCurrentFailure', 
    'hp2_acInputCurrent', 
    'hp2_acInputVoltage', 
    'hp2_acVoltageProtection', 
    'hp2_ambientTempSensorFailure', 
    'hp2_ambientTemperatureFiltered', 
    'hp2_ambientTemperatureFrequencyLimitProtection', 
    'hp2_bottomPlateHeaterEnable', 
    'hp2_circulatingPumpDutyCycle', 
    'hp2_circulatingPumpModelError', 
    'hp2_circulatingPumpPwmFeedBack', 
    'hp2_circulatingPumpPwmFeedForward', 
    'hp2_commissioningExpectedPower', 
    'hp2_compressorCrankcaseHeaterEnable', 
    'hp2_compressorDriverFailure', 
    'hp2_compressorFrequency', 
    'hp2_compressorFrequencyDemand', 
    'hp2_compressorFrequencyMax', 
    'hp2_compressorOilReturnProtection', 
    'hp2_compressorPhaseCurrentFailure', 
    'hp2_compressorPhaseCurrentOverload', 
    'hp2_compressorPhaseCurrentProtection', 
    'hp2_condenserPressure', 
    'hp2_condenserPressureLockProtection', 
    'hp2_condenserPressureSensorFailure', 
    'hp2_condensingTemperature', 
    'hp2_controlPcbModuleCommunicationFailure', 
    'hp2_dcWaterPumpFailure', 
    'hp2_defrostFlag', 
    'hp2_eepromFailure', 
    'hp2_electricalCounterDegradedFlag', 
    'hp2_electricalEnergyCounter', 
    'hp2_evaporatingPressureLockProtection', 
    'hp2_evaporatingTemperature', 
    'hp2_evaporatorCoilTempSensorFailure', 
    'hp2_evaporatorCoilTempSensorProtection', 
    'hp2_evaporatorCoilTemperature', 
    'hp2_evaporatorPressure', 
    'hp2_evaporatorPressureSensorFailure', 
    'hp2_eviInletTempSensorFailure', 
    'hp2_eviOutletTempSensorFailure', 
    'hp2_eviPressureSensorFailure', 
    'hp2_failureAmbientTemperature', 
    'hp2_fanDrivePcbFailure', 
    'hp2_fanFailure', 
    'hp2_firstStartPreHeatProtection', 
    'hp2_flowRate', 
    'hp2_gasDischargeTempSensorFailure', 
    'hp2_gasDischargeTempSensorProtection', 
    'hp2_gasDischargeTemperature', 
    'hp2_gasReturnTempSensorFailure', 
    'hp2_getCirculatingPumpRelay', 
    'hp2_getFanSpeed', 
    'hp2_getFanSpeedMax', 
    'hp2_getMainWorkingMode', 
    'hp2_highPressureCompressorSpeedDown', 
    'hp2_highPressureSwitch', 
    'hp2_highPressureSwitchLockProtection', 
    'hp2_highPressureSwitchProtection', 
    'hp2_inletTemperatureFiltered', 
    'hp2_inletWaterTempSensorFailure', 
    'hp2_innerCoilTempSensorFailure', 
    'hp2_ipmModuleProtection', 
    'hp2_limitedByCop', 
    'hp2_lowPressureCompressorSpeedDown', 
    'hp2_lowPressureSwitch', 
    'hp2_lowPressureSwitchLockProtection', 
    'hp2_lowPressureSwitchProtection', 
    'hp2_mainControlBoardItemNumber', 
    'hp2_mainLineCurrentProtection', 
    'hp2_masterSlaveCommunicationFailure', 
    'hp2_maximumCompressorFrequency', 
    'hp2_minimumCompressorFrequency', 
    'hp2_moduleVDCVoltageFailure', 
    'hp2_outletTemperatureFiltered', 
    'hp2_outletWaterTempSensorFailure', 
    'hp2_pcbFirmwareVersion', 
    'hp2_power', 
    'hp2_powerInput', 
    'hp2_ratedPower', 
    'hp2_setCirculatingPumpDutyCycle', 
    'hp2_setCirculatingPumpRelay', 
    'hp2_setCompressorFrequencyLevel', 
    'hp2_setMainWorkingMode', 
    'hp2_silentModeStatus', 
    'hp2_temperatureOutside', 
    'hp2_temperatureWaterIn', 
    'hp2_temperatureWaterOut', 
    'hp2_thermalCounterDegradedFlag', 
    'hp2_thermalEnergyCounter', 
    'hp2_watchdogCode', 
    'hp2_waterFlowSwitch', 
    'hp2_waterPumpLevel', 
    'message', 
    'qc_controlAction', 
    'qc_controlCommissioningAction', 
    'qc_controllerIdentifier', 
    'qc_cvCounterDegradedFlag', 
    'qc_cvEnergyCounter', 
    'qc_cvPowerOutput', 
    'qc_dayMaxHz', 
    'qc_estimatedPowerDemand', 
    'qc_estimatedPowerDemandFeedback', 
    'qc_estimatedPowerDemandFeedforward', 
    'qc_flowRateFiltered', 
    'qc_fusedFlowSensorDegraded', 
    'qc_houseCounterDegradedFlag', 
    'qc_houseEnergyCounter', 
    'qc_housePowerConsumed', 
    'qc_minimumCOP', 
    'qc_nightMaxHz', 
    'qc_silentModeInt', 
    'qc_stickyPumpProtectionEnabled', 
    'qc_supervisoryControlMode', 
    'qc_supplyTemperatureFiltered', 
    'qc_systemWatchdogCode', 
    'qc_useBothHeatPumps', 
    'qc_watchdogState', 
    'qc_watchdogSubcode', 
    'system_architecture', 
    'system_buttonPushedAt', 
    'system_ccBoilerIdentificationStatus', 
    'system_ccBoilerType', 
    'system_ccCommissioningDone', 
    'system_ccCommissioningMode', 
    'system_ccFlowTemperatureSensorIdentificationStatus', 
    'system_ccHpIdentificationStatus', 
    'system_ccHpTestLevel', 
    'system_ccNumberOfHeatPumps', 
    'system_ccSilentModeSetting', 
    'system_ccThermostatIdentificationStatus', 
    'system_ccThermostatType', 
    'system_cicDiskSpaceUsageTooHigh', 
    'system_cicLoadAvg15TooHigh', 
    'system_cicMemoryTooHigh', 
    'system_cicTempTooHighDuringXTime', 
    'system_cpuTemp', 
    'system_cpuUsage', 
    'system_diskFree', 
    'system_diskTotal', 
    'system_diskUsed', 
    'system_electricityDayTariff', 
    'system_electricityNightTariff', 
    'system_endianness', 
    'system_ethernetPing', 
    'system_gasTariff', 
    'system_hostName', 
    'system_hp1Connected', 
    'system_hp2Connected', 
    'system_hwid', 
    'system_iccid', 
    'system_isBoilerConnected', 
    'system_isButtonDriverAlive', 
    'system_isCicCloudCommandAlive', 
    'system_isCicCloudLoggerAlive', 
    'system_isCicCloudMetricsAlive', 
    'system_isCicCloudProxyAlive', 
    'system_isCicCloudWebserverAlive', 
    'system_isCicControllerManagerAlive', 
    'system_isCicEgdeLogicAlive', 
    'system_isCicNetworkCommandHandlerAlive', 
    'system_isCicNetworkMonitoringAlive', 
    'system_isCicProvisioningAlive', 
    'system_isCicResourceMonitoringAlive', 
    'system_isCloudConnectorAlive', 
    'system_isControllerAlive', 
    'system_isEthernetConnected', 
    'system_isEthernetReachable', 
    'system_isFlowmeterAppAlive', 
    'system_isFlowmeterConnected', 
    'system_isHardwareWatchdogAlive', 
    'system_isLTE', 
    'system_isLedAlive', 
    'system_isLteConnected', 
    'system_isLteReachable', 
    'system_isMenderClientAlive', 
    'system_isMenderConnectAlive', 
    'system_isModbusDriverAlive', 
    'system_isMqttConnected', 
    'system_isOnOffBoilerDriverAlive', 
    'system_isOtBoilerDriverAlive', 
    'system_isOtThermostatDriverAlive', 
    'system_isTemperatureSensorAppAlive', 
    'system_isTemperatureSensorConnected', 
    'system_isThermostatConnected', 
    'system_isWifiConnected', 
    'system_isWifiReachable', 
    'system_loadavg1', 
    'system_loadavg15', 
    'system_loadavg5', 
    'system_ltePing', 
    'system_lteProvisioningstatus', 
    'system_mac', 
    'system_memAvailable', 
    'system_memFree', 
    'system_memTotal', 
    'system_menderId', 
    'system_menderUpdateState', 
    'system_modemImei', 
    'system_modemImsi', 
    'system_modemVer', 
    'system_odu1RevisionId', 
    'system_odu2RevisionId', 
    'system_os', 
    'system_provisioningStatus', 
    'system_quattBuild', 
    'system_quattId', 
    'system_ratedHeatLossHouse', 
    'system_release', 
    'system_serNo', 
    'system_temperatureCutoffHeating', 
    'system_thermostatType', 
    'system_uptime', 
    'system_wifiConnectedSsid', 
    'system_wifiPing', 
    'thermostat_otFtCh2Enabled', 
    'thermostat_otFtChEnabled', 
    'thermostat_otFtControlSetpoint', 
    'thermostat_otFtCoolingEnabled', 
    'thermostat_otFtDhwBlocked', 
    'thermostat_otFtDhwEnabled', 
    'thermostat_otFtMasterProductTypeNumber', 
    'thermostat_otFtMasterProductVersionNumber', 
    'thermostat_otFtMaxRelModulationLevelSetting', 
    'thermostat_otFtMemberIDOfMaster', 
    'thermostat_otFtOpenthermVersionMaster', 
    'thermostat_otFtOtcActive', 
    'thermostat_otFtRoomSetpoint', 
    'thermostat_otFtRoomSetpointCh2', 
    'thermostat_otFtRoomTemperature', 
    'thermostat_otFtSmartPowerImplemented', 
    'thermostat_otFtSummerModeEnabled', 
    'thermostat_otTtBoilerWaterTemp', 
    'thermostat_otTtCh2Mode', 
    'thermostat_otTtChModeActive', 
    'thermostat_otTtCoolingStatus', 
    'thermostat_otTtDhwActive', 
    'thermostat_otTtDiagnosticServiceEvent', 
    'thermostat_otTtFaultIndication', 
    'thermostat_otTtFlameOn', 
    'time_ts']

