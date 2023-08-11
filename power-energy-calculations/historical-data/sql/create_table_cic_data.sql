CREATE TABLE IF NOT EXISTS `cic_data` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `cic_id` CHAR(40) NOT NULL check(cic_id REGEXP 'CIC-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'),
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    `time` TIMESTAMP NOT NULL,
    `hp1_energy_consumed` DECIMAL(14,6),
    `hp1_heat_generated` DECIMAL(14,6),
    `hp1_active` DECIMAL(3,2),
    `hp2_energy_consumed` DECIMAL(14,6),
    `hp2_heat_generated` DECIMAL(14,6),
    `hp2_active` DECIMAL(3,2),
    `boiler_heat_generated` DECIMAL(14,6),
    `boiler_active` DECIMAL(3,2),
    `hp1_data_availability` TINYINT NOT NULL,
    `hp2_data_availability` TINYINT NOT NULL,
    `hp1_defrost` DECIMAL(3,2),
    `OLD_hp1_electrical_energy_counter` DECIMAL(14,6) NOT NULL,
    `OLD_hp2_electrical_energy_counter` DECIMAL(14,6),
    `OLD_hp1_thermal_energy_counter` DECIMAL(14,6) NOT NULL,
    `OLD_hp2_thermal_energy_counter` DECIMAL(14,6),
    `OLD_cv_energy_counter` DECIMAL(14,6) NOT NULL,
    --
    `water_supply_temperature` DECIMAL(6,3),
    `water_return_temperature` DECIMAL(6,3),
    `room_set_temperature` DECIMAL(6,3),
    `room_temperature` DECIMAL(6,3),
    `outside_temperature` DECIMAL(6,3),
    `house_energy_demand` DECIMAL(14,6),
    `anti_freeze_protection` DECIMAL(3,2), -- scm 96-99
    `flow_rate_oos` DECIMAL(3,2), -- qc.watchdogstate==8 + qc.watchdogsubcode==2 / qc.systemwatchdogcode==2
    `inlet_temperature_oos` DECIMAL(3,2), -- qc.watchdogstate==2 + qc.watchdogsubcode==10 / hp1.watchdogcode==10
    -- New counters which will be filled after calculating offsets
    `hp1_electrical_energy_counter` DECIMAL(14,6),
    `hp2_electrical_energy_counter` DECIMAL(14,6),
    `hp1_thermal_energy_counter` DECIMAL(14,6),
    `hp2_thermal_energy_counter` DECIMAL(14,6),
    `cv_energy_counter` DECIMAL(14,6),
    `hp_electrical_energy_counter` DECIMAL(14,6),
    `hp_thermal_energy_counter` DECIMAL(14,6),
    `number_of_rows` INT NOT NULL,
    `quattBuild` CHAR(40),
    PRIMARY KEY (`id`)
);