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
    `hp1_data_availability` TINYINT(1) NOT NULL,
    `hp2_data_availability` TINYINT(1) NOT NULL,
    `hp1_defrost` DECIMAL(3,2),
    `OLD_hp1_electrical_energy_counter` DECIMAL(14,6) NOT NULL,
    `OLD_hp2_electrical_energy_counter` DECIMAL(14,6),
    `OLD_hp1_thermal_energy_counter` DECIMAL(14,6) NOT NULL,
    `OLD_hp2_thermal_energy_counter` DECIMAL(14,6),
    `OLD_cv_energy_counter` DECIMAL(14,6) NOT NULL,
    `number_of_rows` INT NOT NULL,
    PRIMARY KEY (`id`)
);