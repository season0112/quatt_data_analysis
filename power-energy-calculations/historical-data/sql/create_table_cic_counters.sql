CREATE TABLE IF NOT EXISTS `cic_data` (
    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `cic_id` CHAR(40) NOT NULL check(cic_id REGEXP 'CIC-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'),
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    `time` TIMESTAMP NOT NULL,
    `hp1_electrical_energy_counter` DECIMAL(14,6),
    `hp2_electrical_energy_counter` DECIMAL(14,6),
    `hp1_thermal_energy_counter` DECIMAL(14,6),
    `hp2_thermal_energy_counter` DECIMAL(14,6),
    `cv_energy_counter` DECIMAL(14,6),
    `hp_electrical_energy_counter` DECIMAL(14,6),
    `hp_thermal_energy_counter` DECIMAL(14,6),
    UNIQUE INDEX `cic_time` (`cic`, `time`)
);