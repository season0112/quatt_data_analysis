CREATE TABLE IF NOT EXISTS `cic_counter_offset` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `cic_id` CHAR(40) NOT NULL check(cic_id REGEXP 'CIC-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'),
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    `time` TIMESTAMP NOT NULL,
    `hp1_energy_consumed_offset` DECIMAL(14,6),
    `hp1_heat_generated_offset` DECIMAL(14,6),
    `hp2_energy_consumed_offset` DECIMAL(14,6),
    `hp2_heat_generated_offset` DECIMAL(14,6),
    `boiler_heat_generated_offset` DECIMAL(14,6),
    PRIMARY KEY (`id`)
);