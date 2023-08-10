CREATE TABLE IF NOT EXISTS `_cicsWithSoftwareVersion207` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `cic_id` CHAR(40) NOT NULL check(cic_id REGEXP 'CIC-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'),
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    `reached_207_at` TIMESTAMP,
    `data_ready` BOOLEAN,
    `data_migrated` BOOLEAN,
    PRIMARY KEY (`id`)
);