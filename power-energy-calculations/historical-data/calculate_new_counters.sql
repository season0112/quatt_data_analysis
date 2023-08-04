-- TO DO: Adapt query to only sum for software version up to 2.0.4 (exclusive)

-- insert pre-update-counters
INSERT INTO cic_counters (
	cic_id,
    `time`,
    hp1_electrical_energy_counter,
    hp1_thermal_energy_counter,
    hp2_electrical_energy_counter,
    hp2_thermal_energy_counter,
    hp_electrical_energy_counter,
    hp_thermal_energy_counter,
    cv_energy_counter)

WITH pre_update_counters AS (
		SELECT 
			d.cic_id,
			d.`time`,
            o.`time` as offset_time,
			SUM(hp1_energy_consumed) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) + COALESCE(hp1_energy_consumed_offset, 0) AS hp1_electrical_energy_counter,
			SUM(hp2_energy_consumed) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) + COALESCE(hp2_energy_consumed_offset, 0) AS hp2_electrical_energy_counter,
			SUM(hp1_heat_generated) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) + COALESCE(hp1_heat_generated_offset, 0) AS hp1_thermal_energy_counter,
			SUM(hp2_heat_generated) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) + COALESCE(hp2_heat_generated_offset, 0) AS hp2_thermal_energy_counter,
			SUM(boiler_heat_generated) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) + COALESCE(boiler_heat_generated_offset, 0) AS cv_energy_counter
		FROM cic_data d
			LEFT JOIN cic_counter_offset o ON d.cic_id=o.cic_id
		WHERE d.cic_id = 'CIC-17fcd27d-dbd7-561c-887e-faf59bb9ebeb'
)
SELECT cic_id,
	`time`,
    hp1_electrical_energy_counter,
    hp1_thermal_energy_counter,
    hp2_electrical_energy_counter,
    hp2_thermal_energy_counter,
    COALESCE(hp1_electrical_energy_counter,0) 
		+ COALESCE(hp2_electrical_energy_counter,0) AS hp_electrical_energy_counter,
    COALESCE(hp1_thermal_energy_counter,0)
		+ COALESCE(hp2_thermal_energy_counter,0) AS hp_thermal_energy_counter,
	cv_energy_counter
FROM pre_update_counters
WHERE `time` <= offset_time;

-- insert post update counters
INSERT INTO cic_counters (
	cic_id,
    `time`,
    hp1_electrical_energy_counter,
    hp1_thermal_energy_counter,
    hp2_electrical_energy_counter,
    hp2_thermal_energy_counter,
    hp_electrical_energy_counter,
    hp_thermal_energy_counter,
    cv_energy_counter)

WITH post_update_counters AS (
		SELECT 
			d.cic_id,
			d.`time`,
            o.`time` as offset_time,
			OLD_hp1_electrical_energy_counter AS hp1_electrical_energy_counter,
			OLD_hp2_electrical_energy_counter AS hp2_electrical_energy_counter,
			OLD_hp1_thermal_energy_counter AS hp1_thermal_energy_counter,
			OLD_hp2_thermal_energy_counter AS hp2_thermal_energy_counter,
			OLD_cv_energy_counter AS cv_energy_counter
		FROM cic_data d
			LEFT JOIN cic_counter_offset o ON d.cic_id=o.cic_id
		WHERE d.cic_id = 'CIC-17fcd27d-dbd7-561c-887e-faf59bb9ebeb'
)
SELECT cic_id,
	`time`,
    hp1_electrical_energy_counter,
    hp1_thermal_energy_counter,
    hp2_electrical_energy_counter,
    hp2_thermal_energy_counter,
    COALESCE(hp1_electrical_energy_counter,0) 
		+ COALESCE(hp2_electrical_energy_counter,0) AS hp_electrical_energy_counter,
    COALESCE(hp1_thermal_energy_counter,0)
		+ COALESCE(hp2_thermal_energy_counter,0) AS hp_thermal_energy_counter,
	cv_energy_counter
FROM post_update_counters
WHERE `time` > offset_time;


-- UPDATE cic_data e
-- JOIN (
-- 		SELECT 
-- 			d.cic_id,
-- 			d.`time`,
-- 			SUM(hp1_energy_consumed) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - COALESCE(hp1_energy_consumed_offset, 0) AS hp1_electrical_energy_counter,
-- 			SUM(hp2_energy_consumed) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - COALESCE(hp2_energy_consumed_offset, 0) AS hp2_electrical_energy_counter,
-- 			SUM(hp1_heat_generated) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - COALESCE(hp1_heat_generated_offset, 0) AS hp1_thermal_energy_counter,
-- 			SUM(hp2_heat_generated) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - COALESCE(hp2_heat_generated_offset, 0) AS hp2_thermal_energy_counter,
-- 			SUM(boiler_heat_generated) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - COALESCE(boiler_heat_generated_offset, 0) AS cv_energy_counter
-- 		FROM cic_data d
-- 			LEFT JOIN cic_counter_offset o ON d.cic_id=o.cic_id
-- ) AS fc ON e.cic_id=fc.cic_id AND e.`time`=fc.`time`
-- SET e.hp1_electrical_energy_counter = fc.hp1_electrical_energy_counter,
--     e.hp1_thermal_energy_counter = fc.hp1_thermal_energy_counter,
--     e.hp2_electrical_energy_counter = fc.hp2_electrical_energy_counter,
--     e.hp2_thermal_energy_counter = fc.hp2_thermal_energy_counter,
--     e.cv_energy_counter = fc.cv_energy_counter;
 
-- UPDATE cic_data
-- SET 
-- 	hp_electrical_energy_counter = COALESCE(hp1_electrical_energy_counter,0) + COALESCE(hp2_electrical_energy_counter,0),
--     hp_thermal_energy_counter = COALESCE(hp1_thermal_energy_counter,0) + COALESCE(hp2_thermal_energy_counter,0);