-- TO DO: Adapt query to only sum for software version up to 2.0.4 (exclusive)

UPDATE cic_data e
JOIN (
		SELECT 
			d.cic_id,
			d.`time`,
			SUM(hp1_energy_consumed) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - coalesce(hp1_energy_consumed_offset, 0) AS hp1_electrical_energy_counter,
			SUM(hp2_energy_consumed) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - coalesce(hp2_energy_consumed_offset, 0) AS hp2_electrical_energy_counter,
			SUM(hp1_heat_generated) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - coalesce(hp1_heat_generated_offset, 0) AS hp1_thermal_energy_counter,
			SUM(hp2_heat_generated) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - coalesce(hp2_heat_generated_offset, 0) AS hp2_thermal_energy_counter,
			SUM(boiler_heat_generated) OVER (PARTITION BY d.cic_id ORDER BY d.cic_id, d.`time`) - coalesce(boiler_heat_generated_offset, 0) AS cv_energy_counter
		FROM cic_data d
			LEFT JOIN cic_counter_offset o on d.cic_id=o.cic_id and d.`time`=o.`time`
) AS fc on e.cic_id=fc.cic_id AND e.`time`=fc.`time`
SET e.hp1_electrical_energy_counter = fc.hp1_electrical_energy_counter,
    e.hp1_thermal_energy_counter = fc.hp1_thermal_energy_counter,
    e.hp2_electrical_energy_counter = fc.hp2_electrical_energy_counter,
    e.hp2_thermal_energy_counter = fc.hp2_thermal_energy_counter,
    e.cv_energy_counter = fc.cv_energy_counter;
 
UPDATE cic_data
SET 
	hp_electrical_energy_counter = COALESCE(hp1_electrical_energy_counter,0) + COALESCE(hp2_electrical_energy_counter,0),
    hp_thermal_energy_counter = COALESCE(hp1_thermal_energy_counter,0) + COALESCE(hp2_thermal_energy_counter,0);