-- TO DO: replace hp1_energy_consumed with the old counter to calculate offset

INSERT INTO cic_counter_offset (
	cic_id,
    `time`,
    hp1_energy_consumed_offset,
    hp1_heat_generated_offset,
    hp2_energy_consumed_offset,
    hp2_heat_generated_offset,
    boiler_heat_generated_offset
)
with cte_update_time AS(
	SELECT distinct cic_id,
		MIN(`time`) as 'update_time'
	FROM cic_data
	WHERE quattBuild like '2.0.1%'
),
cte_energy_time AS (
	SELECT d.cic_id,
		max(if(`time`<`update_time`,`time`,'')) as offset_time
	FROM cic_data d 
		inner join cte_update_time c1 on c1.cic_id=d.cic_id
	GROUP BY d.cic_id
)
SELECT 
	d.cic_id,
	offset_time,
	SUM(if(`time`=offset_time,OLD_hp1_electrical_energy_counter,0)) - SUM(if(`time`<=offset_time,hp1_energy_consumed,0)) as hp1_energy_consumed_offset,
	SUM(if(`time`=offset_time,OLD_hp1_thermal_energy_counter,0)) - SUM(if(`time`<=offset_time,hp1_heat_generated,0)) as hp1_heat_generated_offset,
	SUM(if(`time`=offset_time,OLD_hp2_electrical_energy_counter,0)) - SUM(if(`time`<=offset_time,hp2_energy_consumed,0)) as hp2_energy_consumed_offset,
	SUM(if(`time`=offset_time,OLD_hp2_thermal_energy_counter,0)) - SUM(if(`time`<=offset_time,hp2_heat_generated,0)) as hp2_heat_generated_offset,
	SUM(if(`time`=offset_time,OLD_cv_energy_counter,0)) - SUM(if(`time`<=offset_time,boiler_heat_generated,0)) as boiler_heat_generated_offset
FROM cic_data d
	INNER JOIN cte_energy_time t on d.cic_id=t.cic_id
group by d.cic_id


-- INSERT INTO cic_counter_offset (
-- 	cic_id,
--     `time`,
--     hp1_energy_consumed_offset,
--     hp1_heat_generated_offset,
--     hp2_energy_consumed_offset,
--     hp2_heat_generated_offset,
--     boiler_heat_generated_offset
-- )

-- with cte_update_time AS(
-- 	SELECT distinct cic_id,
-- 		MIN(`time`) as 'update_time'
-- 	FROM cic_data
-- 	WHERE quattBuild like '2.0.1%'
-- ),
-- cte_energy_time AS (
-- 	SELECT d.cic_id,
-- 		max(if(`time`<`update_time`,`time`,'')) as offset_time
-- 	FROM cic_data d 
-- 		inner join cte_update_time c1 on c1.cic_id=d.cic_id
-- 	GROUP BY d.cic_id
-- )
-- SELECT 
-- 	d.cic_id,
-- 	offset_time,
-- 	SUM(if(`time`=offset_time,hp1_energy_consumed,0)) - SUM(if(`time`<=offset_time,hp1_energy_consumed,0)) as hp1_energy_consumed_offset,
-- 	SUM(if(`time`=offset_time,hp1_heat_generated,0)) - SUM(if(`time`<=offset_time,hp1_heat_generated,0)) as hp1_heat_generated_offset,
-- 	SUM(if(`time`=offset_time,hp2_energy_consumed,0)) - SUM(if(`time`<=offset_time,hp2_energy_consumed,0)) as hp2_energy_consumed_offset,
-- 	SUM(if(`time`=offset_time,hp2_heat_generated,0)) - SUM(if(`time`<=offset_time,hp2_heat_generated,0)) as hp2_heat_generated_offset,
-- 	SUM(if(`time`=offset_time,boiler_heat_generated,0)) - SUM(if(`time`<=offset_time,boiler_heat_generated,0)) as boiler_heat_generated_offset
-- FROM cic_data d
-- 	INNER JOIN cte_energy_time t on d.cic_id=t.cic_id
-- group by d.cic_id