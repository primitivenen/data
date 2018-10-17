use guobiao_tsp_tbls;
drop table if exists ge3_filter_result purge;
create table ge3_filter_result as
SELECT vin, max_volt, min_volt, min_insulation, max_insulation, avg_insulation
FROM (
	SELECT vin, MAX(data_batt_sc_volt_highest) as max_volt, MIN(data_batt_sc_volt_lowest) as min_volt, min(veh_insulation) as min_insulation, 
	max(veh_insulation) as max_insulation, avg(veh_insulation) as avg_insulation
	FROM ge3_filter_all
	GROUP BY vin
) a
ORDER BY vin;
