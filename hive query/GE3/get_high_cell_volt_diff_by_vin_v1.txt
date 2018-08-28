drop table if exists ge3_high_cell_volt_diff_by_vin purge;
create table ge3_high_cell_volt_diff_by_vin as
SELECT vin, counts, first_timestamp, last_timestamp, 
min_cell_volt_diff, max_cell_volt_diff, avg_cell_volt_diff, std_cell_volt_diff, avg_speed, avg_temp_min, avg_temp_max
FROM (
	SELECT vin, COUNT(time_stamp) as counts, MAX(time_stamp) as last_timestamp, MIN(time_stamp) as first_timestamp, min(cell_volt_diff) as min_cell_volt_diff, 
	max(cell_volt_diff) as max_cell_volt_diff, avg(cell_volt_diff) as avg_cell_volt_diff, stddev_pop(cell_volt_diff) as std_cell_volt_diff,
  	avg(veh_spd) as avg_speed, avg(temp_min) as avg_temp_min, avg(temp_max) as avg_temp_max
	FROM ge3_high_cell_volt_diff_records
	GROUP BY vin
) a
WHERE counts > 2
ORDER BY counts DESC, vin;