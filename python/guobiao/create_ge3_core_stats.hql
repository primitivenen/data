use guobiao_tsp_tbls;
DROP TABLE IF EXISTS guobiao_tsp_tbls.ge3_core_stats PURGE;
create table ge3_core_stats as 
select b.vin as vin, cast(a.max_volt as decimal(10,3)) as max_volt, 
	cast(a.min_volt as decimal(10,3)) as min_volt, a.max_insulation, a.min_insulation, 
	cast(a.avg_insulation as decimal(10,3)) as avg_insulation,
	cast(b.battery_temp_max_charging as decimal(10,1)) as battery_temp_max_charging, 
	cast(b.battery_temp_max_driving as decimal(10,1)) as battery_temp_max_driving, 
	cast(b.battery_temp_mean_charging as decimal(10,1)) as battery_temp_mean_charging, 
	cast(b.battery_temp_mean_driving as decimal(10,1)) as battery_temp_mean_driving,
	cast(b.battery_temp_min_charging as decimal(10,1)) as battery_temp_min_charging, 
	cast(b.battery_temp_min_driving as decimal(10,1)) as battery_temp_min_driving, 
	cast(b.cell_volt_diff_mean_charging as decimal(10,4)) as cell_volt_diff_mean_charging, 
	cast(b.cell_volt_diff_mean_driving as decimal(10,4)) as cell_volt_diff_mean_driving, 
	cast(b.normalized_ah_charged_mean as decimal(10,1)) as normalized_ah_charged_mean, 
	cast(b.normalized_ah_consumption_mean as decimal(10,1)) as normalized_ah_consumption_mean, 
	cast(b.normalized_distance_driven_mean as decimal(10,1)) as normalized_distance_driven_mean, b.end_date as day	
from ge3_filter_result a full outer join vin_ranking b
on a.vin=b.vin 
where b.end_date in (select CASE WHEN max(end_date)=date_sub(current_date, 1) THEN max(end_date) ELSE NULL END   from vin_ranking)
