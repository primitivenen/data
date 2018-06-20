DROP TABLE IF EXISTS tsp_tbls.ag2_vehicle_stats PURGE;
CREATE EXTERNAL TABLE tsp_tbls.ag2_vehicle_stats(
	VIN string,
	error_reported int,
	num_records int,
	start_time string,
	end_time string,
	num_days int,
	num_days_active int,
	city_last_day string,
	prov_last_day string,
	start_odo int,
	end_odo int,
	total_distance int,
	total_distance_on_battery int,
	total_distance_on_fuel int,
	daily_dist_avg double,
	daily_dist_max int,
	daily_dist_p99 double,
	daily_batt_dist_avg double,
	daily_batt_dist_max int,
	daily_batt_dist_p99 double,
	daily_fuel_dist_avg double,
	daily_fuel_dist_max int,
	daily_fuel_dist_p99 double,
	total_active_hours double,
	total_charge_hours double,
	total_drive_hours double,
	charge_percent double,
	daily_charge_hours_avg double,
	daily_acc_pos_avg double,
	total_charge_energy_in_kwh double,
	daily_charge_energy_avg_in_kwh double,
	recent_battery_temp_avg_p25 double,
	recent_battery_temp_avg_p75 double,
	recent_battery_temp_max_p25 int,
	recent_battery_temp_max_p75 int,
	recent_battery_temp_min_p25 int,
	recent_battery_temp_min_p75 int,
	recent_battery_SOC_p25 double,
	recent_battery_SOC_p75 double,
	total_battery_curr_drive_sum double,
	total_battery_curr_drive_abs_sum double,
	total_battery_curr_charge_sum double,
	total_battery_curr_charge_abs_sum double,
	total_battery_curr_sum double,
	total_battery_curr_abs_sum double,
	total_battery_energy_throughpt_in_kwh double,
	total_battery_energy_received_in_kwh double,
	total_battery_energy_consumption_in_kwh double,
	total_energy_consumption_on_battery double,
	total_electricity_energy_efficiency double,
	total_fuel_consumption_in_l double,
	total_fuel_efficiency double,
	life_start_odo int,
	battery_changed int,
	battery_life_in_days int,
	battery_life_in_km int,
	battery_life_in_charge_hours double,
	battery_life_in_drive_hours double,
	battery_life_energy_total_sum double,
	battery_life_energy_discharge_sum double,
	battery_life_energy_discharge_on_battery_sum double,
	battery_life_energy_plugin_charge_sum double,
	battery_life_curr_discharge_in_Ah double,
	battery_life_curr_abs_in_Ah double,
	battery_life_dist_on_battery int,
	battery_life_dist_on_fuel int,
	battery_changed_date string,
	battery_life_fuel_consumption double,
	cell_volt_diff_drive_mean_now double,
	cell_volt_diff_drifted int,
	cell_volt_diff_slope_alerted int,
	cell_volt_diff_pred_alerted int,
	total_alerts_now double,
	odo_index int,
	charge_index int,
	alert_index int,
	normalized_dist_on_battery double,
	seg_battery_drive_energy_capacity_in_kwh double,
	seg_battery_drive_curr_capacity_in_Ah double,
	seg_battery_charge_energy_capacity_in_kwh double,
	seg_battery_charge_curr_capacity_in_Ah double,
	new_seg_battery_drive_energy_capacity_in_kwh double,
	new_seg_battery_drive_curr_capacity_in_Ah double,
	new_seg_battery_charge_energy_capacity_in_kwh double,
	new_normalized_dist_on_battery double,
	new_seg_battery_charge_curr_capacity_in_Ah double,
	curr_capacity_drive_SOH double,
	curr_capacity_charge_SOH double,
	BMS_Updated string,
    BMS_Update_Date string,
    battery_usage_high int
)
PARTITIONED BY (day date)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/ag2_vehicle_stats';

SELECT * FROM tsp_tbls.ag2_vehicle_stats LIMIT 10;
