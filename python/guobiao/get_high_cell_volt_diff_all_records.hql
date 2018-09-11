drop table if exists ge3_high_cell_volt_diff_records purge;
create table ge3_high_cell_volt_diff_records as
SELECT vin, day, from_utc_timestamp(to_utc_timestamp(from_unixtime(bigint(ts/1000)), "America/Los_Angeles"), "Asia/Shanghai") as time_stamp, 
veh_soc, veh_curr, cell_volt_diff, temp_min, temp_max, veh_charge_st, veh_spd, veh_acc, veh_brake, 
veh_gear, veh_insulation, veh_odo, veh_volt, loc_st, veh_longitude, veh_latitude
FROM (
	SELECT vin, day, ts, veh_curr, veh_charge_st, 
  	data_batt_sc_volt_highest - data_batt_sc_volt_lowest as cell_volt_diff, data_batt_temp_highest as temp_max,
  	data_batt_temp_lowest as temp_min, veh_spd, veh_odo, veh_volt, veh_soc, loc_st, loc_lon as veh_longitude, loc_lat as veh_latitude,
  	veh_gear, veh_insulation, veh_acce_pedal as veh_acc, veh_brake_pedal as veh_brake
	FROM guobiao_raw_orc
	WHERE vintype = 'A5HEV'	AND day >= from_unixtime(unix_timestamp(date_sub(current_date, 30), 'yyyy-MM-dd'),'yyyyMMdd')
  	AND abs(veh_curr) < 50 AND veh_soc > 9 AND data_batt_temp_lowest >= 15
) a
WHERE cell_volt_diff between 0.1 and 1
ORDER BY vin, time_stamp;