use guobiao_tsp_tbls;
drop table if exists ge3_filter_all purge;
create table ge3_filter_all as
SELECT vin, data_batt_sc_volt_highest, data_batt_sc_volt_lowest, veh_insulation 
FROM (
	SELECT vin, day, ts, CASE WHEN data_batt_sc_volt_highest>=5 THEN NULL ELSE data_batt_sc_volt_highest END AS data_batt_sc_volt_highest, 
          CASE WHEN data_batt_sc_volt_lowest<=1 or data_batt_sc_volt_lowest>=5 THEN NULL ELSE data_batt_sc_volt_lowest END AS data_batt_sc_volt_lowest, CASE WHEN veh_insulation=0 THEN NULL ELSE veh_insulation END AS veh_insulation
	FROM guobiao_raw_orc
	WHERE vintype = 'A5HEV'	AND day >= from_unixtime(unix_timestamp(date_sub(current_date, 30), 'yyyy-MM-dd'),'yyyyMMdd')
) a
ORDER BY vin;
