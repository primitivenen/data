use ag_source;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.created.files=10000000000;
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint=false;

--create table if not exists pre_saving_enviromental_factor
--(
--	fuel_efficiency_mean			double, 
--	distance_on_fuel 				int,
--	distance_driven 				int,
--	elec_energy_efficiency 			double, 
--	distance_on_battery				int,
--	cum_fuel_consumption			double,
--	cum_elec_energy_consumption		double,
--	vin 							string
--)partitioned by (day date);
--
--insert into table pre_saving_enviromental_factor partition(day) 
--select (case when fuel_efficiency_mean is null or fuel_efficiency_mean='NULL' then 0 else fuel_efficiency_mean end) as fuel_efficiency_mean,
--	   (case when distance_on_fuel is null or distance_on_fuel='NULL' then 0 else distance_on_fuel end) as distance_on_fuel,
--	   (case when distance_driven is null or distance_driven='NULL' then 0 else distance_driven end) as distance_driven,
--	   (case when elec_energy_efficiency is null or elec_energy_efficiency='NULL' then 0 else elec_energy_efficiency end) as elec_energy_efficiency,
--	   (case when distance_on_battery is null or distance_on_battery='NULL' then 0 else distance_on_battery end) as distance_on_battery,
--	   (case when cum_fuel_consumption is null or cum_fuel_consumption='NULL' then 0 else cum_fuel_consumption end) as cum_fuel_consumption,
--	   (case when cum_elec_energy_consumption is null or cum_elec_energy_consumption='NULL' then 0 else cum_elec_energy_consumption end) as cum_elec_energy_consumption, 
--	   vin,day from daily_stats;
--create table if not exists saving_enviromental_factor
--(
--	saving_factor			double,
--	enviromental_factor		double,
--	vin						string
--) partitioned by (day date);
--
--insert into table saving_enviromental_factor partition(day) select (fuel_efficiency_mean*(distance_on_fuel/distance_driven)+elec_energy_efficiency/33.7*3.78541*(distance_on_battery/distance_driven)) as saving_factor,(cum_fuel_consumption*(distance_on_fuel/distance_driven)+cum_elec_energy_consumption/33.7*3.78541*(distance_on_battery/distance_driven)) as enviromental_factor,vin,day from pre_saving_enviromental_factor where distance_driven<>0;
--select fuel_efficiency_mean,distance_on_fuel,distance_driven,elec_energy_efficiency,distance_on_battery from daily_stats where day='2015-01-18' and vin='LMGGN1S53E1000035';

create table if not exists saving_enviromental_factor
(
	saving_fuel_factor		double,
	saving_batt_factor		double,
	enviromental_factor		double,
	vin						string
) partitioned by (day date);
insert into table saving_enviromental_factor partition(day) select fuel_efficiency_mean as saving_fuel_factor,elec_energy_efficiency/33.7*3.78541 as saving_batt_factor,(cum_fuel_consumption+cum_elec_energy_consumption/33.7*3.78541) as enviromental_factor,vin,day from pre_saving_enviromental_factor;
