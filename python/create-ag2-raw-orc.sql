USE tsp_tbls;
DROP TABLE IF EXISTS ag2_raw_orc PURGE;
CREATE TABLE `ag2_raw_orc`(
  `vin` string, 
  `tdate` bigint, 
  `sdate1` string, 
  `bms_battst` int, 
  `bms_battcurr` float, 
  `bms_battvolt` int, 
  `bms_insulationst` int, 
  `bms_insulationres` int, 
  `bms_cellvoltmax` float, 
  `bms_cellvoltmin` float, 
  `bms_failurelvl` int, 
  `bms_battsoc` float, 
  `bms_batttempavg` float, 
  `bms_batttempmax` float, 
  `bms_batttempmin` float, 
  `ccs_chargevolt` float, 
  `ccs_chargecur` float, 
  `ccs_chargerstartst` int, 
  `vcu_sysfailmode` int, 
  `mcu_ftm_actrotspd` int, 
  `mcu_ftm_acttorq` float, 
  `mcu_ftm_stmode` int, 
  `mcu_ftm_motoracttemp` int, 
  `mcu_ftm_rotoracttemp` int, 
  `mcu_ftm_inverteracttemp` int, 
  `mcu_ftm_acthv_cur` int, 
  `mcu_ftm_acthv_volt` int, 
  `mcu_ftm_fault_info1` int, 
  `mcu_ftm_fault_info2` int, 
  `mcu_dcdc_failst` int, 
  `mcu_dcdc_stmode` int, 
  `vcu_dcdc_stmodelreq` int, 
  `bms_bat_error_soc_l` int, 
  `bms_bat_error_cell_v_h` int, 
  `bms_bat_error_cell_v_l` int, 
  `bms_bat_error_pack_sumv_h` int, 
  `bms_bat_error_pack_sumv_l` int, 
  `bms_bat_error_cell_t_h` int, 
  `bms_bat_error_t_unbalance` int, 
  `mcu_gm_failst` int, 
  `mcu_ftm_failst` int, 
  `mcu_ftm_fault_info3` int, 
  `mcu_gm_actrotspd` int, 
  `mcu_gm_acttorq` float, 
  `mcu_gm_stmode` int, 
  `mcu_gm_motoracttemp` int, 
  `mcu_gm_rotoracttemp` int, 
  `mcu_gm_inverteracttemp` int, 
  `mcu_gm_acthv_cur` int, 
  `mcu_gm_acthv_vol` int, 
  `ems_engtorq` float, 
  `ems_engspd` int, 
  `ems_accpedalpst` float, 
  `ems_brakepedalst` int, 
  `ems_engwatertemp` int, 
  `hcu_gearfordsp` int, 
  `hcu_oilpressurewarn` int, 
  `hcu_avgfuelconsump` float, 
  `hcu_batchrgdsp` int, 
  `bcs_vehspd` float, 
  `icm_totalodometer` int, 
  `bcm_keyst` int, 
  `hcu_dstoil` int, 
  `hcu_dstbat` int, 
  `ems_faultranksig` int, 
  `srs_crashoutputst` int, 
  `srs_driverseatbeltst` int, 
  `edc_sterrlvlcom` int, 
  `edc_sterrlvlcomsup` int, 
  `edb_sterrlvlhves` int, 
  `edg_sterrlvlgen` int, 
  `edm_sterrlvlmot` int, 
  `ede_sterrlvleng` int, 
  `edv_sterrlvlveh` int, 
  `lon84` float, 
  `lat84` float, 
  `lon02` float, 
  `lat02` float, 
  `bcs_absfaultst` int, 
  `bcs_ebdfaultst` int, 
  `mcu_dcdc_acttemp` float, 
  `bms_hvilst` int, 
  `hcu_hevsysreadyst` int, 
  `bms_balancest` int, 
  `gps_heading` float)
PARTITIONED BY (sdate string)
STORED AS ORC;