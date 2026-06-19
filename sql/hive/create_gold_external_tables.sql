-- Gold external tables
-- Generated from existing Parquet datasets.
-- Do not edit data files from Hive; these are external lakehouse tables.

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`audit_dataset_summary` (
  `dataset_summary_key` STRING,
  `layer_name` STRING,
  `dataset_name` STRING,
  `resource_key` STRING,
  `total_checks` INT,
  `pass_count` INT,
  `warning_count` INT,
  `fail_count` INT,
  `error_count` INT,
  `completeness_score` DOUBLE,
  `validity_score` DOUBLE,
  `conformity_score` DOUBLE,
  `quality_score` DOUBLE,
  `row_count` DOUBLE,
  `null_percentage` DOUBLE,
  `duplicate_rows` DOUBLE,
  `last_checked_at_utc` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/audit_dataset_summary';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`audit_integration_coverage` (
  `coverage_scope` STRING,
  `source_name` STRING,
  `metric_name` STRING,
  `metric_value` DOUBLE,
  `total_records` DOUBLE,
  `matched_records` DOUBLE,
  `unmatched_records` DOUBLE,
  `match_rate` DOUBLE,
  `issue_count` DOUBLE,
  `issue_rate` DOUBLE,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/audit_integration_coverage';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`audit_quality_results` (
  `quality_result_key` STRING,
  `layer_name` STRING,
  `dataset_name` STRING,
  `resource_key` STRING,
  `check_name` STRING,
  `rule_name` STRING,
  `rule_category` STRING,
  `severity` STRING,
  `status` STRING,
  `message` STRING,
  `metric_name` STRING,
  `metric_value` DOUBLE,
  `expected_value` STRING,
  `actual_value` STRING,
  `checked_at_utc` STRING,
  `source_file_path` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/audit_quality_results';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_geography` (
  `geography_key` STRING,
  `ubigeo6` STRING,
  `ccdd` STRING,
  `ccpp` STRING,
  `ccdi` STRING,
  `departamento_nombre` STRING,
  `provincia_nombre` STRING,
  `distrito_nombre` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/dim_geography';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_municipality` (
  `municipality_key` STRING,
  `ubigeo6` STRING,
  `geography_key` STRING,
  `idmunici` STRING,
  `municipalidad_nombre` STRING,
  `tipomuni_codigo` STRING,
  `tipomuni_nombre` STRING,
  `tipo_clasificacion_municipal` STRING,
  `ambito_municipal` STRING,
  `descripcion_tipo` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/dim_municipality';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_renamu_context` (
  `municipality_key` STRING,
  `ubigeo6` STRING,
  `total_computadoras_operativas` INT,
  `cuenta_servicio_internet` BOOLEAN,
  `computadoras_con_acceso_internet` INT,
  `tipo_conexion_internet_codigo` STRING,
  `tipo_conexion_internet_nombre` STRING,
  `usa_siaf` BOOLEAN,
  `usa_sistema_recaudacion_tributaria_municipal` BOOLEAN,
  `usa_sistema_rentas_administracion_tributaria` BOOLEAN,
  `usa_sistema_catastro` BOOLEAN,
  `no_tiene_sistemas_gestion` BOOLEAN,
  `portal_transparencia_estado_codigo` STRING,
  `portal_transparencia_estado_nombre` STRING,
  `tiene_portal_transparencia` BOOLEAN,
  `portal_transparencia_actualizado` BOOLEAN,
  `portal_transparencia_url` STRING,
  `total_personal_dic_2021` INT,
  `total_personal_mar_2022` INT,
  `tiene_personal_locacion_servicios` BOOLEAN,
  `personal_locacion_total_dic_2021` INT,
  `personal_locacion_total_mar_2022` INT,
  `tiene_personal_discapacidad` BOOLEAN,
  `personal_discapacidad_total_dic_2021` INT,
  `personal_discapacidad_total_mar_2022` INT,
  `acepta_pago_efectivo_ventanilla` BOOLEAN,
  `acepta_pago_tarjeta_ventanilla` BOOLEAN,
  `acepta_pago_web_en_linea` BOOLEAN,
  `acepta_otro_medio_pago` BOOLEAN,
  `tiene_personal_exclusivo_administracion_tributaria` BOOLEAN,
  `personal_admin_tributaria_dic_2021` INT,
  `personal_admin_tributaria_mar_2022` INT,
  `tiene_area_ejecucion_coactiva` BOOLEAN,
  `requiere_asistencia_administracion_tributaria` BOOLEAN,
  `requiere_asistencia_catastro` BOOLEAN,
  `requiere_capacitacion_administracion_tributaria` BOOLEAN,
  `requiere_capacitacion_catastro` BOOLEAN,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/dim_renamu_context';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_sismepre_period` (
  `sismepre_period_key` STRING,
  `anio_aplicacion` INT,
  `periodo` INT,
  `anio_estadistica` INT,
  `mes_estadistica` INT,
  `periodo_estadistica_tipo` STRING,
  `is_annual_stat_period` BOOLEAN,
  `periodo_label` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/dim_sismepre_period';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_time` (
  `date_key` INT,
  `fecha_mes` DATE,
  `anio` INT,
  `mes` INT,
  `anio_mes` STRING,
  `trimestre` INT,
  `semestre` INT,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/dim_time';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`fact_predial_statistics` (
  `municipality_key` STRING,
  `sismepre_period_key` STRING,
  `sec_ejec` STRING,
  `ubigeo6` STRING,
  `formulario_id` INT,
  `monto_emision_predial_total` DECIMAL(18,4),
  `monto_recaudacion_predial_total` DECIMAL(18,4),
  `monto_saldo_predial_total` DECIMAL(18,4),
  `ratio_recaudacion_emision` DECIMAL(18,8),
  `numero_predios_total` INT,
  `numero_contribuyentes_predio` INT,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/fact_predial_statistics';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`fact_siaf_income` (
  `municipality_key` STRING,
  `sec_ejec` STRING,
  `date_key` INT,
  `source_resource_key` STRING,
  `source_granularity` STRING,
  `monto_pia` DECIMAL(18,4),
  `monto_pim` DECIMAL(18,4),
  `monto_recaudado` DECIMAL(18,4),
  `has_municipality_match` BOOLEAN,
  `match_status` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/fact_siaf_income';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`mart_municipal_context` (
  `municipality_key` STRING,
  `ubigeo6` STRING,
  `municipalidad_nombre` STRING,
  `geography_key` STRING,
  `departamento_nombre` STRING,
  `provincia_nombre` STRING,
  `distrito_nombre` STRING,
  `tipomuni_codigo` STRING,
  `tipomuni_nombre` STRING,
  `tipo_clasificacion_municipal` STRING,
  `ambito_municipal` STRING,
  `descripcion_tipo` STRING,
  `total_computadoras_operativas` INT,
  `cuenta_servicio_internet` BOOLEAN,
  `computadoras_con_acceso_internet` INT,
  `tipo_conexion_internet_codigo` STRING,
  `tipo_conexion_internet_nombre` STRING,
  `usa_siaf` BOOLEAN,
  `usa_sistema_recaudacion_tributaria_municipal` BOOLEAN,
  `usa_sistema_rentas_administracion_tributaria` BOOLEAN,
  `usa_sistema_catastro` BOOLEAN,
  `no_tiene_sistemas_gestion` BOOLEAN,
  `portal_transparencia_estado_codigo` STRING,
  `portal_transparencia_estado_nombre` STRING,
  `tiene_portal_transparencia` BOOLEAN,
  `portal_transparencia_actualizado` BOOLEAN,
  `portal_transparencia_url` STRING,
  `total_personal_dic_2021` INT,
  `total_personal_mar_2022` INT,
  `tiene_personal_locacion_servicios` BOOLEAN,
  `personal_locacion_total_dic_2021` INT,
  `personal_locacion_total_mar_2022` INT,
  `tiene_personal_discapacidad` BOOLEAN,
  `personal_discapacidad_total_dic_2021` INT,
  `personal_discapacidad_total_mar_2022` INT,
  `acepta_pago_efectivo_ventanilla` BOOLEAN,
  `acepta_pago_tarjeta_ventanilla` BOOLEAN,
  `acepta_pago_web_en_linea` BOOLEAN,
  `acepta_otro_medio_pago` BOOLEAN,
  `tiene_personal_exclusivo_administracion_tributaria` BOOLEAN,
  `personal_admin_tributaria_dic_2021` INT,
  `personal_admin_tributaria_mar_2022` INT,
  `tiene_area_ejecucion_coactiva` BOOLEAN,
  `requiere_asistencia_administracion_tributaria` BOOLEAN,
  `requiere_asistencia_catastro` BOOLEAN,
  `requiere_capacitacion_administracion_tributaria` BOOLEAN,
  `requiere_capacitacion_catastro` BOOLEAN,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/mart_municipal_context';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`mart_municipal_revenue_overview` (
  `municipality_key` STRING,
  `ubigeo6` STRING,
  `municipalidad_nombre` STRING,
  `geography_key` STRING,
  `departamento_nombre` STRING,
  `provincia_nombre` STRING,
  `distrito_nombre` STRING,
  `tipomuni_codigo` STRING,
  `tipomuni_nombre` STRING,
  `tipo_clasificacion_municipal` STRING,
  `ambito_municipal` STRING,
  `descripcion_tipo` STRING,
  `date_key` INT,
  `fecha_mes` DATE,
  `anio` INT,
  `mes` INT,
  `anio_mes` STRING,
  `trimestre` INT,
  `semestre` INT,
  `sec_ejec` STRING,
  `source_resource_key` STRING,
  `source_granularity` STRING,
  `monto_pia` DECIMAL(18,4),
  `monto_pim` DECIMAL(18,4),
  `monto_recaudado` DECIMAL(18,4),
  `has_municipality_match` BOOLEAN,
  `match_status` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/mart_municipal_revenue_overview';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`mart_predial_statistics_overview` (
  `municipality_key` STRING,
  `ubigeo6` STRING,
  `municipalidad_nombre` STRING,
  `geography_key` STRING,
  `departamento_nombre` STRING,
  `provincia_nombre` STRING,
  `distrito_nombre` STRING,
  `tipomuni_codigo` STRING,
  `tipomuni_nombre` STRING,
  `tipo_clasificacion_municipal` STRING,
  `ambito_municipal` STRING,
  `descripcion_tipo` STRING,
  `sismepre_period_key` STRING,
  `anio_aplicacion` INT,
  `periodo` INT,
  `anio_estadistica` INT,
  `mes_estadistica` INT,
  `periodo_estadistica_tipo` STRING,
  `is_annual_stat_period` BOOLEAN,
  `periodo_label` STRING,
  `sec_ejec` STRING,
  `formulario_id` INT,
  `monto_emision_predial_total` DECIMAL(18,4),
  `monto_recaudacion_predial_total` DECIMAL(18,4),
  `monto_saldo_predial_total` DECIMAL(18,4),
  `ratio_recaudacion_emision` DECIMAL(18,8),
  `numero_predios_total` INT,
  `numero_contribuyentes_predio` INT,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/mart_predial_statistics_overview';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`mart_territorial_summary` (
  `geography_key` STRING,
  `departamento_nombre` STRING,
  `provincia_nombre` STRING,
  `distrito_nombre` STRING,
  `tipo_clasificacion_municipal` STRING,
  `ambito_municipal` STRING,
  `total_municipalidades` BIGINT,
  `municipalidades_con_siaf` INT,
  `municipalidades_con_sistema_recaudacion` INT,
  `municipalidades_con_catastro` INT,
  `municipalidades_con_internet` INT,
  `total_computadoras_operativas` BIGINT,
  `total_personal_dic_2021` BIGINT,
  `total_personal_mar_2022` BIGINT,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/mart_territorial_summary';
