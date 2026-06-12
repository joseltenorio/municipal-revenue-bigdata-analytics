-- Gold external tables
-- Generated from existing Parquet datasets.
-- Do not edit data files from Hive; these are external lakehouse tables.

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_municipality` (
  `ubigeo` STRING,
  `sec_ejec` STRING,
  `departamento` STRING,
  `provincia` STRING,
  `distrito` STRING,
  `departamento_nombre` STRING,
  `provincia_nombre` STRING,
  `distrito_nombre` STRING,
  `municipalidad_nombre` STRING,
  `mapping_source` STRING,
  `is_valid_sec_ejec` BOOLEAN,
  `is_valid_ubigeo` BOOLEAN,
  `has_renamu_match` BOOLEAN,
  `idmunici` STRING,
  `departamento_normalizado` STRING,
  `provincia_normalizada` STRING,
  `distrito_normalizado` STRING,
  `tipomuni` STRING,
  `tipomuni_int` INT,
  `municipality_key` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/municipal_revenue/dim_municipality';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_time` (
  `anio` INT,
  `mes` INT,
  `is_annual_record` BOOLEAN,
  `period_key` STRING,
  `year_month_key` STRING,
  `period_label` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/municipal_revenue/dim_time';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`fact_municipal_income_execution` (
  `sec_ejec` STRING,
  `source_dataset` STRING,
  `silver_source_granularity` STRING,
  `anio` INT,
  `mes` INT,
  `nivel_gobierno` STRING,
  `sector` STRING,
  `pliego` STRING,
  `ejecutora` STRING,
  `fuente_financiamiento` STRING,
  `rubro` STRING,
  `tipo_recurso` STRING,
  `generica` STRING,
  `subgenerica` STRING,
  `subgenerica_det` STRING,
  `especifica` STRING,
  `especifica_det` STRING,
  `monto_pia_total` DECIMAL(30,4),
  `monto_pim_total` DECIMAL(30,4),
  `monto_recaudado_total` DECIMAL(30,4),
  `source_record_count` BIGINT,
  `bridge_ubigeo_count` BIGINT,
  `has_municipal_bridge` BOOLEAN,
  `ubigeo` STRING,
  `has_valid_ubigeo` BOOLEAN,
  `has_renamu_match` BOOLEAN,
  `integration_quality_status` STRING,
  `gold_processed_at_utc` STRING,
  `gold_grain` STRING,
  `recaudacion_vs_pim_ratio` DECIMAL(38,8),
  `recaudacion_vs_pia_ratio` DECIMAL(38,8),
  `pim_vs_pia_ratio` DECIMAL(38,8)
)
STORED AS PARQUET
LOCATION '/app/data/gold/municipal_revenue/fact_municipal_income_execution';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`fact_revenue_integration_coverage` (
  `metric_name` STRING,
  `numerator` BIGINT,
  `denominator` BIGINT,
  `coverage_percentage` DOUBLE,
  `description` STRING,
  `coverage_ratio` DOUBLE,
  `gold_processed_at_utc` STRING,
  `source_dataset` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/municipal_revenue/fact_revenue_integration_coverage';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`mart_municipal_revenue_overview` (
  `anio` INT,
  `mes` INT,
  `sec_ejec` STRING,
  `ubigeo` STRING,
  `has_municipal_bridge` BOOLEAN,
  `has_valid_ubigeo` BOOLEAN,
  `has_renamu_match` BOOLEAN,
  `integration_quality_status` STRING,
  `monto_pia_total` DECIMAL(38,4),
  `monto_pim_total` DECIMAL(38,4),
  `monto_recaudado_total` DECIMAL(38,4),
  `source_record_count` BIGINT,
  `recaudacion_vs_pim_ratio` DECIMAL(38,6),
  `recaudacion_vs_pia_ratio` DECIMAL(38,6),
  `pim_vs_pia_ratio` DECIMAL(38,6),
  `gold_processed_at_utc` STRING,
  `gold_grain` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/municipal_revenue/mart_municipal_revenue_overview';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_predial_period` (
  `ano_aplicacion` STRING,
  `periodo` STRING,
  `ano_estadistica` STRING,
  `mes_estadistica` STRING,
  `predial_period_key` STRING,
  `predial_period_label` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/predial_compliance/dim_predial_period';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`fact_predial_compliance` (
  `sec_ejec` STRING,
  `ano_aplicacion` STRING,
  `periodo` STRING,
  `ubigeo` STRING,
  `formulario_id` STRING,
  `ano_estadistica` STRING,
  `mes_estadistica` STRING,
  `source_dataset` STRING,
  `integration_grain` STRING,
  `source_record_count` BIGINT,
  `active_response_count` BIGINT,
  `mon_emisionpredial_afecto_decimal_total` DECIMAL(30,4),
  `mon_emisionpredial_exon_decimal_total` DECIMAL(30,4),
  `mon_baseimponible_afecto_decimal_total` DECIMAL(30,4),
  `mon_baseimponible_exon_decimal_total` DECIMAL(30,4),
  `mon_autoavaluo_inafecto_decimal_total` DECIMAL(30,4),
  `mon_recaudactual_ordin_decimal_total` DECIMAL(30,4),
  `mon_recaudactual_coac_decimal_total` DECIMAL(30,4),
  `mon_recaudanter_ordi_decimal_total` DECIMAL(30,4),
  `mon_recaudanter_coac_decimal_total` DECIMAL(30,4),
  `mon_inicialadultomayor_decimal_total` DECIMAL(30,4),
  `mon_predialadultomayor_decimal_total` DECIMAL(30,4),
  `mon_recuadadultomayor_decimal_total` DECIMAL(30,4),
  `mon_saldopredial_ord_decimal_total` DECIMAL(30,4),
  `mon_saldopredial_coac_decimal_total` DECIMAL(30,4),
  `mon_emisionpredial_inso_decimal_total` DECIMAL(30,4),
  `num_emisionpredial_afecto_decimal_total` DECIMAL(30,4),
  `num_emisionpredial_exon_decimal_total` DECIMAL(30,4),
  `num_emisionpredial_casa_decimal_total` DECIMAL(30,4),
  `num_emisionpredial_otros_decimal_total` DECIMAL(30,4),
  `num_contribadultomayor_decimal_total` DECIMAL(30,4),
  `num_inafectos_decimal_total` DECIMAL(30,4),
  `num_contripredio_decimal_total` DECIMAL(30,4),
  `num_prediousoch_decimal_total` DECIMAL(30,4),
  `num_prediootrouso_decimal_total` DECIMAL(30,4),
  `num_prediototal_decimal_total` DECIMAL(30,4),
  `bridge_ubigeo_count` BIGINT,
  `departamento` STRING,
  `provincia` STRING,
  `distrito` STRING,
  `departamento_nombre` STRING,
  `provincia_nombre` STRING,
  `distrito_nombre` STRING,
  `municipalidad_nombre` STRING,
  `has_municipal_bridge` BOOLEAN,
  `bridge_ubigeo` STRING,
  `has_valid_ubigeo` BOOLEAN,
  `has_renamu_match` BOOLEAN,
  `integration_quality_status` STRING,
  `effective_ubigeo` STRING,
  `gold_processed_at_utc` STRING,
  `gold_grain` STRING,
  `predial_collection_total` DECIMAL(34,4),
  `predial_issue_total` DECIMAL(32,4),
  `predial_balance_total` DECIMAL(31,4),
  `taxpayer_count_total` DECIMAL(31,4),
  `property_count_total` DECIMAL(32,4),
  `predial_effectiveness_ratio` DECIMAL(38,6)
)
STORED AS PARQUET
LOCATION '/app/data/gold/predial_compliance/fact_predial_compliance';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`fact_predial_integration_coverage` (
  `metric_name` STRING,
  `numerator` BIGINT,
  `denominator` BIGINT,
  `coverage_percentage` DOUBLE,
  `description` STRING,
  `coverage_ratio` DOUBLE,
  `source_dataset` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/predial_compliance/fact_predial_integration_coverage';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`mart_predial_compliance_overview` (
  `ano_aplicacion` STRING,
  `periodo` STRING,
  `sec_ejec` STRING,
  `effective_ubigeo` STRING,
  `departamento` STRING,
  `provincia` STRING,
  `distrito` STRING,
  `departamento_nombre` STRING,
  `provincia_nombre` STRING,
  `distrito_nombre` STRING,
  `municipalidad_nombre` STRING,
  `formulario_id` STRING,
  `ano_estadistica` STRING,
  `mes_estadistica` STRING,
  `has_municipal_bridge` BOOLEAN,
  `has_valid_ubigeo` BOOLEAN,
  `has_renamu_match` BOOLEAN,
  `integration_quality_status` STRING,
  `predial_collection_total` DECIMAL(38,4),
  `predial_issue_total` DECIMAL(38,4),
  `predial_balance_total` DECIMAL(38,4),
  `taxpayer_count_total` DECIMAL(38,4),
  `property_count_total` DECIMAL(38,4),
  `source_record_count` BIGINT,
  `active_response_count` BIGINT,
  `predial_effectiveness_ratio` DECIMAL(38,6),
  `gold_processed_at_utc` STRING,
  `gold_grain` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/predial_compliance/mart_predial_compliance_overview';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`mart_predial_ranking` (
  `ano_aplicacion` STRING,
  `periodo` STRING,
  `sec_ejec` STRING,
  `effective_ubigeo` STRING,
  `departamento` STRING,
  `provincia` STRING,
  `distrito` STRING,
  `municipalidad_nombre` STRING,
  `has_renamu_match` BOOLEAN,
  `integration_quality_status` STRING,
  `predial_collection_total` DECIMAL(38,4),
  `predial_issue_total` DECIMAL(38,4),
  `predial_balance_total` DECIMAL(38,4),
  `taxpayer_count_total` DECIMAL(38,4),
  `property_count_total` DECIMAL(38,4),
  `predial_effectiveness_ratio` DECIMAL(38,6),
  `collection_rank_desc` INT,
  `collection_rank_asc` INT,
  `effectiveness_rank_desc` INT,
  `balance_rank_desc` INT,
  `is_top_collection_candidate` BOOLEAN,
  `is_bottom_collection_candidate` BOOLEAN,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/predial_compliance/mart_predial_ranking';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_geography` (
  `ubigeo` STRING,
  `ccdd` STRING,
  `ccpp` STRING,
  `ccdi` STRING,
  `departamento` STRING,
  `provincia` STRING,
  `distrito` STRING,
  `departamento_normalizado` STRING,
  `provincia_normalizada` STRING,
  `distrito_normalizado` STRING,
  `has_complete_territory` BOOLEAN,
  `is_valid_ubigeo` BOOLEAN,
  `department_key` STRING,
  `province_key` STRING,
  `district_key` STRING,
  `geography_key` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/territorial_context/dim_geography';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`dim_municipality_context` (
  `ubigeo` STRING,
  `anio` INT,
  `idmunici` STRING,
  `ccdd` STRING,
  `ccpp` STRING,
  `ccdi` STRING,
  `departamento` STRING,
  `provincia` STRING,
  `distrito` STRING,
  `departamento_normalizado` STRING,
  `provincia_normalizada` STRING,
  `distrito_normalizado` STRING,
  `tipomuni` STRING,
  `tipomuni_int` INT,
  `is_valid_ubigeo` BOOLEAN,
  `has_complete_territory` BOOLEAN,
  `has_municipal_identifier` BOOLEAN,
  `is_valid_tipomuni` BOOLEAN,
  `tipomuni_label` STRING,
  `sec_ejec_count` BIGINT,
  `sec_ejec` STRING,
  `predial_sec_ejec_count` BIGINT,
  `has_predial_match` BOOLEAN,
  `has_valid_bridge_ubigeo` BOOLEAN,
  `municipality_context_key` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/territorial_context/dim_municipality_context';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`fact_territorial_integration_coverage` (
  `metric_name` STRING,
  `numerator` BIGINT,
  `denominator` BIGINT,
  `coverage_percentage` DOUBLE,
  `description` STRING,
  `coverage_ratio` DOUBLE,
  `source_dataset` STRING,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/territorial_context/fact_territorial_integration_coverage';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`mart_municipal_capacity` (
  `ubigeo` STRING,
  `idmunici` STRING,
  `departamento` STRING,
  `provincia` STRING,
  `distrito` STRING,
  `tipomuni` STRING,
  `tipomuni_int` INT,
  `tipomuni_label` STRING,
  `has_predial_match` BOOLEAN,
  `predial_sec_ejec_count` BIGINT,
  `total_personal_dic_2021` DECIMAL(30,4),
  `total_personal_mar_2022` DECIMAL(30,4),
  `total_computadoras_operativas` DECIMAL(38,4),
  `computadoras_con_internet` DECIMAL(30,4),
  `ratio_computadoras_con_internet` DECIMAL(38,8),
  `computadoras_por_trabajador` DECIMAL(38,6),
  `tiene_internet` BOOLEAN,
  `tipo_conexion_internet` STRING,
  `tiene_siaf` BOOLEAN,
  `tiene_srtm` BOOLEAN,
  `tiene_sistema_rentas` BOOLEAN,
  `tiene_catastro` BOOLEAN,
  `requiere_asistencia_administracion_tributaria` BOOLEAN,
  `requiere_asistencia_catastro` BOOLEAN,
  `requiere_capacitacion_administracion_tributaria` BOOLEAN,
  `requiere_capacitacion_catastro` BOOLEAN,
  `renamu_income_total` DECIMAL(38,4),
  `renamu_expense_total` DECIMAL(38,4),
  `has_financial_context` BOOLEAN,
  `worker_metric_available` BOOLEAN,
  `computer_metric_available` BOOLEAN,
  `internet_metric_available` BOOLEAN,
  `state_system_metric_available` BOOLEAN,
  `municipal_system_metric_available` BOOLEAN,
  `technical_assistance_metric_available` BOOLEAN,
  `training_metric_available` BOOLEAN,
  `gold_processed_at_utc` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/territorial_context/mart_municipal_capacity';

CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`mart_territorial_context` (
  `departamento` STRING,
  `provincia` STRING,
  `distrito` STRING,
  `departamento_normalizado` STRING,
  `provincia_normalizada` STRING,
  `distrito_normalizado` STRING,
  `ubigeo` STRING,
  `tipomuni` STRING,
  `tipomuni_int` INT,
  `tipomuni_label` STRING,
  `municipality_count` BIGINT,
  `valid_ubigeo_count` BIGINT,
  `complete_territory_count` BIGINT,
  `predial_match_count` BIGINT,
  `without_predial_match_count` BIGINT,
  `gold_processed_at_utc` STRING,
  `gold_grain` STRING
)
STORED AS PARQUET
LOCATION '/app/data/gold/territorial_context/mart_territorial_context';
