# Transformaciones Silver e integración municipal

## Propósito

Este documento describe la capa **Silver** del proyecto `municipal-revenue-bigdata-analytics`.

Silver limpia, tipa, normaliza y prepara la información para el modelo Gold objetivo. Ya no se considera una fase inicial de Bronze/Silver por fuente, sino una etapa de alineamiento entre Silver integrado, Gold dimensional, Hive y Power BI.

## Relación entre capas

| Capa | Rol |
| --- | --- |
| Bronze | Preservar recursos tabulares con limpieza técnica mínima |
| Silver | Limpiar, tipar, normalizar y preparar datasets por fuente |
| Silver integrado | Resolver llaves técnicas y dejar trazabilidad de cruce |
| Gold | Exponer dimensiones, hechos, marts y auditoría |

## Fuentes y contratos vigentes

### SIAF ingresos

Salida Silver por recurso:

```text
data/silver/siaf_income/resource_key=<resource_key>/
```

La integración Silver agrega los montos a un nivel controlado para evitar cruces fila a fila.

### SISMEPRE

Salida Silver por recurso:

```text
data/silver/sismepre/resource_key=<resource_key>/
```

Para el Gold inicial, SISMEPRE sólo usa:

```text
silver/sismepre/resource_key=esat_estadistica_atm
```

Los otros recursos SISMEPRE pueden existir en Silver por trazabilidad, pero no entran al Gold inicial ni al dashboard principal:

- `respuestas`
- `preguntas`
- `formulario`
- `estadistica`
- `ano_aplicacion`
- `entidad_estado`

### RENAMU

Salida Silver:

```text
data/silver/renamu/resource_key=municipal_context
```

RENAMU completo no vuelve a Gold. En Gold se separa como `dim_renamu_context`.

### Clasificación municipal oficial

Salida Silver:

```text
data/silver/municipal_classification/resource_key=classification_2019
```

Fuente vigente:

- `municipal_classification`
- clasificación municipal oficial MEF 2019
- integración por `ubigeo6`

Todo lo relacionado con:

- `municipal_categories`
- `categorias_municipalidades`
- `CategoriasMunicipalidades.csv`
- matching manual por nombre

se considera legacy.

## Mapa técnico Silver

### `map_sec_ejec_ubigeo`

Este dataset es técnico y no debe tratarse como dimensión de negocio.

Propósito:

- resolver `sec_ejec -> ubigeo6 -> municipality_key`
- conectar SIAF con RENAMU
- conectar SIAF con clasificación municipal
- conectar SIAF con geografía

Campos documentados:

- `sec_ejec`
- `ubigeo6`
- `municipality_key`
- `municipalidad_sismepre_nombre`
- `municipalidad_siaf_nombre`
- `has_siaf_match`
- `has_sismepre_match`
- `has_renamu_match`
- `has_classification_match`
- `match_status`
- `confidence_level`
- `issue_reason`

## Transformaciones por fuente

### SIAF ingresos

Reglas:

- mantener recursos por `resource_key`
- limpiar strings con `trim`
- preservar códigos como string
- tipar año, mes y montos
- generar flags de validez
- conservar montos negativos como hallazgo de calidad, no como error automático

### SISMEPRE

Reglas:

- mantener tablas separadas
- no hacer joins prematuros entre recursos
- tipar periodos y estadística
- generar flags de validez
- conservar granularidad real cuando exista `formulario_id`, `anio_estadistica` y `mes_estadistica`

### RENAMU

Reglas:

- conservar dataset ancho
- preservar columnas originales
- tipar territorio
- normalizar nombres territoriales sin reemplazar el original
- validar `ubigeo`, `ccdd`, `ccpp`, `ccdi` y `tipomuni`

### Clasificación municipal

Reglas:

- `anio = 2019`
- `ubigeo6` como llave territorial
- `tipo_clasificacion_municipal` como clasificación oficial
- `ambito_municipal` como atributo de negocio
- evitar cualquier matching manual por nombre como criterio principal

## Integración Silver

Salidas locales no versionables:

```text
data/silver/integrated/map_sec_ejec_ubigeo/
data/silver/integrated/siaf_municipal_amounts/
data/silver/integrated/municipal_context/
data/silver/integrated/integration_coverage/
```

Las salidas Silver integradas se documentan con un objetivo claro:

- `map_sec_ejec_ubigeo` resuelve la trazabilidad técnica
- `siaf_municipal_amounts` agrega SIAF a una granularidad utilizable
- `municipal_context` conserva contexto RENAMU seleccionado
- `integration_coverage` mide cobertura y calidad del cruce

## Decisiones cerradas para Gold

Silver debe preparar insumos para estas entidades Gold:

- `dim_municipality`
- `dim_geography`
- `dim_renamu_context`
- `dim_time`
- `dim_sismepre_period`
- `fact_siaf_income`
- `fact_predial_statistics`
- `mart_municipal_revenue_overview`
- `mart_predial_statistics_overview`
- `mart_municipal_context`
- `mart_territorial_summary`

## Legacy explícito

Estas referencias se conservan sólo como historia o transición anterior:

- `municipal_entity_bridge`
- `mef_municipal_amounts`
- `renamu_full`
- `municipal_context`
- `dim_municipality_context`
- `fact_municipal_income_execution`
- `fact_predial_compliance`

## Criterio de versionamiento

No versionar salidas locales integradas:

```text
data/silver/integrated/
```

Sí versionar la documentación y el código de integración cuando sea necesario para reproducibilidad.
