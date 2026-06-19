# Arquitectura del proyecto

## Propósito

Definir la arquitectura local del proyecto `municipal-revenue-bigdata-analytics` con foco en Silver integrado, Gold dimensional, Hive y Power BI.

## Flujo

```text
Fuentes públicas
-> Landing
-> Bronze
-> Silver por fuente
-> Silver integrado
-> Gold dimensional
-> Hive
-> Power BI
```

## Convenciones cerradas

- `municipal_classification` es la fuente vigente de clasificación municipal.
- `municipal_categories` y variantes son legacy.
- `map_sec_ejec_ubigeo` es un mapa técnico Silver.
- `dim_municipality` representa la entidad municipal.
- `dim_geography` representa la jerarquía territorial.
- `dim_renamu_context` separa el contexto RENAMU.
- `fact_siaf_income` sale con `municipality_key` resuelto.

## Silver integrado

Silver integrado conserva trazabilidad y resuelve llaves técnicas.

Su propósito principal es:

- conectar SIAF con RENAMU
- conectar SIAF con clasificación municipal
- conectar SIAF con geografía
- evitar joins manuales por nombre

## Gold dimensional

Gold expone:

- dimensiones de entidad, geografía, tiempo, RENAMU y periodos SISMEPRE
- hechos de ingresos SIAF y estadísticas prediales
- marts ejecutivos para Power BI
- auditoría y calidad separadas

## Hive y Power BI

Hive registra las tablas externas.
Power BI consume preferentemente los marts y dimensiones finales.

## Principio de diseño

La arquitectura debe priorizar:

- claridad de llaves
- separación entre entidad y territorio
- trazabilidad técnica
- auditoría separada
- consumo simple en Power BI
