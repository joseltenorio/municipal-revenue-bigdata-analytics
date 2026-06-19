# Profiling de datos

## Propósito

El profiling sirve para observar estructura, nulos, duplicados, tipos, valores representativos y riesgos de integración antes de cerrar Silver y Gold.

## Alcance actual

El profiling cubre:

- Landing
- Bronze
- Silver
- Silver integrado

## Hallazgo clave de modelado

La fuente de clasificación municipal vigente es `municipal_classification`.

Todo lo relacionado con:

- `municipal_categories`
- `categorias_municipalidades`
- `CategoriasMunicipalidades.csv`
- matching manual por nombre

es legacy.

## Uso de los resultados

El profiling se usa para:

- validar llaves candidatas
- medir nulos y duplicados
- revisar montos negativos
- revisar territorio y códigos
- decidir qué entra al Gold inicial

## Lectura para el modelo objetivo

Los resultados de profiling deben alimentar estas decisiones:

- `dim_municipality`
- `dim_geography`
- `dim_renamu_context`
- `dim_sismepre_period`
- `fact_siaf_income`
- `fact_predial_statistics`
- `map_sec_ejec_ubigeo`

## Regla final

El profiling informa el modelo. No lo reemplaza.
