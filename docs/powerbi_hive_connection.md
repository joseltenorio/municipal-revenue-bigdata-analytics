# Conexión Power BI - Hive

## Propósito

Este documento describe el enfoque previsto para conectar Power BI Desktop con el lakehouse local mediante Apache Hive.

Es un documento inicial. La conexión final se validará cuando existan marts Gold listos para consumo analítico.

## Estado actual

HiveServer2 funciona localmente y expone conexión en:

```text
localhost:10000
```

Las bases Hive disponibles son:

- `bronze`
- `silver`
- `gold`

Bronze y Silver ya tienen tablas externas sobre Parquet. Gold existe como base, pero todavía no tiene tablas porque los marts Gold se construirán en una fase posterior.

## Enfoque de conexión

La ruta preferida para Power BI será:

```text
Marts Gold en Parquet
-> Tablas externas Hive
-> HiveServer2
-> ODBC/JDBC
-> Power BI Desktop
```

El modo recomendado para el reporte local es `Import`. No se define `DirectQuery` como requisito del proyecto.

Power BI no debe consumir Bronze como capa final. Silver puede usarse para validaciones técnicas, pero el modelo del reporte debe basarse preferentemente en Gold.

## Configuración esperada

Parámetros esperados para una conexión local:

| Parámetro | Valor esperado |
| --- | --- |
| Host | `localhost` |
| Puerto | `10000` |
| Servicio | HiveServer2 |
| Base preferida | `gold` |
| Modo recomendado | Import |

La configuración exacta del driver ODBC/JDBC dependerá del entorno local y del driver instalado en Windows.

## Validaciones previas necesarias

Antes de conectar Power BI, se debe confirmar:

- HiveServer2 está activo.
- `SHOW DATABASES` muestra `gold`.
- `SHOW TABLES IN gold` muestra los marts ya construidos.
- Las consultas `SELECT COUNT(*)` sobre tablas Gold responden correctamente.
- Los nombres de tablas y columnas son estables para el modelo Power BI.

En el estado actual, `SHOW TABLES IN gold` no devuelve tablas. Esto es esperado porque Gold todavía no existe.

## Fallback controlado

Si la conexión local Power BI - Hive no resulta estable, se podrá usar un fallback exportando marts Gold a CSV o Parquet.

Ese fallback no reemplaza Hive. Hive debe seguir validado como catálogo SQL del lakehouse y como evidencia técnica de consulta sobre Parquet.

## Evidencias futuras

Cuando se construya Gold y se valide Power BI, se deberán conservar evidencias ligeras de:

- HiveServer2 activo.
- `SHOW DATABASES`.
- `SHOW TABLES IN gold`.
- `SELECT COUNT(*)` sobre tablas Gold.
- Configuración ODBC/JDBC usada, si aplica.
- Power BI importando tablas Gold.
- Dashboard final.

No se documentan todavía medidas DAX ni páginas finales del reporte. Ese contenido corresponderá a `docs/powerbi_model.md` cuando el modelo analítico esté definido.

## Límites actuales

- Gold aún no tiene tablas externas.
- La conexión Power BI no se considera cerrada en esta etapa.
- Bronze y Silver no deben presentarse como capa de consumo final.
- Las advertencias de Log4j o SLF4J observadas en Beeline no bloquearon la validación SQL.
