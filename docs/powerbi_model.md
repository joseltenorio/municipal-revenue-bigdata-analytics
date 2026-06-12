# Modelo Semántico y Diseño de Reporte Power BI

## Propósito

Este documento define el modelo semántico y el diseño detallado de las páginas del reporte de **Power BI Desktop** para el proyecto de analítica de ingresos municipales. El objetivo es servir como guía de implementación directa para construir el dashboard final basado en las tablas Gold reales catalogadas en Apache Hive.

---

## 1. Decisiones de Modelado Semántico

Para optimizar el rendimiento y simplificar el diseño en Power BI, el modelo prioriza el uso de **marts analíticos Gold planos** en lugar de relaciones complejas. Esto reduce el uso de relaciones Many-to-Many, previene duplicidades y disminuye el consumo de memoria en el motor Vertipaq.

### Componentes del Modelo Semántico:

1. **Dimensiones Físicas Principales (Tablas de Búsqueda):**
   * [dim_geography](file:///c:/Users/Windows%2011/Desktop/Proyectos/municipal-revenue-bigdata-analytics/data/gold/territorial_context/dim_geography): Filtro jerárquico geográfico (Departamento -> Provincia -> Distrito) mediante llave territorial `ubigeo`.
   * [dim_time](file:///c:/Users/Windows%2011/Desktop/Proyectos/municipal-revenue-bigdata-analytics/data/gold/municipal_revenue/dim_time): Calendario financiero para ingresos (`anio`, `mes`).
   * [dim_municipality](file:///c:/Users/Windows%2011/Desktop/Proyectos/municipal-revenue-bigdata-analytics/data/gold/municipal_revenue/dim_municipality): Puente relacional ejecutor-territorio (`sec_ejec` <-> `ubigeo`).
   * [dim_predial_period](file:///c:/Users/Windows%2011/Desktop/Proyectos/municipal-revenue-bigdata-analytics/data/gold/predial_compliance/dim_predial_period): Calendario especializado para periodos prediales.
   * [dim_municipality_context](file:///c:/Users/Windows%2011/Desktop/Proyectos/municipal-revenue-bigdata-analytics/data/gold/territorial_context/dim_municipality_context): Atributos y tipo de municipalidad.

2. **Dimensión Derivada Presupuestal (`dim_budget_classifier`):**
   * Los clasificadores atómicos presupuestales residen físicamente en `fact_municipal_income_execution`. Para evitar sobrecargar la tabla de hechos, se recomienda **derivar en Power Query** una dimensión llamada `dim_budget_classifier` haciendo una referencia a la fact y removiendo duplicados en las columnas: `generica`, `subgenerica`, `subgenerica_det`, `especifica`, `especifica_det`. Esto normaliza la jerarquía de ingresos sin alterar Hive ni Spark.

3. **Uso de Tablas por Propósito:**
   * **Marts Planos:** Usados para las páginas ejecutivas y agregadas (`mart_municipal_revenue_overview`, `mart_predial_compliance_overview`, `mart_predial_ranking`, `mart_municipal_capacity`, `mart_territorial_context`).
   * **Hechos Detallados (Facts):** `fact_municipal_income_execution` se reserva exclusivamente para páginas de desglose analítico presupuestal por partida.
   * **Hechos Técnicos:** `fact_*_integration_coverage` se reservan para la página técnica de diagnóstico y cobertura.
   * *Advertencia:* No combinar en el mismo visual campos de hechos y de marts que sumaricen la misma métrica (ej. PIA en la fact y en el mart) para prevenir doble conteo.

---

## 2. Definición de Medidas DAX

Todas las medidas deben ser creadas utilizando los nombres reales de las columnas físicas:

```dax
-- ==========================================
-- MEDIDAS DE INGRESOS MUNICIPALES (MEF)
-- ==========================================
PIA Total = SUM(mart_municipal_revenue_overview[monto_pia_total])

PIM Total = SUM(mart_municipal_revenue_overview[monto_pim_total])

Recaudación Total = SUM(mart_municipal_revenue_overview[monto_recaudado_total])

Ratio Recaudación PIM = DIVIDE([Recaudación Total], [PIM Total], 0)

Ratio Recaudación PIA = DIVIDE([Recaudación Total], [PIA Total], 0)

Recaudación Año Anterior = 
CALCULATE(
    [Recaudación Total],
    FILTER(
        ALL(dim_time),
        dim_time[anio] = MAX(dim_time[anio]) - 1 &&
        dim_time[mes] = MAX(dim_time[mes])
    )
)

Variación Recaudación YoY = [Recaudación Total] - [Recaudación Año Anterior]

Variación Recaudación YoY % = DIVIDE([Variación Recaudación YoY], [Recaudación Año Anterior], 0)

-- ==========================================
-- MEDIDAS DEL IMPUESTO PREDIAL
-- ==========================================
Recaudación Predial = SUM(mart_predial_compliance_overview[predial_collection_total])

Emisión Predial = SUM(mart_predial_compliance_overview[predial_issue_total])

Saldo Predial = SUM(mart_predial_compliance_overview[predial_balance_total])

Efectividad Predial = DIVIDE([Recaudación Predial], [Emisión Predial], 0)

Ranking Recaudación = 
RANKX(
    ALL(dim_municipality),
    [Recaudación Total],
    ,
    DESC,
    Dense
)

Ranking Ejecución = 
RANKX(
    ALL(dim_municipality),
    [Ratio Recaudación PIM],
    ,
    DESC,
    Dense
)

Ranking Efectividad Predial = 
RANKX(
    ALL(dim_geography),
    [Efectividad Predial],
    ,
    DESC,
    Dense
)

-- ==========================================
-- MEDIDAS DE CAPACIDADES INSTITUCIONALES (RENAMU)
-- ==========================================
Municipalidades con Internet = 
CALCULATE(
    COUNTROWS(mart_municipal_capacity),
    mart_municipal_capacity[tiene_internet] = TRUE
)

Municipalidades con Catastro = 
CALCULATE(
    COUNTROWS(mart_municipal_capacity),
    mart_municipal_capacity[tiene_catastro] = TRUE
)

Municipalidades con Sistema de Rentas = 
CALCULATE(
    COUNTROWS(mart_municipal_capacity),
    mart_municipal_capacity[tiene_sistema_rentas] = TRUE
)

Promedio Computadoras por Trabajador = AVERAGE(mart_municipal_capacity[computadoras_por_trabajador])

Recaudación por Trabajador = 
DIVIDE(
    [Recaudación Predial],
    SUM(mart_municipal_capacity[total_personal_mar_2022]),
    0
)
```

---

## 3. Estructura del Reporte (8 Páginas)

### 3.1. Páginas Obligatorias (6)

#### 1. Resumen Ejecutivo Municipal
* **Objetivo Analítico:** Visión macro del estado financiero, predial y tecnológico de los municipios.
* **Pregunta de Negocio:** ¿Cuál es la recaudación consolidada a nivel nacional y qué nivel de cobertura tecnológica existe?
* **Tablas Utilizadas:** `mart_municipal_revenue_overview`, `mart_predial_compliance_overview`, `mart_municipal_capacity`, `dim_geography`.
* **Visuales:** Tarjetas (KPI) superiores, Gráfico de barras acumuladas (Avance presupuestal), Gráfico de rosca (Conectividad a Internet).
* **Filtros:** Departamento, Tipo de Municipalidad.
* **KPIs:** PIA, PIM, Recaudación Total, Efectividad Predial, Conectividad %.

#### 2. Ejecución y Tendencia de Ingresos
* **Objetivo Analítico:** Evaluar la velocidad y estacionalidad de la ejecución presupuestal frente al año anterior.
* **Pregunta de Negocio:** ¿Cómo evoluciona mensualmente la recaudación de ingresos comparada con el periodo anterior?
* **Tablas Utilizadas:** `mart_municipal_revenue_overview`, `dim_time`.
* **Visuales:** Gráfico de líneas (Recaudación Año Actual vs Año Anterior por Mes), Gráfico de cascada (Variación YoY de ingresos por Rubro).
* **Filtros:** Año, Rubro de Ingresos, Pliego.
* **KPIs:** Recaudación Total, Recaudación Año Anterior, Avance PIM.

#### 3. Ranking Top/Bottom Municipal
* **Objetivo Analítico:** Clasificar a los municipios según su nivel de recaudación y eficiencia de ejecución.
* **Pregunta de Negocio:** ¿Qué municipalidades registran el mayor volumen de recaudación y cuáles están más rezagadas?
* **Tablas Utilizadas:** `mart_predial_ranking`, `mart_municipal_revenue_overview`, `dim_municipality`.
* **Visuales:** Tabla de ranking dinámico (Top 10 y Bottom 10), Gráfico de barras horizontales (Recaudación de Municipalidades).
* **Filtros:** Departamento, Provincia, Tipo de Municipalidad.
* **KPIs:** Ranking Recaudación, Ranking Ejecución.

#### 4. Control del Impuesto Predial
* **Objetivo Analítico:** Medir la eficiencia de la emisión y cobranza del impuesto predial y el nivel de morosidad.
* **Pregunta de Negocio:** ¿Cuál es la efectividad de cobranza predial por periodo y qué saldo queda pendiente?
* **Tablas Utilizadas:** `mart_predial_compliance_overview`, `dim_predial_period`.
* **Visuales:** Tarjetas (Emisión Predial, Recaudación Predial, Saldo), Gráfico de columnas agrupadas (Recaudación Ordinaria vs Coactiva), Gráfico de dispersión (Contribuyentes vs Efectividad).
* **Filtros:** Año de Aplicación, Periodo Operativo.
* **KPIs:** Efectividad Predial (%), Saldo Predial, Total Contribuyentes.

#### 5. Brechas y Priorización Predial
* **Objetivo Analítico:** Identificar territorios críticos con alta emisión pero baja efectividad de cobro para priorizar la recaudación.
* **Pregunta de Negocio:** ¿Qué distritos presentan la mayor brecha financiera de impuesto predial pendiente de cobro?
* **Tablas Utilizadas:** `mart_predial_compliance_overview`, `dim_geography`.
* **Visuales:** Matriz de priorización con formato condicional (drill-down de Departamento -> Provincia -> Distrito), Árbol de descomposición para la brecha de saldo predial.
* **Filtros:** Departamento, Tipo de Municipalidad.
* **KPIs:** Saldo Predial, Efectividad Predial.

#### 6. Capacidad Institucional RENAMU
* **Objetivo Analítico:** Analizar el equipamiento e infraestructura administrativa y tecnológica de los municipios.
* **Pregunta de Negocio:** ¿Qué porcentaje de municipios opera con catastro, internet, SIAF o sistema de rentas y cómo impacta en sus recursos?
* **Tablas Utilizadas:** `mart_municipal_capacity`, `dim_municipality_context`.
* **Visuales:** Tarjetas de porcentaje de adopción de sistemas, Gráfico de barras (Promedio de computadoras por trabajador por Tipo Municipal), Gráfico de dispersión (Personal vs Computadoras).
* **Filtros:** Departamento, Tipo de Municipalidad.
* **KPIs:** Municipalidades con Internet, Municipalidades con Catastro, Promedio Computadoras por Trabajador.

---

### 3.2. Páginas Complementarias (2)

#### 7. Análisis Territorial y Geográfico
* **Objetivo Analítico:** Visualizar la distribución espacial de la recaudación y las capacidades.
* **Pregunta de Negocio:** ¿Existe un patrón geográfico en la concentración de recaudación predial y avance de ingresos?
* **Tablas Utilizadas:** `mart_municipal_revenue_overview`, `mart_predial_compliance_overview`, `dim_geography`.
* **Visuales:** **Mapa de coropléticos** o burbujas utilizando la latitud/longitud o el Ubigeo nacional.
* **Filtros:** Departamento, Provincia.
* **KPIs:** Recaudación Total, Efectividad Predial.
* *Advertencia:* Si Power BI experimentase fallas al geolocalizar distritos o ubigeos del Perú, se mantendrá un **fallback a barras horizontales por departamento y matrices detalladas** para no bloquear la visualización.

#### 8. Diagnóstico Integrado y Cobertura del Modelo
* **Objetivo Analítico:** Pestaña técnica dedicada a la salud de los datos y auditoría de la integración Medallion.
* **Pregunta de Negocio:** ¿Qué nivel de cobertura técnica y matching territorial existe entre las fuentes MEF, Predial y RENAMU?
* **Tablas Utilizadas:** `fact_revenue_integration_coverage`, `fact_predial_integration_coverage`, `fact_territorial_integration_coverage`.
* **Visuales:** Tarjetas con porcentajes de matching de las fuentes, Tabla detallada con los numeradores y denominadores de cada regla técnica.
* **Filtros:** Capa (Ingresos, Predial, Geografía).
* **KPIs:** Cobertura MEF (%), Cobertura Predial (%), Cobertura Territorial (%).
* *Advertencia:* **Estas métricas son de calidad técnica del modelo**, no representan el desempeño municipal y deben etiquetarse claramente como auditoría del lakehouse.
