# Pipeline End-to-End: Logística E-Commerce + Clima Brasil

**Trabajo Práctico Final — Introducción a la Ingeniería de Datos, MIA 03**

Análisis del impacto de las condiciones climáticas en el rendimiento logístico del e-commerce brasileño (2016–2018), construido con el Modern Data Stack.

---

## Stack Tecnológico

| Capa | Herramienta |
|---|---|
| Extracción e Ingesta | Airbyte OSS (local) |
| Data Warehouse | MotherDuck (DuckDB cloud) |
| Transformación | dbt Core 1.11.7 + dbt-duckdb 1.10.1 |
| Calidad de Datos | dbt-expectations (metaplane) |
| Orquestación | Prefect v3 |
| Visualización | Metabase v0.58.8 + driver DuckDB |
| Contenerización | Docker |

---

## Fuentes de Datos

### 1. Brazilian E-Commerce — Olist
- **URL:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- **Tablas:** orders, order_items, customers, geolocation, sellers, products, order_payments, order_reviews
- **Registros:** ~100,000 órdenes (2016–2018)
- **Schema MotherDuck:** `br_ecommerce`

### 2. Estaciones Meteorológicas Convencionales — INMET
- **URL:** https://www.kaggle.com/datasets/saraivaufc/conventional-weather-stations-brazil
- **Tablas:** conventional_weather_stations, weather_stations_codes, wind_directions_codes
- **Schema MotherDuck:** `br_weather`

**Join key:** `order_purchase_date = reference_date AND customer_state = state_code`

---

## Arquitectura

```
Kaggle CSV (Olist)          Kaggle CSV (INMET Weather)
       │                              │
       └──────────┬───────────────────┘
                  │ Airbyte OSS (11 conexiones)
                  │ Full Refresh | Overwrite
                  ▼
           MotherDuck (iid_final)
           ├── br_ecommerce.*
           └── br_weather.*
                  │
                  │ dbt Core
                  ▼
           MotherDuck (main)
           ├── staging/
           │   ├── stg_orders
           │   ├── stg_customers
           │   ├── stg_order_items
           │   └── stg_weather
           └── marts/ (Kimball)
               ├── dim_customer
               ├── dim_weather
               ├── dim_date
               ├── dim_product
               ├── dim_seller
               └── fct_orders ← tabla central
                  │
                  │ Prefect (orquestación)
                  ▼
            Metabase Dashboard
            └── 5 visualizaciones + 3 filtros
```

---

## Estructura del Proyecto

```
iid_final/
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml
│   │   │   ├── _stg__models.yml
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_order_items.sql
│   │   │   └── stg_weather.sql
│   │   └── marts/
│   │       ├── _marts__models.yml
│   │       ├── dim_customer.sql
│   │       ├── dim_weather.sql
│   │       ├── dim_date.sql
│   │       ├── dim_product.sql
│   │       ├── dim_seller.sql
│   │       └── fct_orders.sql
│   ├── packages.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── prefect/
│   └── ecommerce_pipeline.py
├── screenshots/
│   ├── airbyte_connections.png
│   ├── dbt_run.png
│   ├── dbt_test.png
│   ├── prefect_flow.png
│   └── metabase_dashboard.png
├── Dockerfile.metabase
├── requirements.txt
└── README.md
```

---

## Requisitos Previos

- Python 3.13+ (Anaconda recomendado)
- Docker Desktop
- Airbyte OSS corriendo en `http://localhost:8000`
- Cuenta en MotherDuck con base de datos `iid_final`
- Variable de entorno `MOTHERDUCK_TOKEN` configurada

```bash
export MOTHERDUCK_TOKEN="tu_token_aqui"
```

---

## Instalación

### 1. Clonar e instalar dependencias

```bash
cd iid_final
pip install dbt-duckdb prefect requests
```

### 2. Instalar paquetes dbt

```bash
cd dbt
dbt deps
```

Paquetes utilizados (`packages.yml`):
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: metaplane/dbt_expectations
    version: [">=0.10.0", "<1.0.0"]
  - package: godatadriven/dbt_date
    version: [">=0.10.0", "<1.0.0"]
```

### 3. Verificar conexión a MotherDuck

```bash
dbt debug
# Esperado: All checks passed!
```

---

## Ejecución

### Opción A — Pipeline completo con Prefect (recomendado)

```bash
cd ~/iid_final/prefect
python ecommerce_pipeline.py
```

El pipeline ejecuta en orden:
1. **Airbyte sync** — dispara las 11 conexiones en paralelo
2. **dbt run** — crea los 10 modelos cuando todos los syncs terminan
3. **dbt test** — valida los 11 tests de calidad

### Opción B — Ejecución manual paso a paso

```bash
# 1. Transformación
cd ~/iid_final/dbt
dbt run --no-partial-parse

# 2. Tests de calidad
dbt test

# 3. Full refresh (si hay cambios en clima o dimensiones)
dbt run --select fct_orders --full-refresh
```

---

## Modelo Dimensional (Kimball)

### Decisión de arquitectura

Se optó por **Kimball** sobre OBT por:
- Las dos fuentes son heterogéneas (transaccional + meteorológica)
- Evita duplicar datos de clima por cada ítem de orden
- Facilita agregar nuevas dimensiones sin modificar `fct_orders`
- MotherDuck (columnar) ejecuta joins eficientemente

### Tabla de hechos: `fct_orders`

| Columna | Tipo | Descripción |
|---|---|---|
| order_id | VARCHAR | PK — ID único de orden |
| customer_id | VARCHAR | FK → dim_customer |
| customer_state | VARCHAR | Estado brasileño (2 letras) |
| order_purchase_date | DATE | Fecha de compra |
| actual_delivery_days | INTEGER | Días reales de entrega |
| delivery_delay_days | INTEGER | Retraso vs estimado (negativo = adelantado) |
| is_on_time | BOOLEAN | True si entregó antes del estimado |
| total_price | DECIMAL | Suma de ítems |
| total_freight | DECIMAL | Costo de flete |
| precipitation_mm | DECIMAL | Precipitación promedio durante entrega |
| rain_category | VARCHAR | dry / light / moderate / heavy |
| weather_id | VARCHAR | FK → dim_weather |

### Materializations

| Modelo | Tipo | Razón |
|---|---|---|
| staging/* | view | Siempre actualizado, bajo costo |
| dim_date | table | Estática, costosa de recalcular |
| fct_orders | incremental | Alto volumen, solo procesa nuevos registros |

---

## Tests de Calidad

11 tests implementados, todos en estado PASS:

```
PASS  unique_fct_orders_order_id
PASS  not_null_fct_orders_order_id
PASS  unique_dim_weather_weather_id
PASS  not_null_dim_weather_weather_id
PASS  expect_column_value_lengths_to_equal (order_id = 32 chars)
PASS  expect_column_values_to_be_between (freight 0–10000)
PASS  expect_column_values_to_be_between (delivery_days 0–365)
PASS  expect_column_values_to_be_in_set (is_on_time: true/false)
PASS  expect_table_row_count_to_be_between (90k–200k)
PASS  expect_column_values_to_be_between (precipitation 0–500mm)
PASS  expect_column_values_to_be_in_set (27 estados de Brasil)
```

---

## Dashboard Metabase

### Iniciar Metabase

```bash
cd ~/iid_final

# Primera vez — construir imagen con driver DuckDB
docker build -f Dockerfile.metabase -t metabase-duckdb:latest .

# Correr contenedor
docker run -d -p 3000:3000 \
  --name metabase \
  -v metabase-data:/metabase.db \
  -e MB_PLUGINS_DIR=/plugins \
  metabase-duckdb:latest
```

Acceder en: `http://localhost:3000`

**Conexión:** DuckDB → `md:iid_final?motherduck_token=TU_TOKEN`

### Visualizaciones

| # | Tipo | Pregunta de negocio |
|---|---|---|
| 1 | Number cards (KPIs) | ¿Cuál es el estado general de la operación? |
| 2 | Line chart | ¿Cómo evolucionan las entregas mes a mes? |
| 3 | Bar chart horizontal | ¿Qué estados tienen más retrasos? |
| 4 | Table condicional | ¿Cómo afecta la lluvia al tiempo de entrega? |
| 5 | Table detalle | ¿Qué órdenes críticas coinciden con lluvia intensa? |

### Filtros interactivos

- **Fecha de compra** → `order_purchase_date`
- **Estado** → `customer_state`
- **Condición climática** → `rain_category`

---

## Hallazgos Principales

- **93.1%** de las órdenes entregadas dentro del plazo estimado (SLA)
- Promedio nacional: **12.5 días** de entrega
- Estados del Norte (RR, AP, AM): hasta **28 días** promedio
- Órdenes con lluvia intensa: **+2.3 días** de retraso promedio
- Costo de flete en lluvia moderada/intensa: **7.2% superior** al promedio

---

## Variables de Entorno

```bash
# Requeridas
export MOTHERDUCK_TOKEN="eyJhbGc..."   # Token de MotherDuck

# Airbyte OSS (si cambiaste las credenciales por defecto)
export AIRBYTE_USER="airbyte"
export AIRBYTE_PASS="password"
```

---

## Troubleshooting

**`dbt debug` falla con "no database found"**
```bash
# Verificar nombre exacto de la base de datos
python3 -c "
import duckdb, os
con = duckdb.connect('md:', config={'motherduck_token': os.environ['MOTHERDUCK_TOKEN']})
print(con.execute('SHOW DATABASES').fetchall())
"
```

**`fct_orders` tiene columnas de clima en NULL**
```bash
# Reconstruir completo después de que dim_weather tenga datos
dbt run --select fct_orders --full-refresh
```

**Metabase no ve las tablas**
- Verificar que el token en la cadena de conexión sea válido
- Ir a Admin → Databases → Sync database schema now

**Airbyte sync falla desde Prefect**
```bash
# Verificar que la API responde
curl -u airbyte:password http://localhost:8000/api/v1/health
```

---

## Autor

**Odilón** — Maestría en Inteligencia Artificial, MIA 03  
Trabajo Práctico Final — Febrero 2026
