# Guía Práctica de Apache Spark para Big Data

## Índice
1. [Introducción a Apache Spark](#1-introducción-a-apache-spark)
2. [Arquitectura de Spark](#2-arquitectura-de-spark)
3. [Componentes del Ecosistema Spark](#3-componentes-del-ecosistema-spark)
4. [RDD: Resilient Distributed Datasets](#4-rdd-resilient-distributed-datasets)
5. [DataFrames y Datasets](#5-dataframes-y-datasets)
6. [Spark SQL](#6-spark-sql)
7. [Ejercicios Prácticos con Datasets Reales](#7-ejercicios-prácticos-con-datasets-reales)
8. [Recursos Adicionales](#8-recursos-adicionales)

## 1. Introducción a Apache Spark

### ¿Qué es Apache Spark?
Apache Spark es un framework de computación distribuida diseñado para el procesamiento de datos a gran escala. A diferencia de Hadoop MapReduce, Spark procesa datos en memoria, lo que lo hace significativamente más rápido para muchas cargas de trabajo, especialmente para algoritmos iterativos como los utilizados en machine learning.

### Historia y Evolución
- **2009**: Nace como proyecto de investigación en UC Berkeley
- **2010**: Se convierte en código abierto
- **2013**: Donado a la Apache Software Foundation
- **2014**: Top-level Apache Project
- **Actualidad**: Uno de los proyectos de código abierto más activos en el ecosistema Big Data

### ¿Por qué Spark?
- **Velocidad**: 100x más rápido que Hadoop MapReduce para procesamiento en memoria
- **Facilidad de uso**: APIs en Java, Scala, Python y R
- **Generalidad**: Combinación de SQL, streaming y análisis complejo
- **Compatibilidad**: Funciona con diversas fuentes de datos (HDFS, S3, Cassandra, etc.)

### Consideraciones Técnicas
- **Paradigma de procesamiento**: Basado en operaciones DAG (Directed Acyclic Graph)
- **Modelo de ejecución**: La evaluación perezosa (lazy evaluation) difiere el procesamiento hasta que se necesiten los resultados
- **Gestión de memoria**: Almacenamiento en memoria configurable para datos frecuentemente accedidos
- **Tolerancia a fallos**: Reconstrucción eficiente de datos perdidos mediante linaje

## 2. Arquitectura de Spark

### Componentes Principales
- **Driver Program**: Contiene la aplicación principal y crea el SparkContext
- **Cluster Manager**: Asigna recursos (YARN, Mesos, Kubernetes, Standalone)
- **Worker Nodes**: Ejecutan las tareas de computación
- **Executors**: Procesos JVM que ejecutan tareas en cada nodo

### Flujo de Ejecución
1. La aplicación Spark crea el SparkContext/SparkSession
2. SparkContext se conecta al Cluster Manager
3. Spark adquiere executors en los nodos del cluster
4. Spark envía el código de la aplicación a los executors
5. SparkContext envía tareas a los executors para ejecutar

### Arquitectura de Memoria
- **Storage Memory**: Para almacenamiento en caché y propagación de datos
- **Execution Memory**: Para cálculos como shuffle, joins, sorts y agregaciones
- **User Memory**: Para estructuras de datos definidas por el usuario
- **Reserved Memory**: Para operaciones internas de Spark

### Planificación y Ejecución de Tareas
- **DAG Scheduler**: Divide el grafo en etapas de tareas
- **Task Scheduler**: Asigna tareas a los workers disponibles
- **Block Manager**: Servicio para almacenar y recuperar bloques de datos
- **Shuffle Manager**: Gestiona la transferencia de datos entre etapas

## 3. Componentes del Ecosistema Spark

### Spark Core
- Base del sistema
- Gestión de memoria, recuperación de fallos, planificación y distribución de tareas
- API para RDDs (Resilient Distributed Datasets)
- Operaciones básicas como map, reduce, filter, y collect

### Spark SQL
- Módulo para trabajar con datos estructurados
- Permite consultas SQL y HiveQL
- Proporciona DataFrames y Datasets APIs
- Optimizer (Catalyst) para optimización de consultas

### Spark Streaming
- Procesamiento de datos en tiempo real
- Microbatches para simular streaming
- Integración con múltiples fuentes de datos (Kafka, Flume, HDFS)
- API Structured Streaming para procesamiento de datos continuo

### MLlib
- Biblioteca de machine learning
- Algoritmos escalables para clasificación, regresión, clustering, etc.
- Pipelines de ML y herramientas de evaluación
- Integraciones con bibliotecas numéricas

### GraphX
- API para el procesamiento de grafos
- Algoritmos como PageRank y Connected Components
- Combina la API de grafos con RDDs
- Operaciones para manipulación y análisis de estructuras de grafos

## 4. RDD: Resilient Distributed Datasets

### Fundamentos de RDD
- Colección distribuida e inmutable de objetos
- División de datos en particiones lógicas (procesadas en diferentes nodos)
- Reconstrucción automática en caso de fallo (resiliencia)
- Linaje de transformaciones para recuperación ante fallos

### Creación de RDDs
- **Paralelizando colecciones existentes**:
```python
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
```

- **Desde fuentes externas**:
```python
# Desde archivos
rdd = spark.sparkContext.textFile("hdfs://...")

# Desde Hive
rdd = hiveContext.table("tableName").rdd
```

### Operaciones RDD
- **Transformaciones**: Crean un nuevo RDD (map, filter, flatMap, etc.)
- **Acciones**: Devuelven valores al driver (reduce, collect, count, etc.)
- **Evaluación Perezosa**: Las transformaciones solo se calculan cuando se necesitan

#### Transformaciones Comunes
```python
# Map: aplica función a cada elemento
mapped_rdd = rdd.map(lambda x: x * 2)

# Filter: selecciona elementos que cumplen predicado
filtered_rdd = rdd.filter(lambda x: x > 10)

# FlatMap: aplica función que devuelve iterables y los concatena
words_rdd = text_rdd.flatMap(lambda line: line.split(" "))

# ReduceByKey: agrega valores por clave
word_counts = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
```

#### Acciones Comunes
```python
# Collect: devuelve todos los elementos al driver
result = rdd.collect()

# Count: cuenta elementos
count = rdd.count()

# First: devuelve el primer elemento
first_element = rdd.first()

# Take: devuelve n elementos
n_elements = rdd.take(10)

# Reduce: agrega elementos con función binaria
sum_all = rdd.reduce(lambda a, b: a + b)
```

### Persistencia
- `cache()`: Almacena RDD en memoria
- `persist()`: Permite especificar nivel de almacenamiento (memoria, disco, etc.)
- `unpersist()`: Elimina RDD de la memoria

```python
# Niveles de persistencia
from pyspark import StorageLevel
rdd.persist(StorageLevel.MEMORY_AND_DISK)
```

### Particionamiento
- **HashPartitioner**: Distribución basada en hash de la clave
- **RangePartitioner**: Distribución basada en rangos ordenados de claves
- **CustomPartitioner**: Implementación personalizada para casos específicos

```python
# Reparticionamiento
repartitioned_rdd = rdd.repartition(10)  # Cambia número de particiones

# Particionamiento personalizado (para RDDs de pares)
pair_rdd = rdd.map(lambda x: (x % 10, x))
partitioned_rdd = pair_rdd.partitionBy(10, lambda key: key % 10)
```

### Operaciones Avanzadas
- **Joins**: Inner, outer, left, right
- **Cogroups**: Agrupar datos de múltiples RDDs
- **Aggregations**: Funciones como aggregateByKey, foldByKey
- **Set Operations**: union, intersection, subtract, cartesian

## 5. DataFrames y Datasets

### DataFrames
- Colección distribuida de datos organizados en columnas nombradas
- Similar a una tabla de base de datos relacional
- Optimización automática mediante Catalyst Optimizer
- Implementados sobre RDDs pero con optimizaciones adicionales
- Schema-on-read para inferencia de tipos automática

### Arquitectura Interna de DataFrames
- Representación interna como plan lógico de ejecución
- Árbol de transformaciones que se optimiza mediante reglas
- Conversión a plan físico para ejecución optimizada
- Serialización eficiente usando el formato Tungsten

### Catalyst Optimizer en Detalle
- **Análisis**: Resolución de atributos y tipos
- **Optimización Lógica**: Pushdown de predicados, eliminación de proyecciones innecesarias
- **Planificación Física**: Selección de estrategias de join (broadcast vs. sort-merge)
- **Generación de Código**: Conversión a código Java bytecode para ejecución eficiente

### Creación de DataFrames
```python
# Desde RDDs
rdd = spark.sparkContext.parallelize([(1, "John"), (2, "Alice"), (3, "Bob")])
df = rdd.toDF(["id", "name"])

# Desde archivos
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df = spark.read.json("path/to/file.json")
df = spark.read.parquet("path/to/file.parquet")

# Desde bases de datos
df = spark.read.jdbc(url="jdbc:postgresql:dbserver", table="schema.table", 
                     properties={"user": "username", "password": "password"})

# Desde Hive
df = spark.table("db_name.table_name")
```

### Operaciones Básicas con DataFrames
```python
# Mostrar esquema
df.printSchema()

# Seleccionar columnas
selected_df = df.select("name", "age")

# Filtrar filas
filtered_df = df.filter(df.age > 25)

# Agrupar y agregar
grouped_df = df.groupBy("department").agg({"salary": "avg", "age": "max"})

# Ordenar
sorted_df = df.orderBy(df.age.desc())

# Añadir columnas
df_with_new_col = df.withColumn("salary_double", df.salary * 2)

# Renombrar columnas
renamed_df = df.withColumnRenamed("salary", "income")
```

### Operaciones Avanzadas con DataFrames
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Funciones de ventana
window_spec = Window.partitionBy("department").orderBy("salary")
df_with_rank = df.withColumn("rank", F.rank().over(window_spec))

# Joins
joined_df = df.join(dept_df, df.dept_id == dept_df.id, "inner")

# Pivoting
pivoted_df = df.groupBy("department").pivot("country").agg(F.avg("salary"))

# Funciones definidas por el usuario (UDFs)
from pyspark.sql.types import IntegerType
square_udf = F.udf(lambda x: x*x, IntegerType())
df_with_square = df.withColumn("salary_squared", square_udf(df.salary))
```

### Datasets
- Extensión de DataFrames con tipado fuerte
- API tipada disponible en Scala (`Dataset[T]`) y Java
- En Python solo se dispone de DataFrames debido a la naturaleza dinámica del lenguaje
- Comprobación de tipos en tiempo de compilación (Scala/Java)
- Codificadores personalizados para tipos complejos

### Encoders
- Mecanismo para convertir tipos JVM a formato interno de Spark
- Más eficientes que la serialización Java estándar (hasta 10x)
- Tipos de encoders:
  - **ExpressionEncoder**: Para tipos de usuario (case classes en Scala)
  - **Encoders incorporados**: Para tipos primitivos y estándar

### Ejemplo en Scala (Datasets)
```scala
// Definir una case class
case class Person(name: String, age: Int)

// Crear Dataset
val data = Seq(Person("John", 30), Person("Alice", 25))
val ds = spark.createDataset(data)

// Operaciones tipadas
val filteredDS = ds.filter(_.age > 25)
```

## 6. Spark SQL

### Fundamentos
- Módulo para procesamiento de datos estructurados
- Soporte para SQL estándar y HiveQL
- Conexión JDBC/ODBC para herramientas de BI
- Catálogo compartido de tablas y vistas

### Catálogo y Metastore
- Hive Metastore para almacenar metadatos
- Tablas temporales vs. tablas permanentes
- Catálogo para organizar tablas y vistas

### Consultas SQL en Spark
```python
# Registrar DataFrame como vista temporal
df.createOrReplaceTempView("people")

# Ejecutar consulta SQL
result = spark.sql("""
    SELECT city, AVG(age) as avg_age 
    FROM people 
    WHERE age > 25 
    GROUP BY city
    HAVING AVG(age) > 30
    ORDER BY avg_age DESC
""")

# Mostrar resultados
result.show()
```

### Integración con Hive
```python
# Configurar Hive
spark = SparkSession.builder \
    .appName("SparkWithHive") \
    .enableHiveSupport() \
    .getOrCreate()

# Crear tabla Hive
spark.sql("CREATE TABLE IF NOT EXISTS default.people (name STRING, age INT)")

# Insertar datos
spark.sql("INSERT INTO default.people VALUES ('John', 30), ('Alice', 25)")

# Consultar tabla Hive
result = spark.sql("SELECT * FROM default.people WHERE age > 25")
```

### Tipos de Tablas
- **Managed Tables**: Spark gestiona los datos y metadatos
- **External Tables**: Spark gestiona solo los metadatos

```python
# Tabla gestionada
spark.sql("CREATE TABLE managed_table (id INT, name STRING)")

# Tabla externa
spark.sql("""
    CREATE EXTERNAL TABLE external_table (id INT, name STRING)
    LOCATION '/path/to/external/data'
""")
```

### Optimización de Consultas
- **Catalyst Optimizer**: Framework extensible para optimización de consultas
- **Cost-Based Optimization (CBO)**: Selección de plan basada en estadísticas
- **Predicate Pushdown**: Filtrado temprano de datos
- **Column Pruning**: Lectura selectiva de columnas

### Explicación de Planes de Ejecución
```python
# Ver plan lógico y físico de ejecución
df.explain(True)

# Analizar plan de ejecución para consulta SQL
spark.sql("SELECT * FROM people WHERE age > 25").explain()
```

### Formatos de Datos Soportados
- CSV, JSON, Parquet, ORC, Avro
- JDBC para conexión a bases de datos
- Personalizado a través de Data Sources API

```python
# Lectura y escritura de diferentes formatos
df = spark.read.format("parquet").load("path/to/file.parquet")
df.write.format("orc").mode("overwrite").save("path/to/output")
```

## 7. Ejercicios Prácticos con Datasets Reales

### Ejercicio 1: Análisis de Datos de Ventas

En este ejercicio usaremos el dataset "E-Commerce Sales Data" disponible en Kaggle (https://www.kaggle.com/datasets/benroshan/ecommerce-data).

#### Paso 1: Cargar los datos
```python
# Importar librerías necesarias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Crear SparkSession
spark = SparkSession.builder \
    .appName("E-Commerce Analysis") \
    .getOrCreate()

# Definir esquema para mejor rendimiento
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

# Cargar datos CSV
sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("path/to/ecommerce_data.csv")

# Mostrar esquema y primeras filas
sales_df.printSchema()
sales_df.show(5)
```

#### Paso 2: Transformaciones básicas
```python
# Añadir columna de importe total
sales_df = sales_df.withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))

# Filtrar registros válidos
valid_sales = sales_df.filter(
    col("Quantity") > 0 &
    col("UnitPrice") > 0 &
    col("CustomerID").isNotNull()
)

# Extraer componentes de fecha
sales_with_date = valid_sales \
    .withColumn("InvoiceYear", year(col("InvoiceDate"))) \
    .withColumn("InvoiceMonth", month(col("InvoiceDate"))) \
    .withColumn("InvoiceDay", dayofmonth(col("InvoiceDate")))
```

#### Paso 3: Análisis agregado
```python
# Ventas por país
country_sales = valid_sales \
    .groupBy("Country") \
    .agg(
        sum("TotalAmount").alias("TotalSales"),
        count("InvoiceNo").alias("NumberOfTransactions"),
        countDistinct("CustomerID").alias("UniqueCustomers"),
        avg("TotalAmount").alias("AverageTransactionValue")
    ) \
    .orderBy(desc("TotalSales"))

country_sales.show(10)

# Ventas por mes
monthly_sales = sales_with_date \
    .groupBy("InvoiceYear", "InvoiceMonth") \
    .agg(sum("TotalAmount").alias("MonthlySales")) \
    .orderBy("InvoiceYear", "InvoiceMonth")

monthly_sales.show()
```

#### Paso 4: Análisis de productos más vendidos
```python
# Top 10 productos más vendidos
top_products = valid_sales \
    .groupBy("StockCode", "Description") \
    .agg(
        sum("Quantity").alias("TotalQuantity"),
        sum("TotalAmount").alias("TotalRevenue"),
        count("InvoiceNo").alias("TimesOrdered")
    ) \
    .orderBy(desc("TotalQuantity"))

top_products.show(10)
```

#### Paso 5: Análisis de clientes
```python
# RFM Analysis (Recency, Frequency, Monetary)
from pyspark.sql.window import Window

# Obtener fecha máxima en el dataset
max_date = valid_sales.agg(max("InvoiceDate")).collect()[0][0]

# Calcular métricas RFM
rfm = valid_sales \
    .groupBy("CustomerID") \
    .agg(
        datediff(lit(max_date), max("InvoiceDate")).alias("Recency"),
        countDistinct("InvoiceNo").alias("Frequency"),
        sum("TotalAmount").alias("MonetaryValue")
    )

# Categorizar clientes por segmentos
rfm = rfm \
    .withColumn("RecencyScore", 
                when(col("Recency") <= 30, 5)
                .when(col("Recency") <= 60, 4)
                .when(col("Recency") <= 90, 3)
                .when(col("Recency") <= 120, 2)
                .otherwise(1)) \
    .withColumn("FrequencyScore",
                when(col("Frequency") >= 20, 5)
                .when(col("Frequency") >= 10, 4)
                .when(col("Frequency") >= 5, 3)
                .when(col("Frequency") >= 2, 2)
                .otherwise(1)) \
    .withColumn("MonetaryScore",
                when(col("MonetaryValue") >= 5000, 5)
                .when(col("MonetaryValue") >= 2500, 4)
                .when(col("MonetaryValue") >= 1000, 3)
                .when(col("MonetaryValue") >= 500, 2)
                .otherwise(1))

# Calcular RFM Score final
rfm = rfm.withColumn("RFMScore", 
                     col("RecencyScore") + col("FrequencyScore") + col("MonetaryScore"))

# Mostrar resultados de clientes por segmento
rfm_segments = rfm \
    .withColumn("Segment", 
                when(col("RFMScore") >= 13, "Champions")
                .when(col("RFMScore") >= 10, "Loyal Customers")
                .when(col("RFMScore") >= 7, "Potential Loyalists")
                .when(col("RFMScore") >= 4, "At Risk")
                .otherwise("Hibernating"))

rfm_segments \
    .groupBy("Segment") \
    .agg(
        count("CustomerID").alias("CustomerCount"),
        avg("MonetaryValue").alias("AverageSpend")
    ) \
    .orderBy(desc("AverageSpend")) \
    .show()
```

#### Paso 6: Guardar resultados procesados
```python
# Guardar resultados en formato Parquet
country_sales.write.mode("overwrite").parquet("path/to/output/country_sales")
top_products.write.mode("overwrite").parquet("path/to/output/top_products")
rfm_segments.write.mode("overwrite").parquet("path/to/output/customer_segments")
```

### Ejercicio 2: Análisis de Datos de COVID-19

En este ejercicio utilizaremos el "COVID-19 World Vaccination Progress" dataset disponible en Kaggle (https://www.kaggle.com/datasets/gpreda/covid-world-vaccination-progress).

#### Paso 1: Cargar los datos
```python
# Definir esquema para el dataset de vacunación
vaccination_schema = StructType([
    StructField("country", StringType(), True),
    StructField("iso_code", StringType(), True),
    StructField("date", DateType(), True),
    StructField("total_vaccinations", DoubleType(), True),
    StructField("people_vaccinated", DoubleType(), True),
    StructField("people_fully_vaccinated", DoubleType(), True),
    StructField("daily_vaccinations_raw", DoubleType(), True),
    StructField("daily_vaccinations", DoubleType(), True),
    StructField("total_vaccinations_per_hundred", DoubleType(), True),
    StructField("people_vaccinated_per_hundred", DoubleType(), True),
    StructField("people_fully_vaccinated_per_hundred", DoubleType(), True),
    StructField("daily_vaccinations_per_million", DoubleType(), True),
    StructField("vaccines", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("source_website", StringType(), True)
])

# Cargar datos CSV
vac_df = spark.read \
    .option("header", "true") \
    .schema(vaccination_schema) \
    .csv("path/to/country_vaccinations.csv")

# Mostrar esquema y datos de muestra
vac_df.printSchema()
vac_df.show(5)
```

#### Paso 2: Limpieza y preparación de datos
```python
# Manejo de valores nulos y duplicados
clean_vac_df = vac_df \
    .dropDuplicates(["country", "date"]) \
    .na.fill(0, ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", 
                "daily_vaccinations", "daily_vaccinations_raw"]) \
    .filter(col("country").isNotNull())

# Añadir columnas derivadas
enhanced_vac_df = clean_vac_df \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("week_of_year", weekofyear(col("date"))) \
    .withColumn("vaccination_gap", 
                col("people_vaccinated") - col("people_fully_vaccinated"))

# Crear una vista temporal para consultas SQL
enhanced_vac_df.createOrReplaceTempView("vaccinations")
```

#### Paso 3: Análisis global de vacunación
```python
# Progreso de vacunación global por mes
monthly_progress = spark.sql("""
    SELECT 
        year,
        month,
        SUM(daily_vaccinations) AS total_monthly_vaccinations,
        SUM(daily_vaccinations) / COUNT(DISTINCT country) AS avg_daily_per_country
    FROM vaccinations
    GROUP BY year, month
    ORDER BY year, month
""")

monthly_progress.show()

# Top 10 países con mayor tasa de vacunación completa (última fecha disponible)
latest_by_country = enhanced_vac_df \
    .groupBy("country") \
    .agg(max("date").alias("latest_date"))

latest_data = latest_by_country.join(
    enhanced_vac_df,
    (latest_by_country.country == enhanced_vac_df.country) & 
    (latest_by_country.latest_date == enhanced_vac_df.date)
).select(enhanced_vac_df["*"])

top_vaccination_rates = latest_data \
    .select("country", "people_fully_vaccinated_per_hundred", "date") \
    .orderBy(desc("people_fully_vaccinated_per_hundred")) \
    .limit(10)

top_vaccination_rates.show()
```

#### Paso 4: Análisis de tipos de vacunas
```python
# Dividir la columna de vacunas en un array
from pyspark.sql.functions import split, explode

# Extraer tipos de vacunas utilizadas por cada país
vaccines_by_country = enhanced_vac_df \
    .select("country", "vaccines") \
    .distinct() \
    .withColumn("vaccine_type", explode(split(col("vaccines"), ", ")))

# Contar países que utilizan cada tipo de vacuna
vaccine_popularity = vaccines_by_country \
    .groupBy("vaccine_type") \
    .count() \
    .orderBy(desc("count"))

vaccine_popularity.show()

# Países que utilizan múltiples tipos de vacunas
vaccine_counts_by_country = vaccines_by_country \
    .groupBy("country") \
    .count() \
    .withColumnRenamed("count", "number_of_vaccines") \
    .orderBy(desc("number_of_vaccines"))

vaccine_counts_by_country.show(10)
```

#### Paso 5: Análisis de velocidad y eficiencia de vacunación
```python
# Calcular promedio de vacunaciones diarias por país
daily_avg = enhanced_vac_df \
    .filter(col("daily_vaccinations") > 0) \
    .groupBy("country") \
    .agg(
        avg("daily_vaccinations").alias("avg_daily"),
        max("daily_vaccinations").alias("max_daily"),
        sum("daily_vaccinations").alias("total_vaccinations")
    )

# Calcular eficiencia por población (usando datos per hundred/million)
from pyspark.sql.functions import col, expr

vaccination_efficiency = latest_data \
    .join(daily_avg, "country") \
    .withColumn("days_to_full_vaccination", 
                expr("CASE WHEN daily_vaccinations_per_million > 0 
                     THEN (1000000 - people_fully_vaccinated_per_hundred * 10000) / daily_vaccinations_per_million 
                     ELSE 0 END")) \
    .select(
        "country", 
        "people_vaccinated_per_hundred",
        "people_fully_vaccinated_per_hundred", 
        "daily_vaccinations_per_million",
        "days_to_full_vaccination"
    ) \
    .filter(col("days_to_full_vaccination") > 0) \
    .orderBy("days_to_full_vaccination")

vaccination_efficiency.show(10)
```

#### Paso 6: Análisis de tendencias temporales usando Window Functions
```python
from pyspark.sql.window import Window

# Definir especificación de ventana por país y fecha
window_spec = Window.partitionBy("country").orderBy("date")

# Calcular cambio diario y promedio móvil de 7 días
trend_analysis = enhanced_vac_df \
    .withColumn("prev_day_vaccinations", lag("daily_vaccinations", 1).over(window_spec)) \
    .withColumn("vaccination_change", 
                col("daily_vaccinations") - col("prev_day_vaccinations")) \
    .withColumn("rolling_avg_7day", 
                avg("daily_vaccinations").over(window_spec.rowsBetween(-6, 0))) \
    .select(
        "country", 
        "date", 
        "daily_vaccinations",
        "vaccination_change",
        "rolling_avg_7day"
    )

# Mostrar tendencias para un país específico
trend_analysis \
    .filter(col("country") == "United States") \
    .orderBy("date") \
    .show(10)

# Identificar países con tendencias crecientes vs decrecientes (en el último mes de datos)
latest_month = enhanced_vac_df.agg(max("date")).collect()[0][0]
one_month_ago = latest_month - expr("INTERVAL 30 DAYS")

recent_trends = trend_analysis \
    .filter(col("date").between(one_month_ago, latest_month)) \
    .groupBy("country") \
    .agg(
        avg("vaccination_change").alias("avg_daily_change"),
        avg("rolling_avg_7day").alias("avg_daily_vaccinations")
    ) \
    .withColumn("trend", 
                when(col("avg_daily_change") > 0, "Increasing")
                .when(col("avg_daily_change") < 0, "Decreasing")
                .otherwise("Stable")) \
    .orderBy(desc("avg_daily_vaccinations"))

recent_trends.show(10)
```

#### Paso 7: Guardar resultados analizados
```python
# Guardar resultados en formato Parquet
monthly_progress.write.mode("overwrite").parquet("path/to/output/monthly_progress")
top_vaccination_rates.write.mode("overwrite").parquet("path/to/output/top_countries")
vaccine_popularity.write.mode("overwrite").parquet("path/to/output/vaccine_popularity")
recent_trends.write.mode("overwrite").parquet("path/to/output/vaccination_trends")
```

## 8. Recursos Adicionales

### Documentación Oficial
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API](https://spark.apache.org/docs/latest/api/python/index.html)

### Libros Recomendados
- "Learning Spark" por Holden Karau, Andy Konwinski, Patrick Wendell y Matei Zaharia
- "Spark: The Definitive Guide" por Bill Chambers y Matei Zaharia
- "High Performance Spark" por Holden Karau y Rachel Warren

### Cursos Online
- Databricks Academy
- Coursera: "Big Data Analysis with Spark"
- edX: "Data Science and Engineering with Spark"

### Comunidad
- [Stack Overflow - Spark](https://stackoverflow.com/questions/tagged/apache-spark)
- [Apache Spark User List](https://spark.apache.org/community.html)
- Meetups y conferencias (Spark Summit)

---

## Apéndice: Cheatsheet de Comandos Comunes

### Iniciar Spark Shell
```bash
# PySpark
pyspark --master yarn --num-executors 10

# Scala
spark-shell --master yarn --executor-memory 4g
```

### Operaciones Básicas en PySpark
```python
# Crear SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("MiAplicacion") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# Leer datos
df = spark.read.csv("datos.csv", header=True, inferSchema=True)

# Mostrar esquema
df.printSchema()

# Operaciones básicas
df.select("columna1", "columna2").show()
df.filter(df.edad > 30).show()
df.groupBy("departamento").count().show()

# UDFs (User Defined Functions)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

categorizar = udf(lambda x: "Alto" if x > 100 else "Bajo", StringType())
df.withColumn("categoria", categorizar(df.valor)).show()
```

### Configuración de Logging
```
# En log4j.properties
log4j.rootCategory=WARN, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
```

### Monitoreo con Web UI
- Jobs: http://spark-master:4040/jobs/
- Stages: http://spark-master:4040/stages/
- Storage: http://spark-master:4040/storage/
- Environment: http://spark-master:4040/environment/
- Executors: http://spark-master:4040/executors/
