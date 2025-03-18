{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "title-header"
   },
   "source": [
    "# Guía Práctica de Apache Spark para Big Data\n",
    "\n",
    "Este notebook proporciona una introducción práctica a Apache Spark, desde conceptos básicos hasta operaciones intermedias, utilizando PySpark en Google Colab.\n",
    "\n",
    "## Contenido\n",
    "1. Configuración del entorno en Colab\n",
    "2. Introducción a Apache Spark\n",
    "3. Creación y Operaciones con RDDs\n",
    "4. DataFrames y Datasets\n",
    "5. Spark SQL\n",
    "6. Ejercicio práctico: Análisis de datos reales"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "setup-section"
   },
   "source": [
    "## 1. Configuración del entorno en Colab\n",
    "\n",
    "Primero, necesitamos instalar y configurar PySpark en Google Colab. Ejecuta la siguiente celda:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "install-spark"
   },
   "outputs": [],
   "source": [
    "# Instalar Java\n",
    "!apt-get update\n",
    "!apt-get install openjdk-8-jdk-headless -qq > /dev/null\n",
    "\n",
    "# Instalar PySpark\n",
    "!pip install pyspark==3.3.2 findspark\n",
    "\n",
    "# Configurar variables de entorno\n",
    "import os\n",
    "os.environ[\"JAVA_HOME\"] = \"/usr/lib/jvm/java-8-openjdk-amd64\"\n",
    "os.environ[\"SPARK_HOME\"] = \"/content/spark-3.3.2-bin-hadoop3\"\n",
    "\n",
    "# Descargar Spark si no está presente\n",
    "!wget -q https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz\n",
    "!tar xf spark-3.3.2-bin-hadoop3.tgz\n",
    "\n",
    "# Inicializar findspark\n",
    "import findspark\n",
    "findspark.init()\n",
    "\n",
    "print(\"¡Spark está configurado y listo para usar!\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "create-session"
   },
   "source": [
    "### Crear una SparkSession\n",
    "\n",
    "La SparkSession es el punto de entrada para interactuar con Spark. A partir de Spark 2.0, la SparkSession proporciona un punto de entrada unificado a todas las funcionalidades de Spark."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "spark-session"
   },
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "\n",
    "# Crear una SparkSession\n",
    "spark = SparkSession.builder \\\n",
    "    .appName(\"PySpark Tutorial\") \\\n",
    "    .config(\"spark.driver.memory\", \"2g\") \\\n",
    "    .getOrCreate()\n",
    "\n",
    "# Verificar versión de Spark\n",
    "print(f\"Versión de Apache Spark: {spark.version}\")\n",
    "\n",
    "# SparkContext está disponible como sc\n",
    "sc = spark.sparkContext\n",
    "print(f\"URL de la interfaz web: {sc.uiWebUrl}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "intro-spark"
   },
   "source": [
    "## 2. Introducción a Apache Spark\n",
    "\n",
    "Apache Spark es un framework de computación distribuida diseñado para el procesamiento de grandes volúmenes de datos. Lo que distingue a Spark de otros frameworks como Hadoop MapReduce es su capacidad para procesar datos en memoria, lo que lo hace significativamente más rápido.\n",
    "\n",
    "### Características principales de Spark\n",
    "- **Velocidad**: 100x más rápido que Hadoop MapReduce para procesamiento en memoria\n",
    "- **Facilidad de uso**: APIs en Java, Scala, Python y R\n",
    "- **Generalidad**: Combina SQL, streaming y análisis complejo\n",
    "- **Compatibilidad**: Funciona con diversas fuentes de datos (HDFS, S3, HBase, etc.)\n",
    "\n",
    "### Arquitectura de Spark\n",
    "- **Driver Program**: Contiene la aplicación principal y crea el SparkContext\n",
    "- **Cluster Manager**: Asigna recursos (YARN, Mesos, Kubernetes, Standalone)\n",
    "- **Worker Nodes**: Ejecutan las tareas de computación\n",
    "- **Executors**: Procesos JVM que ejecutan tareas en cada nodo\n",
    "\n",
    "### Componentes del Ecosistema Spark\n",
    "- **Spark Core**: Base del sistema, APIs para RDDs\n",
    "- **Spark SQL**: Procesamiento de datos estructurados\n",
    "- **Spark Streaming**: Procesamiento en tiempo real\n",
    "- **MLlib**: Biblioteca de machine learning\n",
    "- **GraphX**: Procesamiento de grafos"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "rdds-section"
   },
   "source": [
    "## 3. Creación y Operaciones con RDDs\n",
    "\n",
    "Los RDDs (Resilient Distributed Datasets) son la abstracción fundamental de Spark. Son colecciones inmutables de objetos distribuidos a través de un clúster, que pueden ser procesados en paralelo."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "create-rdds"
   },
   "source": [
    "### Creación de RDDs\n",
    "\n",
    "Hay dos formas principales de crear RDDs:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "parallelize"
   },
   "outputs": [],
   "source": [
    "# 1. Paralelizando una colección existente\n",
    "data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]\n",
    "rdd1 = sc.parallelize(data, numSlices=4)  # numSlices es el número de particiones\n",
    "\n",
    "print(f\"RDD1: {rdd1}\")\n",
    "print(f\"Número de particiones: {rdd1.getNumPartitions()}\")\n",
    "print(f\"Primeros 3 elementos: {rdd1.take(3)}\")\n",
    "print(f\"Conteo: {rdd1.count()}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "textfile-rdd"
   },
   "outputs": [],
   "source": [
    "# 2. Referenciando un dataset externo\n",
    "# Primero creamos un archivo de ejemplo\n",
    "!echo \"Línea 1\\nLínea 2\\nLínea 3\\nLínea 4\\nLínea 5\" > ejemplo.txt\n",
    "\n",
    "# Luego creamos un RDD a partir del archivo\n",
    "rdd2 = sc.textFile(\"ejemplo.txt\")\n",
    "\n",
    "print(f\"RDD2: {rdd2}\")\n",
    "print(f\"Contenido: {rdd2.collect()}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "rdd-operations"
   },
   "source": [
    "### Operaciones con RDDs\n",
    "\n",
    "Las operaciones en RDDs se dividen en dos categorías:\n",
    "\n",
    "- **Transformaciones**: Crean un nuevo RDD a partir de uno existente (map, filter, etc.)\n",
    "- **Acciones**: Devuelven un valor al driver después de ejecutar un cálculo (reduce, count, etc.)\n",
    "\n",
    "Las transformaciones son perezosas (lazy), lo que significa que no se ejecutan inmediatamente. En cambio, Spark recuerda las transformaciones aplicadas a un RDD y las ejecuta solo cuando se requiere una acción."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "transformations"
   },
   "outputs": [],
   "source": [
    "# Ejemplos de transformaciones\n",
    "\n",
    "# map: aplica una función a cada elemento del RDD\n",
    "squared = rdd1.map(lambda x: x * x)\n",
    "print(f\"Después de map (x²): {squared.collect()}\")\n",
    "\n",
    "# filter: selecciona elementos que cumplen una condición\n",
    "evens = rdd1.filter(lambda x: x % 2 == 0)\n",
    "print(f\"Números pares: {evens.collect()}\")\n",
    "\n",
    "# flatMap: aplica función que devuelve múltiples elementos\n",
    "rdd_text = sc.parallelize([\"Hola mundo\", \"Apache Spark\", \"Big Data\"])\n",
    "words = rdd_text.flatMap(lambda line: line.split(\" \"))\n",
    "print(f\"Palabras individuales: {words.collect()}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "actions"
   },
   "outputs": [],
   "source": [
    "# Ejemplos de acciones\n",
    "\n",
    "# reduce: agrega los elementos usando una función\n",
    "sum_all = rdd1.reduce(lambda a, b: a + b)\n",
    "print(f\"Suma de todos los elementos: {sum_all}\")\n",
    "\n",
    "# count: devuelve el número de elementos\n",
    "count = rdd1.count()\n",
    "print(f\"Número de elementos: {count}\")\n",
    "\n",
    "# first: devuelve el primer elemento\n",
    "first_element = rdd1.first()\n",
    "print(f\"Primer elemento: {first_element}\")\n",
    "\n",
    "# take: devuelve n elementos\n",
    "first_n = rdd1.take(3)\n",
    "print(f\"Primeros 3 elementos: {first_n}\")\n",
    "\n",
    "# foreach: ejecuta una función en cada elemento (sin retorno)\n",
    "rdd1.foreach(lambda x: print(f\"Elemento: {x}\"))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "pair-rdds"
   },
   "source": [
    "### RDDs de pares (key-value)\n",
    "\n",
    "Los RDDs de pares son RDDs con elementos en forma de tuplas (clave, valor). Estos RDDs ofrecen operaciones adicionales como reduceByKey, join, etc."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "pair-operations"
   },
   "outputs": [],
   "source": [
    "# Crear un RDD de pares\n",
    "pairs_rdd = sc.parallelize([(\"a\", 1), (\"b\", 2), (\"a\", 3), (\"c\", 4), (\"b\", 5), (\"c\", 6)])\n",
    "print(f\"RDD de pares: {pairs_rdd.collect()}\")\n",
    "\n",
    "# reduceByKey: combina valores con la misma clave\n",
    "sums = pairs_rdd.reduceByKey(lambda a, b: a + b)\n",
    "print(f\"Suma por clave: {sums.collect()}\")\n",
    "\n",
    "# groupByKey: agrupa valores con la misma clave\n",
    "grouped = pairs_rdd.groupByKey().mapValues(list)\n",
    "print(f\"Agrupado por clave: {grouped.collect()}\")\n",
    "\n",
    "# Crear otro RDD de pares para demostraciones\n",
    "other_rdd = sc.parallelize([(\"a\", \"x\"), (\"b\", \"y\"), (\"c\", \"z\")])\n",
    "\n",
    "# join: une RDDs por clave\n",
    "joined = pairs_rdd.join(other_rdd)\n",
    "print(f\"Join de RDDs: {joined.collect()}\")\n",
    "\n",
    "# countByKey: cuenta ocurrencias de cada clave\n",
    "counts = pairs_rdd.countByKey()\n",
    "print(f\"Conteo por clave: {counts}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "persistence"
   },
   "source": [
    "### Persistencia (Caching)\n",
    "\n",
    "Cuando queremos reutilizar un RDD múltiples veces, podemos persistirlo en memoria o disco para mejorar el rendimiento."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "cache-persist"
   },
   "outputs": [],
   "source": [
    "# Crear un RDD y realizar una operación costosa\n",
    "large_rdd = sc.parallelize(range(1, 1000000))\n",
    "filtered = large_rdd.filter(lambda x: x % 10 == 0)\n",
    "\n",
    "# Sin persistencia (calculará dos veces)\n",
    "import time\n",
    "\n",
    "start = time.time()\n",
    "count1 = filtered.count()\n",
    "end1 = time.time() - start\n",
    "\n",
    "start = time.time()\n",
    "count2 = filtered.count()\n",
    "end2 = time.time() - start\n",
    "\n",
    "print(f\"Sin persistencia - Primera ejecución: {end1:.2f} segundos\")\n",
    "print(f\"Sin persistencia - Segunda ejecución: {end2:.2f} segundos\")\n",
    "\n",
    "# Con persistencia\n",
    "filtered.cache()  # equivalente a filtered.persist(StorageLevel.MEMORY_ONLY)\n",
    "\n",
    "start = time.time()\n",
    "count3 = filtered.count()\n",
    "end3 = time.time() - start\n",
    "\n",
    "start = time.time()\n",
    "count4 = filtered.count()\n",
    "end4 = time.time() - start\n",
    "\n",
    "print(f\"Con persistencia - Primera ejecución: {end3:.2f} segundos\")\n",
    "print(f\"Con persistencia - Segunda ejecución: {end4:.2f} segundos\")\n",
    "\n",
    "# Liberar RDD de memoria\n",
    "filtered.unpersist()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "word-count-example"
   },
   "source": [
    "### Ejemplo clásico: WordCount\n",
    "\n",
    "Implementaremos el clásico ejemplo de contar palabras en un texto."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "wordcount"
   },
   "outputs": [],
   "source": [
    "# Crear un texto de ejemplo\n",
    "text = \"\"\"Spark es un framework de procesamiento distribuido.\n",
    "Spark es rápido y fácil de usar.\n",
    "Spark puede procesar datos en memoria.\n",
    "Spark tiene APIs para Java, Scala, Python y R.\n",
    "Spark incluye módulos para SQL, streaming, machine learning y procesamiento de grafos.\"\"\"\n",
    "\n",
    "# Crear archivo de texto\n",
    "with open('ejemplo_wordcount.txt', 'w') as f:\n",
    "    f.write(text)\n",
    "\n",
    "# WordCount en Spark\n",
    "lines = sc.textFile(\"ejemplo_wordcount.txt\")\n",
    "\n",
    "# Separar líneas en palabras y convertir a minúsculas\n",
    "words = lines.flatMap(lambda line: line.lower().split())\n",
    "\n",
    "# Eliminar signos de puntuación\n",
    "import re\n",
    "clean_words = words.map(lambda word: re.sub(r'[^\\w]', '', word))\n",
    "\n",
    "# Filtrar palabras vacías\n",
    "filtered_words = clean_words.filter(lambda word: word != '')\n",
    "\n",
    "# Crear pares (palabra, 1)\n",
    "word_pairs = filtered_words.map(lambda word: (word, 1))\n",
    "\n",
    "# Sumar ocurrencias de cada palabra\n",
    "word_counts = word_pairs.reduceByKey(lambda a, b: a + b)\n",
    "\n",
    "# Ordenar por frecuencia (de mayor a menor)\n",
    "sorted_counts = word_counts.sortBy(lambda x: -x[1])\n",
    "\n",
    "# Mostrar resultados\n",
    "for word, count in sorted_counts.collect():\n",
    "    print(f\"{word}: {count}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "dataframes-section"
   },
   "source": [
    "## 4. DataFrames y Datasets\n",
    "\n",
    "A partir de Spark 1.3, se introdujo el concepto de DataFrames, una abstracción de más alto nivel que los RDDs. Los DataFrames representan datos estructurados en formato de tabla, similares a las tablas en bases de datos relacionales."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "create-dataframes"
   },
   "source": [
    "### Creación de DataFrames\n",
    "\n",
    "Hay varias formas de crear DataFrames en Spark:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "df-from-data"
   },
   "outputs": [],
   "source": [
    "# 1. Desde una lista de datos\n",
    "data = [(\"Juan\", 30), (\"Ana\", 25), (\"Carlos\", 35), (\"María\", 28)]\n",
    "df1 = spark.createDataFrame(data, [\"nombre\", \"edad\"])\n",
    "\n",
    "print(\"DataFrame desde lista:\")\n",
    "df1.show()\n",
    "df1.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "df-from-rdd"
   },
   "outputs": [],
   "source": [
    "# 2. Desde un RDD\n",
    "rdd = sc.parallelize([(\"Marketing\", 1000), (\"Ventas\", 2000), (\"IT\", 1500), (\"RH\", 800)])\n",
    "df2 = rdd.toDF([\"departamento\", \"presupuesto\"])\n",
    "\n",
    "print(\"DataFrame desde RDD:\")\n",
    "df2.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "df-from-schema"
   },
   "outputs": [],
   "source": [
    "# 3. Desde datos con esquema explícito\n",
    "from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType\n",
    "\n",
    "# Definir esquema\n",
    "schema = StructType([\n",
    "    StructField(\"producto\", StringType(), True),\n",
    "    StructField(\"precio\", DoubleType(), True),\n",
    "    StructField(\"cantidad\", IntegerType(), True)\n",
    "])\n",
    "\n",
    "# Datos\n",
    "productos = [\n",
    "    (\"Laptop\", 1200.50, 10),\n",
    "    (\"Smartphone\", 800.99, 20),\n",
    "    (\"Tablet\", 450.75, 15),\n",
    "    (\"Auriculares\", 150.25, 30)\n",
    "]\n",
    "\n",
    "df3 = spark.createDataFrame(productos, schema)\n",
    "\n",
    "print(\"DataFrame con esquema explícito:\")\n",
    "df3.show()\n",
    "df3.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "df-from-file"
   },
   "outputs": [],
   "source": [
    "# 4. Desde archivos\n",
    "# Crear un CSV simple para el ejemplo\n",
    "!echo \"id,ciudad,pais,poblacion\\n1,Madrid,España,3200000\\n2,Barcelona,España,1600000\\n3,Lisboa,Portugal,500000\\n4,Paris,Francia,2200000\" > ciudades.csv\n",
    "\n",
    "# Leer CSV\n",
    "df4 = spark.read.csv(\"ciudades.csv\", header=True, inferSchema=True)\n",
    "\n",
    "print(\"DataFrame desde CSV:\")\n",
    "df4.show()\n",
    "df4.printSchema()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "df-operations"
   },
   "source": [
    "### Operaciones con DataFrames\n",
    "\n",
    "Los DataFrames ofrecen una API rica para manipular datos estructurados."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "df-basic-ops"
   },
   "outputs": [],
   "source": [
    "# Operaciones básicas con DataFrames\n",
    "\n",
    "# Seleccionar columnas\n",
    "print(\"Selección de columnas:\")\n",
    "df3.select(\"producto\", \"precio\").show()\n",
    "\n",
    "# Filtrar filas\n",
    "print(\"\\nFiltrando productos con precio > 500:\")\n",
    "df3.filter(df3.precio > 500).show()\n",
    "\n",
    "# Ordenar\n",
    "print(\"\\nProductos ordenados por precio (descendente):\")\n",
    "df3.orderBy(df3.precio.desc()).show()\n",
    "\n",
    "# Añadir columna calculada\n",
    "print(\"\\nAñadiendo columna de valor total:\")\n",
    "df3.withColumn(\"valor_total\", df3.precio * df3.cantidad).show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "df-agg-ops"
   },
   "outputs": [],
   "source": [
    "# Operaciones de agregación\n",
    "\n",
    "# Estadísticas descriptivas\n",
    "print(\"Estadísticas descriptivas:\")\n",
    "df3.describe().show()\n",
    "\n",
    "# Agregaciones específicas\n",
    "print(\"\\nAgregaciones específicas:\")\n",
    "from pyspark.sql.functions import sum, avg, min, max, count\n",
    "\n",
    "df3.select(\n",
    "    sum(\"cantidad\").alias(\"total_unidades\"),\n",
    "    avg(\"precio\").alias(\"precio_promedio\"),\n",
    "    min(\"precio\").alias(\"precio_minimo\"),\n",
    "    max(\"precio\").alias(\"precio_maximo\")\n",
    ").show()\n",
    "\n",
    "# Agrupar y agregar\n",
    "print(\"\\nAgrupación por país y cálculo de población promedio:\")\n",
    "df4.groupBy(\"pais\").agg(\n",
    "    count(\"ciudad\").alias(\"numero_ciudades\"),\n",
    "    sum(\"poblacion\").alias(\"poblacion_total\"),\n",
    "    avg(\"poblacion\").alias(\"poblacion_promedio\")\n",
    ").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "df-join-ops"
   },
   "outputs": [],
   "source": [
    "# Operaciones de join\n",
    "\n",
    "# Crear DataFrames para demostrar joins\n",
    "clientes = spark.createDataFrame([\n",
    "    (1, \"Juan Pérez\", \"juan@example.com\"),\n",
    "    (2, \"María García\", \"maria@example.com\"),\n",
    "    (3, \"Carlos López\", \"carlos@example.com\"),\n",
    "    (4, \"Ana Martínez\", \"ana@example.com\")\n",
    "], [\"id\", \"nombre\", \"email\"])\n",
    "\n",
    "pedidos = spark.createDataFrame([\n",
    "    (101, 1, \"2023-01-15\", 120.50),\n",
    "    (102, 3, \"2023-01-18\", 85.75),\n",
    "    (103, 2, \"2023-01-20\", 220.00),\n",
    "    (104, 1, \"2023-01-25\", 65.30),\n",
    "    (105, 5, \"2023-01-27\", 110.25)  # Cliente 5 no existe\n",
    "], [\"id_pedido\", \"id_cliente\", \"fecha\", \"importe\"])\n",
    "\n",
    "print(\"DataFrame de Clientes:\")\n",
    "clientes.show()\n",
    "\n",
    "print(\"\\nDataFrame de Pedidos:\")\n",
    "pedidos.show()\n",
    "\n",
    "# Inner join\n",
    "print(\"\\nInner Join:\")\n",
    "clientes.join(pedidos, clientes.id == pedidos.id_cliente).show()\n",
    "\n",
    "# Left join\n",
    "print(\"\\nLeft Join:\")\n",
    "clientes.join(pedidos, clientes.id == pedidos.id_cliente, \"left\").show()\n",
    "\n",
    "# Right join\n",
    "print(\"\\nRight Join:\")\n",
    "clientes.join(pedidos, clientes.id == pedidos.id_cliente, \"right\").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "df-udf"
   },
   "source": [
    "### Funciones definidas por el usuario (UDFs)\n",
    "\n",
    "Las UDFs permiten aplicar funciones personalizadas a los datos en DataFrames."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "udf-examples"
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import udf\n",
    "from pyspark.sql.types import StringType, IntegerType\n",
    "\n",
    "# UDF para convertir nombres a mayúsculas\n",
    "upper_udf = udf(lambda x: x.upper(), StringType())\n",
    "\n",
    "print(\"Nombres en mayúsculas:\")\n",
    "clientes.withColumn(\"nombre_mayus\", upper_udf(clientes.nombre)).show()\n",
    "\n",
    "# UDF para clasificar productos según su precio\n",
    "def clasificar_precio(precio):\n",
    "    if precio < 200:\n",
    "        return \"Económico\"\n",
    "    elif precio < 800:\n",
    "        return \"Medio\"\n",
    "    else:\n",
    "        return \"Premium\"\n",
    "\n",
    "clasificar_udf = udf(clasificar_precio, StringType())\n",
    "\n",
    "print(\"\\nProductos clasificados por precio:\")\n",
    "df3.withColumn(\"categoria\", clasificar_udf(df3.precio)).show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "spark-sql-section"
   },
   "source": [
    "## 5. Spark SQL\n",
    "\n",
    "Spark SQL permite ejecutar consultas SQL sobre datos estructurados. Es una capa encima de la API de DataFrames que proporciona una interfaz para programación con SQL."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "temp-views"
   },
   "outputs": [],
   "source": [
    "# Registrar DataFrames como vistas temporales\n",
    "df3.createOrReplaceTempView(\"productos\")\n",
    "df4.createOrReplaceTempView(\"ciudades\")\n",
    "clientes.createOrReplaceTempView(\"clientes\")\n",
    "pedidos.createOrReplaceTempView(\"pedidos\")\n",
    "\n",
    "# Ejecutar consultas SQL\n",
    "print(\"Productos con precio > 500:\")\n",
    "spark.sql(\"\"\"\n",
    "SELECT \n",
    "    producto, \n",
    "    precio, \n",
    "    cantidad, \n",
    "    precio * cantidad AS valor_total\n",
    "FROM productos\n",
    "WHERE precio > 500\n",
    "ORDER BY precio DESC\n",
    "\"\"\").show()\n",
    "\n",
    "print(\"\\nGrupos de ciudades por país:\")\n",
    "spark.sql(\"\"\"\n",
    "SELECT \n",
    "    pais, \n",
    "    COUNT(*) AS num_ciudades, \n",
    "    SUM(poblacion) AS poblacion_total, \n",
    "    AVG(poblacion) AS poblacion_promedio\n",
    "FROM ciudades\n",
    "GROUP BY pais\n",
    "HAVING COUNT(*) > 0\n",
    "\"\"\").show()\n",
    "\n",
    "print(\"\\nClientes con sus pedidos:\")\n",
    "spark.sql(\"\"\"\n",
    "SELECT \n",
    "    c.id, \n",
    "    c.nombre, \n",
    "    p.id_pedido, \n",
    "    p.fecha, \n",
    "    p.importe\n",
    "FROM clientes c\n",
    "LEFT JOIN pedidos p ON c.id = p.id_cliente\n",
    "ORDER BY c.id, p.fecha\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "sql-window-functions"
   },
   "outputs": [],
   "source": [
    "# Ejemplos de funciones de ventana con SQL\n",
    "print(\"Análisis de pedidos por cliente con Window Functions:\")\n",
    "spark.sql(\"\"\"\n",
    "SELECT \n",
    "    c.id, \n",
    "    c.nombre, \n",
    "    p.id_pedido, \n",
    "    p.fecha, \n",
    "    p.importe,\n",
    "    SUM(p.importe) OVER (PARTITION BY c.id) AS total_cliente,\n",
    "    RANK() OVER (PARTITION BY c.id ORDER BY p.importe DESC) AS ranking_importe\n",
    "FROM clientes c\n",
    "JOIN pedidos p ON c.id = p.id_cliente\n",
    "ORDER BY c.id, ranking_importe\n",
    "\"\"\").show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "optimization"
   },
   "source": [
    "### Optimización de consultas\n",
    "\n",
    "Spark SQL utiliza el optimizador Catalyst para transformar consultas y mejorar su rendimiento. Podemos ver el plan de ejecución con el método `explain()`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "explain-plan"
   },
   "outputs": [],
   "source": [
    "# Crear una consulta compleja\n",
    "query = spark.sql(\"\"\"\n",
    "SELECT \n",
    "    c.pais, \n",
    "    COUNT(DISTINCT p.id) AS num_productos, \n",
    "    SUM(p.precio * p.cantidad) AS valor_total\n",
    "FROM ciudades c\n",
    "JOIN (\n",
    "    SELECT \n",
    "        1 AS id_ciudad, \n",
    "        p.* \n",
    "    FROM productos p\n",
    "    WHERE p.precio > 100\n",
    ") p ON c.id = p.id_ciudad\n",
    "GROUP BY c.pais\n",
    "HAVING SUM(p.precio) > 0\n",
    "ORDER BY valor_total DESC\n",
    "\"\"\")\n",
    "\n",
    "# Ver el plan lógico y físico\n",
    "print(\"Plan de ejecución:\")\n",
    "query.explain(True)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "exercise-section"
   },
   "source": [
    "## 6. Ejercicio práctico: Análisis de datos reales\n",
    "\n",
    "Vamos a trabajar con un conjunto de datos real: el dataset de E-Commerce. Primero, descarguemos el dataset desde Kaggle."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "download-dataset"
   },
   "outputs": [],
   "source": [
    "# Instalamos la API de Kaggle y configuramos credenciales\n",
    "!pip install kaggle\n",
    "\n",
    "# Nota: Para usar la API de Kaggle, necesitas tus credenciales\n",
    "# Puedes descargar kaggle.json desde tu cuenta de Kaggle y subir manualmente\n",
    "# O usar un dataset de ejemplo que creamos aquí\n",
    "\n",
    "# Creamos un dataset de ejemplo (simulando datos de E-Commerce)\n",
    "import pandas as pd\n",
    "import numpy as np\n",
    "from datetime import datetime, timedelta\n",
    "\n",
    "# Generar datos aleatorios\n",
    "np.random.seed(42)\n",
    "n_rows = 1000\n",
    "\n",
    "# Productos\n",
    "productos = ['Laptop', 'Smartphone', 'Tablet', 'Auriculares', 'Monitor', 'Teclado', 'Mouse', 'Impresora', 'Cámara', 'Altavoces']\n",
    "categorias = ['Electrónica', 'Informática', 'Accesorios', 'Audio', 'Fotografía']\n",
    "product_category = {\n",
    "    'Laptop': 'Informática',\n",
    "    'Smartphone': 'Electrónica',\n",
    "    'Tablet': 'Electrónica',\n",
    "    'Auriculares': 'Audio',\n",
    "    'Monitor': 'Informática',\n",
    "    'Teclado': 'Accesorios',\n",
    "    'Mouse': 'Accesorios',\n",
    "    'Impresora': 'Informática',\n",
    "    'Cámara': 'Fotografía',\n",
    "    'Altavoces': 'Audio'\n",
    "}\n",
    "\n",
    "# Precios base\n",
    "precios_base = {\n",
    "    'Laptop': 1200,\n",
    "    'Smartphone': 800,\n",
    "    'Tablet': 500,\n",
    "    'Auriculares': 150,\n",
    "    'Monitor': 300,\n",
    "    'Teclado': 80,\n",
    "    'Mouse': 50,\n",
    "    'Impresora': 250,\n",
    "    'Cámara': 600,\n",
    "    'Altavoces': 200\n",
    "}\n",
    "\n",
    "# Países\n",
    "paises = ['España', 'México', 'Argentina', 'Colombia', 'Chile', 'Perú', 'Estados Unidos', 'Reino Unido', 'Francia', 'Alemania']\n",
    "\n",
    "# Generar datos\n",
    "invoice_no = np.arange(1, n_rows+1)\n",
    "stock_code = np.random.randint(10000, 99999, size=n_rows)\n",
    "description = np.random.choice(productos, size=n_rows)\n",
    "quantity = np.random.randint(1, 10, size=n_rows)\n",
    "\n",
    "# Fechas entre 2022-01-01 y 2022-12-31\n",
    "start_date = datetime(2022, 1, 1)\n",
    "end_date = datetime(2022, 12, 31)\n",
    "days_between = (end_date - start_date).days\n",
    "random_days = np.random.randint(0, days_between, size=n_rows)\n",
    "invoice_date = [start_date + timedelta(days=day) for day in random_days]\n",
    "\n",
    "# Precios con variación aleatoria\n",
    "unit_price = [precios_base[prod] * (0.9 + np.random.random() * 0.2) for prod in description]\n",
    "\n",
    "# Clientes (500 clientes únicos)\n",
    "customer_id = np.random.randint(10000, 19999, size=n_rows)\n",
    "country = np.random.choice(paises, size=n_rows)\n",
    "\n",
    "# Categoría\n",
    "category = [product_category[prod] for prod in description]\n",
    "\n",
    "# Crear DataFrame\n",
    "ecommerce_df = pd.DataFrame({\n",
    "    'InvoiceNo': invoice_no,\n",
    "    'StockCode': stock_code,\n",
    "    'Description': description,\n",
    "    'Quantity': quantity,\n",
    "    'InvoiceDate': invoice_date,\n",
    "    'UnitPrice': unit_price,\n",
    "    'CustomerID': customer_id,\n",
    "    'Country': country,\n",
    "    'Category': category\n",
    "})\n",
    "\n",
    "# Guardar como CSV\n",
    "ecommerce_df.to_csv('ecommerce_data.csv', index=False)\n",
    "print(\"Dataset de ejemplo creado con éxito!\")\n",
    "print(ecommerce_df.head())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "load-ecommerce"
   },
   "outputs": [],
   "source": [
    "# Cargar dataset en Spark\n",
    "from pyspark.sql.functions import col, year, month, dayofmonth, to_date, expr\n",
    "\n",
    "# Definir esquema para mejor rendimiento\n",
    "from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType\n",
    "\n",
    "schema = StructType([\n",
    "    StructField(\"InvoiceNo\", StringType(), True),\n",
    "    StructField(\"StockCode\", StringType(), True),\n",
    "    StructField(\"Description\", StringType(), True),\n",
    "    StructField(\"Quantity\", IntegerType(), True),\n",
    "    StructField(\"InvoiceDate\", TimestampType(), True),\n",
    "    StructField(\"UnitPrice\", DoubleType(), True),\n",
    "    StructField(\"CustomerID\", StringType(), True),\n",
    "    StructField(\"Country\", StringType(), True),\n",
    "    StructField(\"Category\", StringType(), True)\n",
    "])\n",
    "\n",
    "# Cargar datos CSV\n",
    "sales_df = spark.read.format(\"csv\") \\\n",
    "    .option(\"header\", \"true\") \\\n",
    "    .option(\"inferSchema\", \"true\") \\\n",
    "    .load(\"ecommerce_data.csv\")\n",
    "\n",
    "# Mostrar esquema y primeras filas\n",
    "sales_df.printSchema()\n",
    "sales_df.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "data-preprocessing"
   },
   "outputs": [],
   "source": [
    "# Preprocesamiento de datos\n",
    "\n",
    "# Añadir columna de importe total\n",
    "sales_df = sales_df.withColumn(\"TotalAmount\", col(\"Quantity\") * col(\"UnitPrice\"))\n",
    "\n",
    "# Filtrar registros válidos\n",
    "valid_sales = sales_df.filter(\n",
    "    col(\"Quantity\") > 0 &\n",
    "    col(\"UnitPrice\") > 0 &\n",
    "    col(\"CustomerID\").isNotNull()\n",
    ")\n",
    "\n",
    "# Extraer componentes de fecha\n",
    "sales_with_date = valid_sales \\\n",
    "    .withColumn(\"InvoiceYear\", year(col(\"InvoiceDate\"))) \\\n",
    "    .withColumn(\"InvoiceMonth\", month(col(\"InvoiceDate\"))) \\\n",
    "    .withColumn(\"InvoiceDay\", dayofmonth(col(\"InvoiceDate\")))\n",
    "\n",
    "# Guardar como vista temporal para SQL\n",
    "sales_with_date.createOrReplaceTempView(\"sales\")\n",
    "\n",
    "# Mostrar el resultado del preprocesamiento\n",
    "sales_with_date.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "sales-analysis"
   },
   "outputs": [],
   "source": [
    "# Análisis 1: Ventas por país\n",
    "country_sales = valid_sales \\\n",
    "    .groupBy(\"Country\") \\\n",
    "    .agg(\n",
    "        sum(\"TotalAmount\").alias(\"TotalSales\"),\n",
    "        count(\"InvoiceNo\").alias(\"NumberOfTransactions\"),\n",
    "        countDistinct(\"CustomerID\").alias(\"UniqueCustomers\"),\n",
    "        avg(\"TotalAmount\").alias(\"AverageTransactionValue\")\n",
    "    ) \\\n",
    "    .orderBy(col(\"TotalSales\").desc())\n",
    "\n",
    "print(\"Ventas por país:\")\n",
    "country_sales.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "monthly-sales"
   },
   "outputs": [],
   "source": [
    "# Análisis 2: Ventas por mes\n",
    "monthly_sales = sales_with_date \\\n",
    "    .groupBy(\"InvoiceYear\", \"InvoiceMonth\") \\\n",
    "    .agg(sum(\"TotalAmount\").alias(\"MonthlySales\")) \\\n",
    "    .orderBy(\"InvoiceYear\", \"InvoiceMonth\")\n",
    "\n",
    "print(\"Ventas mensuales:\")\n",
    "monthly_sales.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "products-analysis"
   },
   "outputs": [],
   "source": [
    "# Análisis 3: Productos más vendidos\n",
    "top_products = valid_sales \\\n",
    "    .groupBy(\"Description\", \"Category\") \\\n",
    "    .agg(\n",
    "        sum(\"Quantity\").alias(\"TotalQuantity\"),\n",
    "        sum(\"TotalAmount\").alias(\"TotalRevenue\"),\n",
    "        count(\"InvoiceNo\").alias(\"TimesOrdered\")\n",
    "    ) \\\n",
    "    .orderBy(col(\"TotalQuantity\").desc())\n",
    "\n",
    "print(\"Top productos más vendidos:\")\n",
    "top_products.show(10)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "category-analysis"
   },
   "outputs": [],
   "source": [
    "# Análisis 4: Ventas por categoría de producto\n",
    "category_sales = valid_sales \\\n",
    "    .groupBy(\"Category\") \\\n",
    "    .agg(\n",
    "        sum(\"TotalAmount\").alias(\"TotalSales\"),\n",
    "        sum(\"Quantity\").alias(\"TotalQuantity\"),\n",
    "        countDistinct(\"Description\").alias(\"UniqueProducts\")\n",
    "    ) \\\n",
    "    .orderBy(col(\"TotalSales\").desc())\n",
    "\n",
    "print(\"Ventas por categoría:\")\n",
    "category_sales.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "customer-rfm"
   },
   "outputs": [],
   "source": [
    "# Análisis 5: RFM Analysis (Recency, Frequency, Monetary)\n",
    "from pyspark.sql.window import Window\n",
    "from pyspark.sql.functions import datediff, max as sql_max, lit, when\n",
    "\n",
    "# Obtener fecha máxima en el dataset\n",
    "max_date = valid_sales.agg(sql_max(\"InvoiceDate\")).collect()[0][0]\n",
    "\n",
    "# Calcular métricas RFM\n",
    "rfm = valid_sales \\\n",
    "    .groupBy(\"CustomerID\") \\\n",
    "    .agg(\n",
    "        datediff(lit(max_date), sql_max(\"InvoiceDate\")).alias(\"Recency\"),\n",
    "        countDistinct(\"InvoiceNo\").alias(\"Frequency\"),\n",
    "        sum(\"TotalAmount\").alias(\"MonetaryValue\")\n",
    "    )\n",
    "\n",
    "# Categorizar clientes por segmentos\n",
    "rfm = rfm \\\n",
    "    .withColumn(\"RecencyScore\", \n",
    "                when(col(\"Recency\") <= 30, 5)\n",
    "                .when(col(\"Recency\") <= 60, 4)\n",
    "                .when(col(\"Recency\") <= 90, 3)\n",
    "                .when(col(\"Recency\") <= 120, 2)\n",
    "                .otherwise(1)) \\\n",
    "    .withColumn(\"FrequencyScore\",\n",
    "                when(col(\"Frequency\") >= 20, 5)\n",
    "                .when(col(\"Frequency\") >= 10, 4)\n",
    "                .when(col(\"Frequency\") >= 5, 3)\n",
    "                .when(col(\"Frequency\") >= 2, 2)\n",
    "                .otherwise(1)) \\\n",
    "    .withColumn(\"MonetaryScore\",\n",
    "                when(col(\"MonetaryValue\") >= 5000, 5)\n",
    "                .when(col(\"MonetaryValue\") >= 2500, 4)\n",
    "                .when(col(\"MonetaryValue\") >= 1000, 3)\n",
    "                .when(col(\"MonetaryValue\") >= 500, 2)\n",
    "                .otherwise(1))\n",
    "\n",
    "# Calcular RFM Score final\n",
    "rfm = rfm.withColumn(\"RFMScore\", \n",
    "                     col(\"RecencyScore\") + col(\"FrequencyScore\") + col(\"MonetaryScore\"))\n",
    "\n",
    "# Mostrar resultados de clientes por segmento\n",
    "rfm_segments = rfm \\\n",
    "    .withColumn(\"Segment\", \n",
    "                when(col(\"RFMScore\") >= 13, \"Champions\")\n",
    "                .when(col(\"RFMScore\") >= 10, \"Loyal Customers\")\n",
    "                .when(col(\"RFMScore\") >= 7, \"Potential Loyalists\")\n",
    "                .when(col(\"RFMScore\") >= 4, \"At Risk\")\n",
    "                .otherwise(\"Hibernating\"))\n",
    "\n",
    "segment_summary = rfm_segments \\\n",
    "    .groupBy(\"Segment\") \\\n",
    "    .agg(\n",
    "        count(\"CustomerID\").alias(\"CustomerCount\"),\n",
    "        avg(\"MonetaryValue\").alias(\"AverageSpend\")\n",
    "    ) \\\n",
    "    .orderBy(desc(\"AverageSpend\"))\n",
    "\n",
    "print(\"Segmentación de clientes (RFM):\")\n",
    "segment_summary.show()\n",
    "\n",
    "# Mostrar algunos ejemplos de clientes por segmento\n",
    "print(\"\\nEjemplos de clientes por segmento:\")\n",
    "rfm_segments.orderBy(desc(\"RFMScore\")).show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "save-output"
   },
   "outputs": [],
   "source": [
    "# Guardar resultados procesados\n",
    "country_sales.write.mode(\"overwrite\").csv(\"country_sales.csv\")\n",
    "top_products.write.mode(\"overwrite\").csv(\"top_products.csv\")\n",
    "rfm_segments.write.mode(\"overwrite\").csv(\"customer_segments.csv\")\n",
    "\n",
    "print(\"Resultados guardados!\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "id": "conclusion"
   },
   "source": [
    "## Conclusión\n",
    "\n",
    "En este notebook, hemos aprendido los fundamentos de Apache Spark, trabajando con RDDs, DataFrames y Spark SQL. También hemos aplicado estos conocimientos a un análisis de datos real de E-Commerce.\n",
    "\n",
    "Conceptos clave que hemos cubierto:\n",
    "1. RDDs: operaciones básicas, transformaciones, acciones y persistencia\n",
    "2. DataFrames: creación, operaciones y agregaciones\n",
    "3. Spark SQL: consultas SQL en datos estructurados\n",
    "4. Análisis de datos reales con técnicas como RFM\n",
    "\n",
    "Próximos pasos para seguir aprendiendo:\n",
    "- Explorar Spark Streaming para datos en tiempo real\n",
    "- Aprender MLlib para machine learning distribuido\n",
    "- Profundizar en GraphX para procesamiento de grafos\n",
    "- Trabajar con clusters de Spark reales\n",
    "\n",
    "Para más información, consulta la [documentación oficial de Apache Spark](https://spark.apache.org/docs/latest/)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "id": "cleanup"
   },
   "outputs": [],
   "source": [
    "# Detener la sesión de Spark\n",
    "spark.stop()\n",
    "print(\"Sesión de Spark detenida.\")"
   ]
  }
 ],
 "metadata": {
  "colab": {
   "provenance": []
  },
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 1
}