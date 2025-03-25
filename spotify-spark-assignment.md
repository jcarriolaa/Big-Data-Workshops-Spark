# Análisis de Datos con Spark: Explorando el Universo Musical de Spotify

## Introducción

En esta tarea, exploraremos un extenso conjunto de datos de canciones de Spotify. Este dataset contiene características de audio detalladas para cientos de miles de canciones, junto con metadatos como artista, año de lanzamiento, popularidad y géneros musicales. A través del análisis de estos datos, podremos descubrir patrones en la evolución de la música a lo largo del tiempo, entender qué hace que algunas canciones sean más populares que otras, y crear sistemas de recomendación basados en características de audio.

## Dataset

**Nombre del Dataset**: Spotify Song Attributes Dataset
**Fuente**: [Kaggle - Spotify Dataset 1921-2020, 600k+ Tracks](https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks)

Este dataset incluye:
- **tracks.csv**: Contiene información sobre más de 600,000 canciones, con características como:
  - `track_id`: Identificador único de la canción
  - `name`: Nombre de la canción
  - `popularity`: Puntuación de popularidad (0-100)
  - `duration_ms`: Duración en milisegundos
  - `explicit`: Si la canción tiene contenido explícito
  - `artists`: Artistas que participaron en la canción
  - `id_artists`: IDs de los artistas
  - `release_date`: Fecha de lanzamiento
  - `danceability`: Qué tan adecuada es la canción para bailar (0.0-1.0)
  - `energy`: Medida de intensidad y actividad (0.0-1.0)
  - `key`: Tonalidad de la canción (0-11)
  - `loudness`: Volumen general en decibelios (dB)
  - `mode`: Modalidad de la canción (mayor o menor)
  - `speechiness`: Presencia de palabras habladas (0.0-1.0)
  - `acousticness`: Medida de si la canción es acústica (0.0-1.0)
  - `instrumentalness`: Predice si una canción no contiene vocales (0.0-1.0)
  - `liveness`: Detecta presencia de audiencia (0.0-1.0)
  - `valence`: Positividad musical de la canción (0.0-1.0)
  - `tempo`: Velocidad o ritmo estimado en BPM
  - `time_signature`: Compás estimado

- **artists.csv**: Información sobre los artistas, incluyendo:
  - `id`: ID único del artista
  - `followers`: Número de seguidores
  - `genres`: Géneros asociados con el artista
  - `name`: Nombre del artista
  - `popularity`: Puntuación de popularidad (0-100)

## Parte 1: Configuración del Entorno y Carga de Datos

### Paso 1: Configurar Spark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Spotify Data Analysis") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Configurar para mejor rendimiento
spark.conf.set("spark.sql.shuffle.partitions", "8")
```

### Paso 2: Cargar y Examinar los Datos

```python
# Carga de archivos
tracks_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("tracks.csv")

artists_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("artists.csv")

# Examinar el esquema
print("Esquema de tracks_df:")
tracks_df.printSchema()

print("\nEsquema de artists_df:")
artists_df.printSchema()

# Ver las primeras filas
print("\nPrimeras 5 filas de tracks_df:")
tracks_df.show(5)

print("\nPrimeras 5 filas de artists_df:")
artists_df.show(5)

# Información básica
track_count = tracks_df.count()
artist_count = artists_df.count()
print(f"Total de canciones: {track_count}")
print(f"Total de artistas: {artist_count}")
```

### Paso 3: Limpieza y Preparación de Datos

En esta etapa, deberás realizar las siguientes tareas:

1. **Verificar valores nulos** en ambos dataframes (tracks_df y artists_df)
   - Identifica qué columnas contienen valores nulos
   - Determina estrategias para manejar estos valores (eliminar, imputar, etc.)

2. **Preparar los datos para el análisis**:
   - Extraer el año de la columna `release_date` (que puede venir en varios formatos)
   - Convertir la duración de milisegundos a minutos para facilitar la interpretación
   - Procesar la columna de artistas para extraer datos útiles
   - Verificar el rango de años para filtrar posibles valores incorrectos
   - Eliminar registros con valores implausibles o atípicos extremos

3. **Crear un dataframe limpio** (`tracks_clean`) que utilizarás para el resto del análisis

```python
# Tu código de limpieza de datos aquí
```

## Parte 2: Análisis Exploratorio de Datos (EDA)

### Paso 4: Distribución de Características Musicales

El objetivo de este paso es entender la distribución de las características de audio:

1. **Selecciona las características de audio** principales como danceability, energy, acousticness, etc.

2. **Calcula estadísticas descriptivas** (min, max, media, mediana, etc.) para cada característica

3. **Visualiza distribuciones** mediante histogramas 

4. **Analiza correlaciones** entre las diferentes características de audio

```python
# Tu código de análisis de distribuciones aquí
```

### Paso 5: Análisis Temporal de Tendencias Musicales

Explora cómo han evolucionado las características musicales a lo largo del tiempo:

1. **Agrupa los datos por década** para analizar tendencias temporales

2. **Calcula promedios** de cada característica de audio por década

3. **Visualiza la evolución** de estas características mediante gráficos de línea

4. **Analiza la cantidad de canciones** por década para entender el volumen de datos

```python
# Tu código de análisis temporal aquí
```

### Paso 6: Análisis de Popularidad

Investiga qué factores están asociados con la popularidad de las canciones:

1. **Crea categorías de popularidad** (Alta, Media, Baja, Muy baja)

2. **Calcula correlaciones** entre características de audio y popularidad

3. **Compara características** entre diferentes categorías de popularidad

4. **Visualiza las diferencias** mediante gráficos apropiados

```python
# Tu código de análisis de popularidad aquí
```

## Parte 3: Preguntas para Analizar (Para Estudiantes)

Los estudiantes deben escribir código Spark para responder las siguientes preguntas:

1. **Evolución del Género Musical**: Identifica los 5 géneros más populares por década desde 1970 hasta 2020. ¿Cómo han cambiado las preferencias musicales a lo largo del tiempo? 
   - *Pista*: Necesitarás combinar `tracks_df` con `artists_df` y explotar la columna de géneros.

2. **Características Distintivas por Género**: Selecciona 3 géneros musicales diferentes (por ejemplo, rock, hip-hop y jazz) y analiza qué características de audio los distinguen. ¿Qué hace que cada género sea único desde el punto de vista de las características de Spotify?
   - *Pista*: Visualiza las diferencias mediante gráficos de radar o boxplots.

3. **Predicción de Éxito**: Desarrolla un modelo de machine learning utilizando Spark ML que prediga si una canción será popular (popularidad > 70) basándose en sus características de audio. ¿Qué tan preciso es tu modelo? ¿Qué características son más importantes para determinar el éxito?
   - *Pista*: Utiliza clasificación binaria (Random Forest o Gradient Boosting) y evalúa con métricas como AUC y precisión.

4. **Análisis de Outliers Musicales**: Identifica las "canciones atípicas" – aquellas cuyas características de audio difieren significativamente de la norma de su época o género. ¿Estas canciones atípicas tienden a ser más o menos populares? ¿Podrían considerarse innovadoras?
   - *Pista*: Utiliza técnicas como Z-score o algoritmos de detección de anomalías.

## Parte 4: Reto Final

Basándote en el análisis exploratorio y en tus respuestas a las preguntas anteriores, desarrolla un sistema de recomendación musical personalizado utilizando Spark. Tu sistema debe:

1. **Definir "perfiles musicales"** basados en clusters de características de audio (usando K-means o similar).

2. **Crear un motor de recomendación** que sugiera canciones basándose en:
   - Similitud de características de audio
   - Popularidad
   - Diversidad temporal (que incluya tanto clásicos como canciones nuevas)
   - Diversidad de género

3. **Implementar una evaluación del sistema** utilizando métricas como:
   - Diversidad de recomendaciones
   - Novedad (serendipity)
   - Precisión de las recomendaciones

4. **Desarrollar una interfaz conceptual** (puede ser un mockup o diagrama) que muestre cómo se presentarían estas recomendaciones a un usuario.

5. **Analizar escalabilidad**: ¿Cómo escalaría tu solución para millones de usuarios y canciones? ¿Qué arquitectura de Big Data propondrías?

## Entregables

1. **Notebook de Spark** con el código completo, incluyendo:
   - Limpieza y preparación de datos
   - Análisis exploratorio con visualizaciones
   - Respuestas a las 4 preguntas de análisis
   - Implementación del sistema de recomendación

2. **Informe técnico** (3-5 páginas) que incluya:
   - Metodología utilizada
   - Principales hallazgos del análisis exploratorio
   - Interpretación de resultados
   - Limitaciones y posibles mejoras

3. **Presentación** (máximo 10 slides) orientada a un equipo de producto de una plataforma de streaming musical, destacando insights que podrían mejorar la experiencia del usuario.

## Recursos Adicionales

- [Documentación de Spotify Web API](https://developer.spotify.com/documentation/web-api/reference/#endpoint-get-audio-features)
- [Documentación de Spark ML](https://spark.apache.org/docs/latest/ml-guide.html)
- [Sistemas de recomendación con Spark](https://spark.apache.org/docs/latest/ml-collaborative-filtering.html)
- [Paper: Music Recommendation Based on Acoustic Features](https://www.researchgate.net/publication/220723656_Music_Recommendation_Based_on_Acoustic_Features_and_User_Access_Patterns)
- [Tutorial de visualización con Python](https://seaborn.pydata.org/tutorial/aesthetics.html)
