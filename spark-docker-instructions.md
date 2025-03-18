# Ejecutando PySpark con Docker localmente

Este documento proporciona instrucciones para configurar y ejecutar un entorno local de PySpark utilizando Docker, lo que te permite tener un entorno aislado y consistente para desarrollar y probar tus aplicaciones de Spark.

## Requisitos previos

- Docker instalado en tu sistema ([Guía de instalación de Docker](https://docs.docker.com/get-docker/))
- Conocimientos básicos de Docker y línea de comandos

## Configuración del entorno

### Paso 1: Crear el Dockerfile

1. Crea un nuevo directorio para tu proyecto:

```bash
mkdir -p spark-jupyter-docker/notebooks
cd spark-jupyter-docker
```

2. Copia el contenido del Dockerfile proporcionado en un archivo llamado `Dockerfile`:

```bash
nano Dockerfile
# (Pega el contenido del archivo Dockerfile y guarda)
```

### Paso 2: Construir la imagen Docker

```bash
docker build -t spark-jupyter:latest .
```

Este proceso puede tardar varios minutos, ya que Docker descargará todas las dependencias necesarias.

### Paso 3: Ejecutar el contenedor

```bash
docker run -it --name spark-jupyter \
    -p 8888:8888 \
    -p 4040:4040 \
    -v $(pwd)/notebooks:/home/jovyan/work/notebooks \
    -v $(pwd)/data:/home/jovyan/work/data \
    spark-jupyter:latest
```

Esto:
- Mapea el puerto 8888 para JupyterLab
- Mapea el puerto 4040 para la interfaz de usuario de Spark
- Monta la carpeta local `notebooks` en el contenedor
- Monta la carpeta local `data` en el contenedor

### Paso 4: Acceder a JupyterLab

Una vez que el contenedor esté en ejecución, abre tu navegador y navega a:

```
http://localhost:8888
```

Deberías ver la interfaz de JupyterLab, donde podrás crear nuevos notebooks para trabajar con PySpark.

## Trabajando con el entorno

### Copiar el Notebook de Spark

1. Descarga el notebook de Spark proporcionado (`spark-notebook-colab.ipynb`)
2. Muévelo a la carpeta `notebooks` que has creado

```bash
mv descargas/spark-notebook-colab.ipynb spark-jupyter-docker/notebooks/
```

3. Abre el notebook desde JupyterLab

### Modificaciones para entorno local

Cuando trabajes con el notebook en un entorno local con Docker, debes realizar los siguientes cambios:

1. **Omitir la instalación de PySpark**: La imagen ya tiene PySpark instalado

```python
# Reemplaza la sección de instalación con:
import findspark
findspark.init()
print("¡Spark está configurado y listo para usar!")
```

2. **Utilizar rutas locales para datos**: Asegúrate de guardar tus archivos en la carpeta `data` montada

```python
# Ejemplo:
df = spark.read.csv("/home/jovyan/work/data/archivo.csv", header=True, inferSchema=True)
```

## Compartir datos con el contenedor Docker

Para cargar datos en tu entorno:

1. Coloca los archivos en la carpeta `data` local
2. Estos archivos estarán disponibles en `/home/jovyan/work/data` dentro del contenedor

## Administración del contenedor

### Detener el contenedor

```bash
docker stop spark-jupyter
```

### Reiniciar el contenedor

```bash
docker start spark-jupyter
```

### Ver los logs

```bash
docker logs spark-jupyter
```

### Eliminar el contenedor

```bash
docker rm spark-jupyter
```

## Solución de problemas comunes

### Problema: Error "Address already in use"

Si ves un error indicando que el puerto 8888 está en uso:

```bash
docker run -it --name spark-jupyter \
    -p 8889:8888 \
    -p 4041:4040 \
    -v $(pwd)/notebooks:/home/jovyan/work/notebooks \
    -v $(pwd)/data:/home/jovyan/work/data \
    spark-jupyter:latest
```

Luego accede a JupyterLab en `http://localhost:8889`.

### Problema: Permisos insuficientes

Si encuentras problemas de permisos con los archivos montados:

```bash
# Desde tu sistema host:
chmod -R 777 notebooks data
```

## Recursos adicionales

- [Documentación oficial de Docker](https://docs.docker.com/)
- [Documentación oficial de Apache Spark](https://spark.apache.org/docs/latest/)
- [Documentación de Jupyter Docker Stacks](https://jupyter-docker-stacks.readthedocs.io/)
