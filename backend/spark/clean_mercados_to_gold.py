import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count, round, desc
import findspark
import os

findspark.init()

import pyspark
from pyspark.sql import SparkSession
import findspark
import os

findspark.init()

# --- ❌ BORRA ESTO ---
# Ya no hace falta definir versiones ni packages aquí
# HADOOP_AWS_VERSION = "3.3.4" 
# AWS_SDK_VERSION = "1.12.500"
# packages = ...

print("Iniciando Spark (Drivers cargados desde Docker)...")

spark = SparkSession.builder \
    .appName("Spark load to silver") \
    .enableHiveSupport() \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_USER', 'test')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_PASSWORD', 'test1234567890')) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("✅ Spark iniciado correctamente.")
# --- PASO 1: LEER DATOS SILVER (PARQUET) ---
print("📥 Leyendo datos desde Silver...", flush=True)
# Spark detecta automáticamente las particiones /year=.../month=...
df_silver = spark.read.parquet("s3a://silver-mercados/mercadona")

# --- PASO 2: TRANSFORMACIÓN DE NEGOCIO (AGREGACIONES) ---
print("⚙️  Calculando estadísticas por categoría...", flush=True)

df_gold = df_silver.groupBy("categoria", "year", "month") \
    .agg(
        count("nombre").alias("total_productos"),
        round(avg("precio"), 2).alias("precio_promedio"),
        min("precio").alias("precio_minimo"),
        max("precio").alias("precio_maximo")
    ) \
    .orderBy(desc("year"), desc("month"), desc("precio_promedio"))

# Mostramos un adelanto en consola
print("--- PREVISUALIZACIÓN DE DATOS GOLD ---")
df_gold.show(10)

# --- PASO 3: ESCRIBIR EN CAPA GOLD ---
print("📤 Escribiendo resumen en Gold...", flush=True)

# Guardamos en el mismo bucket pero en una carpeta nueva 'gold_resumen'
# Usamos coalesce(1) para generar un solo archivo CSV pequeño, 
# ya que los datos agregados ocupan muy poco.
df_gold.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .option("sep","|") \
    .csv("s3a://gold-mercados/mercadona")

print("✅ ¡Pipeline completado! Datos agregados disponibles en MinIO.", flush=True)