import pyspark
from pyspark.sql import SparkSession
#from pyspark.context import SparkContext
from pyspark.sql.functions import col,to_date,year,month
from pyspark.sql.types import DecimalType
import findspark
import os
findspark.init()
print(pyspark.__version__)


try:
    spark.stop()
    print("Session stopped")
except:
    print("No session")

import pyspark
from pyspark.sql import SparkSession
import findspark
import os

findspark.init()


print("Iniciando Spark (Drivers cargados desde Docker)...")

spark = SparkSession.builder \
    .appName("Spark load to silver") \
    .enableHiveSupport() \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_USER")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_PASSWORD")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "10000") \
    .config("spark.hadoop.fs.s3a.paging.maximum", "5000") \
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
    .config("spark.hadoop.fs.s3a.multipart.threshold", "104857600") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .config("spark.hadoop.fs.s3a.connection.ttl", "3600") \
    .config("spark.hadoop.fs.s3a.committer.name", "directory") \
    .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("✅ Spark iniciado correctamente.")
    
df=spark.read.csv(f"/app/data/scrapper_vol/mercados/mercadona.csv",header=True,sep="|")
df_clean=df.filter(col("comercio").isNotNull())
df_silver=df_clean.select(
    col("nombre"),
    col("categoria"),
    col("subcategoria"),
    col("tanaño").cast(DecimalType(10,2)).alias("tamaño"),
    col("precio").cast(DecimalType(10,2)),
    col("iva").cast(DecimalType(10,2)),
    col("cantidad").cast(DecimalType(10,2)),
    col("bulk_precio").cast(DecimalType(10,2)),
    col("unidades"),
    col("comercio"),
    col("embalaje"),
    col("peso__seco").cast(DecimalType(10,2)),
    col("fecha").cast("date")
)
spark.sql("DROP DATABASE mercados_silver CASCADE")
spark.sql("CREATE DATABASE IF NOT EXISTS mercados_silver LOCATION 's3a://silver/mercados/'")
#spark.sql("CREATE DATABASE IF NOT EXISTS mercados_gold   LOCATION 's3a://gold/mercados/'")
df_silver = df_silver \
    .withColumn("year", year(col("fecha"))) \
    .withColumn("month", month(col("fecha")))
#df_silver.write.mode("overwrite").partitionBy("year","month").parquet("s3a://datalake/mercadona/")
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("path", "s3a://silver/mercados/mercadona") \
    .saveAsTable("mercados_silver.mercadona")

#guardamos en bronze
#df.coalesce(1).write.mode("overwrite").parquet("s3a://bronze/mercados/mercadona/")