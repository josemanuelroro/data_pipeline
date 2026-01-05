######This scripts remove the duplicated values of .csv#####

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date,year,month
from pyspark.sql.types import DecimalType
import findspark
import os
import warnings
from datetime import datetime
findspark.init()
print(pyspark.__version__)

warnings.filterwarnings("ignore")

try:
    spark.stop()
    print("Session Stopped")
except:
    print("No session actived")

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
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

for fichero in os.listdir("/app/data/scrapper_vol/mercados"):
    dataframe=fichero.split(".")[0]
    
    df=spark.read.csv(f"/app/data/scrapper_vol/mercados/{dataframe}.csv",header=True,sep="|")
    df_clean=df.filter(col("comercio").isNotNull())\
        .dropDuplicates()\
        .filter(~col(df.columns[0]).startswith('"'))
        
    row_initial=df.count()
    row_final=df_clean.count()
    
    with open("/app/data/scrapper_vol/logs/mercados.txt","a+") as f:
        f.write(f"{dataframe}|{row_initial}|{row_final}|{row_initial-row_final}|{datetime.now().strftime('%Y-%m-%d')}\n")
    
    df_clean.coalesce(1).write.mode("overwrite").parquet(f"s3a://bronze/mercados/{dataframe}/")

jvm = spark.sparkContext._jvm
conf = spark.sparkContext._jsc.hadoopConfiguration()
local_path = jvm.org.apache.hadoop.fs.Path("file:///app/data/scrapper_vol/logs/mercados.txt")
remote_path = jvm.org.apache.hadoop.fs.Path("s3a://bronze/mercados/mercados.txt")
fs = remote_path.getFileSystem(conf)
fs.copyFromLocalFile(False, True, local_path, remote_path)