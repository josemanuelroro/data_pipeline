import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date,year,month,regexp_replace,udf,filter
from pyspark.sql.types import StringType,StructType, StructField,DoubleType
import findspark
import os
import warnings
import requests
from datetime import datetime
findspark.init()
print(pyspark.__version__)

warnings.filterwarnings("ignore")
MI_IP="172.21.0.1"
#se define una structura para traer todo de la api:
api_data = StructType([
    StructField("ip", StringType(), True),
    StructField("country", StringType(), True),
    StructField("regionName", StringType(), True),
    StructField("city", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("isp", StringType(), True),
    StructField("org", StringType(), True),
])
def get_ip_details(ip):
    headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
                }
    try:
        if ip!=MI_IP:
            url = f"http://ip-api.com/json/{ip}?fields=status,country,regionName,city,lat,lon,isp,org,query"
            response = requests.get(url,headers=headers, timeout=3).json()
            #print(response)
            if response['status'] == 'success':
                return {
                        "ip":response["query"],
                        "country": response["country"],
                        "regionName": response["regionName"],
                        "city": response["city"],
                        "lat": response["lat"],
                        "lon": response["lon"],
                        "isp": response["isp"],
                        "org": response["org"]
                        }
        else:
            return {
                        "ip" : MI_IP,
                        "country": "local",
                        "regionName": "local",
                        "city": "local",
                        "lat": 0,
                        "lon": 0,
                        "isp": "local",
                        "org": "local"
                        }
    except Exception as e:
        return {
                        "ip" : "err",
                        "country": "err",
                        "regionName": "err",
                        "city": "err",
                        "lat": 0,
                        "lon": 0,
                        "isp": "err",
                        "org": "err"
                        }
        
get_ip_udf = udf(get_ip_details, api_data)

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


#########################SILVER#########################


try:
    #1 leer fichero
    df=spark.read.csv(f"/app/data/nginx_logs/serv_access.log",sep=" ")

    #2 procesar fichero
    df_fecha =df.withColumn("_c3",to_date(regexp_replace(col("_c3"), "\\[", ""), "dd/MMM/yyyy:HH:mm:ss"))
    df_ips_uniques=df_fecha.select("_c0").distinct()

    df_info_ip=df_ips_uniques.withColumn("info",get_ip_udf(col("_c0")))

    df_info_ip=df_info_ip.select("info.*")
    df_final=df_fecha.join(df_info_ip,df_fecha["_c0"]==df_info_ip["ip"],"left")
    df_final=df_final.select("ip",
                            col("_c3").alias("fecha"),
                            "country",
                            col("regionName").alias("region"),
                            "city",
                            "lat",
                            "lon",
                            "isp",
                            "org")

    #3 guardamos 
    df_final.coalesce(1).write.mode("append").parquet(f"s3a://silver/acceso_logs/")
    #4 vaciado fichero
    with open("/app/data/nginx_logs/serv_access.log", 'r+') as f:
        f.truncate(0)
except:
    print("error")
    
#########################GOLD#########################


#1 leemos silver
df_silver = spark.read.parquet("s3a://silver/acceso_logs/")
#2 limpiamos silver
df_gold=df_silver.filter((col("country")!="local") & (col("country")!="err"))
#3guardamos en gold
df_gold.write.mode("append").partitionBy("fecha").parquet(f"s3a://gold/acceso_logs/")