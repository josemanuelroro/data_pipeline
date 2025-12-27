# %%
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import findspark
import os
findspark.init()
print(pyspark.__version__)
#user=os.getenv('root')
#password=os.getenv('MYSQL_PASSWORD')
#spark.stop()

# %%
spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.driver.extraClassPath", "/datos_comp/spark/mysql-connector-j-8.4.0.jar") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
    

# %%

for i in os.listdir("data/scrapper_vol/mercados"):
    if i.split(".")[-1]=="csv":
        print(f"Subiendo {i}", end=" ")
        df=spark.read.csv(f"data/scrapper_vol/mercados/{i}",header=True,inferSchema=True,sep="|")
        df.write.format('jdbc').options(
        url="jdbc:mysql://mysql_db:3306/mercados_lnd",
        driver='com.mysql.jdbc.Driver',
        dbtable=i.split(".")[0],
        user='root',
        password='gftGFT@2028').mode('overwrite').save()
        print(" -->Completado")
#df = spark.read.format("jdbc").option("url","jdbc:mysql://188.245.233.230:3306/contratos_menores").option("driver","com.mysql.jdbc.Driver").option("dbtable",'lnd_contratos_menores').option("user",'root').option("password",'1123').load()
# %%

#df=spark.read.csv(f"/data/scrapper_vol/eroski.csv",header=True,inferSchema=False,sep="|")
# %%
#df.show(10)
# %%

#df.