FROM python:3.9-slim-bookworm

USER root

# 1. Instalación de Java y herramientas del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 2. Configuración de variables de entorno JAVA
RUN ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /opt/java-custom
ENV JAVA_HOME=/opt/java-custom
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# 3. Instalación de librerías Python
# PySpark descargará la versión más reciente (normalmente compatible con Hadoop 3.3.x)
RUN pip install --no-cache-dir pandas mysql-connector-python findspark && \
    pip install --no-cache-dir pyspark==3.5.3 py4j

# 4. DESCARGA DE JARS (La parte crítica para MinIO y Hive) ⬇️
# Definimos las versiones exactas que necesitas
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.500
ENV MYSQL_CONNECTOR_VERSION=8.4.0

# Usamos Python para encontrar dinámicamente dónde instaló pip la carpeta 'jars' de Spark
# y descargamos los 3 archivos necesarios directamente allí.
RUN SPARK_JARS_PATH=$(python -c "import pyspark; print(pyspark.__path__[0])")/jars && \
    echo "⬇️ Descargando Jars en: $SPARK_JARS_PATH" && \
    \
    # A. MySQL Connector (Para conectar con Hive Metastore)
    curl -L "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" \
    -o $SPARK_JARS_PATH/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar && \
    \
    # B. Hadoop AWS (Para que Spark entienda 's3a://')
    curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o $SPARK_JARS_PATH/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    \
    # C. AWS Java SDK Bundle (Para autenticación y comunicación con MinIO)
    curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    -o $SPARK_JARS_PATH/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    \
    echo "✅ Jars instalados correctamente."

# (Opcional) Copiar tu código, normalmente lo montas con volúmenes en docker-compose
# COPY . .

# Mantener el contenedor encendido esperando comandos
CMD ["tail", "-f", "/dev/null"]