FROM python:3.9-slim-bookworm

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*


RUN ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /opt/java-custom
ENV JAVA_HOME=/opt/java-custom
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app


RUN pip install --no-cache-dir pandas mysql-connector-python findspark && \
    pip install --no-cache-dir pyspark==3.5.3 py4j


ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.500
ENV MYSQL_CONNECTOR_VERSION=8.4.0


RUN SPARK_JARS_PATH=$(python -c "import pyspark; print(pyspark.__path__[0])")/jars && \
    echo "⬇️ Descargando Jars en: $SPARK_JARS_PATH" && \
    \
    # MySQL Connector (Para conectar con Hive Metastore)
    curl -L "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" \
    -o $SPARK_JARS_PATH/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar && \
    \
    # Hadoop AWS ('s3a://')
    curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o $SPARK_JARS_PATH/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    \
    # C. MinIO
    curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    -o $SPARK_JARS_PATH/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    \
    echo "✅ Jars instalados correctamente."


# COPY . .


CMD ["tail", "-f", "/dev/null"]