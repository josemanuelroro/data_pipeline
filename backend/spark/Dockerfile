FROM python:3.9-slim-bookworm

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Setting up Java
RUN ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /opt/java-custom
ENV JAVA_HOME=/opt/java-custom
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# Install libraries
RUN pip install --no-cache-dir pandas mysql-connector-python findspark && \
    pip install --no-cache-dir pyspark py4j

# Donwload connector
RUN curl -L https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar \
    -o /tmp/mysql-connector-j-8.4.0.jar

# Move the connector
RUN mv /tmp/mysql-connector-j-8.4.0.jar $(python -c "import pyspark; print(pyspark.__path__[0])")/jars/mysql-connector-j-8.4.0.jar

# Copy all wo /app directory
#COPY . .

# Keep container running
CMD ["tail", "-f", "/dev/null"]