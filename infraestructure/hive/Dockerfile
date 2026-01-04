FROM apache/hive:4.0.0

USER root


RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*


RUN curl -L https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar \
    -o /opt/hive/lib/mysql-connector-j.jar


RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
    -o /opt/hive/lib/hadoop-aws-3.3.6.jar


RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
    -o /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar

USER hive