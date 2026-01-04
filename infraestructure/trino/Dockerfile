FROM trinodb/trino:latest

USER root



COPY minio.properties.template /etc/trino/catalog/minio.properties.template
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh


RUN chown -R trino:trino /etc/trino/catalog

USER trino

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]