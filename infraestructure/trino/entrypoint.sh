#!/bin/bash

sed -e "s|\${MINIO_USER}|$MINIO_USER|g" \
    -e "s|\${MINIO_PASSWORD}|$MINIO_PASSWORD|g" \
    /etc/trino/catalog/minio.properties.template > /etc/trino/catalog/minio.properties

exec /usr/lib/trino/bin/run-trino