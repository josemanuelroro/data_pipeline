
FROM mysql:8.0

# Copy the initial sql file to the container so it is executed
COPY init.sql /docker-entrypoint-initdb.d/

#CMD ["tail", "-f", "/dev/null"]