FROM nginx:alpine


RUN apk add --no-cache apache2-utils


COPY ./scripts/generate_pass.sh /docker-entrypoint.d/99-generate-pass.sh

# 3. Darle permisos de ejecución
RUN chmod +x /docker-entrypoint.d/99-generate-pass.sh