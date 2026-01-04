FROM nginx:alpine


RUN apk add --no-cache apache2-utils

RUN rm /etc/nginx/conf.d/default.conf
COPY ./scripts/generate_pass.sh /docker-entrypoint.d/99-generate-pass.sh

RUN chmod +x /docker-entrypoint.d/99-generate-pass.sh