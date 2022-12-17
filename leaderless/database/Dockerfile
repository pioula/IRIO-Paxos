FROM postgres:13.9-alpine3.17

ENV POSTGRES_PASSWORD=pass
ENV POSTGRES_USER=postgres

COPY init.sql /docker-entrypoint-initdb.d/