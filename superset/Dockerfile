FROM apache/superset:3.0.2

USER root

RUN pip install mysqlclient
RUN apt-get update && apt-get install -y dos2unix
ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY ./superset-init.sh /superset-init.sh
RUN dos2unix /superset-init.sh && chmod +x /superset-init.sh
RUN pip install pyhive thrift thrift_sasl trino
COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset
ENTRYPOINT [ "/bin/bash", "-c", "/superset-init.sh" ]