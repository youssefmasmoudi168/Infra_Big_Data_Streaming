FROM apache/spark:3.5.0

USER root

# Install config helper if needed, but mainly we need curl
# apache/spark is often based on Ubuntu or generic Linux.
# Try apt-get.
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

ENV JARS_DIR=/opt/spark/jars

COPY jars/*.jar ${JARS_DIR}/

USER spark
