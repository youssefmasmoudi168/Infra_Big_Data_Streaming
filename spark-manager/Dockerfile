FROM apache/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install fastapi uvicorn pydantic

# Copy Spark jars (optional, but good for context)
# We might need the same jars as the spark master for S3A etc.
# But for now we rely on the ones Spark has.

COPY . /opt/spark-manager
WORKDIR /opt/spark-manager/backend

EXPOSE 8000

CMD ["python3", "main.py"]
