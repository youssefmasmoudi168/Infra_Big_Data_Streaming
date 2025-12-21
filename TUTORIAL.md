# Project Tutorial: Running the Big Data Lakehouse

This tutorial guides you through starting and operating the entire ecosystem, from MinIO storage to Trino/Superset visualization.

## 🚀 One-Command Start (Fastest)

If you just want everything running:
```powershell
docker compose -f minio/docker-compose.yaml up -d; `
docker compose -f kafka/docker-compose.yaml up -d; `
docker compose -f spark/docker-compose.yaml up -d; `
docker compose -f trino/docker-compose.yaml up -d; `
docker compose -f superset/docker-compose.yaml up -d
```

---

## 🛠️ Step-by-Step Guide

### 1. Storage & Streaming Foundation
Start MinIO and Kafka first. MinIO will automatically create the `delta` and `checkpoints` buckets.
```powershell
docker compose -f minio/docker-compose.yaml up -d
docker compose -f kafka/docker-compose.yaml up -d
```

### 2. Compute & Orchestration
Start the Spark Cluster and the **Spark Orion** submission portal.
```powershell
docker compose -f spark/docker-compose.yaml up -d
```
> **Access Spark Orion**: [http://localhost:8000/portal/](http://localhost:8000/portal/)

### 3. Analytics Layer
Start Trino. It will automatically register the `logistics_data` table on startup via the `trino-init` service.
```powershell
docker compose -f trino/docker-compose.yaml up -d
```

### 4. Visualization Layer
Start Superset. Note that the first run might take a minute to initialize.
```powershell
docker compose -f superset/docker-compose.yaml up -d --build
```
> **Access Superset**: [http://localhost:8088](http://localhost:8088) (admin/admin)

---

## 📊 Operating the Lakehouse

### A. Submitting Jobs via Spark Orion
1. Open [http://localhost:8000/portal/](http://localhost:8000/portal/).
2. Paste your Python code (e.g., `kafka_to_delta.py`).
3. Click **Execute Job**.
4. Monitor logs in the inline cell output.

### B. Querying in Superset
1. Go to **Settings** -> **Database Connections**.
2. Connect to Trino:
   - **URI**: `trino://admin@trino:8080/delta/default`
3. Go to **SQL Lab** and query:
   ```sql
   SELECT * FROM delta.default.logistics_data LIMIT 10;
   ```

### C. Checkpoint Troubleshooting
If you encounter checkpoint errors when restarting streaming jobs, wipe the checkpoint directory:
```powershell
docker exec -it minio-client mc rm -r --force local/checkpoints/logistics_data
```
