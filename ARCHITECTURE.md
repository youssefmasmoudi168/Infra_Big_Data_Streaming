# Project Architecture: Big Data Lakehouse

This document provides a detailed visual and technical breakdown of the Big Data Lakehouse architecture.

## System Architecture Diagram

```mermaid
graph TB
    subgraph "External Sources"
        Producer["Python Producer"]
    end

    subgraph "Data Ingestion & Streaming"
        Kafka["Kafka Cluster (3 Brokers)"]
        ZK["Zookeeper"]
        Kafka --- ZK
    end

    subgraph "Compute & Orchestration"
        direction TB
        Orion["Spark Orion (FastAPI/UI)"]
        SparkM["Spark Master"]
        SparkW["Spark Workers (x2)"]
        Orion -->|spark-submit| SparkM
        SparkM --> SparkW
    end

    subgraph "Storage Layer"
        MinIO["MinIO Object Storage"]
        Delta["Delta Lake Tables"]
        Check["Checkpoints"]
        MinIO --- Delta
        MinIO --- Check
    end

    subgraph "Analytics & Query Layer"
        Trino["Apache Trino"]
        TrinoInit["Trino Init (Auto-Reg)"]
        TrinoInit -->|Register| Trino
    end

    subgraph "Visualization"
        Superset["Apache Superset"]
    end

    %% Data Flows
    Producer -->|Send JSON| Kafka
    SparkW -->|Read Stream| Kafka
    SparkW -->|Write Delta| MinIO
    Trino -->|Metadata/Query| MinIO
    Superset -->|SQL (Trino Driver)| Trino

    %% Infrastructure
    classDef storage fill:#2d3436,stroke:#00d2ff,color:#fff
    classDef compute fill:#2d3436,stroke:#9d50bb,color:#fff
    classDef query fill:#2d3436,stroke:#4ade80,color:#fff
    
    class MinIO,Delta,Check storage
    class SparkM,SparkW,Orion compute
    class Trino,TrinoInit query
```

## Detailed Component Flow

1.  **Ingestion**: A `Python Producer` sends real-time logistics events to a **Kafka** cluster.
2.  **Processing**: **Spark Orion** submits a `pyspark` streaming job. The Spark workers read from Kafka and write directly to **MinIO** in **Delta Lake** format.
3.  **Storage**: MinIO acts as the S3-compatible backend, housing both the raw Delta data and the execution checkpoints.
4.  **Registration**: A **Trino-Init** sidecar automatically detects the Delta table and registers it within the **Trino** `delta` catalog using a file-based metastore.
5.  **Analytics**: **Apache Trino** executes high-performance SQL queries across the Delta tables.
6.  **Visualization**: **Apache Superset** connects to Trino to provide real-time dashboards and ad-hoc SQL exploratory analysis.

## Networking
All services communicate over the `minio_lake_net` Docker network, ensuring secure and isolated inter-service connectivity.
