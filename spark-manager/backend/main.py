from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
import subprocess
import uuid
import os
import threading
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve Frontend
app.mount("/portal", StaticFiles(directory="/opt/spark-manager/frontend", html=True), name="portal")

JOBS_DIR = "/opt/spark-manager/jobs"
os.makedirs(JOBS_DIR, exist_ok=True)

class CodeSubmission(BaseModel):
    code: str

jobs = {}

def run_spark_job(job_id, file_path):
    jobs[job_id]["status"] = "running"
    
    # Collect external jars
    extra_jars_dir = "/opt/spark/external-jars"
    extra_jars = []
    if os.path.exists(extra_jars_dir):
        extra_jars = [os.path.join(extra_jars_dir, f) for f in os.listdir(extra_jars_dir) if f.endswith(".jar")]
    
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio1:9000",
        "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    ]
    
    if extra_jars:
        cmd.extend(["--jars", ",".join(extra_jars)])
        
    cmd.append(file_path)
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        for line in process.stdout:
            jobs[job_id]["logs"] += line
            
        process.wait()
        
        if process.returncode == 0:
            jobs[job_id]["status"] = "success"
        else:
            jobs[job_id]["status"] = "failed"
            
    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["logs"] += f"\nInternal Error: {str(e)}"

@app.post("/submit")
async def submit_job(submission: CodeSubmission):
    print(f"Received job submission request. Code length: {len(submission.code)}")
    job_id = str(uuid.uuid4())
    file_path = os.path.join(JOBS_DIR, f"{job_id}.py")
    
    with open(file_path, "w") as f:
        f.write(submission.code)
        
    jobs[job_id] = {
        "id": job_id,
        "status": "queued",
        "logs": "",
        "created_at": datetime.now().isoformat()
    }
    
    thread = threading.Thread(target=run_spark_job, args=(job_id, file_path))
    thread.start()
    
    return {"job_id": job_id}

@app.get("/jobs")
async def list_jobs():
    return list(jobs.values())

@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
