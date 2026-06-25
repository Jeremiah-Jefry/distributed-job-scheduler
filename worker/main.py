from fastapi import FastAPI
import os

app = FastAPI()
WORKER_ID = os.getenv("WORKER_ID", "unknown")

@app.get("/health")
async def health():
    return {"worker_id": WORKER_ID, "status": "ok"}