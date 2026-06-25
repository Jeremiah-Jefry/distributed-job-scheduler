from fastapi import FastAPI
import os

app = FastAPI()
NODE_ID = os.getenv("NODE_ID", "unknown")

@app.get("/health")
async def health():
    return {"node_id": NODE_ID, "status": "ok"}