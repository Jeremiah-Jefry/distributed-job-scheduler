#!/usr/bin/env python3
"""
Start all 3 coordinator nodes simultaneously.
Each is a truly independent subprocess with its own DB file.
Usage: python start_cluster.py
"""
import subprocess
import sys
import os
import time

BASE = os.path.dirname(os.path.abspath(__file__))
VENV_PYTHON = os.path.join(BASE, "venv", "Scripts", "uvicorn.exe")

nodes = [
    {"NODE_ID": "coordinator-1", "PEER_URLS": "http://127.0.0.1:8002,http://127.0.0.1:8003", "SELF_URL": "http://127.0.0.1:8001", "DB_PATH": "coordinator_1.db", "PORT": "8001"},
    {"NODE_ID": "coordinator-2", "PEER_URLS": "http://127.0.0.1:8001,http://127.0.0.1:8003", "SELF_URL": "http://127.0.0.1:8002", "DB_PATH": "coordinator_2.db", "PORT": "8002"},
    {"NODE_ID": "coordinator-3", "PEER_URLS": "http://127.0.0.1:8001,http://127.0.0.1:8002", "SELF_URL": "http://127.0.0.1:8003", "DB_PATH": "coordinator_3.db", "PORT": "8003"},
]

workers = [
    {"WORKER_ID": "worker-1", "COORDINATOR_URL": "http://127.0.0.1:8001", "PORT": "8011"},
    {"WORKER_ID": "worker-2", "COORDINATOR_URL": "http://127.0.0.1:8002", "PORT": "8012"},
]

procs = []

print("Starting coordinator cluster...")
for n in nodes:
    env = os.environ.copy()
    env.update(n)
    p = subprocess.Popen(
        [VENV_PYTHON, "coordinator.main:app", "--port", n["PORT"], "--log-level", "warning"],
        cwd=BASE,
        env=env,
    )
    procs.append(p)
    print(f"  Started {n['NODE_ID']} (pid={p.pid}) on port {n['PORT']}")

time.sleep(0.1)  # tiny pause before starting workers

print("Starting workers...")
WORKER_VENV = os.path.join(BASE, "venv", "Scripts", "uvicorn.exe")
for w in workers:
    env = os.environ.copy()
    env.update(w)
    p = subprocess.Popen(
        [WORKER_VENV, "worker.main:app", "--port", w["PORT"], "--log-level", "warning"],
        cwd=BASE,
        env=env,
    )
    procs.append(p)
    print(f"  Started {w['WORKER_ID']} (pid={p.pid}) on port {w['PORT']}")

print("\nAll processes started. Press Ctrl+C to stop.")
try:
    for p in procs:
        p.wait()
except KeyboardInterrupt:
    print("\nStopping all processes...")
    for p in procs:
        p.terminate()
