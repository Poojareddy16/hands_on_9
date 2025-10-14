import socket
import json
import random
import time
from datetime import datetime

HOST = "localhost"
PORT = 9999

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)
print(f"Data generator started. Waiting for Spark connection on {HOST}:{PORT}...")

conn, addr = server_socket.accept()
print(f"Spark connected: {addr}")

while True:
    record = {
        "trip_id": random.randint(1000, 9999),
        "driver_id": random.randint(1, 10),
        "distance_km": round(random.uniform(1.0, 25.0), 2),
        "fare_amount": round(random.uniform(5.0, 120.0), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    conn.send((json.dumps(record) + "\n").encode("utf-8"))
    time.sleep(1)
