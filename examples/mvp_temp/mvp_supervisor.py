#!/usr/bin/env python3
import subprocess
import time
import socket
import sys

def wait_for_port(port, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=1):
                return True
        except (socket.error, ConnectionRefusedError):
            time.sleep(0.2)
    return False

def run():
    # Start gateway
    gateway = subprocess.Popen([sys.executable, "mvp_gateway.py"])
    print("Waiting for gateway to bind...")
    if not wait_for_port(8766):
        print("Gateway did not start, exiting")
        gateway.terminate()
        sys.exit(1)
    print("Gateway ready")
    # Start worker
    worker = subprocess.Popen([sys.executable, "mvp_worker.py"])
    try:
        while True:
            time.sleep(1)
            if worker.poll() is not None:
                print("Worker died, restarting...")
                worker = subprocess.Popen([sys.executable, "mvp_worker.py"])
            if gateway.poll() is not None:
                print("Gateway died, restarting...")
                gateway = subprocess.Popen([sys.executable, "mvp_gateway.py"])
    except KeyboardInterrupt:
        worker.terminate()
        gateway.terminate()
        sys.exit(0)

if __name__ == "__main__":
    run()