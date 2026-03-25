import os
import sys
import time
import subprocess
import multiprocessing
import sys
sys.path.insert(0, ".")
from tests.mock_tally_server import run as run_tally
# Set shared key for both processes
KEY = "GG7ZWHj6oCr6TVyktnMFnMI7JBv_4hNEDqTttcWDZ-o="
os.environ["AGENT_FERNET_KEY"] = KEY

def start_tally():
    run_tally(port=9000)

if __name__ == "__main__":
    # Start mock Tally
    tally_proc = multiprocessing.Process(target=start_tally)
    tally_proc.start()
    print("Mock Tally started")
    time.sleep(1)

    # Start backend
    backend = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", 
         "backend.main:app", "--port", "8000"],
        env={**os.environ, "AGENT_FERNET_KEY": KEY}
    )
    print("Backend started")
    time.sleep(2)

    # Start agent
    agent = subprocess.Popen(
        [sys.executable, "-m", "agent.sync_worker"],
        env={**os.environ, "AGENT_FERNET_KEY": KEY}
    )
    print("Agent started")

    try:
        backend.wait()
    except KeyboardInterrupt:
        print("Shutting down...")
        tally_proc.terminate()
        backend.terminate()
        agent.terminate()