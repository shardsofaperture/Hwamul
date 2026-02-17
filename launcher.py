from __future__ import annotations

import atexit
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable

import webview

APP_NAME = "ProductionPlanner"
HOST = "127.0.0.1"
MAX_PORT_ATTEMPTS = 15
MAX_STARTUP_WAIT_SECONDS = 20.0


def appdata_db_path() -> Path:
    appdata = os.getenv("APPDATA")
    if not appdata:
        raise RuntimeError("APPDATA is not set. This launcher is intended for Windows desktop packaging.")

    db_dir = Path(appdata) / APP_NAME
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / "planner.db"


def candidate_ports(max_attempts: int) -> Iterable[int]:
    for _ in range(max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((HOST, 0))
            yield sock.getsockname()[1]


def wait_for_server(port: int, timeout_seconds: float) -> bool:
    end_time = time.time() + timeout_seconds
    while time.time() < end_time:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            if sock.connect_ex((HOST, port)) == 0:
                return True
        time.sleep(0.2)
    return False


def launch_streamlit(port: int, db_path: Path, app_dir: Path) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["PLANNER_DB_PATH"] = str(db_path)

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_dir / "app.py"),
        "--server.address",
        HOST,
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
    ]

    return subprocess.Popen(cmd, env=env, cwd=str(app_dir))


def terminate_process(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return

    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def run() -> None:
    db_path = appdata_db_path()
    app_dir = Path(__file__).resolve().parent
    streamlit_process: subprocess.Popen[str] | None = None

    for port in candidate_ports(MAX_PORT_ATTEMPTS):
        streamlit_process = launch_streamlit(port, db_path, app_dir)

        if wait_for_server(port, MAX_STARTUP_WAIT_SECONDS):
            break

        terminate_process(streamlit_process)
        streamlit_process = None
    else:
        raise RuntimeError("Unable to start Streamlit on a free localhost port.")

    atexit.register(terminate_process, streamlit_process)

    url = f"http://{HOST}:{port}"
    window = webview.create_window(APP_NAME, url=url, width=1280, height=900)
    try:
        webview.start(gui="edgechromium")
    finally:
        terminate_process(streamlit_process)


if __name__ == "__main__":
    run()
