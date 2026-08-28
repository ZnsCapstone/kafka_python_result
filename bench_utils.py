"""Small reusable process, statistics, and file helpers."""

import gzip
import os
import shutil
import socket
import statistics
import subprocess
import time


def run_cmd_quiet(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()


def run_cmd_full(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def run_cmd_streaming(cmd):
    output = []
    process = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            print(f"  > {line.rstrip()}")
            output.append(line)
    return "".join(output), process.wait()


def wait_for_port(port, timeout=180, *, closed=False):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            is_open = sock.connect_ex(("localhost", port)) == 0
            if is_open != closed:
                return True
        time.sleep(1 if closed else 2)
    return False


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_mean(values):
    values = [v for v in values if isinstance(v, (int, float))]
    return statistics.mean(values) if values else 0.0


def safe_stdev(values):
    values = [v for v in values if isinstance(v, (int, float))]
    return statistics.stdev(values) if len(values) >= 2 else 0.0


def safe_cv(values):
    mean = safe_mean(values)
    return safe_stdev(values) / mean * 100.0 if mean > 0 else 0.0


def write_text(path, content):
    with open(path, "w", encoding="utf-8") as file:
        file.write(content)


def gzip_file(path):
    if not os.path.exists(path):
        return path
    gz_path = f"{path}.gz"
    try:
        with open(path, "rb") as source, gzip.open(gz_path, "wb") as target:
            shutil.copyfileobj(source, target)
        os.remove(path)
        return gz_path
    except OSError as exc:
        print(f"[Warn] gzip failed for {path}: {exc}")
        return path
