"""Kafka filesystem benchmark configuration.

Edit experiment parameters here; runtime orchestration lives in ``bench_runner.py``.
"""

import datetime
import os
import time

TIMESTAMP = time.strftime("%Y%m%d_%H%M%S")
PROGRAM_START_TIME = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

RESULT_DIR = f"results/kafka_fs_bench_{TIMESTAMP}"
ROUND_DIR = f"{RESULT_DIR}/rounds"
RAW_DIR = f"{RESULT_DIR}/raw"
MONITOR_DIR = f"{RESULT_DIR}/monitor"
CSV_DIR = f"{RESULT_DIR}/csv"
ENV_DIR = f"{RESULT_DIR}/env"

KAFKA_PATH = os.path.expanduser("~/kafka-4.2.0-src")
KRAFT_CONFIG = f"{KAFKA_PATH}/config/server.properties"
EXPERIMENT_KRAFT_CONFIG = f"{ENV_DIR}/server-femu.properties"

RAW_ZNS_DEVICE = "/dev/nvme0n1"
RAW_DEVICE_BASENAME = os.path.basename(os.path.realpath(RAW_ZNS_DEVICE))
DM_NAME = "kafka-zns"
FS_DEVICE = f"/dev/mapper/{DM_NAME}"
DM_MODULE_PATH = os.environ.get(
    "DM_ZNS_MODULE_PATH", os.path.expanduser("~/milestone1-1/dm-zns-base.ko")
)
METADATA_ZONES = 6
GC_RESERVE_ZONES = 2

DM_IMPLEMENTATION = "fixed"
DM_IMPLEMENTATION_LABELS = {
    "fixed": "fixed reservation (JW)",
    "dynamic": "dynamic allocation (MJ)",
}

FILESYSTEMS = ("ext4", "f2fs")
MOUNT_POINT = "/result/kafka-logs"
BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "bench-topic"
SEPARATE_METADATA_DIR = True
METADATA_DIR = "/var/lib/kafka-meta"

JAVA_BENCH_CMD = (
    "java -Xms1G -Xmx2G "
    "-Dorg.slf4j.simpleLogger.defaultLogLevel=off "
    "-Dorg.slf4j.simpleLogger.showDateTime=false "
    "-Dorg.slf4j.simpleLogger.showThreadName=false "
    f"-cp {os.path.expanduser('~/Kafka-benchmark/build/libs/kafka-benchmark-1.0.jar')} "
    "com.hanyang.cs.KafkaBenchmark"
)
os.environ["KAFKA_HEAP_OPTS"] = "-Xms1G -Xmx2G"

DEFAULT_ROUNDS = 3
TOPIC_RATE = 5
RECORD_SIZES = [1024, 10240, 102400, 1024000]
FIXED_OPS_BY_RECORD_SIZE = {
    1024: 100000,
    10240: 10000,
    102400: 1000,
    1024000: 100,
}
BOTTLENECK_RULES = {
    "min_disk_util_pct": 80.0,
    "min_cpu_iowait_pct": 5.0,
    "min_await_ms": 5.0,
}
MONITOR_LEAD_SECONDS = 2
WARMUP_SECONDS = 20
MEASURE_DURATION = 60

SCENARIO_TEMPLATES = {
    "scenario_a": {
        "name": "Scenario A", "desc": "Multi Producer Only",
        "producers": 8, "use_consumer": False, "dynamic_topics": False,
    },
    "scenario_b": {
        "name": "Scenario B", "desc": "Multi Producer + Single Consumer",
        "producers": 8, "use_consumer": True, "dynamic_topics": False,
    },
    "scenario_a_dynamic": {
        "name": "Scenario A + Dynamic",
        "desc": "Multi Producer Only + Dynamic Topics",
        "producers": 8, "use_consumer": False, "dynamic_topics": True,
    },
    "scenario_b_dynamic": {
        "name": "Scenario B + Dynamic",
        "desc": "Multi Producer + Single Consumer + Dynamic Topics",
        "producers": 8, "use_consumer": True, "dynamic_topics": True,
    },
}
SCENARIO_KEYS = list(SCENARIO_TEMPLATES)
COMPRESS_AFTER_RUN = True


def initialize_result_directories():
    for directory in (RESULT_DIR, ROUND_DIR, RAW_DIR, MONITOR_DIR, CSV_DIR, ENV_DIR):
        os.makedirs(directory, exist_ok=True)


def configure_dm_implementation(value):
    global DM_IMPLEMENTATION
    implementations = {"0": "fixed", "1": "dynamic"}
    if value not in implementations:
        raise ValueError("implementation must be 0 (fixed/JW) or 1 (dynamic/MJ)")
    DM_IMPLEMENTATION = implementations[value]
