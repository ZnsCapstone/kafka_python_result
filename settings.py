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
    "DM_ZNS_MODULE_PATH", os.path.expanduser("~/dm-zns-base/src/dm-zns-base.ko")
)
METADATA_ZONES = 6
GC_RESERVE_ZONES = 2
# 로그 구조 변환 계층의 GC 이주, WAL, SSTable을 위해 물리 용량의 25%를
# host-visible 논리 주소 밖에 남긴다. 환경변수로 실험별 조정 가능하다.
LOGICAL_CAPACITY_PERCENT = int(os.environ.get("DM_LOGICAL_CAPACITY_PERCENT", "75"))

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

JAVA_BENCH_JAR = os.path.expanduser(
    "~/Kafka-benchmark/build/libs/kafka-benchmark-1.0.jar"
)
JAVA_BENCH_CMD = (
    "java -Xms1G -Xmx2G "
    "-Dorg.slf4j.simpleLogger.defaultLogLevel=off "
    "-Dorg.slf4j.simpleLogger.showDateTime=false "
    "-Dorg.slf4j.simpleLogger.showThreadName=false "
    f"-cp {JAVA_BENCH_JAR} "
    "com.hanyang.cs.KafkaBenchmark"
)
os.environ["KAFKA_HEAP_OPTS"] = "-Xms1G -Xmx2G"

DEFAULT_ROUNDS = 3
TOPIC_RATE = 5
RECORD_SIZES = [1024, 10240, 102400, 1024000]
SATURATION_OPS_BY_RECORD_SIZE = {
    1024: 100000,
    10240: 10000,
    102400: 1000,
    1024000: 100,
}
LATENCY_OPS_BY_RECORD_SIZE = {
    1024: 20000,
    10240: 2000,
    102400: 200,
    1024000: 20,
}
PROFILES = {
    "saturation": {
        "ops_by_record_size": SATURATION_OPS_BY_RECORD_SIZE,
        "max_in_flight_records": 0,
        "max_catch_up_records": 0,
        "max_schedule_lag_ms": 0,
    },
    "latency": {
        "ops_by_record_size": LATENCY_OPS_BY_RECORD_SIZE,
        "max_in_flight_records": 1000,
        "max_catch_up_records": 10,
        "max_schedule_lag_ms": 100,
    },
}
DEFAULT_PROFILE = "saturation"
ACTIVE_PROFILE = DEFAULT_PROFILE
# Backward-compatible name for code that still imports the old setting.
FIXED_OPS_BY_RECORD_SIZE = SATURATION_OPS_BY_RECORD_SIZE
BOTTLENECK_RULES = {
    "min_disk_util_pct": 80.0,
    "min_cpu_iowait_pct": 5.0,
    "min_await_ms": 5.0,
}
MONITOR_LEAD_SECONDS = 2
WARMUP_SECONDS = 20
MEASURE_DURATION = 60
DRAIN_TIMEOUT_SECONDS = 180
FAIL_FAST_STALL_SECONDS = int(os.environ.get("BENCH_FAIL_FAST_STALL_SECONDS", "60"))

# Disk-aging experiment.  These percentages refer to the mounted benchmark
# filesystem, not the guest root filesystem.  Keep at least 20% free because
# dm-zns-base metadata, relocation, and filesystem GC all need working space.
WORKLOAD_MODES = ("fresh", "occupancy", "endurance", "steady")
DEFAULT_WORKLOAD_MODE = "fresh"
ACTIVE_WORKLOAD_MODE = DEFAULT_WORKLOAD_MODE
OCCUPANCY_POINTS = tuple(
    int(value) for value in os.environ.get("BENCH_OCCUPANCY_POINTS", "20,40,60,80").split(",")
    if value.strip()
)
MAX_OCCUPANCY_PERCENT = 80
OCCUPANCY_TOLERANCE_PERCENT = float(
    os.environ.get("BENCH_OCCUPANCY_TOLERANCE_PERCENT", "0.75")
)
OCCUPANCY_STABILIZE_TIMEOUT_SECONDS = int(
    os.environ.get("BENCH_OCCUPANCY_STABILIZE_TIMEOUT_SECONDS", "180")
)
MAX_ACCEPTABLE_ACK_STALL_SECONDS = int(
    os.environ.get("BENCH_MAX_ACK_STALL_SECONDS", "1")
)
PREFILL_FILE = f"{MOUNT_POINT}/.benchmark-prefill"
LONG_DURATION_SECONDS = int(os.environ.get("BENCH_LONG_DURATION_SECONDS", "3600"))
LONG_WARMUP_SECONDS = int(os.environ.get("BENCH_LONG_WARMUP_SECONDS", "300"))
LONG_RECORD_SIZE = int(os.environ.get("BENCH_LONG_RECORD_SIZE", "1024"))
LONG_SCENARIO = os.environ.get("BENCH_LONG_SCENARIO", "scenario_b")
TOPIC_PARTITIONS = 8
RETENTION_SEGMENT_BYTES = int(
    os.environ.get("BENCH_RETENTION_SEGMENT_BYTES", str(128 * 1024**2))
)
RETENTION_SEGMENT_MS = int(os.environ.get("BENCH_RETENTION_SEGMENT_MS", "60000"))
# Total topic retention across every partition.  Kafka's retention.bytes is
# configured per partition, so system_setup divides these values by 8.
ENDURANCE_RETENTION_TOTAL_BYTES = int(
    os.environ.get("BENCH_ENDURANCE_RETENTION_TOTAL_BYTES", str(2 * 1024**3))
)
STEADY_RETENTION_TOTAL_BYTES = int(
    os.environ.get("BENCH_STEADY_RETENTION_TOTAL_BYTES", str(24 * 1024**3))
)
STEADY_WARMUP_SECONDS = int(
    os.environ.get("BENCH_STEADY_WARMUP_SECONDS", "1800")
)

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
SCENARIO_GROUPS = {
    "baseline": ["scenario_a", "scenario_b"],
    "dynamic": ["scenario_a_dynamic", "scenario_b_dynamic"],
    "all": list(SCENARIO_TEMPLATES),
}
DEFAULT_SCENARIO_GROUP = "all"
ACTIVE_SCENARIO_GROUP = DEFAULT_SCENARIO_GROUP
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


def configure_profile(value):
    global ACTIVE_PROFILE, FIXED_OPS_BY_RECORD_SIZE
    if value not in PROFILES:
        raise ValueError(f"profile must be one of: {', '.join(PROFILES)}")
    ACTIVE_PROFILE = value
    FIXED_OPS_BY_RECORD_SIZE = PROFILES[value]["ops_by_record_size"]


def active_profile_config():
    return PROFILES[ACTIVE_PROFILE]


def configure_scenario_group(value):
    global ACTIVE_SCENARIO_GROUP, SCENARIO_KEYS
    if value not in SCENARIO_GROUPS:
        raise ValueError(f"scenario group must be one of: {', '.join(SCENARIO_GROUPS)}")
    ACTIVE_SCENARIO_GROUP = value
    SCENARIO_KEYS = SCENARIO_GROUPS[value]


def configure_workload_mode(value):
    global ACTIVE_WORKLOAD_MODE
    if value not in WORKLOAD_MODES:
        raise ValueError(f"workload mode must be one of: {', '.join(WORKLOAD_MODES)}")
    if any(point <= 0 or point > MAX_OCCUPANCY_PERCENT for point in OCCUPANCY_POINTS):
        raise ValueError(f"occupancy points must be between 1 and {MAX_OCCUPANCY_PERCENT}")
    if tuple(sorted(set(OCCUPANCY_POINTS))) != OCCUPANCY_POINTS:
        raise ValueError("occupancy points must be unique and increasing")
    ACTIVE_WORKLOAD_MODE = value
