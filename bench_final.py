import subprocess
import re
import time
import sys
import statistics
import os
import socket
import datetime
import json
import csv
import signal
import gzip
import shutil
from copy import deepcopy

# =========================================================
# 0. 기본 설정
# =========================================================
timestamp = time.strftime('%Y%m%d_%H%M%S')
program_start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
RESULT_DIR = f"results/kafka_fs_bench_{timestamp}"
ROUND_DIR = f"{RESULT_DIR}/rounds"
RAW_DIR = f"{RESULT_DIR}/raw"
MONITOR_DIR = f"{RESULT_DIR}/monitor"
CSV_DIR = f"{RESULT_DIR}/csv"
ENV_DIR = f"{RESULT_DIR}/env"

for d in [RESULT_DIR, ROUND_DIR, RAW_DIR, MONITOR_DIR, CSV_DIR, ENV_DIR]:
    os.makedirs(d, exist_ok=True)


class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def close(self):
        self.log.close()


# =========================================================
# 1. 환경 설정
# =========================================================
KAFKA_PATH = os.path.expanduser("~/kafka-4.2.0-src")
# 원본 Kafka 설정은 수정하지 않는다. 실행마다 결과 디렉터리에 실험용 설정을 만든다.
KRAFT_CONFIG = f"{KAFKA_PATH}/config/server.properties"
EXPERIMENT_KRAFT_CONFIG = f"{ENV_DIR}/server-femu.properties"

# FEMU guest의 host-managed ZNS namespace. /dev/sda는 guest OS 디스크이므로 사용 금지.
RAW_ZNS_DEVICE = "/dev/nvme0n1"
RAW_DEVICE_BASENAME = os.path.basename(os.path.realpath(RAW_ZNS_DEVICE))
# dm-zns-base가 raw ZNS를 일반 논리 블록 장치로 노출한다.
DM_NAME = "kafka-zns"
FS_DEVICE = f"/dev/mapper/{DM_NAME}"
DM_MODULE_PATH = os.environ.get(
    "DM_ZNS_MODULE_PATH",
    os.path.expanduser("~/milestone1-1/dm-zns-base.ko"),
)
METADATA_ZONES = 6  # manifest 2 + WAL 2 + SSTable 2; dm-zns-base.c와 일치해야 함
GC_RESERVE_ZONES = 2
# 실행 시 첫 번째 인자로 선택: 0=fixed(JW), 1=dynamic(MJ).
# 두 구현은 같은 target 이름(zns-base)과 module 경로를 공유하며, DM table 크기만 다르다.
DM_IMPLEMENTATION = "fixed"
DM_IMPLEMENTATION_LABELS = {
    "fixed": "fixed reservation (JW)",
    "dynamic": "dynamic allocation (MJ)",
}
FILESYSTEMS = ("ext4", "f2fs")
MOUNT_POINT = "/result/kafka-logs"
BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "bench-topic"

# KRaft metadata.log.dir 분리 여부
SEPARATE_METADATA_DIR = True
METADATA_DIR = "/var/lib/kafka-meta"

JAVA_BENCH_CMD = (
    "java "
    "-Xms1G -Xmx2G "
    "-Dorg.slf4j.simpleLogger.defaultLogLevel=off "
    "-Dorg.slf4j.simpleLogger.showDateTime=false "
    "-Dorg.slf4j.simpleLogger.showThreadName=false "
    f"-cp {os.path.expanduser('~/Kafka-benchmark/build/libs/kafka-benchmark-1.0.jar')} "
    "com.hanyang.cs.KafkaBenchmark"
)

os.environ["KAFKA_HEAP_OPTS"] = "-Xms1G -Xmx2G"

DEFAULT_ROUNDS = 3
TOPIC_RATE = 5

RECORD_SIZES = [
    1024,
    10240,
    102400,
    1024000,
]

# record size별 고정 OP/s
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
WARMUP_SECONDS = 30
MEASURE_DURATION = 180

SCENARIO_TEMPLATES = {
    "scenario_a": {
        "name": "Scenario A",
        "desc": "Multi Producer Only",
        "producers": 8,
        "use_consumer": False,
        "dynamic_topics": False,
    },
    "scenario_b": {
        "name": "Scenario B",
        "desc": "Multi Producer + Single Consumer",
        "producers": 8,
        "use_consumer": True,
        "dynamic_topics": False,
    },
    "scenario_a_dynamic": {
        "name": "Scenario A + Dynamic",
        "desc": "Multi Producer Only + Dynamic Topics",
        "producers": 8,
        "use_consumer": False,
        "dynamic_topics": True,
    },
    "scenario_b_dynamic": {
        "name": "Scenario B + Dynamic",
        "desc": "Multi Producer + Single Consumer + Dynamic Topics",
        "producers": 8,
        "use_consumer": True,
        "dynamic_topics": True,
    }
}

SCENARIO_KEYS = [
    "scenario_a",
    "scenario_b",
    "scenario_a_dynamic",
    "scenario_b_dynamic",
]

COMPRESS_AFTER_RUN = True


# =========================================================
# 2. 공통 유틸
# =========================================================
def run_cmd_quiet(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip()

def run_cmd_full(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)

def run_cmd_streaming(cmd):
    full_output = []
    process = subprocess.Popen(
        cmd, shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            print(f"  > {line.rstrip()}")
            full_output.append(line)
    return_code = process.wait()
    return "".join(full_output), return_code

def wait_for_port(port, timeout=180):
    start_time = time.time()
    while time.time() - start_time < timeout:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            if sock.connect_ex(("localhost", port)) == 0:
                return True
        time.sleep(2)
    return False


def wait_for_port_closed(port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            if sock.connect_ex(("localhost", port)) != 0:
                return True
        time.sleep(1)
    return False

def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def safe_mean(values):
    vals = [v for v in values if isinstance(v, (int, float))]
    return statistics.mean(vals) if vals else 0.0
    
def safe_stdev(values):
    vals = [v for v in values if isinstance(v, (int, float))]
    return statistics.stdev(vals) if len(vals) >= 2 else 0.0

def safe_cv(values):
    m = safe_mean(values)
    if m <= 0:
        return 0.0
    return (safe_stdev(values) / m) * 100.0

def write_text(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def gzip_file(path):
    if not os.path.exists(path):
        return path
    gz_path = path + ".gz"
    try:
        with open(path, "rb") as fin, gzip.open(gz_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        os.remove(path)
        return gz_path
    except Exception as e:
        print(f"[Warn] gzip failed for {path}: {e}")
        return path


# =========================================================
# 3. 환경 정보 수집 (재현성)
# =========================================================
def capture_environment():
    env_path = f"{ENV_DIR}/system_info.txt"
    sections = []
    sections.append(("Date", datetime.datetime.now().isoformat()))
    sections.append(("uname -a", run_cmd_quiet("uname -a")))
    sections.append(("/etc/os-release", run_cmd_quiet("cat /etc/os-release")))
    sections.append(("CPU info (lscpu)", run_cmd_quiet("lscpu")))
    sections.append(("Memory (free -h)", run_cmd_quiet("free -h")))
    sections.append(("Disk (lsblk)", run_cmd_quiet("lsblk")))
    sections.append(("Disk (df -h)", run_cmd_quiet("df -h")))
    sections.append(("Kernel cmdline", run_cmd_quiet("cat /proc/cmdline")))
    sections.append(("vm.dirty_ratio", run_cmd_quiet("sysctl vm.dirty_ratio")))
    sections.append(("vm.dirty_background_ratio", run_cmd_quiet("sysctl vm.dirty_background_ratio")))
    sections.append(("vm.swappiness", run_cmd_quiet("sysctl vm.swappiness")))
    sections.append(("iostat version", run_cmd_quiet("iostat -V | head -1")))
    sections.append(("vmstat version", run_cmd_quiet("vmstat -V")))
    sections.append(("mkfs.ext4 version", run_cmd_quiet("mkfs.ext4 -V 2>&1 | head -1")))
    sections.append(("mkfs.f2fs version", run_cmd_quiet("mkfs.f2fs -V 2>&1 | head -1")))
    sections.append(("Java version", run_cmd_quiet("java -version 2>&1")))
    sections.append(("Kafka path", KAFKA_PATH))
    sections.append(("Kafka config", run_cmd_quiet(f"cat {EXPERIMENT_KRAFT_CONFIG} | head -50")))
    sections.append(("Raw ZNS device", RAW_ZNS_DEVICE))
    sections.append(("DM device", FS_DEVICE))
    sections.append(("DM module", DM_MODULE_PATH))
    sections.append(("DM implementation", DM_IMPLEMENTATION_LABELS[DM_IMPLEMENTATION]))
    sections.append(("DM table", run_cmd_quiet(f"sudo dmsetup table {DM_NAME} 2>&1 || true")))

    blocks = []
    for name, value in sections:
        blocks.append(f"### {name}\n{value}\n")
    write_text(env_path, "\n".join(blocks))
    print(f"[Env] System info saved: {env_path}")


# =========================================================
# 4. Device-mapper / 디스크 / 파일시스템 준비
# =========================================================
def dm_module_name():
    """Kernel module names use underscores even when the .ko filename has hyphens."""
    return os.path.basename(DM_MODULE_PATH).removesuffix(".ko").replace("-", "_")


def configure_dm_implementation(value):
    """Select the table shape while keeping the target/module interface identical."""
    global DM_IMPLEMENTATION
    implementations = {"0": "fixed", "1": "dynamic"}
    if value not in implementations:
        raise ValueError("implementation must be 0 (fixed/JW) or 1 (dynamic/MJ)")
    DM_IMPLEMENTATION = implementations[value]


def unmount_log_device(strict=True):
    # Broker shutdown can return before its final file handles and writeback are gone.
    # Never use lazy unmount before resetting the underlying ZNS device.
    last_res = None
    for attempt in range(1, 6):
        res = subprocess.run(
            f"sudo umount {MOUNT_POINT}", shell=True, capture_output=True, text=True
        )
        if res.returncode == 0 or "not mounted" in res.stderr.lower():
            return
        last_res = res
        if attempt < 5:
            print(f"[Setup] Mount still busy; retrying unmount ({attempt}/5) ...")
            run_cmd_quiet("sudo sync")
            time.sleep(2)

    busy_res = run_cmd_full(f"sudo fuser -vm {MOUNT_POINT} 2>&1 || true")
    message = f"umount failed: {last_res.stderr.strip()}\n{busy_res.stdout.strip()}"
    if strict:
        raise RuntimeError(message)
    print(f"[Warn] {message}")


def remove_dm_stack(strict=True):
    """Remove the mapper before unloading its target; dtr() flushes the target WAL."""
    remove = run_cmd_full(f"sudo dmsetup remove {DM_NAME}")
    if remove.returncode != 0:
        stderr = remove.stderr.lower()
        if "no such device" not in stderr and "not found" not in stderr:
            message = f"dmsetup remove failed:\n{remove.stdout}\n{remove.stderr}"
            if strict:
                raise RuntimeError(message)
            print(f"[Warn] {message}")

    unload = run_cmd_full(f"sudo rmmod {dm_module_name()}")
    if unload.returncode != 0:
        stderr = unload.stderr.lower()
        if "not currently loaded" not in stderr and "no such file" not in stderr:
            message = f"rmmod failed:\n{unload.stdout}\n{unload.stderr}"
            if strict:
                raise RuntimeError(message)
            print(f"[Warn] {message}")


def zns_logical_sectors():
    zoned = run_cmd_quiet(f"cat /sys/block/{RAW_DEVICE_BASENAME}/queue/zoned")
    if zoned != "host-managed":
        raise RuntimeError(
            f"{RAW_ZNS_DEVICE} is not a host-managed ZNS device (zoned={zoned!r})"
        )

    try:
        with open(f"/sys/block/{RAW_DEVICE_BASENAME}/queue/chunk_sectors", encoding="utf-8") as f:
            zone_sectors = int(f.read().strip())
        with open(f"/sys/block/{RAW_DEVICE_BASENAME}/queue/nr_zones", encoding="utf-8") as f:
            nr_zones = int(f.read().strip())
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cannot read ZNS geometry for {RAW_ZNS_DEVICE}: {exc}") from exc

    if DM_IMPLEMENTATION == "dynamic":
        # MJ implementation manages USER_DATA/WAL/SSTABLE/GC zones internally.
        return nr_zones * zone_sectors

    reserved = METADATA_ZONES + GC_RESERVE_ZONES
    if nr_zones <= reserved:
        raise RuntimeError(
            f"ZNS has {nr_zones} zones, but fixed dm-zns-base needs {reserved} reserved zones"
        )
    return (nr_zones - reserved) * zone_sectors


def create_dm_target():
    if not os.path.exists(DM_MODULE_PATH):
        raise RuntimeError(
            f"dm-zns-base module not found: {DM_MODULE_PATH}\n"
            "Build dm-zns-base.c into dm-zns-base.ko first, or set DM_ZNS_MODULE_PATH."
        )

    load = run_cmd_full(f"sudo insmod {DM_MODULE_PATH}")
    if load.returncode != 0:
        raise RuntimeError(f"insmod failed:\n{load.stdout}\n{load.stderr}")

    logical_sectors = zns_logical_sectors()
    table = f"0 {logical_sectors} zns-base {RAW_ZNS_DEVICE}\n"
    create = subprocess.run(
        ["sudo", "dmsetup", "create", DM_NAME],
        input=table,
        text=True,
        capture_output=True,
    )
    if create.returncode != 0:
        # Do not leave a registered but unused module after a failed target creation.
        run_cmd_quiet(f"sudo rmmod {dm_module_name()} || true")
        raise RuntimeError(f"dmsetup create failed:\n{create.stdout}\n{create.stderr}")

    print(f"[DM] Created {FS_DEVICE}: {logical_sectors} sectors "
          f"({logical_sectors * 512 / 1024**3:.2f} GiB)")


def setup_env(fs_type):
    if fs_type not in FILESYSTEMS:
        raise ValueError(f"Unsupported filesystem: {fs_type}")

    print(f"\n[Setup] Resetting FEMU ZNS and mounting {fs_type} through {DM_NAME} ...")

    # Kafka가 열린 파일을 남긴 채 lazy unmount 되는 것을 막기 위해 먼저 종료한다.
    run_cmd_quiet("pkill -9 -f kafka.Kafka || true")
    run_cmd_quiet("pkill -9 -f QuorumPeerMain || true")
    run_cmd_quiet("pkill -9 -f kafka || true")
    time.sleep(2)

    unmount_log_device(strict=True)
    remove_dm_stack(strict=True)

    # 이 guest의 util-linux blkzone은 -a를 지원하지 않으며, 범위를 생략하면 전체 장치를 reset한다.
    reset = run_cmd_full(f"sudo blkzone reset {RAW_ZNS_DEVICE}")
    if reset.returncode != 0:
        raise RuntimeError(f"zone reset failed:\n{reset.stdout}\n{reset.stderr}")

    create_dm_target()
    wipe = run_cmd_full(f"sudo wipefs -a {FS_DEVICE}")
    if wipe.returncode != 0:
        raise RuntimeError(f"wipefs failed:\n{wipe.stdout}\n{wipe.stderr}")

    if fs_type == "ext4":
        fmt = run_cmd_full(f"sudo mkfs.ext4 -F -E nodiscard -L kafka_ext4 {FS_DEVICE}")
    else:
        # dm-zns-base accepts only read/write/flush; it deliberately rejects discard.
        fmt = run_cmd_full(f"sudo mkfs.f2fs -f -t 0 -l kafka_f2fs {FS_DEVICE}")

    if fmt.returncode != 0:
        print("[CRITICAL ERROR] mkfs failed")
        print(fmt.stdout)
        print(fmt.stderr)
        raise RuntimeError("mkfs failed")

    run_cmd_quiet(f"sudo mkdir -p {MOUNT_POINT}")
    mount_res = run_cmd_full(
        f"sudo mount -o noatime,nodiratime,nodiscard {FS_DEVICE} {MOUNT_POINT}"
    )
    if mount_res.returncode != 0:
        print("[CRITICAL ERROR] mount failed")
        print(mount_res.stdout)
        print(mount_res.stderr)
        raise RuntimeError("mount failed")

    run_cmd_quiet(f"sudo rm -rf {MOUNT_POINT}/lost+found")
    run_cmd_quiet(f"sudo chmod 777 {MOUNT_POINT}")

    print(run_cmd_quiet(f"lsblk {FS_DEVICE}"))
    print(run_cmd_quiet(f"mount | grep {DM_NAME}"))

    run_cmd_quiet("sudo sync")
    run_cmd_quiet("echo 3 | sudo tee /proc/sys/vm/drop_caches")

    if SEPARATE_METADATA_DIR:
        run_cmd_quiet(f"sudo mkdir -p {METADATA_DIR}")
        run_cmd_quiet(f"sudo rm -rf {METADATA_DIR}/*")
        run_cmd_quiet(f"sudo chmod 777 {METADATA_DIR}")

        


# =========================================================
# 5. KRaft Kafka 제어
# =========================================================
def validate_kafka_environment():
    print("[Check] Validating Kafka environment ...")

    if "4.2" not in KAFKA_PATH:
        print("[WARN] KAFKA_PATH 에 4.2 문자열이 없습니다.")
        print(f"       Current KAFKA_PATH = {KAFKA_PATH}")

    required = [
        f"{KAFKA_PATH}/bin/kafka-storage.sh",
        f"{KAFKA_PATH}/bin/kafka-server-start.sh",
        f"{KAFKA_PATH}/bin/kafka-server-stop.sh",
        KRAFT_CONFIG,
    ]
    for p in required:
        if not os.path.exists(p):
            print(f"[CRITICAL ERROR] Required file not found: {p}")
            sys.exit(1)


def prepare_experiment_kraft_config():
    """Make KRaft format and broker start use identical log directories."""
    with open(KRAFT_CONFIG, "r", encoding="utf-8") as source:
        lines = source.readlines()

    managed_keys = ("log.dirs", "metadata.log.dir")
    retained = [
        line for line in lines
        if not any(line.strip().startswith(f"{key}=") for key in managed_keys)
    ]
    retained.append(f"\nlog.dirs={MOUNT_POINT}\n")
    if SEPARATE_METADATA_DIR:
        retained.append(f"metadata.log.dir={METADATA_DIR}\n")
    write_text(EXPERIMENT_KRAFT_CONFIG, "".join(retained))
    print(f"[Env] Experiment Kafka config saved: {EXPERIMENT_KRAFT_CONFIG}")


def control_kafka(action):
    if action == "stop":
        print("[Service] Stopping Kafka KRaft broker ...")
        run_cmd_quiet(f"{KAFKA_PATH}/bin/kafka-server-stop.sh")
        if not wait_for_port_closed(9092, timeout=15):
            run_cmd_quiet("sudo fuser -k 9092/tcp || true")
            run_cmd_quiet("pkill -TERM -f 'kafka.Kafka' || true")
            if not wait_for_port_closed(9092, timeout=10):
                run_cmd_quiet("pkill -KILL -f 'kafka.Kafka' || true")
                wait_for_port_closed(9092, timeout=5)
        run_cmd_quiet("sudo sync")
        time.sleep(2)

    elif action == "start":
        print("[Service] Starting Kafka in KRaft mode ...")

        cluster_id = run_cmd_quiet(f"{KAFKA_PATH}/bin/kafka-storage.sh random-uuid")
        if not cluster_id:
            print("[CRITICAL ERROR] Failed to generate KRaft cluster id")
            sys.exit(1)

        format_cmd = (
            f"{KAFKA_PATH}/bin/kafka-storage.sh format "
            f"-t {cluster_id} "
            f"-c {EXPERIMENT_KRAFT_CONFIG} "
            f"--standalone "
            f"--ignore-formatted"
        )
        fmt = run_cmd_full(format_cmd)
        if fmt.returncode != 0:
            print("[CRITICAL ERROR] KRaft storage format failed")
            print(fmt.stdout)
            print(fmt.stderr)
            sys.exit(1)

        # 1000KB record 도 정상 처리되도록 message.max.bytes 와 replica.fetch.max.bytes 명시.
        # (기본값 ~1MB 라 1000KB 가 헤더 포함하면 아슬아슬하게 거부될 수 있음)
        overrides = [
            f"--override num.partitions=8",
            f"--override offsets.topic.replication.factor=1",
            f"--override transaction.state.log.replication.factor=1",
            f"--override transaction.state.log.min.isr=1",
            f"--override default.replication.factor=1",
            f"--override log.retention.check.interval.ms=1000",
            f"--override log.segment.delete.delay.ms=0",
            f"--override auto.create.topics.enable=false",
            f"--override message.max.bytes=10485760",
            f"--override replica.fetch.max.bytes=10485760",
        ]
        start_cmd = (
            f"{KAFKA_PATH}/bin/kafka-server-start.sh -daemon {EXPERIMENT_KRAFT_CONFIG} "
            + " ".join(overrides)
        )
        run_cmd_full(start_cmd)

        if not wait_for_port(9092, timeout=180):
            print("[CRITICAL ERROR] Kafka KRaft broker start timeout")
            sys.exit(1)
        time.sleep(8)

    else:
        raise ValueError(f"Unknown action: {action}")


def recreate_main_topic():
    print("[Service] Recreating benchmark topic ...")
    run_cmd_quiet(
        f"{KAFKA_PATH}/bin/kafka-topics.sh "
        f"--bootstrap-server {BOOTSTRAP} "
        f"--delete --topic {TOPIC_NAME} --if-exists"
    )
    time.sleep(3)
    run_cmd_quiet(
        f"{KAFKA_PATH}/bin/kafka-topics.sh "
        f"--bootstrap-server {BOOTSTRAP} "
        f"--create --topic {TOPIC_NAME} "
        f"--partitions 8 --replication-factor 1 --if-not-exists"
    )
    time.sleep(3)



# =========================================================
# 6. 모니터링 수집 (iostat / vmstat)
# =========================================================
def start_monitors(prefix):
    iostat_path = f"{prefix}_iostat.txt"
    vmstat_path = f"{prefix}_vmstat.txt"

    iostat_cmd = f"iostat -y -dxm 1 {RAW_DEVICE_BASENAME}"
    vmstat_cmd = "vmstat 1"

    iostat_f = open(iostat_path, "w", encoding="utf-8")
    vmstat_f = open(vmstat_path, "w", encoding="utf-8")

    iostat_proc = subprocess.Popen(
        iostat_cmd, shell=True, stdout=iostat_f,
        stderr=subprocess.STDOUT, text=True, preexec_fn=os.setsid
    )
    vmstat_proc = subprocess.Popen(
        vmstat_cmd, shell=True, stdout=vmstat_f,
        stderr=subprocess.STDOUT, text=True, preexec_fn=os.setsid
    )
    time.sleep(MONITOR_LEAD_SECONDS)

    return {
        "iostat_proc": iostat_proc,
        "vmstat_proc": vmstat_proc,
        "iostat_file": iostat_f,
        "vmstat_file": vmstat_f,
        "iostat_path": iostat_path,
        "vmstat_path": vmstat_path,
    }


def stop_monitors(mon):
    for proc_key in ["iostat_proc", "vmstat_proc"]:
        proc = mon.get(proc_key)
        if proc:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception:
                pass
    time.sleep(1)
    for fkey in ["iostat_file", "vmstat_file"]:
        f = mon.get(fkey)
        if f:
            try:
                f.close()
            except Exception:
                pass

def parse_iostat_file(path, skip_samples=0):
    """
    iostat 출력에서 '실제 디스크에 도달한' I/O 지표 파싱:
      - %util       : 디스크 컨트롤러 사용률 (%)
      - await       : I/O queue 진입 ~ 완료까지 평균 시간 (ms) = block-level latency
      - r_await/w_await : read/write 분리
      - rkB/s, wkB/s    : 초당 디스크에 도달한 KB
                          (최신 sysstat 은 rMB/s, wMB/s 로 표기됨 → 자동으로 KB 환산)

    이 중 await 가 보고서에서 "block-level latency" 로 사용됨 (app-level latency 와 비교).
    """
    rows = []
    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    headers = None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("Device"):
            headers = re.split(r"\s+", line)
            continue
        if headers and line.startswith(RAW_DEVICE_BASENAME):
            parts = re.split(r"\s+", line)
            if len(parts) == len(headers):
                rows.append(dict(zip(headers, parts)))

    if skip_samples > 0:
        rows = rows[skip_samples:]
    if not rows:
        return {}

    def col_values(*names):
        for name in names:
            if name in rows[0]:
                return [safe_float(r.get(name, 0.0)) for r in rows]
        return []

    util_vals    = col_values("%util", "util")

    # ----- await: 단일 컬럼이 있으면 사용, 없으면 r_await/w_await 가중 평균 -----
    await_vals   = col_values("await")
    r_await_vals = col_values("r_await")
    w_await_vals = col_values("w_await")

    # ----- throughput: rMB/s 우선 (최신), 없으면 rkB/s (구식). 단위는 KB/s 로 통일 -----
    # (CSV 컬럼명이 block_rkB_s_avg 이므로 KB/s 로 환산해서 저장)
    rkB_vals_raw = col_values("rMB/s")
    wkB_vals_raw = col_values("wMB/s")
    if rkB_vals_raw:
        # rMB/s -> rkB/s
        rkB_vals = [v * 1024.0 for v in rkB_vals_raw]
    else:
        rkB_vals = col_values("rkB/s")
    if wkB_vals_raw:
        wkB_vals = [v * 1024.0 for v in wkB_vals_raw]
    else:
        wkB_vals = col_values("wkB/s")

    # ----- await 합성: 요청 수(r/s, w/s) 가중 평균 -----
    rps_vals = col_values("r/s")
    wps_vals = col_values("w/s")
    if not await_vals and (r_await_vals or w_await_vals):
        synthesized = []
        n = max(len(r_await_vals), len(w_await_vals))
        for i in range(n):
            r_aw = r_await_vals[i] if i < len(r_await_vals) else 0.0
            w_aw = w_await_vals[i] if i < len(w_await_vals) else 0.0
            rps = rps_vals[i] if i < len(rps_vals) else 0.0
            wps = wps_vals[i] if i < len(wps_vals) else 0.0
            total_iops = rps + wps
            if total_iops > 0:
                weighted = (r_aw * rps + w_aw * wps) / total_iops
            elif w_aw > 0 or r_aw > 0:
                # throughput 0 인데 await 가 있으면 (드문 경우) 단순 평균
                nonzero = [x for x in [r_aw, w_aw] if x > 0]
                weighted = sum(nonzero) / len(nonzero) if nonzero else 0.0
            else:
                weighted = 0.0
            synthesized.append(weighted)
        await_vals = synthesized

    rqm_vals     = col_values("%rrqm", "%rrqm/s")
    wqm_vals     = col_values("%wrqm", "%wrqm/s")

    return {
        "samples": len(rows),
        "util_avg": safe_mean(util_vals),
        "util_max": max(util_vals) if util_vals else 0.0,
        "await_avg": safe_mean(await_vals),
        "await_max": max(await_vals) if await_vals else 0.0,
        "r_await_avg": safe_mean(r_await_vals),
        "w_await_avg": safe_mean(w_await_vals),
        "rkB_s_avg": safe_mean(rkB_vals),
        "wkB_s_avg": safe_mean(wkB_vals),
        "rps_avg": safe_mean(rps_vals),
        "wps_avg": safe_mean(wps_vals),
        "aqu_sz_avg": safe_mean(col_values("aqu-sz", "avgqu-sz")),
        "rrqm_avg": safe_mean(rqm_vals),
        "wrqm_avg": safe_mean(wqm_vals),
    }


def parse_vmstat_file(path, skip_samples=0):
    rows = []
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    header = None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("procs -----------memory----------"):
            continue
        if re.match(r"^r\s+b\s+swpd\s+", line):
            header = re.split(r"\s+", line)
            continue
        if header and re.match(r"^\d+", line):
            parts = re.split(r"\s+", line)
            if len(parts) == len(header):
                rows.append(dict(zip(header, parts)))

    if skip_samples > 0:
        rows = rows[skip_samples:]
    if not rows:
        return {}

    def vals(name):
        return [safe_float(r.get(name, 0.0)) for r in rows if name in r]

    return {
        "samples": len(rows),
        "cpu_us_avg": safe_mean(vals("us")),
        "cpu_sy_avg": safe_mean(vals("sy")),
        "cpu_id_avg": safe_mean(vals("id")),
        "cpu_wa_avg": safe_mean(vals("wa")),
        "cpu_st_avg": safe_mean(vals("st")),
        "bi_avg": safe_mean(vals("bi")),
        "bo_avg": safe_mean(vals("bo")),
        "cs_avg": safe_mean(vals("cs")),
    }


# =========================================================
# 7. Java benchmark 실행 및 파싱
# =========================================================
def build_java_cmd(config):
    return (
        f"{JAVA_BENCH_CMD} "
        f"--record-size {config['record_size']} "
        f"--target-ops {config['target_ops']} "
        f"--producers {config['producers']} "
        f"--use-consumer {str(config['use_consumer']).lower()} "
        f"--dynamic-topics {str(config['dynamic_topics']).lower()} "
        f"--dynamic-topic-rate {config['dynamic_topic_rate']} "
        f"--warmup-sec {config['warmup_sec']} "
        f"--duration {config['duration']}"
    )


def parse_java_metrics(output):
    """
    Java 출력 파싱.

    참고: 여기서 추출되는 latency 들 (avg_ms, p50_ms, ... )은
          모두 'app-level latency' (producer.send → broker ACK = 페이지 캐시까지의 시간).
    iostat 의 await 가 별도로 'block-level latency' 를 제공.
    """
    metrics = {
        "total_requests": 0,
        # App-level latency : broker 페이지 캐시 도달 시점.
        "avg_ms": 0.0,
        "p50_ms": 0.0,
        "p90_ms": 0.0,
        "p99_ms": 0.0,
        "p999_ms": 0.0,
        "max_ms": 0.0,
        "achieved_ops": 0.0,
        "achieved_pct": 0.0,
        "total_sent": 0,
        "send_errors": 0,
    }
    try:
        m = re.search(r"Total Requests\s*:\s*(\d+)", output)
        if m: metrics["total_requests"] = int(m.group(1))

        m = re.search(r"Average\s*:\s*([\d.]+)\s*ms", output)
        if m: metrics["avg_ms"] = float(m.group(1))

        m = re.search(r"p50.*?:\s*([\d.]+)\s*ms", output)
        if m: metrics["p50_ms"] = float(m.group(1))

        m = re.search(r"p90\s*:\s*([\d.]+)\s*ms", output)
        if m: metrics["p90_ms"] = float(m.group(1))

        m = re.search(r"p99\s*:\s*([\d.]+)\s*ms", output)
        if m: metrics["p99_ms"] = float(m.group(1))

        m = re.search(r"p999\s*:\s*([\d.]+)\s*ms", output)
        if m: metrics["p999_ms"] = float(m.group(1))

        m = re.search(r"Max\s*:\s*([\d.]+)\s*ms", output)
        if m: metrics["max_ms"] = float(m.group(1))

        m = re.search(r"Achieved OP/s\s*:\s*([\d.]+)", output)
        if m: metrics["achieved_ops"] = float(m.group(1))

        m = re.search(r"Achieved/Target.*?:\s*([\d.]+)", output)
        if m: metrics["achieved_pct"] = float(m.group(1))

        m = re.search(r"Total Sent.*?:\s*(\d+)", output)
        if m: metrics["total_sent"] = int(m.group(1))

        m = re.search(r"Send Errors\s*:\s*(\d+)", output)
        if m: metrics["send_errors"] = int(m.group(1))

    except Exception as e:
        print(f"[Parse Error] {e}")

    if metrics["total_requests"] == 0:
        print("[WARN] total_requests=0 — Java 출력 파싱 실패 가능성. raw output 확인 필요.")

    return metrics


def merge_metrics(java_m, iostat_m, vmstat_m):
    merged = {}
    merged.update(java_m or {})
    merged.update(iostat_m or {})
    merged.update(vmstat_m or {})
    return merged


def detect_bottleneck(metrics):
    util       = metrics.get("util_avg", 0.0)
    iowait     = metrics.get("cpu_wa_avg", 0.0)
    await_avg  = metrics.get("await_avg", 0.0)

    util_trigger   = util       >= BOTTLENECK_RULES["min_disk_util_pct"]
    iowait_trigger = iowait     >= BOTTLENECK_RULES["min_cpu_iowait_pct"]
    await_trigger  = await_avg  >= BOTTLENECK_RULES["min_await_ms"]

    triggered = []
    if util_trigger:
        triggered.append(f"disk util avg {util:.2f}% >= {BOTTLENECK_RULES['min_disk_util_pct']:.2f}%")
    if iowait_trigger:
        triggered.append(f"cpu iowait avg {iowait:.2f}% >= {BOTTLENECK_RULES['min_cpu_iowait_pct']:.2f}%")
    if await_trigger:
        triggered.append(f"disk await avg {await_avg:.2f} ms >= {BOTTLENECK_RULES['min_await_ms']:.2f} ms")

    is_bottleneck = util_trigger and (await_trigger or iowait_trigger)
    return {"is_bottleneck": is_bottleneck, "reasons": triggered}


def run_benchmark_once(fs_type, scenario_key, config, round_idx, phase_tag):
    print(f"\n--- [{phase_tag.upper()}] {fs_type.upper()} / {scenario_key} / "
          f"{config['record_size']}B / {config['target_ops']} ops ---")

    run_prefix = (
        f"{MONITOR_DIR}/r{round_idx}_{fs_type}_{scenario_key}_"
        f"{config['record_size']}B_{config['target_ops']}ops_{phase_tag}"
    )
    monitors = start_monitors(run_prefix)
    try:
        cmd = build_java_cmd(config)
        output, return_code = run_cmd_streaming(cmd)
    finally:
        stop_monitors(monitors)

    if return_code != 0:
        print(f"[CRITICAL ERROR] Java benchmark failed with exit code {return_code}")
        raise RuntimeError(f"{scenario_key} failed")

    java_metrics = parse_java_metrics(output)
    skip_samples = config.get("warmup_sec", 0) + MONITOR_LEAD_SECONDS
    iostat_metrics = parse_iostat_file(monitors["iostat_path"], skip_samples=skip_samples)
    vmstat_metrics = parse_vmstat_file(monitors["vmstat_path"], skip_samples=skip_samples)
    merged = merge_metrics(java_metrics, iostat_metrics, vmstat_metrics)
    bottleneck = detect_bottleneck(merged)

    raw_path = (
        f"{RAW_DIR}/r{round_idx}_{fs_type}_{scenario_key}_"
        f"{config['record_size']}B_{config['target_ops']}ops_{phase_tag}.txt"
    )
    write_text(raw_path, output)

    # app-level p99 와 block-level await
    print(
        f"  > Req={merged.get('total_requests', 0)} | "
        f"Achieved={merged.get('achieved_ops', 0.0):.1f} OP/s "
        f"({merged.get('achieved_pct', 0.0):.1f}%) | "
        f"App-Avg={merged.get('avg_ms', 0.0):.2f}ms | "
        f"App-P99={merged.get('p99_ms', 0.0):.2f}ms | "
        f"DiskUtil={merged.get('util_avg', 0.0):.2f}% | "
        f"Block-await={merged.get('await_avg', 0.0):.2f}ms | "
        f"iowait={merged.get('cpu_wa_avg', 0.0):.2f}% | "
        f"Bottleneck={bottleneck['is_bottleneck']}"
    )

    iostat_final = monitors["iostat_path"]
    vmstat_final = monitors["vmstat_path"]
    raw_final = raw_path
    if COMPRESS_AFTER_RUN:
        raw_final = gzip_file(raw_path)
        iostat_final = gzip_file(monitors["iostat_path"])
        vmstat_final = gzip_file(monitors["vmstat_path"])

    return {
        "config": deepcopy(config),
        "metrics": merged,
        "bottleneck": bottleneck,
        "raw_output_path": raw_final,
        "monitor_files": {
            "iostat": iostat_final,
            "vmstat": vmstat_final,
        }
    }


# =========================================================
# 8. 결과 저장
# =========================================================
def flatten_result_rows(all_results):
    rows = []
    for fs_type, fs_payload in all_results.items():
        for record_size, rs_payload in fs_payload.items():
            for scenario_key, sc_payload in rs_payload.items():
                for round_entry in sc_payload["rounds"]:
                    m = round_entry["metrics"]
                    b = round_entry["bottleneck"]
                    rows.append({
                        "filesystem": fs_type,
                        "record_size": record_size,
                        "scenario": scenario_key,
                        "round": round_entry["round"],
                        "target_ops": round_entry["config"]["target_ops"],
                        "duration": round_entry["config"]["duration"],
                        "warmup_sec": round_entry["config"]["warmup_sec"],
                        "producers": round_entry["config"]["producers"],
                        "use_consumer": round_entry["config"]["use_consumer"],
                        "dynamic_topics": round_entry["config"]["dynamic_topics"],
                        "dynamic_topic_rate": round_entry["config"]["dynamic_topic_rate"],
                        "total_requests": m.get("total_requests", 0),
                        "achieved_ops": m.get("achieved_ops", 0.0),
                        "achieved_pct": m.get("achieved_pct", 0.0),
                        "total_sent": m.get("total_sent", 0),
                        "send_errors": m.get("send_errors", 0),
                        # App-level latency (broker ACK = 페이지 캐시 시점까지)
                        "app_avg_ms": m.get("avg_ms", 0.0),
                        "app_p50_ms": m.get("p50_ms", 0.0),
                        "app_p90_ms": m.get("p90_ms", 0.0),
                        "app_p99_ms": m.get("p99_ms", 0.0),
                        "app_p999_ms": m.get("p999_ms", 0.0),
                        "app_max_ms": m.get("max_ms", 0.0),
                        # Block-level metrics (iostat — 실제 디스크에 도달)
                        "block_util_avg": m.get("util_avg", 0.0),
                        "block_util_max": m.get("util_max", 0.0),
                        "block_await_avg": m.get("await_avg", 0.0),
                        "block_await_max": m.get("await_max", 0.0),
                        "block_r_await_avg": m.get("r_await_avg", 0.0),
                        "block_w_await_avg": m.get("w_await_avg", 0.0),
                        "block_rkB_s_avg": m.get("rkB_s_avg", 0.0),
                        "block_wkB_s_avg": m.get("wkB_s_avg", 0.0),
                        # CPU
                        "cpu_us_avg": m.get("cpu_us_avg", 0.0),
                        "cpu_sy_avg": m.get("cpu_sy_avg", 0.0),
                        "cpu_id_avg": m.get("cpu_id_avg", 0.0),
                        "cpu_wa_avg": m.get("cpu_wa_avg", 0.0),
                        # Bottleneck 판정
                        "bottleneck": b.get("is_bottleneck", False),
                        "bottleneck_reasons": " | ".join(b.get("reasons", [])),
                        "raw_output_path": round_entry["raw_output_path"],
                        "iostat_path": round_entry["monitor_files"]["iostat"],
                        "vmstat_path": round_entry["monitor_files"]["vmstat"],
                    })
    return rows


def save_csv_reports(all_results):
    rows = flatten_result_rows(all_results)
    full_csv = f"{CSV_DIR}/full_results.csv"
    if rows:
        with open(full_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    # 요약 CSV
    summary_csv = f"{CSV_DIR}/summary_by_fs_recordsize_scenario.csv"
    grouped = {}
    for row in rows:
        key = (row["filesystem"], row["record_size"], row["scenario"])
        grouped.setdefault(key, []).append(row)

    summary_rows = []
    for (fs_type, record_size, scenario), items in grouped.items():
        avg_list   = [x["app_avg_ms"]      for x in items]
        p50_list   = [x["app_p50_ms"]      for x in items]
        p90_list   = [x["app_p90_ms"]      for x in items]
        p99_list   = [x["app_p99_ms"]      for x in items]
        max_list   = [x["app_max_ms"]      for x in items]
        req_list   = [x["total_requests"]  for x in items]
        util_list  = [x["block_util_avg"]  for x in items]
        await_list = [x["block_await_avg"] for x in items]
        wkb_list   = [x["block_wkB_s_avg"] for x in items]
        wa_list    = [x["cpu_wa_avg"]      for x in items]
        ops_list   = [x["target_ops"]      for x in items]
        ach_list   = [x["achieved_ops"]    for x in items]
        ach_pct    = [x["achieved_pct"]    for x in items]
        err_list   = [x["send_errors"]     for x in items]

        summary_rows.append({
            "filesystem": fs_type,
            "record_size": record_size,
            "scenario": scenario,
            "rounds": len(items),
            "target_ops_mean": safe_mean(ops_list),
            "achieved_ops_mean": safe_mean(ach_list),
            "achieved_pct_mean": safe_mean(ach_pct),
            "send_errors_mean": safe_mean(err_list),
            "requests_mean": safe_mean(req_list),
            "app_avg_mean": safe_mean(avg_list),
            "app_avg_stdev": safe_stdev(avg_list),
            "app_avg_cv_pct": safe_cv(avg_list),
            "app_p50_mean": safe_mean(p50_list),
            "app_p90_mean": safe_mean(p90_list),
            "app_p99_mean": safe_mean(p99_list),
            "app_p99_stdev": safe_stdev(p99_list),
            "app_p99_cv_pct": safe_cv(p99_list),
            "app_max_mean": safe_mean(max_list),
            # Block-level metrics
            "block_util_mean": safe_mean(util_list),
            "block_await_mean": safe_mean(await_list),
            "block_await_stdev": safe_stdev(await_list),
            "block_await_cv_pct": safe_cv(await_list),
            "block_wkB_s_mean": safe_mean(wkb_list),
            "cpu_iowait_mean": safe_mean(wa_list),
            "bottleneck_rounds": sum(1 for x in items if x["bottleneck"]),
        })

    if summary_rows:
        with open(summary_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            writer.writeheader()
            writer.writerows(summary_rows)

    print(f"[Success] CSV saved: {full_csv}")
    print(f"[Success] CSV saved: {summary_csv}")


def save_json_snapshot(all_results):
    path = f"{RESULT_DIR}/results_snapshot.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(f"[Success] JSON snapshot saved: {path}")


def save_summary_report(all_results, current_round, final=False):
    """
    Two-level latency report.
      - App-level: producer.send -> ACK (broker 페이지 캐시 도달까지의 시간).
      - Block-level: iostat 의 await (실제 디스크에 도달한 I/O 의 평균 처리 시간).
      두 layer 가 따로 표시되어 fs 의 영향이 어느 layer 에 어떻게 나타나는지 보여줌.
    """
    report_path = f"{RESULT_DIR}/summary_report.txt"
    lines = []
    lines.append("============================================================")
    lines.append("  KAFKA FILESYSTEM PERFORMANCE EXPERIMENT REPORT (M1-#1)")
    lines.append("============================================================")
    lines.append(f"Start Time : {program_start_time}")
    lines.append(f"Current Round Completed : {current_round}")
    if final:
        lines.append(f"End Time   : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("Latency Layers (이 보고서가 측정/구분하는 두 가지 latency)")
    lines.append("  - App-level   : producer.send → broker ACK = 'broker 의 페이지 캐시 도달까지'")
    lines.append("                  (Kafka client 가 인지하는 latency)")
    lines.append("  - Block-level : iostat 의 await = 'I/O 가 block layer 큐에 들어간 후 완료까지'")
    lines.append("                  (실제 디스크에 도달한 I/O 의 평균 처리 시간)")
    lines.append("  두 값의 차이가 OS 페이지 캐시 + Kafka 큐잉의 latency 흡수 효과를 보여줌.")
    lines.append("  fs 의 차이가 어느 layer 에서 나타나는지에 따라 ZNS 도입 motivation 이 달라짐.")
    lines.append("")
    lines.append("Experiment Scope")
    lines.append("- Kafka 4.2 (KRaft single broker, replication=1)")
    lines.append(f"- metadata.log.dir separated: {SEPARATE_METADATA_DIR}")
    lines.append(f"- Filesystems: {', '.join(FILESYSTEMS)} on {FS_DEVICE} (dm-zns-base over FEMU ZNS)")
    lines.append(f"- DM implementation: {DM_IMPLEMENTATION_LABELS[DM_IMPLEMENTATION]}")
    if DM_IMPLEMENTATION == "fixed":
        lines.append(f"- Raw ZNS: {RAW_ZNS_DEVICE}; fixed reserved zones: metadata={METADATA_ZONES}, GC={GC_RESERVE_ZONES}")
    else:
        lines.append(f"- Raw ZNS: {RAW_ZNS_DEVICE}; zone roles allocated dynamically by the DM target")
    lines.append("- Filesystem format/mount discard disabled because dm-zns-base supports read/write/flush only")
    lines.append(f"- Record sizes: {RECORD_SIZES}")
    lines.append("- Scenarios: A, B, A+Dynamic, B+Dynamic")
    lines.append(f"- Dynamic topic rate (/sec): {TOPIC_RATE}")
    lines.append(f"- Measure    : {MEASURE_DURATION}s (warmup {WARMUP_SECONDS}s)")
    lines.append("- Send mode  : ASYNC (callback-based latency measurement)")
    lines.append("- OPS mode   : fixed (no calibration)")
    lines.append(f"- FIXED_OPS_BY_RECORD_SIZE: {FIXED_OPS_BY_RECORD_SIZE}")
    lines.append("")

    for fs_type in FILESYSTEMS:
        lines.append(f"######################### FILESYSTEM: {fs_type.upper()} #########################")
        for record_size in RECORD_SIZES:
            lines.append(f"\n[Record Size: {record_size} Bytes]")
            for scenario_key in SCENARIO_KEYS:
                payload = all_results.get(fs_type, {}).get(record_size, {}).get(scenario_key, {})
                rounds_data = payload.get("rounds", [])
                if not rounds_data:
                    lines.append(f"  - {scenario_key}: no data")
                    continue

                # App-level
                avg_v   = [x["metrics"].get("avg_ms", 0.0)         for x in rounds_data]
                p50_v   = [x["metrics"].get("p50_ms", 0.0)         for x in rounds_data]
                p90_v   = [x["metrics"].get("p90_ms", 0.0)         for x in rounds_data]
                p99_v   = [x["metrics"].get("p99_ms", 0.0)         for x in rounds_data]
                max_v   = [x["metrics"].get("max_ms", 0.0)         for x in rounds_data]
                # Throughput / errors
                req_v   = [x["metrics"].get("total_requests", 0)   for x in rounds_data]
                ops_v   = [x["config"].get("target_ops", 0)        for x in rounds_data]
                ach_v   = [x["metrics"].get("achieved_ops", 0.0)   for x in rounds_data]
                ach_pct = [x["metrics"].get("achieved_pct", 0.0)   for x in rounds_data]
                err_v   = [x["metrics"].get("send_errors", 0)      for x in rounds_data]
                # Block-level
                util_v  = [x["metrics"].get("util_avg", 0.0)       for x in rounds_data]
                awt_v   = [x["metrics"].get("await_avg", 0.0)      for x in rounds_data]
                wkb_v   = [x["metrics"].get("wkB_s_avg", 0.0)      for x in rounds_data]
                wa_v    = [x["metrics"].get("cpu_wa_avg", 0.0)     for x in rounds_data]
                bn_cnt  = sum(1 for x in rounds_data if x["bottleneck"].get("is_bottleneck"))

                lines.append(f"  [{scenario_key}]")
                lines.append(f"    rounds                : {len(rounds_data)}")
                lines.append(f"    target_ops mean       : {safe_mean(ops_v):.2f}")
                lines.append(f"    achieved_ops mean     : {safe_mean(ach_v):.2f}")
                lines.append(f"    achieved/target mean  : {safe_mean(ach_pct):.2f} %")
                lines.append(f"    total_requests mean   : {safe_mean(req_v):.2f}")
                lines.append(f"    send_errors mean      : {safe_mean(err_v):.2f}")
                lines.append(f"    -- App-level latency (send -> ACK = page cache 도달) --")
                lines.append(f"    app avg / stdev       : {safe_mean(avg_v):.2f} / {safe_stdev(avg_v):.2f} ms  (CV={safe_cv(avg_v):.1f}%)")
                lines.append(f"    app p50               : {safe_mean(p50_v):.2f} ms")
                lines.append(f"    app p90               : {safe_mean(p90_v):.2f} ms")
                lines.append(f"    app p99 / stdev       : {safe_mean(p99_v):.2f} / {safe_stdev(p99_v):.2f} ms  (CV={safe_cv(p99_v):.1f}%)")
                lines.append(f"    app max               : {safe_mean(max_v):.2f} ms")
                lines.append(f"    -- Block-level (iostat: 실제 디스크 I/O) --")
                lines.append(f"    block disk util       : {safe_mean(util_v):.2f} %")
                lines.append(f"    block await mean      : {safe_mean(awt_v):.2f} ms  (CV={safe_cv(awt_v):.1f}%)")
                lines.append(f"    block write throughput: {safe_mean(wkb_v):.2f} kB/s")
                lines.append(f"    cpu iowait mean       : {safe_mean(wa_v):.2f} %")
                lines.append(f"    bottleneck rounds     : {bn_cnt}/{len(rounds_data)}")
        lines.append("\n")

    write_text(report_path, "\n".join(lines))
    print(f"[Success] Summary report saved: {report_path}")


# =========================================================
# 10. 메인 루프
# =========================================================
def init_result_structure():
    result = {}
    for fs_type in FILESYSTEMS:
        result[fs_type] = {}
        for record_size in RECORD_SIZES:
            result[fs_type][record_size] = {}
            for scenario_key in SCENARIO_KEYS:
                result[fs_type][record_size][scenario_key] = {"rounds": []}
    return result


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python3 bench_final.py <0=fixed|1=dynamic> [rounds]")
        sys.exit(2)

    try:
        configure_dm_implementation(sys.argv[1])
        rounds = int(sys.argv[2]) if len(sys.argv) == 3 else DEFAULT_ROUNDS
        if rounds < 1:
            raise ValueError("rounds must be positive")
    except ValueError as exc:
        print(f"[CRITICAL ERROR] {exc}")
        print("Usage: python3 bench_final.py <0=fixed|1=dynamic> [rounds]")
        sys.exit(2)

    print(f"[Config] DM implementation: {DM_IMPLEMENTATION_LABELS[DM_IMPLEMENTATION]}")

    validate_kafka_environment()
    prepare_experiment_kraft_config()
    capture_environment()

    all_results = init_result_structure()

    completed = False
    try:
        for record_size in RECORD_SIZES:
            if record_size not in FIXED_OPS_BY_RECORD_SIZE:
                raise ValueError(
                    f"FIXED_OPS_BY_RECORD_SIZE에 record_size={record_size} 값이 없습니다."
                )
            for r in range(1, rounds + 1):
                round_log_path = f"{ROUND_DIR}/size_{record_size}_round_{r}.txt"
                round_logger = Logger(round_log_path)
                orig_stdout = sys.stdout
                sys.stdout = round_logger

                try:
                    print(f"\n{'='*80}")
                    print(f" RECORD SIZE {record_size} / ROUND {r}/{rounds} START ")
                    print(f"{'='*80}")

                    for fs_type in FILESYSTEMS:
                        print(f"\n{'#'*70}")
                        print(f"# FILESYSTEM = {fs_type.upper()}")
                        print(f"{'#'*70}")
                        print(f"[Record Size] {record_size} Bytes")

                        for scenario_key in SCENARIO_KEYS:
                            scenario = deepcopy(SCENARIO_TEMPLATES[scenario_key])
                            chosen_ops = FIXED_OPS_BY_RECORD_SIZE[record_size]
                            print(
                                f"[Fixed OP/s] {fs_type.upper()} / {scenario_key} / "
                                f"{record_size}B -> target_ops={chosen_ops}"
                            )

                            control_kafka("stop")
                            setup_env(fs_type)
                            control_kafka("start")
                            recreate_main_topic()

                            measure_config = {
                                "record_size": record_size,
                                "target_ops": chosen_ops,
                                "producers": scenario["producers"],
                                "use_consumer": scenario["use_consumer"],
                                "dynamic_topics": scenario["dynamic_topics"],
                                "dynamic_topic_rate": TOPIC_RATE,
                                "warmup_sec": WARMUP_SECONDS,
                                "duration": MEASURE_DURATION,
                            }
                            measured = run_benchmark_once(
                                fs_type=fs_type,
                                scenario_key=scenario_key,
                                config=measure_config,
                                round_idx=r,
                                phase_tag="measure"
                            )
                            measured["round"] = r
                            measured["calibration_reference"] = {
                                "mode": "fixed",
                                "chosen_ops": chosen_ops,
                            }

                            all_results[fs_type][record_size][scenario_key]["rounds"].append(measured)

                            save_json_snapshot(all_results)
                            save_csv_reports(all_results)
                            save_summary_report(all_results, current_round=r, final=False)

                except Exception as e:
                    orig_stdout.write(
                        f"\n[CRITICAL ERROR] Record Size {record_size} Round {r} failed: {e}\n"
                    )
                    raise
                finally:
                    sys.stdout = orig_stdout
                    round_logger.close()
                    print(f"[Done] Record Size {record_size} Round {r} log saved: {round_log_path}")

        save_json_snapshot(all_results)
        save_csv_reports(all_results)
        save_summary_report(all_results, current_round=rounds, final=True)
        completed = True

    finally:
        control_kafka("stop")
        unmount_log_device(strict=False)
        remove_dm_stack(strict=False)
        run_cmd_quiet("sudo sync")
        if completed:
            print("\n" + "=" * 80)
            print(f"[Finish] All {rounds} rounds completed.")
            print(f"Check '{RESULT_DIR}' for reports.")
            print("=" * 80)
        else:
            print("[Finish] Benchmark stopped before completion; partial results were retained.")
