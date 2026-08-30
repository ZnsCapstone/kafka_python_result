"""FEMU ZNS, filesystem, Kafka KRaft, and reproducibility setup."""

import datetime
import os
import subprocess
import sys
import time

import settings as cfg
from bench_utils import run_cmd_full, run_cmd_quiet, wait_for_port, write_text


def capture_environment():
    sections = [
        ("Date", datetime.datetime.now().isoformat()),
        ("uname -a", run_cmd_quiet("uname -a")),
        ("/etc/os-release", run_cmd_quiet("cat /etc/os-release")),
        ("CPU info (lscpu)", run_cmd_quiet("lscpu")),
        ("Memory (free -h)", run_cmd_quiet("free -h")),
        ("Disk (lsblk)", run_cmd_quiet("lsblk")),
        ("Disk (df -h)", run_cmd_quiet("df -h")),
        ("Kernel cmdline", run_cmd_quiet("cat /proc/cmdline")),
        ("vm.dirty_ratio", run_cmd_quiet("sysctl vm.dirty_ratio")),
        ("vm.dirty_background_ratio", run_cmd_quiet("sysctl vm.dirty_background_ratio")),
        ("vm.swappiness", run_cmd_quiet("sysctl vm.swappiness")),
        ("iostat version", run_cmd_quiet("iostat -V | head -1")),
        ("vmstat version", run_cmd_quiet("vmstat -V")),
        ("mkfs.ext4 version", run_cmd_quiet("mkfs.ext4 -V 2>&1 | head -1")),
        ("mkfs.f2fs version", run_cmd_quiet("mkfs.f2fs -V 2>&1 | head -1")),
        ("Java version", run_cmd_quiet("java -version 2>&1")),
        ("Java benchmark JAR", cfg.JAVA_BENCH_JAR),
        ("Java benchmark SHA-256", run_cmd_quiet(f"sha256sum {cfg.JAVA_BENCH_JAR}")),
        ("Benchmark profile", cfg.ACTIVE_PROFILE),
        ("Profile config", str(cfg.active_profile_config())),
        ("Scenario group", cfg.ACTIVE_SCENARIO_GROUP),
        ("Kafka path", cfg.KAFKA_PATH),
        ("Kafka config", run_cmd_quiet(f"cat {cfg.EXPERIMENT_KRAFT_CONFIG} | head -50")),
        ("Raw ZNS device", cfg.RAW_ZNS_DEVICE),
        ("DM device", cfg.FS_DEVICE),
        ("DM module", cfg.DM_MODULE_PATH),
        ("DM implementation", cfg.DM_IMPLEMENTATION_LABELS[cfg.DM_IMPLEMENTATION]),
        ("DM table", run_cmd_quiet(f"sudo dmsetup table {cfg.DM_NAME} 2>&1 || true")),
    ]
    content = "\n".join(f"### {name}\n{value}\n" for name, value in sections)
    path = f"{cfg.ENV_DIR}/system_info.txt"
    write_text(path, content)
    print(f"[Env] System info saved: {path}")


def dm_module_name():
    return os.path.basename(cfg.DM_MODULE_PATH).removesuffix(".ko").replace("-", "_")


def unmount_log_device(strict=True):
    last_result = None
    for attempt in range(1, 6):
        result = subprocess.run(
            f"sudo umount {cfg.MOUNT_POINT}", shell=True, capture_output=True, text=True
        )
        if result.returncode == 0 or "not mounted" in result.stderr.lower():
            return
        last_result = result
        if attempt < 5:
            print(f"[Setup] Mount still busy; retrying unmount ({attempt}/5) ...")
            run_cmd_quiet("sudo sync")
            time.sleep(2)
    busy = run_cmd_full(f"sudo fuser -vm {cfg.MOUNT_POINT} 2>&1 || true")
    message = f"umount failed: {last_result.stderr.strip()}\n{busy.stdout.strip()}"
    if strict:
        raise RuntimeError(message)
    print(f"[Warn] {message}")


def remove_dm_stack(strict=True):
    result = run_cmd_full(f"sudo dmsetup remove {cfg.DM_NAME}")
    if result.returncode != 0 and not any(
        text in result.stderr.lower() for text in ("no such device", "not found")
    ):
        message = f"dmsetup remove failed:\n{result.stdout}\n{result.stderr}"
        if strict:
            raise RuntimeError(message)
        print(f"[Warn] {message}")

    result = run_cmd_full(f"sudo rmmod {dm_module_name()}")
    if result.returncode != 0 and not any(
        text in result.stderr.lower() for text in ("not currently loaded", "no such file")
    ):
        message = f"rmmod failed:\n{result.stdout}\n{result.stderr}"
        if strict:
            raise RuntimeError(message)
        print(f"[Warn] {message}")


def zns_logical_sectors():
    zoned = run_cmd_quiet(f"cat /sys/block/{cfg.RAW_DEVICE_BASENAME}/queue/zoned")
    if zoned != "host-managed":
        raise RuntimeError(
            f"{cfg.RAW_ZNS_DEVICE} is not a host-managed ZNS device (zoned={zoned!r})"
        )
    try:
        with open(
            f"/sys/block/{cfg.RAW_DEVICE_BASENAME}/queue/chunk_sectors", encoding="utf-8"
        ) as file:
            zone_sectors = int(file.read().strip())
        with open(
            f"/sys/block/{cfg.RAW_DEVICE_BASENAME}/queue/nr_zones", encoding="utf-8"
        ) as file:
            nr_zones = int(file.read().strip())
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cannot read ZNS geometry for {cfg.RAW_ZNS_DEVICE}: {exc}") from exc

    if cfg.DM_IMPLEMENTATION == "dynamic":
        return nr_zones * zone_sectors
    reserved = cfg.METADATA_ZONES + cfg.GC_RESERVE_ZONES
    if nr_zones <= reserved:
        raise RuntimeError(
            f"ZNS has {nr_zones} zones, but fixed dm-zns-base needs {reserved} reserved zones"
        )
    return (nr_zones - reserved) * zone_sectors


def create_dm_target():
    if not os.path.exists(cfg.DM_MODULE_PATH):
        raise RuntimeError(
            f"dm-zns-base module not found: {cfg.DM_MODULE_PATH}\n"
            "Build dm-zns-base.c into dm-zns-base.ko first, or set DM_ZNS_MODULE_PATH."
        )
    result = run_cmd_full(f"sudo insmod {cfg.DM_MODULE_PATH}")
    if result.returncode != 0:
        raise RuntimeError(f"insmod failed:\n{result.stdout}\n{result.stderr}")
    logical_sectors = zns_logical_sectors()
    result = subprocess.run(
        ["sudo", "dmsetup", "create", cfg.DM_NAME],
        input=f"0 {logical_sectors} zns-base {cfg.RAW_ZNS_DEVICE}\n",
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        run_cmd_quiet(f"sudo rmmod {dm_module_name()} || true")
        raise RuntimeError(f"dmsetup create failed:\n{result.stdout}\n{result.stderr}")
    print(
        f"[DM] Created {cfg.FS_DEVICE}: {logical_sectors} sectors "
        f"({logical_sectors * 512 / 1024**3:.2f} GiB)"
    )


def stop_stale_kafka_processes():
    """Stop only known Kafka JVM main classes before replacing the log device.

    Do not use a broad pattern such as ``pkill -f kafka`` here.  The benchmark
    runner itself normally lives below a ``kafka_python_result`` directory, so
    that pattern also matches and kills the Python process when an absolute
    script path is used.
    """
    for process_class in ("kafka.Kafka", "QuorumPeerMain"):
        run_cmd_quiet(f"pkill -9 -f '{process_class}' || true")


def setup_filesystem(fs_type):
    if fs_type not in cfg.FILESYSTEMS:
        raise ValueError(f"Unsupported filesystem: {fs_type}")
    print(f"\n[Setup] Resetting FEMU ZNS and mounting {fs_type} through {cfg.DM_NAME} ...")
    stop_stale_kafka_processes()
    time.sleep(2)
    unmount_log_device()
    remove_dm_stack()

    result = run_cmd_full(f"sudo blkzone reset {cfg.RAW_ZNS_DEVICE}")
    if result.returncode != 0:
        raise RuntimeError(f"zone reset failed:\n{result.stdout}\n{result.stderr}")
    create_dm_target()
    result = run_cmd_full(f"sudo wipefs -a {cfg.FS_DEVICE}")
    if result.returncode != 0:
        raise RuntimeError(f"wipefs failed:\n{result.stdout}\n{result.stderr}")

    if fs_type == "ext4":
        result = run_cmd_full(f"sudo mkfs.ext4 -F -E nodiscard -L kafka_ext4 {cfg.FS_DEVICE}")
    else:
        result = run_cmd_full(f"sudo mkfs.f2fs -f -t 0 -l kafka_f2fs {cfg.FS_DEVICE}")
    if result.returncode != 0:
        raise RuntimeError(f"mkfs failed:\n{result.stdout}\n{result.stderr}")

    run_cmd_quiet(f"sudo mkdir -p {cfg.MOUNT_POINT}")
    result = run_cmd_full(
        f"sudo mount -o noatime,nodiratime,nodiscard {cfg.FS_DEVICE} {cfg.MOUNT_POINT}"
    )
    if result.returncode != 0:
        raise RuntimeError(f"mount failed:\n{result.stdout}\n{result.stderr}")
    run_cmd_quiet(f"sudo rm -rf {cfg.MOUNT_POINT}/lost+found")
    run_cmd_quiet(f"sudo chmod 777 {cfg.MOUNT_POINT}")
    print(run_cmd_quiet(f"lsblk {cfg.FS_DEVICE}"))
    print(run_cmd_quiet(f"mount | grep {cfg.DM_NAME}"))
    run_cmd_quiet("sudo sync")
    run_cmd_quiet("echo 3 | sudo tee /proc/sys/vm/drop_caches")
    if cfg.SEPARATE_METADATA_DIR:
        run_cmd_quiet(f"sudo mkdir -p {cfg.METADATA_DIR}")
        run_cmd_quiet(f"sudo rm -rf {cfg.METADATA_DIR}/*")
        run_cmd_quiet(f"sudo chmod 777 {cfg.METADATA_DIR}")


def validate_kafka_environment():
    print("[Check] Validating Kafka environment ...")
    if "4.2" not in cfg.KAFKA_PATH:
        print(f"[WARN] KAFKA_PATH에 4.2 문자열이 없습니다: {cfg.KAFKA_PATH}")
    required = [
        f"{cfg.KAFKA_PATH}/bin/kafka-storage.sh",
        f"{cfg.KAFKA_PATH}/bin/kafka-server-start.sh",
        f"{cfg.KAFKA_PATH}/bin/kafka-server-stop.sh",
        cfg.KRAFT_CONFIG,
        cfg.JAVA_BENCH_JAR,
    ]
    missing = [path for path in required if not os.path.exists(path)]
    if missing:
        raise RuntimeError("Required benchmark files not found:\n" + "\n".join(missing))


def prepare_experiment_kraft_config():
    with open(cfg.KRAFT_CONFIG, "r", encoding="utf-8") as source:
        lines = source.readlines()
    managed_keys = ("log.dirs", "metadata.log.dir")
    lines = [
        line for line in lines
        if not any(line.strip().startswith(f"{key}=") for key in managed_keys)
    ]
    lines.append(f"\nlog.dirs={cfg.MOUNT_POINT}\n")
    if cfg.SEPARATE_METADATA_DIR:
        lines.append(f"metadata.log.dir={cfg.METADATA_DIR}\n")
    write_text(cfg.EXPERIMENT_KRAFT_CONFIG, "".join(lines))
    print(f"[Env] Experiment Kafka config saved: {cfg.EXPERIMENT_KRAFT_CONFIG}")


def control_kafka(action):
    if action == "stop":
        print("[Service] Stopping Kafka KRaft broker ...")
        run_cmd_quiet(f"{cfg.KAFKA_PATH}/bin/kafka-server-stop.sh")
        if not wait_for_port(9092, timeout=15, closed=True):
            run_cmd_quiet("sudo fuser -k 9092/tcp || true")
            run_cmd_quiet("pkill -TERM -f 'kafka.Kafka' || true")
            if not wait_for_port(9092, timeout=10, closed=True):
                run_cmd_quiet("pkill -KILL -f 'kafka.Kafka' || true")
                wait_for_port(9092, timeout=5, closed=True)
        run_cmd_quiet("sudo sync")
        time.sleep(2)
        return
    if action != "start":
        raise ValueError(f"Unknown action: {action}")

    print("[Service] Starting Kafka in KRaft mode ...")
    cluster_id = run_cmd_quiet(f"{cfg.KAFKA_PATH}/bin/kafka-storage.sh random-uuid")
    if not cluster_id:
        raise RuntimeError("Failed to generate KRaft cluster id")
    result = run_cmd_full(
        f"{cfg.KAFKA_PATH}/bin/kafka-storage.sh format -t {cluster_id} "
        f"-c {cfg.EXPERIMENT_KRAFT_CONFIG} --standalone --ignore-formatted"
    )
    if result.returncode != 0:
        raise RuntimeError(f"KRaft storage format failed:\n{result.stdout}\n{result.stderr}")
    overrides = " ".join([
        "--override num.partitions=8",
        "--override offsets.topic.replication.factor=1",
        "--override transaction.state.log.replication.factor=1",
        "--override transaction.state.log.min.isr=1",
        "--override default.replication.factor=1",
        "--override log.retention.check.interval.ms=1000",
        "--override log.segment.delete.delay.ms=0",
        "--override auto.create.topics.enable=false",
        "--override message.max.bytes=10485760",
        "--override replica.fetch.max.bytes=10485760",
    ])
    run_cmd_full(
        f"{cfg.KAFKA_PATH}/bin/kafka-server-start.sh -daemon "
        f"{cfg.EXPERIMENT_KRAFT_CONFIG} {overrides}"
    )
    if not wait_for_port(9092, timeout=180):
        raise RuntimeError("Kafka KRaft broker start timeout")
    time.sleep(8)


def recreate_main_topic():
    print("[Service] Recreating benchmark topic ...")
    run_cmd_quiet(
        f"{cfg.KAFKA_PATH}/bin/kafka-topics.sh --bootstrap-server {cfg.BOOTSTRAP} "
        f"--delete --topic {cfg.TOPIC_NAME} --if-exists"
    )
    time.sleep(3)
    run_cmd_quiet(
        f"{cfg.KAFKA_PATH}/bin/kafka-topics.sh --bootstrap-server {cfg.BOOTSTRAP} "
        f"--create --topic {cfg.TOPIC_NAME} --partitions 8 --replication-factor 1 --if-not-exists"
    )
    time.sleep(3)


def count_kafka_topics():
    """Return the broker's current topic count, or -1 when the query fails."""
    result = run_cmd_full(
        f"{cfg.KAFKA_PATH}/bin/kafka-topics.sh --bootstrap-server {cfg.BOOTSTRAP} --list"
    )
    if result.returncode != 0:
        return -1
    return len([line for line in result.stdout.splitlines() if line.strip()])
