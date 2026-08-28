"""Benchmark execution, host monitoring, and performance metric parsing."""

import os
import re
import signal
import subprocess
import time
from copy import deepcopy

import settings as cfg
from bench_utils import gzip_file, run_cmd_streaming, safe_float, safe_mean, write_text


def start_monitors(prefix):
    paths = {"iostat": f"{prefix}_iostat.txt", "vmstat": f"{prefix}_vmstat.txt"}
    files = {name: open(path, "w", encoding="utf-8") for name, path in paths.items()}
    processes = {
        "iostat": subprocess.Popen(
            f"iostat -y -dxm 1 {cfg.RAW_DEVICE_BASENAME}", shell=True,
            stdout=files["iostat"], stderr=subprocess.STDOUT, text=True,
            preexec_fn=os.setsid,
        ),
        "vmstat": subprocess.Popen(
            "vmstat 1", shell=True, stdout=files["vmstat"],
            stderr=subprocess.STDOUT, text=True, preexec_fn=os.setsid,
        ),
    }
    time.sleep(cfg.MONITOR_LEAD_SECONDS)
    return {"paths": paths, "files": files, "processes": processes}


def stop_monitors(monitors):
    for process in monitors["processes"].values():
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        except (OSError, ProcessLookupError):
            pass
    time.sleep(1)
    for file in monitors["files"].values():
        file.close()


def parse_iostat_file(path, skip_samples=0):
    if not os.path.exists(path):
        return {}
    rows, headers = [], None
    with open(path, "r", encoding="utf-8", errors="ignore") as file:
        for raw_line in file:
            line = raw_line.strip()
            if line.startswith("Device"):
                headers = re.split(r"\s+", line)
            elif headers and line.startswith(cfg.RAW_DEVICE_BASENAME):
                parts = re.split(r"\s+", line)
                if len(parts) == len(headers):
                    rows.append(dict(zip(headers, parts)))
    rows = rows[skip_samples:]
    if not rows:
        return {}

    def columns(*names):
        for name in names:
            if name in rows[0]:
                return [safe_float(row.get(name)) for row in rows]
        return []

    util = columns("%util", "util")
    await_values = columns("await")
    read_await, write_await = columns("r_await"), columns("w_await")
    reads, writes = columns("r/s"), columns("w/s")
    if not await_values and (read_await or write_await):
        await_values = []
        for index in range(max(len(read_await), len(write_await))):
            r_await = read_await[index] if index < len(read_await) else 0.0
            w_await = write_await[index] if index < len(write_await) else 0.0
            rps = reads[index] if index < len(reads) else 0.0
            wps = writes[index] if index < len(writes) else 0.0
            if rps + wps > 0:
                await_values.append((r_await * rps + w_await * wps) / (rps + wps))
            else:
                nonzero = [value for value in (r_await, w_await) if value > 0]
                await_values.append(safe_mean(nonzero))

    read_kb, write_kb = columns("rMB/s"), columns("wMB/s")
    read_kb = [value * 1024 for value in read_kb] if read_kb else columns("rkB/s")
    write_kb = [value * 1024 for value in write_kb] if write_kb else columns("wkB/s")
    return {
        "samples": len(rows),
        "util_avg": safe_mean(util), "util_max": max(util) if util else 0.0,
        "await_avg": safe_mean(await_values),
        "await_max": max(await_values) if await_values else 0.0,
        "r_await_avg": safe_mean(read_await), "w_await_avg": safe_mean(write_await),
        "rkB_s_avg": safe_mean(read_kb), "wkB_s_avg": safe_mean(write_kb),
        "rps_avg": safe_mean(reads), "wps_avg": safe_mean(writes),
        "aqu_sz_avg": safe_mean(columns("aqu-sz", "avgqu-sz")),
        "rrqm_avg": safe_mean(columns("%rrqm", "%rrqm/s")),
        "wrqm_avg": safe_mean(columns("%wrqm", "%wrqm/s")),
    }


def parse_vmstat_file(path, skip_samples=0):
    if not os.path.exists(path):
        return {}
    rows, header = [], None
    with open(path, "r", encoding="utf-8", errors="ignore") as file:
        for raw_line in file:
            line = raw_line.strip()
            if re.match(r"^r\s+b\s+swpd\s+", line):
                header = re.split(r"\s+", line)
            elif header and re.match(r"^\d+", line):
                parts = re.split(r"\s+", line)
                if len(parts) == len(header):
                    rows.append(dict(zip(header, parts)))
    rows = rows[skip_samples:]
    if not rows:
        return {}

    def mean(name):
        return safe_mean([safe_float(row.get(name)) for row in rows if name in row])

    return {
        "samples": len(rows), "cpu_us_avg": mean("us"), "cpu_sy_avg": mean("sy"),
        "cpu_id_avg": mean("id"), "cpu_wa_avg": mean("wa"), "cpu_st_avg": mean("st"),
        "bi_avg": mean("bi"), "bo_avg": mean("bo"), "cs_avg": mean("cs"),
    }


def build_java_cmd(config):
    return (
        f"{cfg.JAVA_BENCH_CMD} --record-size {config['record_size']} "
        f"--target-ops {config['target_ops']} --producers {config['producers']} "
        f"--use-consumer {str(config['use_consumer']).lower()} "
        f"--dynamic-topics {str(config['dynamic_topics']).lower()} "
        f"--dynamic-topic-rate {config['dynamic_topic_rate']} "
        f"--warmup-sec {config['warmup_sec']} --duration {config['duration']}"
    )


def parse_java_metrics(output):
    metrics = {
        "total_requests": 0, "avg_ms": 0.0, "p50_ms": 0.0, "p90_ms": 0.0,
        "p99_ms": 0.0, "p999_ms": 0.0, "max_ms": 0.0, "achieved_ops": 0.0,
        "achieved_pct": 0.0, "total_sent": 0, "send_errors": 0,
    }
    patterns = {
        "total_requests": (r"Total Requests\s*:\s*(\d+)", int),
        "avg_ms": (r"Average\s*:\s*([\d.]+)\s*ms", float),
        "p50_ms": (r"p50.*?:\s*([\d.]+)\s*ms", float),
        "p90_ms": (r"p90\s*:\s*([\d.]+)\s*ms", float),
        "p99_ms": (r"p99\s*:\s*([\d.]+)\s*ms", float),
        "p999_ms": (r"p999\s*:\s*([\d.]+)\s*ms", float),
        "max_ms": (r"Max\s*:\s*([\d.]+)\s*ms", float),
        "achieved_ops": (r"Achieved OP/s\s*:\s*([\d.]+)", float),
        "achieved_pct": (r"Achieved/Target.*?:\s*([\d.]+)", float),
        "total_sent": (r"Total Sent.*?:\s*(\d+)", int),
        "send_errors": (r"Send Errors\s*:\s*(\d+)", int),
    }
    for key, (pattern, converter) in patterns.items():
        match = re.search(pattern, output)
        if match:
            metrics[key] = converter(match.group(1))
    if metrics["total_requests"] == 0:
        print("[WARN] total_requests=0 — Java 출력 파싱 실패 가능성. raw output 확인 필요.")
    return metrics


def detect_bottleneck(metrics):
    util, iowait, await_avg = (
        metrics.get("util_avg", 0.0), metrics.get("cpu_wa_avg", 0.0),
        metrics.get("await_avg", 0.0),
    )
    rules = cfg.BOTTLENECK_RULES
    util_hit = util >= rules["min_disk_util_pct"]
    iowait_hit = iowait >= rules["min_cpu_iowait_pct"]
    await_hit = await_avg >= rules["min_await_ms"]
    reasons = []
    if util_hit:
        reasons.append(f"disk util avg {util:.2f}% >= {rules['min_disk_util_pct']:.2f}%")
    if iowait_hit:
        reasons.append(f"cpu iowait avg {iowait:.2f}% >= {rules['min_cpu_iowait_pct']:.2f}%")
    if await_hit:
        reasons.append(f"disk await avg {await_avg:.2f} ms >= {rules['min_await_ms']:.2f} ms")
    return {"is_bottleneck": util_hit and (await_hit or iowait_hit), "reasons": reasons}


def run_benchmark_once(fs_type, scenario_key, config, round_idx, phase_tag="measure"):
    print(
        f"\n--- [{phase_tag.upper()}] {fs_type.upper()} / {scenario_key} / "
        f"{config['record_size']}B / {config['target_ops']} ops ---"
    )
    prefix = (
        f"{cfg.MONITOR_DIR}/r{round_idx}_{fs_type}_{scenario_key}_"
        f"{config['record_size']}B_{config['target_ops']}ops_{phase_tag}"
    )
    monitors = start_monitors(prefix)
    try:
        output, return_code = run_cmd_streaming(build_java_cmd(config))
    finally:
        stop_monitors(monitors)
    if return_code != 0:
        raise RuntimeError(f"{scenario_key} failed with Java exit code {return_code}")

    skip = config.get("warmup_sec", 0) + cfg.MONITOR_LEAD_SECONDS
    metrics = parse_java_metrics(output)
    metrics.update(parse_iostat_file(monitors["paths"]["iostat"], skip))
    metrics.update(parse_vmstat_file(monitors["paths"]["vmstat"], skip))
    bottleneck = detect_bottleneck(metrics)
    raw_path = (
        f"{cfg.RAW_DIR}/r{round_idx}_{fs_type}_{scenario_key}_"
        f"{config['record_size']}B_{config['target_ops']}ops_{phase_tag}.txt"
    )
    write_text(raw_path, output)
    print(
        f"  > Req={metrics.get('total_requests', 0)} | "
        f"Achieved={metrics.get('achieved_ops', 0.0):.1f} OP/s "
        f"({metrics.get('achieved_pct', 0.0):.1f}%) | "
        f"App-Avg={metrics.get('avg_ms', 0.0):.2f}ms | "
        f"App-P99={metrics.get('p99_ms', 0.0):.2f}ms | "
        f"DiskUtil={metrics.get('util_avg', 0.0):.2f}% | "
        f"Block-await={metrics.get('await_avg', 0.0):.2f}ms | "
        f"iowait={metrics.get('cpu_wa_avg', 0.0):.2f}% | "
        f"Bottleneck={bottleneck['is_bottleneck']}"
    )
    paths = monitors["paths"]
    if cfg.COMPRESS_AFTER_RUN:
        raw_path = gzip_file(raw_path)
        paths = {name: gzip_file(path) for name, path in paths.items()}
    return {
        "config": deepcopy(config), "metrics": metrics, "bottleneck": bottleneck,
        "raw_output_path": raw_path, "monitor_files": paths,
    }
