"""JSON, CSV, and human-readable benchmark result output."""

import csv
import datetime
import json

import settings as cfg
from bench_utils import safe_cv, safe_mean, safe_stdev, write_text


def flatten_result_rows(all_results):
    rows = []
    for fs_type, fs_payload in all_results.items():
        for record_size, size_payload in fs_payload.items():
            for scenario, scenario_payload in size_payload.items():
                for entry in scenario_payload["rounds"]:
                    metrics, bottleneck = entry["metrics"], entry["bottleneck"]
                    rows.append({
                        "filesystem": fs_type, "record_size": record_size,
                        "scenario": scenario, "round": entry["round"],
                        "target_ops": entry["config"]["target_ops"],
                        "measure_sec": entry["config"]["measure_sec"],
                        "drain_timeout_sec": entry["config"]["drain_timeout_sec"],
                        "warmup_sec": entry["config"]["warmup_sec"],
                        "producers": entry["config"]["producers"],
                        "use_consumer": entry["config"]["use_consumer"],
                        "dynamic_topics": entry["config"]["dynamic_topics"],
                        "dynamic_topic_rate": entry["config"]["dynamic_topic_rate"],
                        "total_requests": metrics.get("total_requests", 0),
                        "achieved_ops": metrics.get("achieved_ops", 0.0),
                        "achieved_pct": metrics.get("achieved_pct", 0.0),
                        "total_sent": metrics.get("total_sent", 0),
                        "send_errors": metrics.get("send_errors", 0),
                        "drain_time_sec": metrics.get("drain_time_sec", 0.0),
                        "drain_completed": metrics.get("drain_completed", False),
                        "app_avg_ms": metrics.get("avg_ms", 0.0),
                        "app_p50_ms": metrics.get("p50_ms", 0.0),
                        "app_p90_ms": metrics.get("p90_ms", 0.0),
                        "app_p99_ms": metrics.get("p99_ms", 0.0),
                        "app_p999_ms": metrics.get("p999_ms", 0.0),
                        "app_max_ms": metrics.get("max_ms", 0.0),
                        "block_util_avg": metrics.get("util_avg", 0.0),
                        "block_util_max": metrics.get("util_max", 0.0),
                        "block_await_avg": metrics.get("await_avg", 0.0),
                        "block_await_max": metrics.get("await_max", 0.0),
                        "block_r_await_avg": metrics.get("r_await_avg", 0.0),
                        "block_w_await_avg": metrics.get("w_await_avg", 0.0),
                        "block_rkB_s_avg": metrics.get("rkB_s_avg", 0.0),
                        "block_wkB_s_avg": metrics.get("wkB_s_avg", 0.0),
                        "cpu_us_avg": metrics.get("cpu_us_avg", 0.0),
                        "cpu_sy_avg": metrics.get("cpu_sy_avg", 0.0),
                        "cpu_id_avg": metrics.get("cpu_id_avg", 0.0),
                        "cpu_wa_avg": metrics.get("cpu_wa_avg", 0.0),
                        "bottleneck": bottleneck.get("is_bottleneck", False),
                        "bottleneck_reasons": " | ".join(bottleneck.get("reasons", [])),
                        "raw_output_path": entry["raw_output_path"],
                        "iostat_path": entry["monitor_files"]["iostat"],
                        "vmstat_path": entry["monitor_files"]["vmstat"],
                    })
    return rows


def save_csv_reports(all_results):
    rows = flatten_result_rows(all_results)
    full_path = f"{cfg.CSV_DIR}/full_results.csv"
    if rows:
        with open(full_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    grouped = {}
    for row in rows:
        grouped.setdefault(
            (row["filesystem"], row["record_size"], row["scenario"]), []
        ).append(row)
    summaries = []
    for (fs_type, size, scenario), items in grouped.items():
        values = lambda key: [item[key] for item in items]
        summaries.append({
            "filesystem": fs_type, "record_size": size, "scenario": scenario,
            "rounds": len(items),
            "target_ops_mean": safe_mean(values("target_ops")),
            "achieved_ops_mean": safe_mean(values("achieved_ops")),
            "achieved_pct_mean": safe_mean(values("achieved_pct")),
            "send_errors_mean": safe_mean(values("send_errors")),
            "drain_time_sec_mean": safe_mean(values("drain_time_sec")),
            "drain_completed_rounds": sum(item["drain_completed"] for item in items),
            "requests_mean": safe_mean(values("total_requests")),
            "app_avg_mean": safe_mean(values("app_avg_ms")),
            "app_avg_stdev": safe_stdev(values("app_avg_ms")),
            "app_avg_cv_pct": safe_cv(values("app_avg_ms")),
            "app_p50_mean": safe_mean(values("app_p50_ms")),
            "app_p90_mean": safe_mean(values("app_p90_ms")),
            "app_p99_mean": safe_mean(values("app_p99_ms")),
            "app_p99_stdev": safe_stdev(values("app_p99_ms")),
            "app_p99_cv_pct": safe_cv(values("app_p99_ms")),
            "app_max_mean": safe_mean(values("app_max_ms")),
            "block_util_mean": safe_mean(values("block_util_avg")),
            "block_await_mean": safe_mean(values("block_await_avg")),
            "block_await_stdev": safe_stdev(values("block_await_avg")),
            "block_await_cv_pct": safe_cv(values("block_await_avg")),
            "block_wkB_s_mean": safe_mean(values("block_wkB_s_avg")),
            "cpu_iowait_mean": safe_mean(values("cpu_wa_avg")),
            "bottleneck_rounds": sum(item["bottleneck"] for item in items),
        })
    summary_path = f"{cfg.CSV_DIR}/summary_by_fs_recordsize_scenario.csv"
    if summaries:
        with open(summary_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(summaries[0]))
            writer.writeheader()
            writer.writerows(summaries)
    print(f"[Success] CSV saved: {full_path}")
    print(f"[Success] CSV saved: {summary_path}")


def save_json_snapshot(all_results):
    path = f"{cfg.RESULT_DIR}/results_snapshot.json"
    with open(path, "w", encoding="utf-8") as file:
        json.dump(all_results, file, indent=2, ensure_ascii=False)
    print(f"[Success] JSON snapshot saved: {path}")


def save_summary_report(all_results, current_round, final=False):
    lines = [
        "============================================================",
        "  KAFKA FILESYSTEM PERFORMANCE EXPERIMENT REPORT (M1-#1)",
        "============================================================",
        f"Start Time : {cfg.PROGRAM_START_TIME}",
        f"Current Round Completed : {current_round}",
    ]
    if final:
        lines.append(f"End Time   : {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    lines.extend([
        "", "Latency Layers",
        "  - App-level   : producer.send → broker ACK (Kafka client latency)",
        "  - Block-level : iostat await (block queue entry → completion)", "",
        "Experiment Scope",
        "- Kafka 4.2 (KRaft single broker, replication=1)",
        f"- metadata.log.dir separated: {cfg.SEPARATE_METADATA_DIR}",
        f"- Filesystems: {', '.join(cfg.FILESYSTEMS)} on {cfg.FS_DEVICE}",
        f"- DM implementation: {cfg.DM_IMPLEMENTATION_LABELS[cfg.DM_IMPLEMENTATION]}",
        f"- Raw ZNS: {cfg.RAW_ZNS_DEVICE}",
        f"- Record sizes: {cfg.RECORD_SIZES}",
        f"- Dynamic topic rate (/sec): {cfg.TOPIC_RATE}",
        f"- Measure: {cfg.MEASURE_DURATION}s (warmup {cfg.WARMUP_SECONDS}s)",
        f"- Drain timeout: {cfg.DRAIN_TIMEOUT_SECONDS}s",
        f"- FIXED_OPS_BY_RECORD_SIZE: {cfg.FIXED_OPS_BY_RECORD_SIZE}", "",
    ])
    for fs_type in cfg.FILESYSTEMS:
        lines.append(f"################ FILESYSTEM: {fs_type.upper()} ################")
        for size in cfg.RECORD_SIZES:
            lines.append(f"\n[Record Size: {size} Bytes]")
            for scenario in cfg.SCENARIO_KEYS:
                rounds = all_results.get(fs_type, {}).get(size, {}).get(scenario, {}).get("rounds", [])
                if not rounds:
                    lines.append(f"  - {scenario}: no data")
                    continue
                metric = lambda key: [entry["metrics"].get(key, 0.0) for entry in rounds]
                bottlenecks = sum(entry["bottleneck"].get("is_bottleneck", False) for entry in rounds)
                lines.extend([
                    f"  [{scenario}] rounds: {len(rounds)}",
                    f"    target/achieved ops : {safe_mean([e['config']['target_ops'] for e in rounds]):.2f} / {safe_mean(metric('achieved_ops')):.2f}",
                    f"    achieved/target     : {safe_mean(metric('achieved_pct')):.2f} %",
                    f"    requests/errors     : {safe_mean(metric('total_requests')):.2f} / {safe_mean(metric('send_errors')):.2f}",
                    f"    drain time/completed: {safe_mean(metric('drain_time_sec')):.2f} sec / {sum(metric('drain_completed'))}/{len(rounds)}",
                    f"    app avg / stdev     : {safe_mean(metric('avg_ms')):.2f} / {safe_stdev(metric('avg_ms')):.2f} ms (CV={safe_cv(metric('avg_ms')):.1f}%)",
                    f"    app p50 / p90       : {safe_mean(metric('p50_ms')):.2f} / {safe_mean(metric('p90_ms')):.2f} ms",
                    f"    app p99 / stdev     : {safe_mean(metric('p99_ms')):.2f} / {safe_stdev(metric('p99_ms')):.2f} ms (CV={safe_cv(metric('p99_ms')):.1f}%)",
                    f"    app max             : {safe_mean(metric('max_ms')):.2f} ms",
                    f"    block util / await  : {safe_mean(metric('util_avg')):.2f} % / {safe_mean(metric('await_avg')):.2f} ms",
                    f"    block write         : {safe_mean(metric('wkB_s_avg')):.2f} kB/s",
                    f"    cpu iowait          : {safe_mean(metric('cpu_wa_avg')):.2f} %",
                    f"    bottleneck rounds   : {bottlenecks}/{len(rounds)}",
                ])
        lines.append("")
    path = f"{cfg.RESULT_DIR}/summary_report.txt"
    write_text(path, "\n".join(lines))
    print(f"[Success] Summary report saved: {path}")
