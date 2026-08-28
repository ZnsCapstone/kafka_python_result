"""Top-level experiment orchestration."""

import sys
from copy import deepcopy

import settings as cfg
from bench_logging import TeeLogger
from bench_utils import run_cmd_quiet
from performance import run_benchmark_once
from reporting import save_csv_reports, save_json_snapshot, save_summary_report
from system_setup import (
    capture_environment, control_kafka, prepare_experiment_kraft_config,
    recreate_main_topic, remove_dm_stack, setup_filesystem,
    unmount_log_device, validate_kafka_environment,
)


def init_result_structure():
    return {
        fs: {size: {scenario: {"rounds": []} for scenario in cfg.SCENARIO_KEYS}
             for size in cfg.RECORD_SIZES}
        for fs in cfg.FILESYSTEMS
    }


def parse_arguments(argv):
    if len(argv) < 2 or len(argv) > 3:
        raise ValueError("Usage: python3 bench_final.py <0=fixed|1=dynamic> [rounds]")
    cfg.configure_dm_implementation(argv[1])
    rounds = int(argv[2]) if len(argv) == 3 else cfg.DEFAULT_ROUNDS
    if rounds < 1:
        raise ValueError("rounds must be positive")
    return rounds


def persist_results(results, round_number, final=False):
    save_json_snapshot(results)
    save_csv_reports(results)
    save_summary_report(results, round_number, final=final)


def run(argv=None):
    argv = sys.argv if argv is None else argv
    try:
        rounds = parse_arguments(argv)
    except (ValueError, TypeError) as exc:
        print(f"[CRITICAL ERROR] {exc}")
        return 2

    cfg.initialize_result_directories()
    print(f"[Config] DM implementation: {cfg.DM_IMPLEMENTATION_LABELS[cfg.DM_IMPLEMENTATION]}")
    validate_kafka_environment()
    prepare_experiment_kraft_config()
    capture_environment()
    results = init_result_structure()
    completed = False
    try:
        for record_size in cfg.RECORD_SIZES:
            if record_size not in cfg.FIXED_OPS_BY_RECORD_SIZE:
                raise ValueError(f"FIXED_OPS_BY_RECORD_SIZE에 {record_size} 값이 없습니다.")
            for round_number in range(1, rounds + 1):
                log_path = f"{cfg.ROUND_DIR}/size_{record_size}_round_{round_number}.txt"
                logger, original_stdout = TeeLogger(log_path), sys.stdout
                sys.stdout = logger
                try:
                    print(f"\n{'=' * 80}\n RECORD SIZE {record_size} / ROUND {round_number}/{rounds} START \n{'=' * 80}")
                    for fs_type in cfg.FILESYSTEMS:
                        print(f"\n{'#' * 70}\n# FILESYSTEM = {fs_type.upper()}\n{'#' * 70}")
                        print(f"[Record Size] {record_size} Bytes")
                        for scenario_key in cfg.SCENARIO_KEYS:
                            scenario = deepcopy(cfg.SCENARIO_TEMPLATES[scenario_key])
                            chosen_ops = cfg.FIXED_OPS_BY_RECORD_SIZE[record_size]
                            print(f"[Fixed OP/s] {fs_type.upper()} / {scenario_key} / {record_size}B -> target_ops={chosen_ops}")
                            control_kafka("stop")
                            setup_filesystem(fs_type)
                            control_kafka("start")
                            recreate_main_topic()
                            measured = run_benchmark_once(
                                fs_type, scenario_key,
                                {
                                    "record_size": record_size, "target_ops": chosen_ops,
                                    "producers": scenario["producers"],
                                    "use_consumer": scenario["use_consumer"],
                                    "dynamic_topics": scenario["dynamic_topics"],
                                    "dynamic_topic_rate": cfg.TOPIC_RATE,
                                    "warmup_sec": cfg.WARMUP_SECONDS,
                                    "duration": cfg.MEASURE_DURATION,
                                },
                                round_number,
                            )
                            measured["round"] = round_number
                            measured["calibration_reference"] = {
                                "mode": "fixed", "chosen_ops": chosen_ops,
                            }
                            results[fs_type][record_size][scenario_key]["rounds"].append(measured)
                            persist_results(results, round_number)
                except Exception as exc:
                    original_stdout.write(
                        f"\n[CRITICAL ERROR] Record Size {record_size} Round {round_number} failed: {exc}\n"
                    )
                    raise
                finally:
                    sys.stdout = original_stdout
                    logger.close()
                    print(f"[Done] Record Size {record_size} Round {round_number} log saved: {log_path}")
        persist_results(results, rounds, final=True)
        completed = True
        return 0
    finally:
        control_kafka("stop")
        unmount_log_device(strict=False)
        remove_dm_stack(strict=False)
        run_cmd_quiet("sudo sync")
        if completed:
            print(f"\n{'=' * 80}\n[Finish] All {rounds} rounds completed.")
            print(f"Check '{cfg.RESULT_DIR}' for reports.\n{'=' * 80}")
        else:
            print("[Finish] Benchmark stopped before completion; partial results were retained.")
