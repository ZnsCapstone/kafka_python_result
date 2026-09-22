# WIP: GC mapping overflow investigation

Run in the benchmark guest with the diagnostic dm-zns-base module, built for
the running kernel (experiment baseline: 5.15.0-186-generic). Never run against
the host's devices. The existing benchmark resets the configured FEMU device.

Build the module first:

```bash
cd ~/dm-zns-base/src
make clean
make
modinfo -p ./dm-zns-base.ko | grep gc_diag_budget
cd ~/kafka_python_result
bash run-gc-diagnostic.sh f2fs 75 0
```

This starts one filesystem with 300s warmup and 600s measurement, 20K target
as selected by benchmark 4, 128MiB/60s segments, and reserve/low/high=2/4/5.
Check the printed target rate before interpreting results. The first run
reproduces the failing F2FS capacity/discard settings. It does not run every
filesystem automatically. Checkpoint, compaction and normal GC logs remain on.

One-factor comparisons, each resetting the experiment device:

```bash
bash run-gc-diagnostic.sh f2fs 75 1  # only discard changes
bash run-gc-diagnostic.sh f2fs 65 0  # only capacity changes from baseline
DIAG_DURATION_SECONDS=3600 bash run-gc-diagnostic.sh ext4 75 0
```

Each invocation creates a unique results-gc-diag.* directory with UTC-offset
timestamps, module/JAR hashes, commits, environment, console output, kernel log
and exit status. Standard JSON/CSV/raw results remain in results/. The suite's
exit code alone does not indicate validity: inspect JSON validity.invalid_reasons.
Keep the guest boot's journal until collection finishes; do not reboot during
the test. kernel.log is collected from the journal at exit, so journal retention
or rate limiting can still lose records; check journal_exit in status.txt.

`gc_diag_budget` defaults to zero in the module and ordinary benchmark setup.
The wrapper enables eight audits per module load. On live>used, an audit sorts
the exact GC view by physical address and logs up to 16 overlapping or
outside-write-pointer references plus totals. `cached=0` means the GC cycle
built a new view; `cached=1` means it reused one. An overlap in this view is
not by itself proof of on-disk corruption. Correlate it with relocation,
checkpoint and compaction logs. Audits do not change victim selection, mappings
or reserve policy, but allocate memory and consume CPU: use these runs for
diagnosis, not published latency comparisons.

This instrumentation does not yet establish whether ext4 excess consumption
comes from Kafka offset replay, benchmark accounting, or stale storage reads.
Preserve that run's broker logs and raw benchmark output as separate evidence.
