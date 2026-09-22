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

## ext4 excess-consumption diagnosis

Build the accompanying Kafka-benchmark WIP before running:

```bash
cd ~/Kafka-benchmark
./gradlew test jar
cd ~/kafka_python_result
DIAG_DURATION_SECONDS=3600 bash run-gc-diagnostic.sh ext4 75 0
```

The wrapper exports BENCH_INTEGRITY_DIAG=1. Confirm `[IntegrityDiag] run=...`
appears before measurement; an old JAR silently ignores the environment variable.
Every produced record carries a run UUID header. Consumer diagnostics retain
partition offset and producer sequence high-water marks, print at most 64
anomalous records with UTC timestamps, and print uncapped aggregate counts.
Normal runs without the environment variable do not add headers or audit records.

- offset_back: an offset at/below the partition high water was delivered again.
- sequence_back with increasing offsets: sequence reuse or reordering at new
  offsets; it is not by itself proof that the storage layer duplicated data.
- foreign_run/missing_run: records not identified as belonging to this run.
  These records do not update the current run's sequence high water.

High-water checks are not an exact unique-record set and cannot distinguish
every delayed record from a duplicate. Existing validity counters are unchanged.
The extra header changes record overhead, so diagnostic throughput is not directly
comparable with the uninstrumented baseline.

At exit the wrapper also copies the broker logs directory, including rotations,
to broker-logs/ (potentially large, root-owned files). It can include earlier
runs; correlate timestamps. Check broker_copy_exit in status.txt. Archive rotation
can still discard old events during long runs. A missing diagnostic start marker,
failed collection, or absent logs must not be treated as evidence of no anomaly.
