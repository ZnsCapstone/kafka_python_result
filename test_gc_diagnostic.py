"""Exercise diagnostic collection with fake commands; never opens a block device."""
import datetime
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class DiagnosticCollectionTest(unittest.TestCase):
    def test_wrapper_uses_legacy_journal_timestamp_and_collects_logs(self):
        with tempfile.TemporaryDirectory(prefix="gc-diagnostic-test-") as directory:
            root = Path(directory)
            binary = root / "bin"
            binary.mkdir()
            for name, body in {
                "sudo": 'if [ "$1" = -v ]; then exit 0; fi\nexec "$@"\n',
                "modinfo": "echo 'gc_diag_budget:diagnostic budget'\n",
                "git": "echo fake-commit\n",
                "journalctl": 'printf "%s\\n" "$@"\n',
            }.items():
                path = binary / name
                path.write_text("#!/bin/sh\n" + body)
                path.chmod(0o755)
            shutil.copyfile(Path(__file__).with_name("run-gc-diagnostic.sh"),
                            root / "run-gc-diagnostic.sh")
            (root / "run-benchmark.sh").write_text("#!/bin/sh\necho fake-benchmark\n")
            for name in ("dm-zns-base/src/dm-zns-base.ko",
                         "Kafka-benchmark/build/libs/kafka-benchmark-1.0.jar",
                         "kafka-4.2.0-src/logs/server.log"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("test fixture\n")
            subprocess.run(["bash", str(root / "run-gc-diagnostic.sh"), "ext4", "75", "0"],
                           env={"PATH": f"{binary}:{os.environ['PATH']}", "HOME": str(root),
                                "DIAG_DURATION_SECONDS": "1"},
                           check=True, capture_output=True, text=True)
            folders = list(root.glob("results-gc-diag.*"))
            self.assertEqual(len(folders), 1)
            result = folders[0]
            args = (result / "kernel.log").read_text().splitlines()
            for option in ("--since", "--until"):
                timestamp = args[args.index(option) + 1]
                datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S UTC")
            self.assertEqual((result / "status.txt").read_text().strip(),
                             "benchmark_exit=0 journal_exit=0 broker_copy_exit=0")
            self.assertTrue((result / "broker-logs/server.log").is_file())


if __name__ == "__main__":
    unittest.main()
