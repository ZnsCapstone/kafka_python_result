import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import system_setup


class SystemSetupTest(unittest.TestCase):
    def test_stop_stale_kafka_processes_uses_only_jvm_main_classes(self):
        with patch.object(system_setup, "run_cmd_quiet") as run_cmd:
            system_setup.stop_stale_kafka_processes()

        commands = [call.args[0] for call in run_cmd.call_args_list]
        self.assertEqual([
            "pkill -9 -f 'kafka.Kafka' || true",
            "pkill -9 -f 'QuorumPeerMain' || true",
        ], commands)
        self.assertNotIn("pkill -9 -f kafka || true", commands)

    @patch.object(system_setup.os, "statvfs")
    def test_filesystem_usage_uses_mounted_target_blocks(self, statvfs):
        statvfs.return_value = Mock(
            f_blocks=100, f_frsize=4096, f_bfree=25, f_bavail=20
        )
        usage = system_setup.filesystem_usage()
        self.assertEqual(409600, usage["total_bytes"])
        self.assertEqual(307200, usage["used_bytes"])
        self.assertAlmostEqual(78.947368, usage["used_percent"], places=5)

    def test_prefill_rejects_unsafe_occupancy(self):
        with self.assertRaises(ValueError):
            system_setup.fill_filesystem_to(81)

    @patch.object(system_setup.time, "sleep")
    @patch.object(system_setup.time, "monotonic")
    @patch.object(system_setup, "filesystem_usage")
    def test_waits_until_occupancy_returns_to_target(
            self, filesystem_usage, monotonic, _sleep):
        filesystem_usage.side_effect = [
            {"used_percent": 24.0}, {"used_percent": 23.0},
            {"used_percent": 20.4}, {"used_percent": 20.4},
            {"used_percent": 20.4}, {"used_percent": 20.4},
        ]
        monotonic.side_effect = [0, 1, 2, 3, 4, 5, 6]
        usage = system_setup.wait_for_filesystem_usage(20)
        self.assertEqual(20.4, usage["used_percent"])

    def test_existing_kraft_cluster_id_is_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = os.path.join(directory, "data")
            metadata_dir = os.path.join(directory, "metadata")
            os.makedirs(data_dir)
            os.makedirs(metadata_dir)
            for path in (
                os.path.join(data_dir, "meta.properties"),
                os.path.join(metadata_dir, "meta.properties"),
            ):
                with open(path, "w", encoding="utf-8") as file:
                    file.write("version=1\ncluster.id=stable-cluster-id\nnode.id=1\n")
            with patch.object(system_setup.cfg, "MOUNT_POINT", data_dir), \
                    patch.object(system_setup.cfg, "METADATA_DIR", metadata_dir):
                self.assertEqual(
                    "stable-cluster-id", system_setup.existing_kraft_cluster_id()
                )

    def test_existing_kraft_cluster_id_rejects_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = os.path.join(directory, "data")
            metadata_dir = os.path.join(directory, "metadata")
            os.makedirs(data_dir)
            os.makedirs(metadata_dir)
            with open(os.path.join(data_dir, "meta.properties"), "w", encoding="utf-8") as file:
                file.write("cluster.id=cluster-a\n")
            with open(os.path.join(metadata_dir, "meta.properties"), "w", encoding="utf-8") as file:
                file.write("cluster.id=cluster-b\n")
            with patch.object(system_setup.cfg, "MOUNT_POINT", data_dir), \
                    patch.object(system_setup.cfg, "METADATA_DIR", metadata_dir):
                with self.assertRaisesRegex(RuntimeError, "cluster id mismatch"):
                    system_setup.existing_kraft_cluster_id()


if __name__ == "__main__":
    unittest.main()
