import io
import os
import stat
import tempfile
import unittest
from unittest.mock import Mock, mock_open, patch

import system_setup


class SystemSetupTest(unittest.TestCase):
    @patch.object(system_setup, "run_cmd_quiet", return_value="host-managed")
    def test_dynamic_logical_size_excludes_gc_reserve(self, _run_cmd):
        geometry = [io.StringIO("4194304\n"), io.StringIO("16\n")]
        with patch("builtins.open", side_effect=geometry), \
                patch.object(system_setup.cfg, "DM_IMPLEMENTATION", "dynamic"), \
                patch.object(system_setup.cfg, "GC_RESERVE_ZONES", 2), \
                patch.object(system_setup.cfg, "LOGICAL_CAPACITY_PERCENT", 75):
            self.assertEqual(12 * 4194304, system_setup.zns_logical_sectors())

    @patch.object(system_setup, "run_cmd_quiet", return_value="host-managed")
    def test_fixed_logical_size_excludes_metadata_and_gc_reserve(self, _run_cmd):
        geometry = [io.StringIO("4194304\n"), io.StringIO("16\n")]
        with patch("builtins.open", side_effect=geometry), \
                patch.object(system_setup.cfg, "DM_IMPLEMENTATION", "fixed"), \
                patch.object(system_setup.cfg, "METADATA_ZONES", 6), \
                patch.object(system_setup.cfg, "GC_RESERVE_ZONES", 2), \
                patch.object(system_setup.cfg, "LOGICAL_CAPACITY_PERCENT", 75):
            self.assertEqual(8 * 4194304, system_setup.zns_logical_sectors())

    @patch.object(system_setup, "run_cmd_quiet", return_value="host-managed")
    def test_logical_capacity_percent_must_leave_overprovisioning(self, _run_cmd):
        geometry = [io.StringIO("4194304\n"), io.StringIO("16\n")]
        with patch("builtins.open", side_effect=geometry), \
                patch.object(system_setup.cfg, "LOGICAL_CAPACITY_PERCENT", 100):
            with self.assertRaisesRegex(RuntimeError, "between 1 and 99"):
                system_setup.zns_logical_sectors()

    @patch.object(system_setup.time, "sleep")
    @patch.object(system_setup, "run_cmd_quiet")
    def test_topic_retention_is_divided_across_partitions(self, run_cmd, _sleep):
        with patch.object(system_setup.cfg, "TOPIC_PARTITIONS", 8), \
             patch.object(system_setup.cfg, "RETENTION_SEGMENT_BYTES", 128 * 1024**2), \
             patch.object(system_setup.cfg, "RETENTION_SEGMENT_MS", 60000):
            system_setup.recreate_main_topic(2 * 1024**3)

        create_command = run_cmd.call_args_list[1].args[0]
        self.assertIn("retention.bytes=268435456", create_command)
        self.assertIn("cleanup.policy=delete", create_command)
        self.assertIn("segment.bytes=134217728", create_command)

    def test_stop_stale_kafka_processes_uses_only_jvm_main_classes(self):
        with patch.object(system_setup, "run_cmd_quiet") as run_cmd:
            system_setup.stop_stale_kafka_processes()

        commands = [call.args[0] for call in run_cmd.call_args_list]
        self.assertEqual([
            "pkill -9 -f 'kafka.Kafka' || true",
            "pkill -9 -f 'QuorumPeerMain' || true",
        ], commands)
        self.assertNotIn("pkill -9 -f kafka || true", commands)

    @patch.object(system_setup.os.path, "ismount", return_value=False)
    @patch.object(system_setup.subprocess, "run")
    def test_unmount_skips_missing_or_unmounted_path(self, run, _ismount):
        system_setup.unmount_log_device()
        run.assert_not_called()

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


    @patch.object(system_setup, "run_cmd_quiet", return_value="")
    @patch.object(system_setup.os, "stat")
    def test_cns_validation_accepts_unmounted_conventional_device(
            self, os_stat, _run_cmd):
        os_stat.return_value = Mock(st_mode=stat.S_IFBLK)
        with patch.object(system_setup.cfg, "FS_DEVICE", "/dev/sdb"), \
             patch.object(system_setup.cfg, "RAW_DEVICE_BASENAME", "sdb"), \
             patch("builtins.open", mock_open(read_data="none\n")):
            system_setup.validate_cns_device()

    @patch.object(system_setup, "run_cmd_quiet", return_value="/")
    @patch.object(system_setup.os, "stat")
    def test_cns_validation_rejects_device_with_mounted_child(
            self, os_stat, _run_cmd):
        os_stat.return_value = Mock(st_mode=stat.S_IFBLK)
        with patch.object(system_setup.cfg, "FS_DEVICE", "/dev/sda"), \
             patch.object(system_setup.cfg, "RAW_DEVICE_BASENAME", "sda"):
            with self.assertRaisesRegex(RuntimeError, "child device is mounted"):
                system_setup.validate_cns_device()

    @patch.object(system_setup, "run_cmd_quiet", return_value="")
    @patch.object(system_setup.os, "stat")
    def test_cns_validation_rejects_zoned_device(self, os_stat, _run_cmd):
        os_stat.return_value = Mock(st_mode=stat.S_IFBLK)
        with patch.object(system_setup.cfg, "FS_DEVICE", "/dev/nvme0n1"), \
             patch.object(system_setup.cfg, "RAW_DEVICE_BASENAME", "nvme0n1"), \
             patch("builtins.open", mock_open(read_data="host-managed\n")):
            with self.assertRaisesRegex(RuntimeError, "requires a conventional"):
                system_setup.validate_cns_device()

if __name__ == "__main__":
    unittest.main()
