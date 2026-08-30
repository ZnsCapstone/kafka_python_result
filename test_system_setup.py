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


if __name__ == "__main__":
    unittest.main()
