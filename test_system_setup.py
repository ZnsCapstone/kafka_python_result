import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
