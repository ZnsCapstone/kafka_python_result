import unittest

import bench_runner
import settings as cfg


class BenchRunnerTest(unittest.TestCase):
    def setUp(self):
        self.profile = cfg.ACTIVE_PROFILE
        self.scenario_group = cfg.ACTIVE_SCENARIO_GROUP
        self.scenario_keys = cfg.SCENARIO_KEYS
        self.workload_mode = cfg.ACTIVE_WORKLOAD_MODE

    def tearDown(self):
        cfg.configure_profile(self.profile)
        cfg.ACTIVE_SCENARIO_GROUP = self.scenario_group
        cfg.SCENARIO_KEYS = self.scenario_keys
        cfg.ACTIVE_WORKLOAD_MODE = self.workload_mode

    def test_parse_arguments_accepts_long_mode(self):
        rounds = bench_runner.parse_arguments(
            ["bench_final.py", "1", "2", "latency", "baseline", "long"]
        )
        self.assertEqual(2, rounds)
        self.assertEqual("long", cfg.ACTIVE_WORKLOAD_MODE)
        self.assertEqual(["scenario_a", "scenario_b"], cfg.SCENARIO_KEYS)

    def test_parse_arguments_rejects_unknown_mode(self):
        with self.assertRaises(ValueError):
            bench_runner.parse_arguments(
                ["bench_final.py", "1", "1", "latency", "baseline", "overnight"]
            )


if __name__ == "__main__":
    unittest.main()
