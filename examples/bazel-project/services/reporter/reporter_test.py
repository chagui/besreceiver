"""Tests for the TaskForge reporter."""

import unittest

from services.reporter import reporter


class ReporterTest(unittest.TestCase):

    def test_generate_report_basic(self):
        tasks = [
            {"id": "t1", "name": "job-a", "priority": 3, "status": "completed", "duration_ms": 100},
            {"id": "t2", "name": "job-b", "priority": 1, "status": "failed", "duration_ms": 50},
        ]
        report = reporter.generate_report(tasks)
        self.assertIn("TaskForge Report", report)
        self.assertIn("Total tasks:      2", report)
        self.assertIn("Completed:        1", report)
        self.assertIn("Failed:           1", report)
        self.assertIn("50.0%", report)

    def test_generate_report_empty(self):
        report = reporter.generate_report([])
        self.assertIn("Total tasks:      0", report)

    def test_priority_name(self):
        self.assertEqual(reporter.priority_name(4), "CRITICAL")
        self.assertEqual(reporter.priority_name(1), "LOW")
        self.assertEqual(reporter.priority_name(99), "UNKNOWN")

    def test_sample_tasks_structure(self):
        tasks = reporter.create_sample_tasks()
        self.assertEqual(len(tasks), 5)
        for t in tasks:
            self.assertIn("id", t)
            self.assertIn("name", t)
            self.assertIn("priority", t)
            self.assertIn("status", t)


if __name__ == "__main__":
    unittest.main()
