"""TaskForge Reporter -- generates task summary reports from stdin or sample data."""

import json
import sys
from datetime import datetime, timezone


PRIORITY_NAMES = {1: "LOW", 2: "MEDIUM", 3: "HIGH", 4: "CRITICAL"}


def priority_name(p):
    """Return human-readable priority string."""
    return PRIORITY_NAMES.get(p, "UNKNOWN")


def create_sample_tasks():
    """Create sample task data for demonstration."""
    return [
        {"id": "task-001", "name": "deploy-api", "priority": 4, "status": "completed", "duration_ms": 500},
        {"id": "task-002", "name": "run-migrations", "priority": 3, "status": "completed", "duration_ms": 200},
        {"id": "task-003", "name": "send-notifications", "priority": 2, "status": "completed", "duration_ms": 100},
        {"id": "task-004", "name": "cleanup-logs", "priority": 1, "status": "completed", "duration_ms": 50},
        {"id": "task-005", "name": "flaky-job", "priority": 1, "status": "failed", "duration_ms": 50},
    ]


def generate_report(tasks):
    """Generate a summary report from a list of task result dicts."""
    total = len(tasks)
    completed = sum(1 for t in tasks if t["status"] == "completed")
    failed = sum(1 for t in tasks if t["status"] == "failed")
    total_duration = sum(t.get("duration_ms", 0) for t in tasks)
    avg_duration = total_duration / total if total > 0 else 0

    by_priority = {}
    for t in tasks:
        p = priority_name(t.get("priority", 0))
        by_priority.setdefault(p, []).append(t)

    lines = [
        "=" * 60,
        "  TaskForge Report",
        "  Generated: {}".format(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")),
        "=" * 60,
        "",
        "Summary:",
        "  Total tasks:      {}".format(total),
        "  Completed:        {}".format(completed),
        "  Failed:           {}".format(failed),
        "  Success rate:     {:.1f}%".format(completed / total * 100 if total > 0 else 0),
        "  Avg duration:     {:.0f}ms".format(avg_duration),
        "  Total duration:   {}ms".format(total_duration),
        "",
        "By Priority:",
    ]

    for pname in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        group = by_priority.get(pname, [])
        if group:
            ok = sum(1 for t in group if t["status"] == "completed")
            lines.append("  {:10s}  {} tasks, {} ok, {} failed".format(
                pname, len(group), ok, len(group) - ok))

    lines.append("")
    lines.append("Details:")
    for t in tasks:
        marker = "ok" if t["status"] == "completed" else "FAIL"
        lines.append("  [{:4s}] {:20s}  {:10s}  {}ms".format(
            marker, t["name"], priority_name(t.get("priority", 0)),
            t.get("duration_ms", 0)))

    return "\n".join(lines)


def main():
    """Entry point: reads JSON from stdin or uses sample data."""
    if not sys.stdin.isatty():
        try:
            tasks = json.load(sys.stdin)
        except json.JSONDecodeError:
            tasks = create_sample_tasks()
    else:
        tasks = create_sample_tasks()

    print(generate_report(tasks))


if __name__ == "__main__":
    main()
