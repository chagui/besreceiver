// TaskForge Engine -- high-performance task execution engine.
// Uses the hash library for task fingerprinting and proto for task definitions.

#include <ctime>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "libs/hash/hash.h"
#include "proto/task.pb.h"

struct EngineResult {
  std::string task_id;
  std::string fingerprint;
  bool success;
  int duration_ms;
};

EngineResult Execute(const taskforge::Task& task) {
  // Compute fingerprint from task name + priority.
  std::ostringstream oss;
  oss << task.name() << ":" << task.priority();
  std::string fingerprint = taskforge::HashHex(oss.str());

  // Simulate execution: MEDIUM and above succeed.
  bool success = (task.priority() >= taskforge::MEDIUM);
  int duration = 10 * static_cast<int>(task.priority());

  return {task.id(), fingerprint, success, duration};
}

int main() {
  auto make_task = [](const std::string& id, const std::string& name,
                      taskforge::Priority prio) {
    taskforge::Task t;
    t.set_id(id);
    t.set_name(name);
    t.set_priority(prio);
    t.set_status(taskforge::PENDING);
    t.set_created_at(std::time(nullptr));
    return t;
  };

  std::vector<taskforge::Task> tasks;
  tasks.push_back(make_task("eng-001", "compile-kernel", taskforge::CRITICAL));
  tasks.push_back(make_task("eng-002", "link-binary", taskforge::HIGH));
  tasks.push_back(make_task("eng-003", "run-sanitizer", taskforge::MEDIUM));
  tasks.push_back(make_task("eng-004", "optional-lint", taskforge::LOW));

  std::cout << "TaskForge Engine: executing " << tasks.size() << " tasks"
            << std::endl;

  int succeeded = 0;
  int failed = 0;

  for (const auto& task : tasks) {
    auto result = Execute(task);
    std::cout << "  [" << (result.success ? " OK " : "FAIL") << "] "
              << task.name() << "  fp=" << result.fingerprint << "  "
              << result.duration_ms << "ms" << std::endl;
    if (result.success) {
      ++succeeded;
    } else {
      ++failed;
    }
  }

  std::cout << "Engine done: " << succeeded << " succeeded, " << failed
            << " failed" << std::endl;
  return failed > 0 ? 1 : 0;
}
