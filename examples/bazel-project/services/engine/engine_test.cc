#include <cassert>
#include <iostream>
#include <string>

#include "libs/hash/hash.h"
#include "proto/task.pb.h"

int main() {
  // Proto round-trip.
  taskforge::Task task;
  task.set_id("test-001");
  task.set_name("unit-test");
  task.set_priority(taskforge::HIGH);
  task.set_status(taskforge::PENDING);

  std::string serialized;
  assert(task.SerializeToString(&serialized));

  taskforge::Task deserialized;
  assert(deserialized.ParseFromString(serialized));
  assert(deserialized.id() == "test-001");
  assert(deserialized.name() == "unit-test");
  assert(deserialized.priority() == taskforge::HIGH);

  // Fingerprint determinism.
  std::string fp1 = taskforge::HashHex("unit-test:3");
  std::string fp2 = taskforge::HashHex("unit-test:3");
  assert(fp1 == fp2);
  assert(fp1.size() == 16);

  // Different inputs produce different fingerprints.
  std::string fp3 = taskforge::HashHex("other-test:1");
  assert(fp1 != fp3);

  // Labels map.
  (*task.mutable_labels())["env"] = "prod";
  assert(task.labels().at("env") == "prod");

  std::cout << "PASSED: all engine tests" << std::endl;
  return 0;
}
