#include "libs/hash/hash.h"

#include <cassert>
#include <iostream>

int main() {
  // Determinism: same input always produces same output.
  auto h1 = taskforge::Hash("task-001");
  auto h2 = taskforge::Hash("task-001");
  assert(h1 == h2);

  // Different inputs produce different hashes.
  auto h3 = taskforge::Hash("task-002");
  assert(h1 != h3);

  // Hex output is 16 characters.
  auto hex = taskforge::HashHex("task-001");
  assert(hex.size() == 16);

  // Empty string hashes to the FNV offset basis.
  assert(taskforge::Hash("") == 14695981039346656037ULL);

  std::cout << "PASSED: all hash tests" << std::endl;
  return 0;
}
