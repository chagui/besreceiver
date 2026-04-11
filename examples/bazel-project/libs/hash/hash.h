#ifndef TASKFORGE_LIBS_HASH_HASH_H_
#define TASKFORGE_LIBS_HASH_HASH_H_

#include <cstdint>
#include <string>

namespace taskforge {

// FNV-1a 64-bit hash. Deterministic, fast, no external deps.
uint64_t Hash(const std::string& input);

// Returns hex-encoded hash string (16 characters).
std::string HashHex(const std::string& input);

}  // namespace taskforge

#endif  // TASKFORGE_LIBS_HASH_HASH_H_
