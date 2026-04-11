#include "libs/hash/hash.h"

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>

namespace taskforge {

uint64_t Hash(const std::string& input) {
  constexpr uint64_t kOffsetBasis = 14695981039346656037ULL;
  constexpr uint64_t kPrime = 1099511628211ULL;

  uint64_t hash = kOffsetBasis;
  for (unsigned char c : input) {
    hash ^= static_cast<uint64_t>(c);
    hash *= kPrime;
  }
  return hash;
}

std::string HashHex(const std::string& input) {
  std::ostringstream oss;
  oss << std::hex << std::setfill('0') << std::setw(16) << Hash(input);
  return oss.str();
}

}  // namespace taskforge
