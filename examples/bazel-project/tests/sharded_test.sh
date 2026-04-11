#!/usr/bin/env bash
# Exercises sharded test execution: shard_count=2 produces two TestResult
# events with different shard indices in the BES stream.
if [[ -n "${TEST_SHARD_STATUS_FILE}" ]]; then
  touch "${TEST_SHARD_STATUS_FILE}"
fi
echo "shard index: ${TEST_SHARD_INDEX:-unset}"
true
