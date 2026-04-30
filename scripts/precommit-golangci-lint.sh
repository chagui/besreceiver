#!/usr/bin/env bash
# Run golangci-lint on the unique package directories of staged Go files.
# Invoked by .pre-commit-config.yaml (fast-path); CI runs lint over the whole module.
set -euo pipefail

if [ "$#" -eq 0 ]; then
  exit 0
fi

# Reduce staged file paths to unique package directories (./pkg/foo).
# bash 3.2 compatible (macOS default) — no mapfile.
pkgs=()
while IFS= read -r dir; do
  case "$dir" in
    .) pkgs+=(".") ;;
    *) pkgs+=("./$dir") ;;
  esac
done < <(printf '%s\n' "$@" | xargs -n1 dirname | sort -u)

exec golangci-lint run "${pkgs[@]}"
