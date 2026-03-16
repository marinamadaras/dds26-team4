#!/usr/bin/env bash

set -euo pipefail

# Usage:
#   bash scripts/stack_down.sh
#
# What this does:
# 1. Stops and removes the compose services.
# 2. Removes the compose network.
# 3. Removes the compose volumes so the next start is a clean slate.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${REPO_ROOT}"

docker compose down --remove-orphans --volumes

echo "Stack stopped and compose volumes removed."
