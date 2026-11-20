#!/usr/bin/env bash
# bd-u6o2l: install toron's commit fence once per clone.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
git -C "$root" config core.hooksPath scripts/hooks
echo "hooks path: $(git -C "$root" config core.hooksPath)"
