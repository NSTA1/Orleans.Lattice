#!/usr/bin/env bash
set -euo pipefail

# actions/cache emits an empty string on a miss.
cache_hit="${1?Expected the browser cache-hit flag}"
shift
if [ "$#" -eq 0 ]; then
  echo "::error::No Playwright browsers requested."
  exit 1
fi
for browser in "$@"; do
  case "$browser" in
    chromium|firefox|webkit) ;;
    *) echo "::error::Unknown Playwright browser: $browser"; exit 1 ;;
  esac
done

script="$(find test/lattice.explorer.uitests/bin/Release -name playwright.ps1 -print -quit)"
if [ -z "$script" ]; then
  echo "::error::playwright.ps1 not found under the UI build output."
  exit 1
fi
runner="$(dirname "$(realpath "${BASH_SOURCE[0]}")")/run-playwright-install.py"
pwsh="$(command -v pwsh)"

install_with_retry() {
  local attempt code mirror_step=0
  local -a command
  for attempt in 1 2 3; do
    command=(python3 "$runner" 120 10)
    if [ "$mirror_step" -gt 0 ]; then
      command+=(--apt-mirror-step "$mirror_step")
    fi
    command+=("$pwsh" "$script" "$@")
    # Elevate the supervisor too, so its signals can reach apt and dpkg.
    # Downloads stay unprivileged to preserve the caller's browser cache.
    if [ "$1" = install-deps ]; then
      command=(sudo --non-interactive -- "${command[@]}")
    fi
    echo "Playwright $*: attempt $attempt/3 (120-second limit)."
    if "${command[@]}"; then
      return 0
    else
      code=$?
    fi
    echo "::warning::Playwright $* attempt $attempt/3 failed with exit code $code."
    if [ "$attempt" -lt 3 ]; then
      if [ "$1" = install-deps ] && { [ "$code" -eq 124 ] || [ "$code" -eq 137 ]; }; then
        mirror_step=$((mirror_step + 1))
      fi
      sleep 5
    fi
  done
  echo "::error::Playwright $* failed after three bounded attempts."
  return "$code"
}

install_with_retry install-deps "$@"
if [ "$cache_hit" != "true" ]; then
  install_with_retry install "$@"
fi
