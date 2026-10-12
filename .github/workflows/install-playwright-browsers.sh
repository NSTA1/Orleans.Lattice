#!/usr/bin/env bash
set -euo pipefail

cache_hit="${1:?Expected the browser cache-hit flag}"
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

install_with_retry() {
  local attempt code
  for attempt in 1 2 3; do
    echo "Playwright $*: attempt $attempt/3 (120-second limit)."
    if timeout --kill-after=10s 120s pwsh "$script" "$@"; then
      return 0
    else
      code=$?
    fi
    echo "::warning::Playwright $* attempt $attempt/3 failed with exit code $code."
    if [ "$attempt" -lt 3 ]; then
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
