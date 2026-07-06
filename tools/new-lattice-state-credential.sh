#!/usr/bin/env bash
#
# new-lattice-state-credential.sh
#
# Generates a Lattice state-API credential hash (PBKDF2-HMAC-SHA256) in the
# self-describing encoding consumed by the server-side EnvVarCredentialAuthorizer:
#
#     pbkdf2-sha256$<iterations>$<base64-salt>$<base64-derived-key>
#
# The hash is published to the server process through an environment variable
# named LATTICE_STATE_USER_<username>. The username lives in the variable name;
# the encoded hash is the value. This script never transmits or stores the
# plaintext password: it is read from a no-echo prompt, from stdin, or from a
# named environment variable, hashed in-process via openssl, and only the hash
# is written to stdout. All diagnostics go to stderr so that the 'value' and
# 'json' formats pipe cleanly.
#
# This is the POSIX-shell counterpart of tools/New-LatticeStateCredential.ps1.
# For an identical salt, password, and iteration count, both scripts emit the
# byte-identical encoding.
#
# Exit codes:
#   0  success
#   2  bad input (missing/invalid username, no password source, bad arguments)
#   3  password policy rejected
#
# Dependencies: bash, openssl (>= 3.0 for `openssl kdf`), and coreutils
# (od, tr). No third-party modules.

set -euo pipefail

PROG="$(basename "$0")"

err() {
    printf '%s\n' "$*" >&2
}

die() {
    local code="$1"
    shift
    err "error: $*"
    exit "$code"
}

usage() {
    cat >&2 <<USAGE
Usage: $PROG --username <name> [options]

Options:
  -u, --username <name>     Credential username (required; env-var-name-safe).
      --password-stdin      Read the password from stdin (one line).
      --password-env <name> Read the password from the named environment variable.
  -i, --iterations <n>      PBKDF2 iteration count (default 210000).
  -f, --format <fmt>        Output format: env (default), dotenv, export, value, json.
      --allow-weak-password Bypass the password-strength policy (discouraged).
  -h, --help                Show this help.

With no password source the password is read from a no-echo prompt.
USAGE
}

USERNAME=""
PASSWORD_STDIN=0
PASSWORD_ENV=""
ITERATIONS=210000
FORMAT="env"
ALLOW_WEAK=0

while [ "$#" -gt 0 ]; do
    case "$1" in
        -u|--username)
            [ "$#" -ge 2 ] || die 2 "missing value for $1"
            USERNAME="$2"; shift 2 ;;
        --username=*) USERNAME="${1#*=}"; shift ;;
        --password-stdin) PASSWORD_STDIN=1; shift ;;
        --password-env)
            [ "$#" -ge 2 ] || die 2 "missing value for $1"
            PASSWORD_ENV="$2"; shift 2 ;;
        --password-env=*) PASSWORD_ENV="${1#*=}"; shift ;;
        -i|--iterations)
            [ "$#" -ge 2 ] || die 2 "missing value for $1"
            ITERATIONS="$2"; shift 2 ;;
        --iterations=*) ITERATIONS="${1#*=}"; shift ;;
        -f|--format)
            [ "$#" -ge 2 ] || die 2 "missing value for $1"
            FORMAT="$2"; shift 2 ;;
        --format=*) FORMAT="${1#*=}"; shift ;;
        --allow-weak-password) ALLOW_WEAK=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die 2 "unknown argument: $1" ;;
    esac
done

# --- Validate username charset (must be env-var-name-safe) --------------------
[ -n "$USERNAME" ] || { usage; die 2 "username is required."; }
case "$USERNAME" in
    [A-Za-z_]*) : ;;
    *) die 2 "username '$USERNAME' is not a valid environment-variable name segment." ;;
esac
if printf '%s' "$USERNAME" | grep -q '[^A-Za-z0-9_]'; then
    die 2 "username '$USERNAME' is not a valid environment-variable name segment (allowed: letters, digits, underscore; must not start with a digit)."
fi

case "$FORMAT" in
    env|dotenv|export|value|json) : ;;
    *) die 2 "unknown format '$FORMAT' (expected env, dotenv, export, value, or json)." ;;
esac

case "$ITERATIONS" in
    ''|*[!0-9]*) die 2 "iterations must be a positive integer." ;;
esac
[ "$ITERATIONS" -ge 1 ] || die 2 "iterations must be a positive integer."

command -v openssl >/dev/null 2>&1 || die 2 "openssl is required but was not found on PATH."

# --- Resolve the plaintext password without echoing it ------------------------
if [ "$PASSWORD_STDIN" -eq 1 ] && [ -n "$PASSWORD_ENV" ]; then
    die 2 "specify only one password source (--password-stdin or --password-env)."
fi

PASSWORD=""
if [ "$PASSWORD_STDIN" -eq 1 ]; then
    IFS= read -r PASSWORD || PASSWORD=""
elif [ -n "$PASSWORD_ENV" ]; then
    # Validate the variable NAME before dereferencing it. The value used to be
    # read with `eval "PASSWORD=\${$PASSWORD_ENV-...}"`, which evaluates the
    # supplied name as shell source: a crafted name such as 'X-$(command)'
    # would execute arbitrary commands (command injection). Reject anything
    # that is not an env-var-name-safe token, then dereference with bash
    # indirect expansion (${!name}), which only reads the named variable and
    # never evaluates its contents as code.
    case "$PASSWORD_ENV" in
        [A-Za-z_]*) : ;;
        *) die 2 "--password-env name '$PASSWORD_ENV' is not a valid environment-variable name." ;;
    esac
    if printf '%s' "$PASSWORD_ENV" | grep -q '[^A-Za-z0-9_]'; then
        die 2 "--password-env name '$PASSWORD_ENV' is not a valid environment-variable name (allowed: letters, digits, underscore; must not start with a digit)."
    fi
    PASSWORD="${!PASSWORD_ENV-__LATTICE_UNSET__}"
    [ "$PASSWORD" != "__LATTICE_UNSET__" ] || die 2 "environment variable '$PASSWORD_ENV' is not set."
else
    printf 'Password for %s: ' "$USERNAME" >&2
    stty -echo 2>/dev/null || true
    IFS= read -r PASSWORD || PASSWORD=""
    stty echo 2>/dev/null || true
    printf '\n' >&2
fi

# --- Enforce the password policy before hashing -------------------------------
if [ "$ALLOW_WEAK" -ne 1 ]; then
    policy_ok=1
    [ "${#PASSWORD}" -ge 8 ] || policy_ok=0
    printf '%s' "$PASSWORD" | grep -q '[A-Z]' || policy_ok=0
    printf '%s' "$PASSWORD" | grep -q '[a-z]' || policy_ok=0
    printf '%s' "$PASSWORD" | grep -q '[0-9]' || policy_ok=0
    if [ "$policy_ok" -ne 1 ]; then
        die 3 "password does not satisfy policy: minimum 8 characters with at least one uppercase letter, one lowercase letter, and one digit. Use --allow-weak-password to override (discouraged)."
    fi
fi

# --- Generate (or accept an injected) salt ------------------------------------
# LATTICE_CRED_SALT_B64 forces a deterministic salt; it exists ONLY so the
# cross-shell parity test can compare this script against the PowerShell
# counterpart. Never set it in production.
if [ -n "${LATTICE_CRED_SALT_B64-}" ]; then
    SALT_B64="$LATTICE_CRED_SALT_B64"
    if ! printf '%s' "$SALT_B64" | openssl base64 -d -A >/dev/null 2>&1; then
        die 2 "LATTICE_CRED_SALT_B64 is not valid base64."
    fi
    salt_len=$(printf '%s' "$SALT_B64" | openssl base64 -d -A | wc -c)
    [ "$salt_len" -ge 16 ] || die 2 "LATTICE_CRED_SALT_B64 must decode to at least 16 bytes."
else
    SALT_B64="$(openssl rand -base64 16)"
fi

# Derive the hex salt that `openssl kdf` requires from the base64 salt.
SALT_HEX="$(printf '%s' "$SALT_B64" | openssl base64 -d -A | od -An -v -tx1 | tr -d ' \n')"

# --- Derive the key (PBKDF2-HMAC-SHA256, 32-byte output) ----------------------
DERIVED_B64="$(openssl kdf -keylen 32 \
    -kdfopt digest:SHA256 \
    -kdfopt "pass:$PASSWORD" \
    -kdfopt "hexsalt:$SALT_HEX" \
    -kdfopt "iter:$ITERATIONS" \
    -binary PBKDF2 | openssl base64 -A)"

HASH="pbkdf2-sha256\$${ITERATIONS}\$${SALT_B64}\$${DERIVED_B64}"
ENV_NAME="LATTICE_STATE_USER_${USERNAME}"

# --- Emit in the requested format (secret only to stdout) ---------------------
case "$FORMAT" in
    env|dotenv) printf '%s=%s\n' "$ENV_NAME" "$HASH" ;;
    export) printf "export %s='%s'\n" "$ENV_NAME" "$HASH" ;;
    value) printf '%s\n' "$HASH" ;;
    json) printf '{"username":"%s","envName":"%s","hash":"%s"}\n' "$USERNAME" "$ENV_NAME" "$HASH" ;;
esac

err "ok: generated credential for '$USERNAME' ($ITERATIONS iterations)."
exit 0
