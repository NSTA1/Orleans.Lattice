#Requires -Version 5.1
<#
.SYNOPSIS
    Generates a Lattice state-API credential hash (PBKDF2-HMAC-SHA256).

.DESCRIPTION
    Produces the self-describing credential encoding consumed by the server-side
    EnvVarCredentialAuthorizer:

        pbkdf2-sha256$<iterations>$<base64-salt>$<base64-derived-key>

    The hash is published to the server process through an environment variable
    named LATTICE_STATE_USER_<username>. The username lives in the variable name;
    the encoded hash is the value. This script never transmits or stores the
    plaintext password: it is read from a no-echo prompt, from stdin, or from a
    named environment variable, hashed in-process, and only the hash is written
    to stdout. All diagnostics go to stderr so that the 'value' and 'json'
    formats pipe cleanly.

    This is the PowerShell counterpart of tools/new-lattice-state-credential.sh.
    For an identical salt, password, and iteration count, both scripts emit the
    byte-identical encoding.

.PARAMETER Username
    The credential username. Must be a valid environment-variable name segment
    (^[A-Za-z_][A-Za-z0-9_]*$) so that LATTICE_STATE_USER_<username> is a legal
    variable name.

.PARAMETER PasswordStdin
    Read the password from stdin (a single line, trailing newline trimmed)
    instead of prompting. Use for non-interactive automation.

.PARAMETER PasswordEnv
    Read the password from the named environment variable instead of prompting.

.PARAMETER Iterations
    PBKDF2 iteration count recorded in the encoding. Defaults to 210000.

.PARAMETER Format
    Output format: env (default), dotenv, export, value, or json.

.PARAMETER AllowWeakPassword
    Bypass the password-strength policy. Discouraged; off by default.

.OUTPUTS
    The credential encoding (or formatted variant) on stdout.

.NOTES
    Exit codes:
      0  success
      2  bad input (missing/invalid username, no password source, bad arguments)
      3  password policy rejected
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string] $Username,

    [switch] $PasswordStdin,

    [string] $PasswordEnv,

    [int] $Iterations = 210000,

    [ValidateSet('env', 'dotenv', 'export', 'value', 'json')]
    [string] $Format = 'env',

    [switch] $AllowWeakPassword
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Diagnostic([string] $message) {
    [Console]::Error.WriteLine($message)
}

function Exit-WithError([int] $code, [string] $message) {
    Write-Diagnostic "error: $message"
    exit $code
}

# --- Validate username charset (must be env-var-name-safe) --------------------
if ($Username -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') {
    Exit-WithError 2 "username '$Username' is not a valid environment-variable name segment (allowed: letters, digits, underscore; must not start with a digit)."
}

if ($Iterations -lt 1) {
    Exit-WithError 2 "iterations must be a positive integer."
}

# --- Resolve the plaintext password without echoing it ------------------------
$password = $null
if ($PasswordStdin) {
    if (-not [string]::IsNullOrEmpty($PasswordEnv)) {
        Exit-WithError 2 "specify only one password source (-PasswordStdin or -PasswordEnv)."
    }
    $password = [Console]::In.ReadLine()
    if ($null -eq $password) { $password = '' }
}
elseif (-not [string]::IsNullOrEmpty($PasswordEnv)) {
    $value = [Environment]::GetEnvironmentVariable($PasswordEnv)
    if ($null -eq $value) {
        Exit-WithError 2 "environment variable '$PasswordEnv' is not set."
    }
    $password = $value
}
else {
    $secure = Read-Host -AsSecureString -Prompt "Password for '$Username'"
    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToGlobalAllocUnicode($secure)
    try {
        $password = [Runtime.InteropServices.Marshal]::PtrToStringUni($ptr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeGlobalAllocUnicode($ptr)
    }
}

# --- Enforce the password policy before hashing -------------------------------
if (-not $AllowWeakPassword) {
    $policyFailed = $false
    if ($password.Length -lt 8) { $policyFailed = $true }
    if ($password -cnotmatch '[A-Z]') { $policyFailed = $true }
    if ($password -cnotmatch '[a-z]') { $policyFailed = $true }
    if ($password -notmatch '[0-9]') { $policyFailed = $true }
    if ($policyFailed) {
        Exit-WithError 3 "password does not satisfy policy: minimum 8 characters with at least one uppercase letter, one lowercase letter, and one digit. Use -AllowWeakPassword to override (discouraged)."
    }
}

# --- Generate (or accept an injected) salt ------------------------------------
# LATTICE_CRED_SALT_B64 forces a deterministic salt; it exists ONLY so the
# cross-shell parity test can compare this script against the bash counterpart.
# Never set it in production.
$saltOverride = [Environment]::GetEnvironmentVariable('LATTICE_CRED_SALT_B64')
if (-not [string]::IsNullOrEmpty($saltOverride)) {
    try {
        $salt = [Convert]::FromBase64String($saltOverride)
    }
    catch {
        Exit-WithError 2 "LATTICE_CRED_SALT_B64 is not valid base64."
    }
    if ($salt.Length -lt 16) {
        Exit-WithError 2 "LATTICE_CRED_SALT_B64 must decode to at least 16 bytes."
    }
}
else {
    $salt = New-Object byte[] 16
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($salt)
}

# --- Derive the key (PBKDF2-HMAC-SHA256, 32-byte output) ----------------------
$passwordBytes = [System.Text.Encoding]::UTF8.GetBytes($password)
$derived = [System.Security.Cryptography.Rfc2898DeriveBytes]::Pbkdf2(
    $passwordBytes,
    $salt,
    $Iterations,
    [System.Security.Cryptography.HashAlgorithmName]::SHA256,
    32)

$saltB64 = [Convert]::ToBase64String($salt)
$keyB64 = [Convert]::ToBase64String($derived)
$hash = "pbkdf2-sha256`$$Iterations`$$saltB64`$$keyB64"
$envName = "LATTICE_STATE_USER_$Username"

# --- Emit in the requested format (secret only to stdout) ---------------------
switch ($Format) {
    'env' { [Console]::Out.WriteLine("$envName=$hash") }
    'dotenv' { [Console]::Out.WriteLine("$envName=$hash") }
    'export' { [Console]::Out.WriteLine("export $envName='$hash'") }
    'value' { [Console]::Out.WriteLine($hash) }
    'json' {
        $obj = [ordered]@{ username = $Username; envName = $envName; hash = $hash }
        [Console]::Out.WriteLine(($obj | ConvertTo-Json -Compress))
    }
}

Write-Diagnostic "ok: generated credential for '$Username' ($Iterations iterations)."
exit 0
