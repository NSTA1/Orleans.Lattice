#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Deploy the lattice-bench VM (Bicep). Idempotent.

.DESCRIPTION
	Reads parameters from vm.parameters.local.ps1 (preferred) or vm.parameters.ps1.
	Creates the resource group if missing, deploys main.bicep, prints the SSH command.

.PARAMETER ParametersFile
	Optional explicit path to a parameters .ps1 file. Defaults to
	parameters.local.ps1 (preferred) or parameters.ps1 in this folder.

.PARAMETER NamePrefix
	Optional override for NamePrefix from the parameters file. Used for the
	VM name, NIC, NSG, PIP, DNS label, storage account, and (with 'rg-'
	prefixed) the resource group name. Also doubles as the ~/.ssh/config
	host alias. Pass this to spin up additional VMs side-by-side without
	editing the parameters file.

.EXAMPLE
	./deploy-vm.ps1
.EXAMPLE
	./deploy-vm.ps1 -NamePrefix lattice-bench-spike
#>
[CmdletBinding()]
param(
	[string] $ParametersFile,
	[string] $NamePrefix,
	[string] $VmSize,
	[ValidateRange(1, 8)]
	[int] $WalAccountCount = 1
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not $ParametersFile) {
	$local = Join-Path $here 'parameters.local.ps1'
	$default = Join-Path $here 'parameters.ps1'
	$ParametersFile = if (Test-Path $local) { $local } else { $default }
}
# Infra (Bicep + cloud-init + units) lives alongside us under ../infra/.
$infraDir = Join-Path (Split-Path -Parent $here) 'infra'
Write-Host "Loading parameters from $ParametersFile" -ForegroundColor Cyan
$p = & $ParametersFile

# Apply -NamePrefix override before validation. When the operator passes a
# new prefix, also derive the resource group name from it (rg-<prefix>) so
# side-by-side deployments are fully isolated by default. The operator can
# still set a custom RG by editing the hashtable returned from $p after we
# load it - but typically -NamePrefix alone is enough.
if ($NamePrefix) {
	Write-Host "Overriding NamePrefix: '$($p.NamePrefix)' -> '$NamePrefix'" -ForegroundColor Yellow
	$p.NamePrefix = $NamePrefix
	$p.ResourceGroup = "rg-$NamePrefix"
	Write-Host "  ResourceGroup set to '$($p.ResourceGroup)'" -ForegroundColor Yellow
}
if ($VmSize) {
	Write-Host "Overriding VmSize: '$($p.VmSize)' -> '$VmSize'" -ForegroundColor Yellow
	$p.VmSize = $VmSize
}

foreach ($req in 'SubscriptionId','ResourceGroup','Location','NamePrefix','VmSize','AdminUsername','SshPublicKeyPath') {
	if ([string]::IsNullOrWhiteSpace([string]$p[$req])) { throw "Parameter '$req' is required (see $ParametersFile)." }
}

# Auto-generate the SSH key pair if missing. Convention: the operator points
# SshPublicKeyPath at "<key>.pub"; we derive the private key path by stripping
# the .pub suffix.
if (-not (Test-Path $p.SshPublicKeyPath)) {
	$privPath = $p.SshPublicKeyPath
	if ($privPath.EndsWith('.pub')) { $privPath = $privPath.Substring(0, $privPath.Length - 4) }
	$keyDir = Split-Path -Parent $privPath
	if (-not (Test-Path $keyDir)) { New-Item -ItemType Directory -Path $keyDir -Force | Out-Null }
	Write-Host "SSH key not found; generating at $privPath" -ForegroundColor Yellow
	# -N "" (a genuinely empty argument), never -N '""'. The latter passes two
	# literal quote characters as the passphrase, so the key is encrypted with a
	# passphrase nobody knows - and every later ssh/scp then blocks forever on a
	# passphrase prompt that has no TTY to read from.
	& ssh-keygen -t ed25519 -f $privPath -N "" -C "$($p.NamePrefix)-vm" -q
	if ($LASTEXITCODE -ne 0) { throw 'ssh-keygen failed.' }
}
$sshKey = (Get-Content -Raw $p.SshPublicKeyPath).Trim()
$privKeyPath = if ($p.SshPublicKeyPath.EndsWith('.pub')) { $p.SshPublicKeyPath.Substring(0, $p.SshPublicKeyPath.Length - 4) } else { $p.SshPublicKeyPath }

# Fail fast on a passphrase-encrypted private key.
#
# Every ssh/scp in this rig runs non-interactively from a script, so an
# encrypted key cannot be unlocked: ssh prompts for the passphrase, finds no
# TTY, and the call blocks indefinitely. The symptom is a deploy or update that
# "hangs" with no output at the SSH step - not an error, just silence - so it is
# worth naming here rather than leaving to be diagnosed on the wire.
#
# An OpenSSH-format private key records its cipher in the header; an
# unencrypted key names the cipher "none", an encrypted one names a real cipher
# (aes256-ctr) and a KDF (bcrypt).
if (Test-Path $privKeyPath) {
	$keyText = Get-Content -Raw $privKeyPath
	$keyBody = ($keyText -replace '-----[A-Z ]+-----', '') -replace '\s', ''
	$isEncrypted = $false
	try {
		$keyBytes = [Convert]::FromBase64String($keyBody)
		$header = [Text.Encoding]::ASCII.GetString($keyBytes[0..([Math]::Min(80, $keyBytes.Length - 1))])
		$isEncrypted = $header -match 'aes|bcrypt'
	} catch {
		# Not an OpenSSH-format key (PEM/PKCS#8); fall back to the PEM marker.
		$isEncrypted = $keyText -match 'ENCRYPTED'
	}
	if ($isEncrypted) {
		throw @"
The private key '$privKeyPath' is passphrase-encrypted, which this rig cannot use.

Every ssh and scp call here runs non-interactively, so ssh would block forever
waiting for a passphrase prompt it can never display - the deploy appears to
hang at the SSH step with no error.

Either:
  * load the key into ssh-agent first (Start-Service ssh-agent; ssh-add '$privKeyPath'), or
  * point SshPublicKeyPath at a dedicated passphrase-less key for the rig:
      ssh-keygen -t ed25519 -f `$HOME/.ssh/id_$($p.NamePrefix) -N "" -C '$($p.NamePrefix)-bench'
    and set SshPublicKeyPath = '~/.ssh/id_$($p.NamePrefix).pub' in your parameters file.
"@
	}
}

$allowed = $p.AllowedSshSourceAddress
if ([string]::IsNullOrWhiteSpace($allowed)) {
	Write-Host 'AllowedSshSourceAddress blank; auto-detecting public IP...' -ForegroundColor Yellow
	$ip = (Invoke-RestMethod -Uri 'https://api.ipify.org?format=json').ip
	$allowed = "$ip/32"
	Write-Host "  detected: $allowed" -ForegroundColor Yellow
}

Write-Host "Setting subscription $($p.SubscriptionId)" -ForegroundColor Cyan
az account set --subscription $p.SubscriptionId | Out-Null

Write-Host "Ensuring resource group $($p.ResourceGroup) in $($p.Location)" -ForegroundColor Cyan
az group create --name $p.ResourceGroup --location $p.Location --output none

$bicep = Join-Path $infraDir 'main.bicep'
$cloudInit = Join-Path $infraDir 'cloud-init.yaml'
$customDataB64 = ''
if (Test-Path $cloudInit) {
	$bytes = [System.IO.File]::ReadAllBytes($cloudInit)
	$customDataB64 = [Convert]::ToBase64String($bytes)
	Write-Host "Using cloud-init from $cloudInit ($([math]::Round($bytes.Length/1KB,1)) KiB)" -ForegroundColor Cyan
}
$deployName = "lattice-bench-$(Get-Date -Format 'yyyyMMddHHmmss')"

Write-Host "Deploying $deployName ..." -ForegroundColor Cyan
$result = az deployment group create `
	--resource-group $p.ResourceGroup `
	--name $deployName `
	--template-file $bicep `
	--parameters `
		location=$($p.Location) `
		namePrefix=$($p.NamePrefix) `
		vmSize=$($p.VmSize) `
		adminUsername=$($p.AdminUsername) `
		sshPublicKey="$sshKey" `
		allowedSshSourceAddress=$allowed `
		autoShutdownTimeZone=$($p.AutoShutdownTimeZone) `
		autoShutdownTime=$($p.AutoShutdownTime) `
		osDiskSizeGB=$($p.OsDiskSizeGB) `
		customDataBase64="$customDataB64" `
		walAccountCount=$WalAccountCount `
	--output json | ConvertFrom-Json

$out = $result.properties.outputs
Write-Host ''
Write-Host '=== Deployment complete ===' -ForegroundColor Green
Write-Host ("VM name      : {0}" -f $out.vmName.value)
Write-Host ("Public IP    : {0}" -f $out.publicIp.value)
Write-Host ("FQDN         : {0}" -f $out.fqdn.value)
Write-Host ("SSH allowed  : {0}" -f $allowed)
Write-Host ("Auto-shutdown: {0} {1}" -f $p.AutoShutdownTime, $p.AutoShutdownTimeZone)
Write-Host ("Storage acct : {0}" -f $out.storageAccountName.value)
Write-Host ("Table endpt  : {0}" -f $out.storageTableEndpoint.value)
Write-Host ("Blob endpt   : {0}" -f $out.storageBlobEndpoint.value)
Write-Host ("VM identity  : {0}" -f $out.vmPrincipalId.value)
Write-Host ("Connect      : {0}" -f $out.sshCommand.value)

# --- post-deploy: wire ~/.ssh/config + cache host key + wait for SSH ---
$fqdn = $out.fqdn.value
$adminUser = $p.AdminUsername
$hostAlias = $p.NamePrefix

Write-Host ''
Write-Host '=== Configuring local SSH ===' -ForegroundColor Cyan
$sshCfg = Join-Path $HOME '.ssh/config'
$sshCfgDir = Split-Path -Parent $sshCfg
if (-not (Test-Path $sshCfgDir)) { New-Item -ItemType Directory -Path $sshCfgDir -Force | Out-Null }
$blockMarker = "# >>> $hostAlias (managed by deploy-vm.ps1) >>>"
$blockEnd    = "# <<< $hostAlias <<<"
$block = @"
$blockMarker
Host $hostAlias $fqdn
	HostName $fqdn
	User $adminUser
	IdentityFile $($privKeyPath -replace '\\','/')
	IdentitiesOnly yes
	StrictHostKeyChecking accept-new
	ServerAliveInterval 15
	ServerAliveCountMax 3
$blockEnd
"@
if (Test-Path $sshCfg) {
	$existing = Get-Content -Raw $sshCfg
	if ($existing -match [regex]::Escape($blockMarker)) {
		# Replace existing managed block.
		$pattern = "(?s)" + [regex]::Escape($blockMarker) + ".*?" + [regex]::Escape($blockEnd) + "\r?\n?"
		$updated = [regex]::Replace($existing, $pattern, '')
		[System.IO.File]::WriteAllText($sshCfg, ($updated.TrimEnd() + "`n`n" + $block))
		Write-Host "  ~/.ssh/config: updated managed block for '$hostAlias'" -ForegroundColor Green
	} else {
		Add-Content -Path $sshCfg -Value "`n$block"
		Write-Host "  ~/.ssh/config: appended managed block for '$hostAlias'" -ForegroundColor Green
	}
} else {
	[System.IO.File]::WriteAllText($sshCfg, $block)
	Write-Host "  ~/.ssh/config: created with managed block for '$hostAlias'" -ForegroundColor Green
}

Write-Host 'Waiting for SSH to come up + cloud-init to finish...' -ForegroundColor Cyan
# Purge any stale known_hosts entries for this FQDN/alias. A re-deploy after a
# teardown re-uses the public DNS name but gets a NEW host key, so a leftover
# entry from a prior VM will cause every probe below to silently reject with
# 'WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!' and the wait loop will
# spin until the 8-minute deadline.
$kh = Join-Path $HOME '.ssh/known_hosts'
if (Test-Path $kh) {
	foreach ($name in @($hostAlias, $fqdn)) {
		& ssh-keygen -R $name -f $kh 2>$null | Out-Null
	}
}

$deadline = (Get-Date).AddMinutes(8)
$sshReady = $false
$probeErr = $null
while ((Get-Date) -lt $deadline) {
	$probe = & ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 -o BatchMode=yes $hostAlias 'echo ready' 2>&1
	if ($LASTEXITCODE -eq 0 -and ($probe -join '').Trim() -eq 'ready') { $sshReady = $true; break }
	$probeErr = ($probe -join ' ').Trim()
	Start-Sleep -Seconds 5
}
if (-not $sshReady) {
	# Surface the last probe error rather than only the timeout. The common
	# causes are distinguishable and each has a different fix:
	#   'Permission denied (publickey)' -> the VM has a different key than the
	#     one this parameters file names, or the key is passphrase-encrypted and
	#     BatchMode is (correctly) refusing to prompt.
	#   connection refused / timed out -> NSG source-address drift (your public
	#     IP changed since deploy) or the VM is still booting.
	throw "SSH did not come up within 8 minutes. Last probe error: $probeErr"
}
Write-Host '  SSH up.' -ForegroundColor Green

# Every ssh/scp below carries BatchMode=yes. Without it, any prompt (passphrase,
# host-key confirmation, password fallback) blocks forever in a non-interactive
# script instead of failing - which presents as a silent hang rather than an
# error. BatchMode turns each of those into a prompt-free non-zero exit.
Write-Host 'Waiting for cloud-init...' -ForegroundColor Cyan
$ciState = & ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o BatchMode=yes $hostAlias 'cloud-init status --wait 2>&1; echo EXIT:$?'
Write-Host ($ciState -join "`n")

# Verify the silo's expected toolchain landed via cloud-init. If not, run
# bootstrap.sh as a fallback so the VM is always usable post-deploy.
$dotnetVer = (& ssh -o StrictHostKeyChecking=accept-new -o BatchMode=yes $hostAlias '/usr/bin/dotnet --version 2>/dev/null || true').Trim()
if (-not $dotnetVer) {
	Write-Host 'cloud-init did not install .NET; running bootstrap.sh fallback...' -ForegroundColor Yellow
	$bs = Join-Path $infraDir 'bootstrap.sh'
	$tmp = New-TemporaryFile
	try {
		[System.IO.File]::WriteAllText($tmp.FullName, ((Get-Content -Raw $bs) -replace "`r`n","`n"))
		& scp -o BatchMode=yes $tmp.FullName "${hostAlias}:/tmp/bootstrap.sh" | Out-Null
	} finally { Remove-Item $tmp.FullName -Force -ErrorAction SilentlyContinue }
	& ssh -o BatchMode=yes $hostAlias 'chmod +x /tmp/bootstrap.sh && /tmp/bootstrap.sh'
	$dotnetVer = (& ssh -o BatchMode=yes $hostAlias '/usr/bin/dotnet --version').Trim()
}
Write-Host "  dotnet $dotnetVer" -ForegroundColor Green

Write-Host ''
Write-Host '=== Infra deploy complete; running update-vm to publish silo + producer ===' -ForegroundColor Cyan
$updateScript = Join-Path $here 'update.ps1'
$updateArgs = @{}
if ($NamePrefix)    { $updateArgs.NamePrefix    = $p.NamePrefix }
if ($ParametersFile) { $updateArgs.ParametersFile = $ParametersFile }
& $updateScript @updateArgs
if ($LASTEXITCODE -ne 0) { throw "update-vm.ps1 failed (exit $LASTEXITCODE)." }

Write-Host ''
Write-Host '=== End-to-end deploy complete ===' -ForegroundColor Green
$prefixArg = if ($NamePrefix) { " -NamePrefix $($p.NamePrefix)" } else { '' }
Write-Host "Run a cohort:  ./benchmark/azure-throughput/scripts/run-cohort.ps1$prefixArg -Vehicles 4000 -TickHz 5 -DurationSec 30" -ForegroundColor Yellow
Write-Host "Tail logs:     ./benchmark/azure-throughput/scripts/vm.ps1 logs$prefixArg" -ForegroundColor Yellow
