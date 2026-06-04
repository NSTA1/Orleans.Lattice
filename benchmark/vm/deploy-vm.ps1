#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Deploy the lattice-wedge VM (Bicep). Idempotent.

.DESCRIPTION
	Reads parameters from vm.parameters.local.ps1 (preferred) or vm.parameters.ps1.
	Creates the resource group if missing, deploys main.bicep, prints the SSH command.

.PARAMETER ParametersFile
	Optional explicit path to a parameters .ps1 file.

.PARAMETER NamePrefix
	Optional override for NamePrefix from the parameters file. Used for the
	VM name, NIC, NSG, PIP, DNS label, storage account, and (with 'rg-'
	prefixed) the resource group name. Also doubles as the ~/.ssh/config
	host alias. Pass this to spin up additional VMs side-by-side without
	editing the parameters file.

.EXAMPLE
	./deploy-vm.ps1
.EXAMPLE
	./deploy-vm.ps1 -NamePrefix lattice-wedge-spike
#>
[CmdletBinding()]
param(
	[string] $ParametersFile,
	[string] $NamePrefix,
	[string] $VmSize
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not $ParametersFile) {
	$local = Join-Path $here 'vm.parameters.local.ps1'
	$default = Join-Path $here 'vm.parameters.ps1'
	$ParametersFile = if (Test-Path $local) { $local } else { $default }
}
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
	& ssh-keygen -t ed25519 -f $privPath -N '""' -C "$($p.NamePrefix)-vm" -q
	if ($LASTEXITCODE -ne 0) { throw 'ssh-keygen failed.' }
}
$sshKey = (Get-Content -Raw $p.SshPublicKeyPath).Trim()
$privKeyPath = if ($p.SshPublicKeyPath.EndsWith('.pub')) { $p.SshPublicKeyPath.Substring(0, $p.SshPublicKeyPath.Length - 4) } else { $p.SshPublicKeyPath }

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

$bicep = Join-Path $here 'main.bicep'
$cloudInit = Join-Path $here 'cloud-init.yaml'
$customDataB64 = ''
if (Test-Path $cloudInit) {
	$bytes = [System.IO.File]::ReadAllBytes($cloudInit)
	$customDataB64 = [Convert]::ToBase64String($bytes)
	Write-Host "Using cloud-init from $cloudInit ($([math]::Round($bytes.Length/1KB,1)) KiB)" -ForegroundColor Cyan
}
$deployName = "lattice-wedge-$(Get-Date -Format 'yyyyMMddHHmmss')"

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
$deadline = (Get-Date).AddMinutes(8)
$sshReady = $false
while ((Get-Date) -lt $deadline) {
	$probe = & ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 -o BatchMode=yes $hostAlias 'echo ready' 2>$null
	if ($LASTEXITCODE -eq 0 -and $probe.Trim() -eq 'ready') { $sshReady = $true; break }
	Start-Sleep -Seconds 5
}
if (-not $sshReady) { throw 'SSH did not come up within 8 minutes.' }
Write-Host '  SSH up.' -ForegroundColor Green

Write-Host 'Waiting for cloud-init...' -ForegroundColor Cyan
$ciState = & ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 $hostAlias 'cloud-init status --wait 2>&1; echo EXIT:$?'
Write-Host ($ciState -join "`n")

# Verify the silo's expected toolchain landed via cloud-init. If not, run
# bootstrap.sh as a fallback so the VM is always usable post-deploy.
$dotnetVer = (& ssh -o StrictHostKeyChecking=accept-new $hostAlias '/usr/bin/dotnet --version 2>/dev/null || true').Trim()
if (-not $dotnetVer) {
	Write-Host 'cloud-init did not install .NET; running bootstrap.sh fallback...' -ForegroundColor Yellow
	$bs = Join-Path $here 'bootstrap.sh'
	$tmp = New-TemporaryFile
	try {
		[System.IO.File]::WriteAllText($tmp.FullName, ((Get-Content -Raw $bs) -replace "`r`n","`n"))
		& scp $tmp.FullName "${hostAlias}:/tmp/bootstrap.sh" | Out-Null
	} finally { Remove-Item $tmp.FullName -Force -ErrorAction SilentlyContinue }
	& ssh $hostAlias 'chmod +x /tmp/bootstrap.sh && /tmp/bootstrap.sh'
	$dotnetVer = (& ssh $hostAlias '/usr/bin/dotnet --version').Trim()
}
Write-Host "  dotnet $dotnetVer" -ForegroundColor Green

Write-Host ''
Write-Host '=== Infra deploy complete; running update-vm to publish silo + producer ===' -ForegroundColor Cyan
$updateScript = Join-Path $here 'update-vm.ps1'
$updateArgs = @{}
if ($NamePrefix)    { $updateArgs.NamePrefix    = $p.NamePrefix }
if ($ParametersFile) { $updateArgs.ParametersFile = $ParametersFile }
& $updateScript @updateArgs
if ($LASTEXITCODE -ne 0) { throw "update-vm.ps1 failed (exit $LASTEXITCODE)." }

Write-Host ''
Write-Host '=== End-to-end deploy complete ===' -ForegroundColor Green
$prefixArg = if ($NamePrefix) { " -NamePrefix $($p.NamePrefix)" } else { '' }
Write-Host "Run a cohort:  ./benchmark/vm/run-cohort.ps1$prefixArg -Vehicles 4000 -TickHz 5 -DurationSec 30" -ForegroundColor Yellow
Write-Host "Tail logs:     ./benchmark/vm/vm.ps1 logs$prefixArg" -ForegroundColor Yellow
