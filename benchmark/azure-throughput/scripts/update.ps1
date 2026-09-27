#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Sync source, build, and (re)start the silo on the lattice-bench VM.

.DESCRIPTION
	Inner-loop deploy script. On each run:
	  1. Reads VM coords + storage outputs (every WAL table endpoint
		 included) from the last successful Bicep deployment, and runs
		 infra/bootstrap.sh on the VM when the dotnet SDK is missing.
	  2. Renders lattice-silo.service and lattice-producer.service from the
		 templates (substitutes storage endpoints, extra WAL account URIs +
		 admin user), SCPs them into place and enables both. Idempotent.
	  3. Streams the tracked file set (git ls-files) to /opt/lattice/src on
		 the VM as a tar archive over ssh. Files are added or overwritten,
		 never deleted, so a file removed from the repository stays on the
		 VM.
	  4. SSH-runs dotnet publish for the azure-throughput Silo (output to
		 /opt/lattice/publish) and Producer (output to
		 /opt/lattice/publish-producer).
	  5. Stops the producer (if running) and restarts lattice-silo.

	First-run extras (idempotent): copies the unit files to
	/etc/systemd/system/, enables them, ensures dotnet SDK is present (the
	cloud-init step usually handles this, but we re-check in case the
	bootstrap hadn't finished when the VM first became reachable).

.PARAMETER NoBuild
	Skip the source sync + build; just restart the existing
	/opt/lattice/publish (the units are still re-synced unless
	-SkipUnitSync).

.PARAMETER NoRestart
	Skip the restart at the end (e.g. you want to inspect before starting).

.PARAMETER Clean
	Wipe /opt/lattice/publish and /opt/lattice/publish-producer before
	publishing.

.PARAMETER SkipUnitSync
	Don't re-render or re-copy the systemd units (faster inner loop when
	only source changed and unit + env are already correct).

.EXAMPLE
	./update.ps1                     # full sync + build + restart
	./update.ps1 -NoBuild            # just bounce the service
	./update.ps1 -Clean              # clean publish output
#>
[CmdletBinding()]
param(
	[switch] $NoBuild,
	[switch] $NoRestart,
	[switch] $Clean,
	[switch] $SkipUnitSync,
	[string] $ParametersFile,
	[string] $NamePrefix
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $here '../../..')
$infraDir = Join-Path (Split-Path -Parent $here) 'infra'

if (-not $ParametersFile) {
	$local = Join-Path $here 'parameters.local.ps1'
	$default = Join-Path $here 'parameters.ps1'
	$ParametersFile = if (Test-Path $local) { $local } else { $default }
}
$p = & $ParametersFile
if ($NamePrefix) { $p.NamePrefix = $NamePrefix; $p.ResourceGroup = "rg-$NamePrefix" }

$vmName = "$($p.NamePrefix)-vm"
$pipName = "$($p.NamePrefix)-pip"
$rg = $p.ResourceGroup
$adminUser = $p.AdminUsername

az account set --subscription $p.SubscriptionId | Out-Null

function Invoke-AzQuery {
	[CmdletBinding()]
	param([Parameter(Mandatory)][string[]] $AzArgs)
	$out = & az @AzArgs 2>&1
	if ($LASTEXITCODE -ne 0) { throw "az $($AzArgs -join ' ') failed: $out" }
	return ($out | Out-String).Trim()
}

Write-Host "Looking up VM coordinates..." -ForegroundColor Cyan
$fqdn = Invoke-AzQuery -AzArgs @('network','public-ip','show','-g',$rg,'-n',$pipName,'--query','dnsSettings.fqdn','-o','tsv')
$publicIp = Invoke-AzQuery -AzArgs @('network','public-ip','show','-g',$rg,'-n',$pipName,'--query','ipAddress','-o','tsv')
$powerState = Invoke-AzQuery -AzArgs @('vm','get-instance-view','-g',$rg,'-n',$vmName,'--query',"instanceView.statuses[?starts_with(code, 'PowerState/')].code | [0]",'-o','tsv')
if ($powerState -ne 'PowerState/running') {
	throw "VM is '$powerState'. Run ./vm.ps1 start first."
}

Write-Host "Fetching last deployment outputs..." -ForegroundColor Cyan
$lastDeploy = Invoke-AzQuery -AzArgs @('deployment','group','list','-g',$rg,'--query',"[?properties.provisioningState=='Succeeded'] | sort_by(@, &properties.timestamp) | [-1].name",'-o','tsv')
if (-not $lastDeploy) { throw "No successful deployment found in resource group $rg. Run deploy-vm.ps1 first." }
$outsJson = Invoke-AzQuery -AzArgs @('deployment','group','show','-g',$rg,'-n',$lastDeploy,'--query','properties.outputs','-o','json')
$outs = $outsJson | ConvertFrom-Json
$tableEndpoint = $outs.storageTableEndpoint.value
$blobEndpoint  = $outs.storageBlobEndpoint.value
$storageAcct   = $outs.storageAccountName.value
# All WAL table endpoints (index 0 == primary). Accounts 1.. become the silo's
# BENCH_WAL_EXTRA_ACCOUNT_URIS so the bench can spread WAL partitions across
# accounts. Older deployments without this output degrade to a single account.
$allTableEndpoints = @()
if ($outs.PSObject.Properties.Name -contains 'storageTableEndpointsAll') {
	$allTableEndpoints = @($outs.storageTableEndpointsAll.value)
}
$extraTableEndpoints = if ($allTableEndpoints.Count -gt 1) { $allTableEndpoints[1..($allTableEndpoints.Count - 1)] } else { @() }
$walExtraAccountUris = ($extraTableEndpoints -join ';')

$sshTarget = "$adminUser@$fqdn"
$sshOpts = @(
	'-o','StrictHostKeyChecking=accept-new',
	'-o','ServerAliveInterval=15',
	'-o','ServerAliveCountMax=3',
	'-o','ConnectTimeout=10',
	'-o','BatchMode=yes'
)

function Invoke-Ssh {
	param([Parameter(Mandatory)] [string] $Command,
		  [int] $TimeoutSec = 60)
	& ssh @sshOpts $sshTarget "timeout $TimeoutSec sh -c `"$($Command -replace '"','\"')`""
	if ($LASTEXITCODE -ne 0) { throw "ssh '$Command' failed (exit $LASTEXITCODE)." }
}
function Invoke-SshQuiet {
	param([Parameter(Mandatory)] [string] $Command,
		  [int] $TimeoutSec = 30)
	& ssh @sshOpts $sshTarget "timeout $TimeoutSec sh -c `"$($Command -replace '"','\"')`"" 2>&1 | Out-Null
}

Write-Host "VM         : $vmName ($publicIp / $fqdn)" -ForegroundColor Green
Write-Host "Storage    : $storageAcct" -ForegroundColor Green
Write-Host "WAL accts  : $($allTableEndpoints.Count) (extra: $walExtraAccountUris)" -ForegroundColor Green
Write-Host "SSH        : $sshTarget" -ForegroundColor Green

# --- 1. wait for cloud-init to finish (no-op on subsequent runs) ---
Write-Host 'Checking VM bootstrap state...' -ForegroundColor Cyan
# Avoid --wait: if cloud-init was cleaned or never ran, --wait blocks forever.
# Just snapshot the status (with a hard timeout) and move on; the dotnet check
# below is the real gate.
$ciState = & ssh @sshOpts $sshTarget 'timeout 5 cloud-init status 2>/dev/null || echo "status: unknown"'
Write-Host "  cloud-init: $ciState"
$dotnetVersion = (& ssh @sshOpts $sshTarget 'timeout 10 /usr/bin/dotnet --version 2>/dev/null || true').Trim()
if (-not $dotnetVersion) {
	Write-Host 'dotnet SDK not present. Running bootstrap.sh...' -ForegroundColor Yellow
	$bs = Join-Path $infraDir 'bootstrap.sh'
	$tmp = New-TemporaryFile
	try {
		[System.IO.File]::WriteAllText($tmp.FullName, ((Get-Content -Raw $bs) -replace "`r`n","`n"))
		& scp @sshOpts $tmp.FullName "${sshTarget}:/tmp/bootstrap.sh" | Out-Null
		if ($LASTEXITCODE -ne 0) { throw 'scp of bootstrap.sh failed.' }
	} finally { Remove-Item $tmp.FullName -Force -ErrorAction SilentlyContinue }
	Invoke-Ssh 'chmod +x /tmp/bootstrap.sh && /tmp/bootstrap.sh'
	$dotnetVersion = (& ssh @sshOpts $sshTarget '/usr/bin/dotnet --version').Trim()
}
Invoke-Ssh 'test -d /opt/lattice/src && test -d /opt/lattice/publish || (sudo mkdir -p /opt/lattice/src /opt/lattice/publish /opt/lattice/logs && sudo chown -R azureuser:azureuser /opt/lattice)'
Write-Host "  dotnet $dotnetVersion"

# --- 2. systemd units (silo + producer) ---
if (-not $SkipUnitSync) {
	Write-Host 'Rendering + installing systemd units...' -ForegroundColor Cyan
	function Install-Unit([string]$name) {
		$tpl = Get-Content -Raw (Join-Path $infraDir $name)
		$rendered = $tpl `
			-replace '__ADMIN_USER__', $adminUser `
			-replace '__TABLE_ENDPOINT__', $tableEndpoint `
			-replace '__BLOB_ENDPOINT__', $blobEndpoint `
			-replace '__WAL_EXTRA_ACCOUNT_URIS__', $walExtraAccountUris `
			-replace '__STORAGE_ACCOUNT__', $storageAcct
		$tmp = New-TemporaryFile
		try {
			[System.IO.File]::WriteAllText($tmp.FullName, $rendered, [System.Text.UTF8Encoding]::new($false))
			& scp @sshOpts $tmp.FullName "${sshTarget}:/tmp/$name" | Out-Null
			if ($LASTEXITCODE -ne 0) { throw "scp of $name failed." }
		} finally { Remove-Item $tmp.FullName -Force -ErrorAction SilentlyContinue }
		$base = [System.IO.Path]::GetFileNameWithoutExtension($name)
		Invoke-Ssh "sudo install -m 0644 /tmp/$name /etc/systemd/system/$name"
	}
	Install-Unit 'lattice-silo.service'
	Install-Unit 'lattice-producer.service'
	Invoke-Ssh 'sudo systemctl daemon-reload && sudo systemctl enable lattice-silo lattice-producer >/dev/null 2>&1 || true'
}

# --- 3. sync source ---
if (-not $NoBuild) {
	Write-Host 'Syncing source tree (git ls-files | tar over ssh)...' -ForegroundColor Cyan
	# git ls-files emits exactly the tracked file set - no bin/, obj/,
	# .git/, .vs/, .run/, or any other build artefact. Avoids tar's
	# exclude-glob ambiguities entirely.
	Invoke-Ssh 'mkdir -p /opt/lattice/src'
	Push-Location $repoRoot
	try {
		$fileList = & git ls-files
		if ($LASTEXITCODE -ne 0) { throw 'git ls-files failed (not a git repo?).' }
		Write-Host ("  $($fileList.Count) tracked files; streaming...") -ForegroundColor DarkGray
		$tmpList = New-TemporaryFile
		try {
			[System.IO.File]::WriteAllText($tmpList.FullName, ($fileList -join "`n") + "`n")
			# 5-minute server-side cap on the receive side.
			& tar -czf - -T $tmpList.FullName | & ssh @sshOpts $sshTarget 'timeout 300 tar -xzf - -C /opt/lattice/src'
			if ($LASTEXITCODE -ne 0) { throw "tar|ssh sync failed (exit $LASTEXITCODE)." }
		} finally { Remove-Item $tmpList.FullName -Force -ErrorAction SilentlyContinue }
	} finally { Pop-Location }

	# --- 4. publish ---
	if ($Clean) {
		Write-Host 'Cleaning previous publish output...' -ForegroundColor Cyan
		Invoke-Ssh 'rm -rf /opt/lattice/publish /opt/lattice/publish-producer; mkdir -p /opt/lattice/publish /opt/lattice/publish-producer'
	}
	Write-Host 'Building + publishing silo + producer on the VM...' -ForegroundColor Cyan
	$pubScript = @'
#!/usr/bin/env bash
set -euo pipefail
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1
cd /opt/lattice/src
/usr/bin/dotnet publish benchmark/azure-throughput/Silo/VehicleFleetSimulator.AzureThroughput.Silo.csproj -c Release -o /opt/lattice/publish --nologo
/usr/bin/dotnet publish benchmark/azure-throughput/Producer/VehicleFleetSimulator.AzureThroughput.Producer.csproj -c Release -o /opt/lattice/publish-producer --nologo
'@
	$tmpPub = New-TemporaryFile
	try {
		[System.IO.File]::WriteAllText($tmpPub.FullName, ($pubScript -replace "`r`n","`n"))
		& scp @sshOpts $tmpPub.FullName "${sshTarget}:/tmp/publish.sh" | Out-Null
		if ($LASTEXITCODE -ne 0) { throw 'scp of publish.sh failed.' }
	} finally { Remove-Item $tmpPub.FullName -Force -ErrorAction SilentlyContinue }
	Invoke-Ssh -TimeoutSec 600 'chmod +x /tmp/publish.sh && bash /tmp/publish.sh'
}

# --- 5. restart ---
if (-not $NoRestart) {
	Write-Host 'Stopping producer (if running) + restarting silo...' -ForegroundColor Cyan
	Invoke-Ssh 'sudo systemctl stop lattice-producer 2>/dev/null || true; sudo systemctl restart lattice-silo'
	Start-Sleep -Seconds 1
	$active = & ssh @sshOpts $sshTarget 'systemctl is-active lattice-silo'
	Write-Host "  lattice-silo is-active: $active" -ForegroundColor Green
	Write-Host ''
	Write-Host 'Run a cohort with:  ./benchmark/azure-throughput/scripts/run-cohort.ps1' -ForegroundColor Yellow
	Write-Host 'Tail silo logs:     ./benchmark/azure-throughput/scripts/vm.ps1 logs' -ForegroundColor Yellow
}

Write-Host 'Done.' -ForegroundColor Green
