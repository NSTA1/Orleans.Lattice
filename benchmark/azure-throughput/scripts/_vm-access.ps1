#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Shared helper: resolve how to reach the lattice-bench VM over ssh/scp.

.DESCRIPTION
	Dot-source this file. It supports the two network layouts main.bicep can
	deploy:

	  public  (default) - the VM has a public IP + DNS label and an NSG rule
	          admitting the operator's IP. ssh/scp go straight to the FQDN.

	  private (deploy.ps1 -PrivateNetwork) - for subscriptions whose policy
	          forbids VM public IPs and public storage access. The VM has no
	          public IP; ssh/scp go through an `az network bastion tunnel`
	          port-forward on 127.0.0.1 that this helper starts on demand and
	          keeps running in the background.

	The layout is recorded on the resource group as the tag
	latticeBenchNetwork=private (stamped by deploy.ps1), so every script
	(update.ps1, run-cohort.ps1, vm.ps1, performance-report.ps1) detects it
	without being told.

	Resolve-BenchVmSsh returns:
	  Private  - $true when the tunnel is in use.
	  Target   - the user@host argument for ssh / the host prefix for scp.
	  SshOpts  - extra -o options to append to the caller's own ssh/scp
	             options. Uses `-o Port=` (not -p/-P) so the same array works
	             for both ssh and scp, and `-o HostKeyAlias=<prefix>` so the
	             host key is recorded against the VM rather than against
	             127.0.0.1:<port>. When -SshPublicKeyPath is given, it also
	             carries -o IdentityFile=<private key> and IdentitiesOnly=yes,
	             because 127.0.0.1 matches no ~/.ssh/config Host block.
	  Display  - a human-readable description of the endpoint.

	The tunnel's PID and port are kept under benchmark/.run/bastion/<prefix>.json
	(gitignored). The port is derived deterministically from the prefix, so the
	~/.ssh/config block deploy.ps1 writes stays valid across tunnel restarts.
	Stop-BenchBastionTunnel stops it; performance-report.ps1 does so at teardown.
#>

function Get-BenchTunnelStateDir {
	# $PSScriptRoot inside a function resolves to the file that defines it
	# (this one), whichever script dot-sourced it. A function rather than a
	# script-scoped variable, so it is right in every caller's scope.
	return [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../../.run/bastion'))
}

function Test-BenchPrivateNetwork {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $ResourceGroup)
	$tag = & az group show --name $ResourceGroup --query 'tags.latticeBenchNetwork' -o tsv --only-show-errors 2>$null
	return ("$tag".Trim() -eq 'private')
}

function Get-BenchTunnelPort {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $NamePrefix)
	# Stable across processes and platforms (unlike String.GetHashCode).
	$md5 = [System.Security.Cryptography.MD5]::Create()
	try { $h = $md5.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($NamePrefix)) } finally { $md5.Dispose() }
	return 50000 + ((([int]$h[0] -shl 8) -bor [int]$h[1]) % 10000)
}

function Get-BenchTunnelStatePath {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $NamePrefix)
	return (Join-Path (Get-BenchTunnelStateDir) "$NamePrefix.json")
}

function Get-BenchProcessTree {
	# The tunnel is launched through the az wrapper (az.cmd / a shell script),
	# which runs Python as a child. Stopping only the wrapper would orphan the
	# Python process that actually holds the port, so collect the whole tree.
	[CmdletBinding()] param([Parameter(Mandatory)][int] $RootId)
	$ids = [System.Collections.Generic.List[int]]::new()
	$queue = [System.Collections.Generic.Queue[int]]::new()
	$queue.Enqueue($RootId)
	while ($queue.Count -gt 0) {
		$id = $queue.Dequeue()
		$ids.Add($id)
		$children = @()
		if ($IsWindows) {
			$children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$id" -ErrorAction SilentlyContinue | ForEach-Object { [int]$_.ProcessId })
		} else {
			$children = @(& pgrep -P $id 2>$null | ForEach-Object { [int]$_ })
		}
		foreach ($c in $children) { if (-not $ids.Contains($c)) { $queue.Enqueue($c) } }
	}
	return $ids
}

function Stop-BenchBastionTunnel {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $NamePrefix)
	$statePath = Get-BenchTunnelStatePath -NamePrefix $NamePrefix
	if (-not (Test-Path $statePath)) { return }
	$state = Get-Content -Raw $statePath | ConvertFrom-Json
	if ($state.pid -and (Get-Process -Id $state.pid -ErrorAction SilentlyContinue)) {
		$tree = Get-BenchProcessTree -RootId ([int]$state.pid)
		# Children first, so the wrapper cannot respawn or report on them.
		for ($i = $tree.Count - 1; $i -ge 0; $i--) {
			Stop-Process -Id $tree[$i] -Force -ErrorAction SilentlyContinue
		}
		Write-Host "[bastion] stopped tunnel for '$NamePrefix' (pid $($state.pid), port $($state.port))" -ForegroundColor DarkGray
	}
	Remove-Item $statePath -Force -ErrorAction SilentlyContinue
}

function Start-BenchBastionTunnel {
	<#
	.SYNOPSIS
		Ensure an `az network bastion tunnel` to the VM's port 22 is listening
		on 127.0.0.1 and return its local port. Reuses a live tunnel.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $ResourceGroup,
		[Parameter(Mandatory)][string] $NamePrefix,
		[int] $ReadyTimeoutSec = 90
	)
	$statePath = Get-BenchTunnelStatePath -NamePrefix $NamePrefix
	if (Test-Path $statePath) {
		$state = Get-Content -Raw $statePath | ConvertFrom-Json
		if ($state.pid -and (Get-Process -Id $state.pid -ErrorAction SilentlyContinue)) {
			return [int]$state.port
		}
		Remove-Item $statePath -Force -ErrorAction SilentlyContinue
	}

	# The tunnel command lives in the 'bastion' CLI extension.
	& az extension show --name bastion -o none 2>$null
	if ($LASTEXITCODE -ne 0) {
		Write-Host '[bastion] installing the az bastion extension' -ForegroundColor DarkGray
		& az extension add --name bastion --only-show-errors -o none
		if ($LASTEXITCODE -ne 0) { throw 'az extension add --name bastion failed.' }
	}

	$port = Get-BenchTunnelPort -NamePrefix $NamePrefix
	$probe = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
	try { $probe.Start() } catch {
		throw "Local port $port (derived from prefix '$NamePrefix') is already in use, so the Bastion tunnel cannot bind it. Free the port, or stop a stale tunnel with: ./vm.ps1 tunnel-stop -NamePrefix $NamePrefix"
	} finally { $probe.Stop() }

	# When the caller redirects stderr (e.g. `deploy.ps1 *>&1`), az warnings are
	# captured alongside stdout, so keep only the resource-id line.
	$vmId = "$(& az vm show --resource-group $ResourceGroup --name "$NamePrefix-vm" --query id -o tsv --only-show-errors 2>$null |
		Where-Object { "$_" -like '/subscriptions/*' } | Select-Object -First 1)".Trim()
	if (-not $vmId) { throw "VM '$NamePrefix-vm' not found in '$ResourceGroup'." }

	$stateDir = Get-BenchTunnelStateDir
	if (-not (Test-Path $stateDir)) { New-Item -ItemType Directory -Path $stateDir -Force | Out-Null }
	$outLog = Join-Path (Get-BenchTunnelStateDir) "$NamePrefix.out.log"
	$errLog = Join-Path (Get-BenchTunnelStateDir) "$NamePrefix.err.log"
	$azPath = (Get-Command az -CommandType Application | Select-Object -First 1).Source
	$tunnelArgs = @(
		'network', 'bastion', 'tunnel',
		'--name', "$NamePrefix-bastion",
		'--resource-group', $ResourceGroup,
		'--target-resource-id', $vmId,
		'--resource-port', '22',
		'--port', "$port",
		'--only-show-errors'
	)
	Write-Host "[bastion] opening tunnel $NamePrefix-bastion -> $NamePrefix-vm:22 on 127.0.0.1:$port" -ForegroundColor Cyan
	$proc = Start-Process -FilePath $azPath -ArgumentList $tunnelArgs -NoNewWindow -PassThru `
		-RedirectStandardOutput $outLog -RedirectStandardError $errLog
	@{ pid = $proc.Id; port = $port; resourceGroup = $ResourceGroup } | ConvertTo-Json | Set-Content -Path $statePath -Encoding utf8

	# Ready once the local port accepts a connection.
	$deadline = (Get-Date).AddSeconds($ReadyTimeoutSec)
	while ((Get-Date) -lt $deadline) {
		if ($proc.HasExited) {
			$err = (Get-Content -Raw $errLog -ErrorAction SilentlyContinue)
			Remove-Item $statePath -Force -ErrorAction SilentlyContinue
			throw "az network bastion tunnel exited (code $($proc.ExitCode)) before becoming ready: $err"
		}
		$client = [System.Net.Sockets.TcpClient]::new()
		try {
			if ($client.ConnectAsync([System.Net.IPAddress]::Loopback, $port).Wait(1000) -and $client.Connected) {
				return $port
			}
		} catch { } finally { $client.Dispose() }
		Start-Sleep -Seconds 1
	}
	Stop-BenchBastionTunnel -NamePrefix $NamePrefix
	throw "Bastion tunnel on 127.0.0.1:$port did not become ready within ${ReadyTimeoutSec}s. See $errLog."
}

function Resolve-BenchVmSsh {
	<#
	.SYNOPSIS
		Return how to ssh/scp to the VM: Target (user@host), extra SshOpts,
		Private flag and a Display string. Starts the Bastion tunnel when the
		resource group is in private-network mode.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $ResourceGroup,
		[Parameter(Mandatory)][string] $NamePrefix,
		[Parameter(Mandatory)][string] $AdminUsername,
		# Public key from the parameters file; its private half is passed as
		# IdentityFile in private mode, because 127.0.0.1 matches no ~/.ssh/config
		# block (public mode gets the key from the block matching the FQDN).
		[string] $SshPublicKeyPath,
		# Resolve without starting a tunnel (status displays).
		[switch] $NoTunnel
	)
	if (Test-BenchPrivateNetwork -ResourceGroup $ResourceGroup) {
		$port = if ($NoTunnel) { Get-BenchTunnelPort -NamePrefix $NamePrefix } else { Start-BenchBastionTunnel -ResourceGroup $ResourceGroup -NamePrefix $NamePrefix }
		$opts = @('-o', "Port=$port", '-o', "HostKeyAlias=$NamePrefix")
		if ($SshPublicKeyPath) {
			$keyPath = if ($SshPublicKeyPath.EndsWith('.pub')) { $SshPublicKeyPath.Substring(0, $SshPublicKeyPath.Length - 4) } else { $SshPublicKeyPath }
			$opts += @('-o', "IdentityFile=$($keyPath -replace '\\','/')", '-o', 'IdentitiesOnly=yes')
		}
		return [pscustomobject]@{
			Private = $true
			Target  = "$AdminUsername@127.0.0.1"
			SshOpts = $opts
			Port    = $port
			Display = "127.0.0.1:$port (Bastion tunnel via $NamePrefix-bastion)"
		}
	}
	$fqdn = "$(& az network public-ip show -g $ResourceGroup -n "$NamePrefix-pip" --query dnsSettings.fqdn -o tsv --only-show-errors 2>$null)".Trim()
	return [pscustomobject]@{
		Private = $false
		Target  = if ($fqdn) { "$AdminUsername@$fqdn" } else { $null }
		SshOpts = @()
		Port    = 22
		Display = $fqdn
	}
}
