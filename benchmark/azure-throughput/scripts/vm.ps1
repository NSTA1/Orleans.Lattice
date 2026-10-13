#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Start / stop / status / ssh / refresh-ip helper for the lattice-bench VM.

.PARAMETER Action
	start    - deallocate -> running
	stop     - deallocate (no compute charges)
	status   - print power state, public IP, SSH command
	ssh      - open an interactive SSH session
	refresh-ip - update NSG rule to your current public IP (run from a new network)
	logs     - follow the silo journal over ssh
	tunnel   - (private-network mode) open the Bastion tunnel and print the
	           ssh command; it keeps running in the background
	tunnel-stop - (private-network mode) stop the background Bastion tunnel

	In private-network mode (deploy.ps1 -PrivateNetwork; the resource group is
	tagged latticeBenchNetwork=private) ssh and logs go through the Bastion
	tunnel, which is started on demand, and refresh-ip is a no-op.

.EXAMPLE
	./vm.ps1 start
	./vm.ps1 status
	./vm.ps1 ssh
	./vm.ps1 stop
	./vm.ps1 refresh-ip
	./vm.ps1 tunnel
#>
[CmdletBinding()]
param(
	[Parameter(Mandatory, Position = 0)]
	[ValidateSet('start','stop','status','ssh','refresh-ip','logs','tunnel','tunnel-stop')]
	[string] $Action,

	[string] $ParametersFile,
	[string] $NamePrefix
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_vm-access.ps1')
if (-not $ParametersFile) {
	$local = Join-Path $here 'parameters.local.ps1'
	$default = Join-Path $here 'parameters.ps1'
	$ParametersFile = if (Test-Path $local) { $local } else { $default }
}
$p = & $ParametersFile
if ($NamePrefix) { $p.NamePrefix = $NamePrefix; $p.ResourceGroup = "rg-$NamePrefix" }

$vmName = "$($p.NamePrefix)-vm"
$nsgName = "$($p.NamePrefix)-nsg"
$pipName = "$($p.NamePrefix)-pip"
$rg = $p.ResourceGroup

az account set --subscription $p.SubscriptionId | Out-Null
$isPrivate = Test-BenchPrivateNetwork -ResourceGroup $rg

function Get-PublicIp {
	az network public-ip show -g $rg -n $pipName --query ipAddress -o tsv
}
function Get-Fqdn {
	az network public-ip show -g $rg -n $pipName --query dnsSettings.fqdn -o tsv
}
function Get-PowerState {
	az vm get-instance-view -g $rg -n $vmName --query "instanceView.statuses[?starts_with(code, 'PowerState/')].code | [0]" -o tsv
}

switch ($Action) {
	'start' {
		Write-Host "Starting $vmName ..." -ForegroundColor Cyan
		az vm start -g $rg -n $vmName --output none
		if ($isPrivate) {
			Write-Host "Started. Connect with: ./vm.ps1 ssh   (or: ssh $($p.NamePrefix) after ./vm.ps1 tunnel)" -ForegroundColor Green
		} else {
			$ip = Get-PublicIp
			$fqdn = Get-Fqdn
			Write-Host "Started. ssh $($p.AdminUsername)@$fqdn   ($ip)" -ForegroundColor Green
		}
	}
	'stop' {
		Write-Host "Deallocating $vmName ..." -ForegroundColor Cyan
		az vm deallocate -g $rg -n $vmName --output none
		Write-Host 'Stopped (deallocated; no compute charges).' -ForegroundColor Green
	}
	'status' {
		$state = Get-PowerState
		$rule = az network nsg rule show -g $rg --nsg-name $nsgName -n AllowSshFromOperator --query sourceAddressPrefix -o tsv
		Write-Host ("VM           : {0}" -f $vmName)
		Write-Host ("State        : {0}" -f $state)
		if ($isPrivate) {
			$access = Resolve-BenchVmSsh -ResourceGroup $rg -NamePrefix $p.NamePrefix -AdminUsername $p.AdminUsername -SshPublicKeyPath $p.SshPublicKeyPath -NoTunnel
			$statePath = Get-BenchTunnelStatePath -NamePrefix $p.NamePrefix
			$tunnelUp = $false
			if (Test-Path $statePath) {
				$t = Get-Content -Raw $statePath | ConvertFrom-Json
				$tunnelUp = [bool]($t.pid -and (Get-Process -Id $t.pid -ErrorAction SilentlyContinue))
			}
			Write-Host ("Network      : private (no public IP)")
			Write-Host ("Bastion      : {0}-bastion" -f $p.NamePrefix)
			Write-Host ("Tunnel       : {0}" -f $(if ($tunnelUp) { "running on 127.0.0.1:$($access.Port)" } else { 'not running (./vm.ps1 tunnel)' }))
			Write-Host ("SSH allowed  : {0}" -f $rule)
			Write-Host ("Connect      : ssh {0}   (via the tunnel)" -f $p.NamePrefix)
		} else {
			$ip = Get-PublicIp
			$fqdn = Get-Fqdn
			Write-Host ("Public IP    : {0}" -f $ip)
			Write-Host ("FQDN         : {0}" -f $fqdn)
			Write-Host ("SSH allowed  : {0}" -f $rule)
			Write-Host ("Connect      : ssh {0}@{1}" -f $p.AdminUsername, $fqdn)
		}
	}
	'ssh' {
		$access = Resolve-BenchVmSsh -ResourceGroup $rg -NamePrefix $p.NamePrefix -AdminUsername $p.AdminUsername -SshPublicKeyPath $p.SshPublicKeyPath
		$sshArgs = @($access.SshOpts) + @($access.Target)
		ssh @sshArgs
	}
	'refresh-ip' {
		if ($isPrivate) {
			Write-Host 'Private-network mode: SSH is admitted from the Bastion subnet only; there is no operator-IP rule to refresh.' -ForegroundColor Yellow
			break
		}
		$ip = (Invoke-RestMethod -Uri 'https://api.ipify.org?format=json').ip
		$cidr = "$ip/32"
		Write-Host "Updating NSG AllowSshFromOperator -> $cidr" -ForegroundColor Cyan
		az network nsg rule update -g $rg --nsg-name $nsgName -n AllowSshFromOperator `
			--source-address-prefixes $cidr --output none
		Write-Host 'NSG rule updated.' -ForegroundColor Green
	}
	'logs' {
		$access = Resolve-BenchVmSsh -ResourceGroup $rg -NamePrefix $p.NamePrefix -AdminUsername $p.AdminUsername -SshPublicKeyPath $p.SshPublicKeyPath
		$sshArgs = @($access.SshOpts) + @($access.Target, 'journalctl -fu lattice-silo --output=cat')
		ssh @sshArgs
	}
	'tunnel' {
		if (-not $isPrivate) { throw "Resource group '$rg' is not in private-network mode; ssh the VM directly (./vm.ps1 ssh)." }
		$port = Start-BenchBastionTunnel -ResourceGroup $rg -NamePrefix $p.NamePrefix
		Write-Host "Tunnel listening on 127.0.0.1:$port. Connect with: ssh $($p.NamePrefix)" -ForegroundColor Green
		Write-Host 'Stop it with: ./vm.ps1 tunnel-stop' -ForegroundColor DarkGray
	}
	'tunnel-stop' {
		Stop-BenchBastionTunnel -NamePrefix $p.NamePrefix
	}
}