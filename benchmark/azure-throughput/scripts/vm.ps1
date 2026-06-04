#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Start / stop / status / ssh / refresh-ip helper for the lattice-wedge VM.

.PARAMETER Action
	start    - deallocate -> running
	stop     - deallocate (no compute charges)
	status   - print power state, public IP, SSH command
	ssh      - open an interactive SSH session
	refresh-ip - update NSG rule to your current public IP (run from a new network)

.EXAMPLE
	./vm.ps1 start
	./vm.ps1 status
	./vm.ps1 ssh
	./vm.ps1 stop
	./vm.ps1 refresh-ip
#>
[CmdletBinding()]
param(
	[Parameter(Mandatory, Position = 0)]
	[ValidateSet('start','stop','status','ssh','refresh-ip','logs')]
	[string] $Action,

	[string] $ParametersFile,
	[string] $NamePrefix
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
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
		$ip = Get-PublicIp
		$fqdn = Get-Fqdn
		Write-Host "Started. ssh $($p.AdminUsername)@$fqdn   ($ip)" -ForegroundColor Green
	}
	'stop' {
		Write-Host "Deallocating $vmName ..." -ForegroundColor Cyan
		az vm deallocate -g $rg -n $vmName --output none
		Write-Host 'Stopped (deallocated; no compute charges).' -ForegroundColor Green
	}
	'status' {
		$state = Get-PowerState
		$ip = Get-PublicIp
		$fqdn = Get-Fqdn
		$rule = az network nsg rule show -g $rg --nsg-name $nsgName -n AllowSshFromOperator --query sourceAddressPrefix -o tsv
		Write-Host ("VM           : {0}" -f $vmName)
		Write-Host ("State        : {0}" -f $state)
		Write-Host ("Public IP    : {0}" -f $ip)
		Write-Host ("FQDN         : {0}" -f $fqdn)
		Write-Host ("SSH allowed  : {0}" -f $rule)
		Write-Host ("Connect      : ssh {0}@{1}" -f $p.AdminUsername, $fqdn)
	}
	'ssh' {
		$fqdn = Get-Fqdn
		ssh "$($p.AdminUsername)@$fqdn"
	}
	'refresh-ip' {
		$ip = (Invoke-RestMethod -Uri 'https://api.ipify.org?format=json').ip
		$cidr = "$ip/32"
		Write-Host "Updating NSG AllowSshFromOperator -> $cidr" -ForegroundColor Cyan
		az network nsg rule update -g $rg --nsg-name $nsgName -n AllowSshFromOperator `
			--source-address-prefixes $cidr --output none
		Write-Host 'NSG rule updated.' -ForegroundColor Green
	}
	'logs' {
		$fqdn = Get-Fqdn
		ssh "$($p.AdminUsername)@$fqdn" 'journalctl -fu lattice-silo --output=cat'
	}
}
