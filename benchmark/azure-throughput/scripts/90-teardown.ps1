#requires -Version 7
<#
.SYNOPSIS
    Deletes the entire resource group provisioned by 10-provision.ps1.
.DESCRIPTION
    Irreversible. The script prompts for confirmation unless -Force is supplied.
#>

[CmdletBinding()]
param(
    [switch] $Force
)

$ErrorActionPreference = 'Stop'

$ctxPath = Join-Path $PSScriptRoot '.context.json'
if (-not (Test-Path $ctxPath)) {
    throw "Missing $ctxPath; nothing to tear down."
}
$ctx = Get-Content $ctxPath | ConvertFrom-Json

if (-not $Force) {
    $confirm = Read-Host "Delete resource group '$($ctx.ResourceGroup)' and all contents? Type 'yes' to proceed"
    if ($confirm -ne 'yes') {
        Write-Host "[teardown] aborted." -ForegroundColor Yellow
        return
    }
}

Write-Host "[teardown] deleting $($ctx.ResourceGroup) (no-wait) ..." -ForegroundColor Cyan
az group delete --name $ctx.ResourceGroup --yes --no-wait | Out-Host

Remove-Item $ctxPath -Force -ErrorAction SilentlyContinue
$yaml = Join-Path $PSScriptRoot '.aci-deploy.yaml'
Remove-Item $yaml -Force -ErrorAction SilentlyContinue

Write-Host "[teardown] queued. Verify with 'az group show -n $($ctx.ResourceGroup)' (expects NotFound)." -ForegroundColor Green
