#!/usr/bin/env pwsh
#requires -Version 7.0
<#
.SYNOPSIS
	Offline regressions for how the Layer 3 ACA rig survives transient az
	faults on its non-idempotent calls and its park: the update and job-start
	reconcile (Invoke-AcaSiloUpdate, Start-AcaJobExecution) and the drain wait
	(Wait-AcaSiloDrained). az and Start-Sleep are stubbed; needs no Azure.

.DESCRIPTION
	The incident these pin: a connection reset (WinError 10054) hit the CLI's
	status poll after `containerapp update` had been accepted, so az exited
	non-zero while the revision was minted and came up. The cohort aborted a
	healthy cluster, and its park then timed out on a drain that was merely
	slow.

.EXAMPLE
	pwsh -File benchmark/azure-throughput/scripts/Test-AcaTransientReconcile.ps1
#>
[CmdletBinding()] param()
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$passed = 0
function Assert-Case([string] $Name, [bool] $Condition) {
	if (-not $Condition) { throw "FAIL: $Name" }
	$script:passed++
	Write-Host "PASS: $Name"
}

$tokens = $null; $errors = $null
$common = [System.Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot 'aca-common.ps1'), [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) { throw ($errors -join "`n") }
foreach ($fn in $common.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
	. ([scriptblock]::Create($fn.Extent.Text))
}

# az and Start-Sleep are shadowed by functions, which PowerShell resolves
# before the executable and the cmdlet. Each case installs $script:handler,
# which receives the joined argument line and returns @{ Code; Out }.
$script:calls = [System.Collections.Generic.List[string]]::new()
$script:handler = $null
function az {
	$line = $args -join ' '
	$script:calls.Add($line)
	$r = & $script:handler $line
	$global:LASTEXITCODE = $r.Code
	return $r.Out
}
function Start-Sleep { param([int] $Seconds) }
function Reset-Az([scriptblock] $Handler) { $script:calls.Clear(); $script:handler = $Handler }
function Get-CallCount([string] $Pattern) { @($script:calls | Where-Object { $_ -match $Pattern }).Count }
function Ok($out) { @{ Code = 0; Out = $out } }

$ctx = @{ siloApp = 'silo-t'; producerJob = 'prod-t'; resourceGroup = 'rg-t' }
$reset = @{ Code = 1; Out = @(
		"ERROR: ('Connection aborted.', ConnectionResetError(10054, 'An existing connection was forcibly closed by the remote host', None, 10054, None))"
	) }
$updateArgs = @('containerapp', 'update', '--name', 'silo-t', '--resource-group', 'rg-t', '--min-replicas', '1', '--max-replicas', '1', '-o', 'none')
function State([string] $Rev, [string] $State = 'Succeeded', [int] $Min = 0, [int] $Max = 0) {
	Ok (@{ rev = $Rev; state = $State; min = $Min; max = $Max } | ConvertTo-Json -Compress)
}

# --- classifier --------------------------------------------------------------
Assert-Case 'a 10054 connection reset is transient' (Test-AzTransientFailure -Output $reset.Out)
Assert-Case 'a bare WinError 10054 is transient' (Test-AzTransientFailure -Output @('[WinError 10054] An existing connection was forcibly closed by the remote host'))
Assert-Case 'a not-found error is not transient' (-not (Test-AzTransientFailure -Output @('ERROR: (ResourceNotFound) The Resource was not found.')))

# --- update: landed behind a status-poll fault (the observed incident) --------
$script:updated = $false
Reset-Az {
	param($l)
	if ($l -match '^containerapp update') { $script:updated = $true; return $reset }
	if ($l -match '^containerapp show') { if ($script:updated) { return State 'silo-t--0000014' 'Succeeded' 1 1 } else { return State 'silo-t--0000013' 'Succeeded' 0 0 } }
	throw "unexpected az $l"
}
Invoke-AcaSiloUpdate -Context $ctx -UpdateArgs $updateArgs -Count 1 3>$null
Assert-Case 'an update that landed behind a connection reset is not re-issued' ((Get-CallCount '^containerapp update') -eq 1)

# --- update: still provisioning, then landed ----------------------------------
$script:shows = 0
Reset-Az {
	param($l)
	if ($l -match '^containerapp update') { return $reset }
	if ($l -match '^containerapp show') {
		$script:shows++
		if ($script:shows -eq 1) { return State 'r1' }
		if ($script:shows -le 4) { return State 'r1' 'InProgress' }
		return State 'r2' 'Succeeded' 1 1
	}
	throw "unexpected az $l"
}
Invoke-AcaSiloUpdate -Context $ctx -UpdateArgs $updateArgs -Count 1 3>$null
Assert-Case 'an in-progress update is waited out, not re-issued' ((Get-CallCount '^containerapp update') -eq 1 -and $script:shows -eq 5)

# --- update: provably not landed, re-issued once ------------------------------
$script:updates = 0
Reset-Az {
	param($l)
	if ($l -match '^containerapp update') { $script:updates++; if ($script:updates -eq 1) { return $reset } else { return Ok @() } }
	if ($l -match '^containerapp show') { return State 'r1' 'Succeeded' 0 0 }
	throw "unexpected az $l"
}
Invoke-AcaSiloUpdate -Context $ctx -UpdateArgs $updateArgs -Count 1 3>$null
Assert-Case 'an update that settled without a new revision is re-issued once' ($script:updates -eq 2)

# --- update: no-op update (already pinned, revision active) -------------------
Reset-Az {
	param($l)
	if ($l -match '^containerapp update') { return $reset }
	if ($l -match '^containerapp show') { return State 'r1' 'Succeeded' 1 1 }
	if ($l -match '^containerapp revision show') { return Ok 'true' }
	throw "unexpected az $l"
}
Invoke-AcaSiloUpdate -Context $ctx -UpdateArgs $updateArgs -Count 1 3>$null
Assert-Case 'an update whose target state already holds is treated as landed' ((Get-CallCount '^containerapp update') -eq 1)

# --- update: non-transient failure throws without a re-issue ------------------
Reset-Az {
	param($l)
	if ($l -match '^containerapp update') { return @{ Code = 1; Out = @('ERROR: (InvalidParameterValue) bad env') } }
	if ($l -match '^containerapp show') { return State 'r1' }
	throw "unexpected az $l"
}
$threw = $false
try { Invoke-AcaSiloUpdate -Context $ctx -UpdateArgs $updateArgs -Count 1 3>$null } catch { $threw = $_.Exception.Message -match 'InvalidParameterValue' }
Assert-Case 'a non-transient update failure throws at once' ($threw -and (Get-CallCount '^containerapp update') -eq 1)

# --- update: every attempt fails transiently and none lands -------------------
Reset-Az {
	param($l)
	if ($l -match '^containerapp update') { return $reset }
	if ($l -match '^containerapp show') { return State 'r1' 'Succeeded' 0 0 }
	throw "unexpected az $l"
}
$threw = $false
try { Invoke-AcaSiloUpdate -Context $ctx -UpdateArgs $updateArgs -Count 1 -MaxAttempts 2 3>$null } catch { $threw = $_.Exception.Message -match 'did not land after 2' }
Assert-Case 'the update stops after its attempt budget' ($threw -and (Get-CallCount '^containerapp update') -eq 2)

# --- update: unknown outcome is not guessed -----------------------------------
Reset-Az {
	param($l)
	if ($l -match '^containerapp update') { return $reset }
	if ($l -match '^containerapp show') { return State 'r1' 'InProgress' }
	throw "unexpected az $l"
}
$threw = $false
try { Invoke-AcaSiloUpdate -Context $ctx -UpdateArgs $updateArgs -Count 1 -ReconcileTimeoutSec 0 3>$null } catch { $threw = $_.Exception.Message -match 'could not be determined' }
Assert-Case 'an undeterminable update outcome throws rather than retrying blind' ($threw -and (Get-CallCount '^containerapp update') -eq 1)

# --- job start: landed behind a fault -----------------------------------------
$script:started = $false
Reset-Az {
	param($l)
	if ($l -match '^containerapp job start') { $script:started = $true; return $reset }
	if ($l -match '^containerapp job execution list') { if ($script:started) { return Ok @('prod-t-old', 'prod-t-new') } else { return Ok @('prod-t-old') } }
	throw "unexpected az $l"
}
$exec = Start-AcaJobExecution -Context $ctx 3>$null
Assert-Case 'a job start that landed behind a fault returns its execution without a second start' ($exec -eq 'prod-t-new' -and (Get-CallCount '^containerapp job start') -eq 1)

# --- job start: clean success --------------------------------------------------
Reset-Az {
	param($l)
	if ($l -match '^containerapp job start') { return Ok '{"name":"prod-t-x","id":"/x"}' }
	if ($l -match '^containerapp job execution list') { return Ok @() }
	throw "unexpected az $l"
}
Assert-Case 'a clean job start returns the execution it names' ((Start-AcaJobExecution -Context $ctx) -eq 'prod-t-x')

# --- job start: not landed, re-issued -----------------------------------------
$script:starts = 0
Reset-Az {
	param($l)
	if ($l -match '^containerapp job start') { $script:starts++; if ($script:starts -eq 1) { return $reset } else { return Ok '{"name":"prod-t-2"}' } }
	if ($l -match '^containerapp job execution list') { return Ok @('prod-t-old') }
	throw "unexpected az $l"
}
$exec = Start-AcaJobExecution -Context $ctx -ReconcileTimeoutSec 0 3>$null
Assert-Case 'a job start that provably did not land is re-issued' ($exec -eq 'prod-t-2' -and $script:starts -eq 2)

# --- job start: ambiguous ----------------------------------------------------
$script:started = $false
Reset-Az {
	param($l)
	if ($l -match '^containerapp job start') { $script:started = $true; return $reset }
	if ($l -match '^containerapp job execution list') { if ($script:started) { return Ok @('a', 'b', 'c') } else { return Ok @('a') } }
	throw "unexpected az $l"
}
$threw = $false
try { Start-AcaJobExecution -Context $ctx 3>$null | Out-Null } catch { $threw = $_.Exception.Message -match 'ambiguous' }
Assert-Case 'two new executions after a fault are reported, not picked between' ($threw -and (Get-CallCount '^containerapp job start') -eq 1)

# --- drain: a revision that turns active mid-park is deactivated again ---------
$script:replicaPolls = 0
$script:deactivated = $false
Reset-Az {
	param($l)
	if ($l -match '^containerapp replica list') {
		$script:replicaPolls++
		if ($script:deactivated) { return Ok '[]' }
		return Ok '[{"properties":{"runningState":"Running"}}]'
	}
	if ($l -match '^containerapp revision list') { return Ok 'silo-t--0000014' }
	if ($l -match '^containerapp revision deactivate.*silo-t--0000014') { $script:deactivated = $true; return Ok @() }
	throw "unexpected az $l"
}
Wait-AcaSiloDrained -Context $ctx -TimeoutSec 60 -RevisitEvery 2 3>$null
Assert-Case 'the drain re-deactivates a late-active revision and then completes' ($script:deactivated -and (Get-CallCount '^containerapp revision deactivate') -eq 1)

# --- drain: slow but every revision inactive gets the grace -------------------
$script:replicaPolls = 0
Reset-Az {
	param($l)
	if ($l -match '^containerapp replica list') {
		$script:replicaPolls++
		if ($script:replicaPolls -ge 3) { return Ok '[]' }
		return Ok '[{"properties":{"runningState":"Running"}}]'
	}
	if ($l -match '^containerapp revision list') { return Ok '' }
	throw "unexpected az $l"
}
$warnings = $null
Wait-AcaSiloDrained -Context $ctx -TimeoutSec 0 -DrainGraceSec 60 -RevisitEvery 1 -WarningVariable warnings 3>$null
Assert-Case 'a slow drain with every revision inactive is granted grace instead of failing' ($script:replicaPolls -eq 3 -and ($warnings -join ' ') -match 'waiting up to 60s more')

# --- drain: a revision still active at the deadline fails ---------------------
Reset-Az {
	param($l)
	if ($l -match '^containerapp replica list') { return Ok '[{"properties":{"runningState":"Running"}}]' }
	if ($l -match '^containerapp revision list') { return Ok 'stuck-rev' }
	if ($l -match '^containerapp revision deactivate') { return Ok @() }
	throw "unexpected az $l"
}
$threw = $false
try { Wait-AcaSiloDrained -Context $ctx -TimeoutSec 0 -DrainGraceSec 60 -RevisitEvery 1 3>$null } catch { $threw = $_.Exception.Message -match 'Timed out' }
Assert-Case 'a drain with a revision still active gets no grace' $threw

# --- park listing stays fail-closed -------------------------------------------
Reset-Az {
	param($l)
	if ($l -match '^containerapp revision list') { return $reset }
	throw "unexpected az $l"
}
$threw = $false
try { Get-AcaActiveSiloRevisionNames -Context $ctx -Purpose 'parking' 3>$null | Out-Null } catch { $threw = $_.Exception.Message -match 'Could not enumerate active revisions' }
Assert-Case 'an unreadable revision list throws instead of reading as empty' $threw
Assert-Case 'the revision list retries transient faults before giving up' ((Get-CallCount '^containerapp revision list') -eq 9)

Remove-Item Function:\az, Function:\Start-Sleep
Write-Host "All $passed transient-reconcile cases passed." -ForegroundColor Green
