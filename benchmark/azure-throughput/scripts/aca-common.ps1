<#
	Shared helpers for the multi-silo ("Layer 3") Azure Container Apps rig.

	Dot-sourced by deploy-aca.ps1, run-cohort-aca.ps1 and by
	benchmark/performance-report.ps1's Layer 3 path, so naming, the Azure CLI
	invocation convention, the run-context file format and - most importantly -
	the teardown ownership guard all have exactly one definition.
#>

Set-StrictMode -Version Latest

# Tag stamped on every resource group this rig creates. Invoke-AcaTeardown
# will not delete a group that does not carry it.
$script:AcaRunTagName = 'latticeBenchRun'
$AcaRunTagName = $script:AcaRunTagName

function Get-AcaRunRoot {
	<#
	.SYNOPSIS
		Directory holding per-run artefacts (context file, harvested logs).
	#>
	$root = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path '.run/aca'
	if (-not (Test-Path $root)) { New-Item -ItemType Directory -Path $root -Force | Out-Null }
	return $root
}

function Get-AcaNames {
	<#
	.SYNOPSIS
		Derive every resource name for a run from its prefix.
	.DESCRIPTION
		Storage accounts and container registries accept only lower-case
		alphanumerics and must be globally unique, so they get a sanitised,
		length-clamped form of the prefix. Everything else can carry the
		readable 'kind-prefix' shape.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $NamePrefix
	)
	$flat = ($NamePrefix -replace '[^a-zA-Z0-9]', '').ToLowerInvariant()
	if ($flat.Length -lt 3) { throw "NamePrefix '$NamePrefix' has fewer than 3 alphanumeric characters." }
	if ($flat.Length -gt 16) { $flat = $flat.Substring(0, 16) }

	return [ordered]@{
		Flat        = $flat
		Rg          = "rg-$NamePrefix"
		Storage     = "st$flat"
		Acr         = "acr$flat"
		Workspace   = "log-$NamePrefix"
		Env         = "env-$NamePrefix"
		SiloApp     = "silo-$NamePrefix"
		ProducerJob = "prod-$NamePrefix"
	}
}

function Invoke-Az {
	<#
	.SYNOPSIS
		Run `az` with the supplied argument array and throw on a non-zero exit.
	.DESCRIPTION
		The Azure CLI reports failure through the exit code, not through a
		PowerShell exception, so an unchecked call silently continues after an
		error and the fault surfaces much later as a confusing missing
		resource. Every az call in this rig goes through here.

		-PassthruOutput streams output to the host instead of capturing it,
		for long operations (image builds) whose progress the operator needs
		to see.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string[]] $Args,
		[switch] $PassthruOutput,
		[switch] $AllowFailure
	)
	if ($PassthruOutput) {
		& az @Args
		$code = $LASTEXITCODE
		if ($code -ne 0 -and -not $AllowFailure) { throw "az $($Args -join ' ') failed (exit $code)" }
		return $null
	}
	$out = & az @Args 2>&1
	$code = $LASTEXITCODE
	if ($code -ne 0 -and -not $AllowFailure) {
		throw "az $($Args -join ' ') failed (exit $code): $($out -join [Environment]::NewLine)"
	}
	return ($out -join [Environment]::NewLine)
}

function Test-AzGroupExists {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $Name)
	$r = Invoke-Az @('group', 'exists', '--name', $Name) -AllowFailure
	return ($r.Trim() -eq 'true')
}

function Test-AzResourceExists {
	<#
	.SYNOPSIS
		Existence probe for the handful of resource kinds this rig creates.
	.DESCRIPTION
		Makes every create step idempotent, so a run interrupted part-way can
		be resumed with -ReuseRg rather than restarted from an empty group.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][ValidateSet('storage','acr','acaenv','acaapp','acajob')][string] $Kind,
		[Parameter(Mandatory)][string] $Name,
		[Parameter(Mandatory)][string] $ResourceGroup
	)
	$cmd = switch ($Kind) {
		'storage' { @('storage', 'account', 'show') }
		'acr'     { @('acr', 'show') }
		'acaenv'  { @('containerapp', 'env', 'show') }
		'acaapp'  { @('containerapp', 'show') }
		'acajob'  { @('containerapp', 'job', 'show') }
	}
	Invoke-Az ($cmd + @('--name', $Name, '--resource-group', $ResourceGroup, '-o', 'none')) -AllowFailure | Out-Null
	return ($LASTEXITCODE -eq 0)
}

function Assert-AcaRunGroup {
	<#
	.SYNOPSIS
		Prove a resource group was created by this rig for this prefix.
	.DESCRIPTION
		This is the blast-radius guard, and it is the reason every create path
		tags the group. Resource-group names are derived from an operator-
		supplied prefix, so a typo or a copy-pasted prefix can name a group
		that exists for a completely unrelated purpose. Deleting a group is
		irreversible and takes everything in it, so the rig refuses to act on
		any group that does not carry its own run tag with the matching value.

		Deliberately NOT a "does it look like one of ours" heuristic on the
		name: the whole failure mode being prevented is a name collision.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $ResourceGroup,
		[Parameter(Mandatory)][string] $NamePrefix
	)
	if (-not (Test-AzGroupExists -Name $ResourceGroup)) {
		throw "Resource group $ResourceGroup does not exist."
	}
	$tag = Invoke-Az @(
		'group', 'show', '--name', $ResourceGroup,
		'--query', "tags.$script:AcaRunTagName", '-o', 'tsv'
	) -AllowFailure
	$tag = if ($null -eq $tag) { '' } else { $tag.Trim() }
	if ($tag -ne $NamePrefix) {
		throw ("Refusing to operate on resource group '$ResourceGroup': its '$script:AcaRunTagName' tag is " +
			"'$tag' but this run is '$NamePrefix'. This group was not created by this benchmark run. " +
			'Delete it by hand if you are certain.')
	}
}

function Get-AcaContextPath {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $NamePrefix)
	return Join-Path (Get-AcaRunRoot) "$NamePrefix.context.json"
}

function Save-AcaContext {
	[CmdletBinding()] param([Parameter(Mandatory)][System.Collections.IDictionary] $Context)
	$path = Get-AcaContextPath -NamePrefix $Context.namePrefix
	$Context | ConvertTo-Json -Depth 8 | Set-Content -Path $path -Encoding utf8
	return $path
}

function Read-AcaContext {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $NamePrefix)
	$path = Get-AcaContextPath -NamePrefix $NamePrefix
	if (-not (Test-Path $path)) { throw "No ACA run context at $path. Run deploy-aca.ps1 first." }
	$raw = Get-Content -Path $path -Raw | ConvertFrom-Json
	$ht = @{}
	foreach ($p in $raw.PSObject.Properties) { $ht[$p.Name] = $p.Value }
	return $ht
}

function Set-AcaSiloCount {
	<#
	.SYNOPSIS
		Pin the silo app to exactly N replicas and wait for them to be running.
	.DESCRIPTION
		min = max with no scale rule, so the platform holds the count steady
		for the whole cohort instead of drifting under load - the silo count
		IS the independent variable of this experiment, so it must not move
		while a cohort is measured.

		N = 0 is the between-cohort resting state. Leaving replicas up between
		cohorts is billed compute doing no work, so the caller scales to zero
		the moment a cohort's measurement is harvested.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][System.Collections.IDictionary] $Context,
		[Parameter(Mandatory)][ValidateRange(0, 30)][int] $Count,
		[int] $TimeoutSec = 600
	)
	Write-Host "[aca] scaling $($Context.siloApp) to $Count replica(s)" -ForegroundColor Cyan
	Invoke-Az @(
		'containerapp', 'update',
		'--name', $Context.siloApp,
		'--resource-group', $Context.resourceGroup,
		'--min-replicas', "$Count",
		'--max-replicas', "$Count",
		'-o', 'none'
	) | Out-Null

	if ($Count -eq 0) {
		Wait-AcaSiloReplicas -Context $Context -Expected 0 -TimeoutSec $TimeoutSec | Out-Null
		return
	}
	return Wait-AcaSiloReplicas -Context $Context -Expected $Count -TimeoutSec $TimeoutSec
}

function Wait-AcaSiloReplicas {
	<#
	.SYNOPSIS
		Block until the silo app reports exactly the expected running replica
		count, and return their pod IPs.
	.DESCRIPTION
		Starting the producer before every silo has joined the cluster would
		measure a partially-formed cluster and attribute the shortfall to the
		topology, which is precisely the variable under test. Waiting for the
		platform's replica list is necessary but not sufficient - the caller
		additionally waits for cluster membership - so this is the first of
		two gates, not the only one.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][System.Collections.IDictionary] $Context,
		[Parameter(Mandatory)][int] $Expected,
		[int] $TimeoutSec = 600
	)
	$deadline = (Get-Date).AddSeconds($TimeoutSec)
	$lastSeen = -1
	while ((Get-Date) -lt $deadline) {
		$json = Invoke-Az @(
			'containerapp', 'replica', 'list',
			'--name', $Context.siloApp,
			'--resource-group', $Context.resourceGroup,
			'-o', 'json'
		) -AllowFailure
		$running = @()
		if ($LASTEXITCODE -eq 0 -and $json -and $json.Trim()) {
			try {
				$replicas = $json | ConvertFrom-Json
				$running = @($replicas | Where-Object { $_.properties.runningState -eq 'Running' })
			} catch { $running = @() }
		}
		if ($running.Count -ne $lastSeen) {
			Write-Host "[aca]   replicas running: $($running.Count)/$Expected" -ForegroundColor DarkGray
			$lastSeen = $running.Count
		}
		if ($running.Count -eq $Expected) { return @($running | ForEach-Object { $_.name }) }
		Start-Sleep -Seconds 5
	}
	throw "Timed out after ${TimeoutSec}s waiting for $($Context.siloApp) to report $Expected running replica(s) (last saw $lastSeen)."
}

function Invoke-AcaTeardown {
	<#
	.SYNOPSIS
		Delete a run's resource group, but only after proving the rig owns it.
	.DESCRIPTION
		Two-step on purpose. The ownership assertion runs first and throws on
		any mismatch, so a wrong prefix fails loudly and deletes nothing. Only
		then is the group deleted.

		The delete is NOT --no-wait by default: an unattended sweep that fires
		a delete and exits cannot tell a queued delete from a refused one, and
		"no containers left running" is a guarantee the caller has to be able
		to verify. Pass -NoWait when a human is watching.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $NamePrefix,
		[switch] $NoWait
	)
	$rg = "rg-$NamePrefix"
	if (-not (Test-AzGroupExists -Name $rg)) {
		Write-Host "[aca] teardown: $rg already gone" -ForegroundColor DarkGray
		return
	}
	Assert-AcaRunGroup -ResourceGroup $rg -NamePrefix $NamePrefix

	Write-Host "[aca] teardown: deleting $rg (ownership tag verified)" -ForegroundColor Cyan
	$args = @('group', 'delete', '--name', $rg, '--yes')
	if ($NoWait) { $args += '--no-wait' }
	Invoke-Az $args -AllowFailure -PassthruOutput

	if (-not $NoWait) {
		if (Test-AzGroupExists -Name $rg) {
			Write-Warning "[aca] teardown: $rg still exists after delete returned. Verify manually: az group delete --name $rg --yes"
		} else {
			Write-Host "[aca] teardown: $rg deleted" -ForegroundColor Green
		}
	}
}

function Assert-NoStrayBenchContainers {
	<#
	.SYNOPSIS
		Report any container app in the subscription that belongs to this rig.
	.DESCRIPTION
		The end-of-run safety net. Billed compute left running after an
		interrupted sweep is invisible until the invoice arrives, so the sweep
		asserts the steady state it expects - zero rig container apps - rather
		than assuming its own teardown worked.

		Scoped by the run tag, so a container app belonging to something else
		entirely is reported as untouched context and never acted upon.
	#>
	[CmdletBinding()] param([string] $NamePrefix)
	$json = Invoke-Az @('containerapp', 'list', '-o', 'json') -AllowFailure
	if ($LASTEXITCODE -ne 0 -or -not $json -or -not $json.Trim()) { return @() }
	try { $apps = @($json | ConvertFrom-Json) } catch { return @() }
	$stray = @()
	foreach ($a in $apps) {
		$rg = $a.resourceGroup
		if ($NamePrefix -and $rg -ne "rg-$NamePrefix") { continue }
		$stray += [pscustomobject]@{ Name = $a.name; ResourceGroup = $rg }
	}
	if ($stray.Count -gt 0) {
		Write-Warning "[aca] $($stray.Count) container app(s) still present: $($stray.Name -join ', ')"
	} else {
		Write-Host '[aca] verified: no rig container apps running' -ForegroundColor Green
	}
	return $stray
}
