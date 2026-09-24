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
		[Parameter(Mandatory)][string[]] $AzArgs,
		[switch] $PassthruOutput,
		[switch] $AllowFailure
	)
	if ($PassthruOutput) {
		& az @AzArgs
		$code = $LASTEXITCODE
		if ($code -ne 0 -and -not $AllowFailure) { throw "az $($AzArgs -join ' ') failed (exit $code)" }
		return $null
	}
	$out = & az @AzArgs 2>&1
	$code = $LASTEXITCODE
	if ($code -ne 0 -and -not $AllowFailure) {
		throw "az $($AzArgs -join ' ') failed (exit $code): $($out -join [Environment]::NewLine)"
	}
	# Drop the CLI's own diagnostic chatter before returning. `az containerapp`
	# unconditionally prints
	#     WARNING: The behavior of this command has been altered by the
	#     following extension: containerapp
	# on the SAME stream we capture, so a `--query ... -o tsv` call that should
	# yield one bare value yields the warning line followed by the value. That
	# is not a cosmetic problem: the caller does `.Trim()` and stores the
	# result, and the warning silently becomes the "value" - an environment id
	# that is actually a sentence, persisted into the run context and only
	# discovered when something downstream tries to use it. Strip here, once,
	# rather than at each of the couple of dozen call sites.
	$clean = @($out | Where-Object {
		$line = [string]$_
		$line -notmatch '^\s*WARNING:\s' -and $line -notmatch '^\s*$'
	})
	if (@($out).Count -gt 0 -and $clean.Count -eq 0) { return '' }
	return ($clean -join [Environment]::NewLine)
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
		[Parameter(Mandatory)][ValidateSet('storage','acr','acaenv','acaapp','acajob','workspace')][string] $Kind,
		[Parameter(Mandatory)][string] $Name,
		[Parameter(Mandatory)][string] $ResourceGroup
	)
	# Log Analytics is the odd one out: its show verb takes --workspace-name,
	# not --name, so it cannot share the common tail below.
	if ($Kind -eq 'workspace') {
		Invoke-Az @('monitor', 'log-analytics', 'workspace', 'show',
			'--workspace-name', $Name, '--resource-group', $ResourceGroup, '-o', 'none') -AllowFailure | Out-Null
		return ($LASTEXITCODE -eq 0)
	}
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
		Pin the silo app to exactly N replicas, or park it at zero.
	.DESCRIPTION
		min = max with no scale rule, so the platform holds the count steady
		for the whole cohort instead of drifting under load - the silo count
		IS the independent variable of this experiment, so it must not move
		while a cohort is measured.

		N = 0 is the between-cohort resting state, and reaching a true zero
		takes a different verb. `--min-replicas 0` does NOT empty a rule-less
		app: ACA has nothing to scale on, so it holds one replica
		indefinitely. `--max-replicas 0` is rejected outright ("must be in the
		range [1,1000]"), and this CLI build has no `containerapp stop`. The
		mechanism that does work is deactivating the active revision, which
		reports runningState=Stopped with replicas=0 and bills nothing.

		Coming back up is free of extra ceremony because every cohort sets its
		own env vars first, and `--set-env-vars` mints a fresh active revision
		- so the deactivated one is never reactivated, just superseded.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][System.Collections.IDictionary] $Context,
		[Parameter(Mandatory)][ValidateRange(0, 30)][int] $Count,
		[string[]] $EnvVars = @(),
		[int] $TimeoutSec = 600
	)

	if ($Count -eq 0) {
		Write-Host "[aca] parking $($Context.siloApp) at zero replicas" -ForegroundColor Cyan
		# Fail-closed listing. An empty result here is ambiguous: it means
		# either "already parked" or "the list call failed and we are about to
		# deactivate nothing". Those are opposite situations and the second one
		# silently leaves billable replicas running, which is exactly the
		# failure this function exists to prevent - observed once as a 300s
		# scale-to-zero timeout that left 2 replicas up with no explanation.
		# So distinguish them by asking the platform for the replica count,
		# and only treat an empty active-revision list as success when the app
		# genuinely has no replicas.
		$active = @()
		$listOk = $false
		for ($attempt = 1; $attempt -le 3 -and -not $listOk; $attempt++) {
			$tsv = Invoke-Az @(
				'containerapp', 'revision', 'list',
				'--name', $Context.siloApp,
				'--resource-group', $Context.resourceGroup,
				'--all',
				'--query', "[?properties.active].name",
				'-o', 'tsv'
			) -AllowFailure
			if ($LASTEXITCODE -eq 0) {
				$listOk = $true
				$active = @(($tsv -split "`r?`n") | ForEach-Object { $_.Trim() } | Where-Object { $_ })
			} else {
				Write-Warning "[aca] revision list failed (attempt $attempt/3) while parking $($Context.siloApp); retrying"
				Start-Sleep -Seconds 5
			}
		}
		if (-not $listOk) {
			throw "Could not enumerate active revisions for $($Context.siloApp) while parking it at zero. Replicas may still be running and billing; check: az containerapp revision list -n $($Context.siloApp) -g $($Context.resourceGroup) --all"
		}

		foreach ($rev in $active) {
			Write-Host "[aca]   deactivating revision $rev" -ForegroundColor DarkGray
			Invoke-Az @(
				'containerapp', 'revision', 'deactivate',
				'--name', $Context.siloApp,
				'--resource-group', $Context.resourceGroup,
				'--revision', $rev,
				'-o', 'none'
			) -AllowFailure | Out-Null
		}
		if ($active.Count -eq 0) {
			Write-Host "[aca]   no active revisions to deactivate" -ForegroundColor DarkGray
		}
		Wait-AcaSiloReplicas -Context $Context -Expected 0 -TimeoutSec $TimeoutSec | Out-Null
		return
	}

	Write-Host "[aca] scaling $($Context.siloApp) to $Count replica(s)" -ForegroundColor Cyan
	# ONE update, carrying both the env vars and the replica pinning.
	#
	# This must not be split into two `az containerapp update` calls. Every
	# update mints a NEW revision, and the app runs in Single revision mode,
	# so a second update supersedes the first: the replicas that had already
	# started, joined the clustering table and formed a cluster are torn down
	# and replaced. Observed directly - a cohort produced revisions
	# --0000004 (env) and --0000005 (scale) 2.5 minutes apart, so the cluster
	# the cohort waited for was destroyed immediately after it formed, and
	# the producer then measured whatever happened to be up.
	#
	# Combining them means exactly one revision per cohort, which is also
	# what makes the revision list a faithful audit log of the sweep.
	$updateArgs = @(
		'containerapp', 'update',
		'--name', $Context.siloApp,
		'--resource-group', $Context.resourceGroup
	)
	if ($EnvVars -and $EnvVars.Count -gt 0) {
		$updateArgs += '--set-env-vars'
		$updateArgs += $EnvVars
	}
	$updateArgs += @('--min-replicas', "$Count", '--max-replicas', "$Count", '-o', 'none')
	Invoke-Az $updateArgs | Out-Null

	$replicas = Wait-AcaSiloReplicas -Context $Context -Expected $Count -TimeoutSec $TimeoutSec

	# Retire every superseded revision before handing the cluster to the
	# caller.
	#
	# Single revision mode deactivates the previous revision on its own, but
	# it does so LAZILY: the old replicas keep running while they drain, and
	# ACA gives no completion signal. Every cohort so far logged "2 active
	# revisions" at this point, which means the measurement window could open
	# with up to 2N silos alive instead of N - all of them pointed at the one
	# shared Azure Storage account.
	#
	# That is not merely untidy, it is a measurement and reliability defect.
	# It caused an intermittent hard failure: with double the silos competing
	# for the per-silo WAL replay permit queue, the incoming cohort's warm-up
	# was refused admission ("the per-silo WAL replay permit queue already
	# holds 17 admitted waiter(s) ... the smoothed queue wait is 23527 ms")
	# and the producer aborted after exhausting its 12 retries. It presented
	# as flakiness because it depends purely on how fast the previous
	# revision happened to drain.
	#
	# So make the hand-over explicit and synchronous: deactivate every
	# revision except the newest, then wait for the replica count to settle
	# back to exactly $Count. Deactivation is best-effort per revision (one
	# already-deactivating revision must not abort a sweep), but the
	# subsequent wait is authoritative.
	#
	# "Newest" is decided by properties.createdTime, NOT by sorting the
	# revision names. A revision name is `<app>--<suffix>`, and ACA mints
	# the suffix two different ways in the same app: a zero-padded ordinal
	# (`--0000001`) when it names the revision itself, and a random string
	# (`--aux0gii`) otherwise. Those do not share an ordering, so an
	# alphabetical sort is not a recency order and can rank an older
	# revision last. That is not hypothetical - it stalled a sweep here:
	# `--aux0gii` (19:56:38) sorted above the newer `--0000001` (19:57:18),
	# so the live revision was retired and the stale one kept, the app was
	# left with zero active revisions, and the wait below polled for a
	# replica count that could never arrive.
	$activeTsv = Invoke-Az @(
		'containerapp', 'revision', 'list',
		'--name', $Context.siloApp,
		'--resource-group', $Context.resourceGroup,
		'--all',
		'--query', "[?properties.active].[name,properties.createdTime]",
		'-o', 'tsv'
	) -AllowFailure
	if ($LASTEXITCODE -eq 0 -and $activeTsv) {
		$active = @(($activeTsv -split "`r?`n") |
			ForEach-Object { $_.Trim() } |
			Where-Object { $_ } |
			ForEach-Object {
				$parts = $_ -split "`t"
				$created = [datetime]::MinValue
				if ($parts.Count -gt 1) { [void][datetime]::TryParse($parts[1], [ref]$created) }
				[pscustomobject]@{ Name = $parts[0].Trim(); Created = $created }
			} |
			Sort-Object Created)
		if ($active.Count -gt 1) {
			$keep = $active[-1].Name
			foreach ($rev in $active) {
				if ($rev.Name -eq $keep) { continue }
				Write-Host "[aca]   retiring superseded revision $($rev.Name)" -ForegroundColor DarkGray
				Invoke-Az @(
					'containerapp', 'revision', 'deactivate',
					'--name', $Context.siloApp,
					'--resource-group', $Context.resourceGroup,
					'--revision', $rev.Name,
					'-o', 'none'
				) -AllowFailure | Out-Null
			}
			$replicas = Wait-AcaSiloReplicas -Context $Context -Expected $Count -TimeoutSec $TimeoutSec
		}
	} else {
		Write-Warning "[aca] could not enumerate revisions after scaling $($Context.siloApp); a superseded revision may still be draining into this cohort."
	}

	return $replicas
}

function Reset-AcaBenchTables {
	<#
	.SYNOPSIS
		Delete every table in the rig's storage account except the ones named
		in -Keep, so the next cohort starts against empty storage.
	.DESCRIPTION
		Each cohort mints a fresh tree, but that tree used to land in the same
		WAL and grain-state tables as every tree before it. The old trees
		never go away: their registry entries and grain state stay in the
		shared grain-state table, so every new cluster enumerates them and
		runs background work against them (WAL GC floor retries, the storage
		usage fan-out) during the measured window. With about 17-19 such trees
		accumulated, that work correlated with the multi-second stalls
		tracked in #3458, and the effect grew from cohort to cohort.

		Deletion runs through the ARM management plane
		(Microsoft.Storage/.../tableServices/default/tables), so the operator
		needs no data-plane role on the account. It MUST only run while the
		silos are parked: deleting a table under a live silo produces
		arbitrary faults rather than a clean slate. The caller guarantees that.

		Azure Tables refuses to recreate a just-deleted name for a while
		(TableBeingDeleted), so the caller is expected to point the next
		cohort at fresh table names rather than the ones deleted here.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][System.Collections.IDictionary] $Context,
		[string[]] $Keep = @('OrleansSiloInstances')
	)
	$sub = (Invoke-Az @('account', 'show', '--query', 'id', '-o', 'tsv')).Trim()
	$base = "https://management.azure.com/subscriptions/$sub/resourceGroups/$($Context.resourceGroup)/providers/Microsoft.Storage/storageAccounts/$($Context.storage)/tableServices/default/tables"
	$api = 'api-version=2023-05-01'
	$listJson = Invoke-Az @('rest', '--method', 'get', '--url', "${base}?$api", '-o', 'json')
	$names = @(($listJson | ConvertFrom-Json).value | ForEach-Object { [string]$_.name })
	$doomed = @($names | Where-Object { $_ -notin $Keep })
	foreach ($n in $doomed) {
		Invoke-Az @('rest', '--method', 'delete', '--url', "$base/${n}?$api", '-o', 'none') | Out-Null
	}
	Write-Host "[aca] storage reset: deleted $($doomed.Count) table(s), kept $(@($names | Where-Object { $_ -in $Keep }).Count)" -ForegroundColor DarkGray
	return $doomed
}

function Get-AcaActiveSiloRevision {
	<#
	.SYNOPSIS
		Return the name of the silo app's single active revision.
	.DESCRIPTION
		Because Set-AcaSiloCount issues exactly one `az containerapp update`
		per cohort, and the app runs in Single revision mode, the active
		revision is a one-to-one label for the cohort's silos. That makes it
		the strongest available filter for the silo log harvest: superseded
		revisions from earlier cohorts remain in Log Analytics indefinitely
		and emit the same line shapes, and neither a time window nor an app
		name can separate them. A revision name can.

		Returns $null if no revision is active (the parked state), which the
		caller should treat as "nothing to harvest" rather than an error.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][System.Collections.IDictionary] $Context
	)
	$json = Invoke-Az @(
		'containerapp', 'revision', 'list',
		'--name', $Context.siloApp,
		'--resource-group', $Context.resourceGroup,
		'--query', "[?properties.active].name",
		'-o', 'json'
	) -AllowFailure
	if ($LASTEXITCODE -ne 0 -or -not $json -or -not $json.Trim()) { return $null }
	try { $active = @($json | ConvertFrom-Json) } catch { return $null }
	$active = @($active | Where-Object { $_ })
	if ($active.Count -eq 0) { return $null }
	if ($active.Count -gt 1) {
		# Single revision mode should make this impossible. If it happens the
		# cohort's silo set is ambiguous, so say so rather than silently
		# harvesting one arbitrary revision's histograms.
		Write-Warning "[aca] $($Context.siloApp) has $($active.Count) active revisions ($($active -join ', ')); expected exactly 1. Harvesting the newest."
	}
	return @($active | Sort-Object)[-1]
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

function Wait-AcaJobExecution {
	<#
	.SYNOPSIS
		Block until a job execution leaves its running state; return that state.
	.DESCRIPTION
		Returns rather than throws on Failed. A failed producer is a real
		measurement outcome the caller needs to record against the cell and
		carry on from, not a reason to abandon a sweep that may have hours of
		completed cells behind it.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][System.Collections.IDictionary] $Context,
		[Parameter(Mandatory)][string] $ExecutionName,
		[int] $TimeoutSec = 1800
	)
	$deadline = (Get-Date).AddSeconds($TimeoutSec)
	$last = ''
	while ((Get-Date) -lt $deadline) {
		$json = Invoke-Az @(
			'containerapp', 'job', 'execution', 'show',
			'--name', $Context.producerJob,
			'--resource-group', $Context.resourceGroup,
			'--job-execution-name', $ExecutionName,
			'-o', 'json'
		) -AllowFailure
		if ($LASTEXITCODE -eq 0 -and $json -and $json.Trim()) {
			try {
				$state = ($json | ConvertFrom-Json).properties.status
				if ($state -and $state -ne $last) {
					Write-Host "[aca]   job execution: $state" -ForegroundColor DarkGray
					$last = $state
				}
				if ($state -in @('Succeeded', 'Failed', 'Stopped', 'Degraded')) { return $state }
			} catch { }
		}
		Start-Sleep -Seconds 10
	}
	Write-Warning "[aca] job execution $ExecutionName still running after ${TimeoutSec}s; stopping it"
	Invoke-Az @(
		'containerapp', 'job', 'stop',
		'--name', $Context.producerJob, '--resource-group', $Context.resourceGroup,
		'--job-execution-name', $ExecutionName, '-o', 'none'
	) -AllowFailure | Out-Null
	return 'TimedOut'
}

function Get-AcaJobLog {
	<#
	.SYNOPSIS
		Harvest a job execution's stdout from Log Analytics, in order.
	.DESCRIPTION
		Retries until the producer's terminal "[producer] DONE" marker is
		present or the wait budget is exhausted, because workspace ingestion
		lags the run by a minute or two and a query issued the instant the job
		finishes reliably returns a truncated log. Truncation here is
		especially dangerous: the harness parses per-second lines, so a short
		log does not look broken, it looks like a slower cohort. Waiting for
		the marker is what distinguishes "the run ended" from "the log has
		caught up with the run".

		Falls back to whatever the workspace has if the marker never arrives -
		a producer that crashed never printed one, and its partial log is the
		evidence for why the cell failed.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][System.Collections.IDictionary] $Context,
		[Parameter(Mandatory)][string] $ExecutionName,
		[Parameter(Mandatory)][datetime] $SinceUtc,
		[Parameter(Mandatory)][string] $TreeId,
		[int] $MaxWaitSec = 420
	)
	# Anchor every decision to THIS cohort's tree id.
	#
	# Neither the time filter nor the execution filter is sufficient on its
	# own. A previous cohort's container can still be draining inside the
	# window, and a superseded silo revision running in `tcp` ingest mode
	# emits the very same `[producer] ...` / `[silo] t=` line shapes from its
	# own standalone benchmark. Both were observed: a harvest returned a
	# complete PREVIOUS run, terminated on that run's `[producer] DONE`, and
	# handed back a log whose first 9,500 lines belonged to a different
	# experiment - while looking perfectly well-formed.
	#
	# The tree id is minted per cohort and appears in the producer's
	# `settings treeId=...` banner, so slicing from the LAST occurrence of
	# that banner is a positive identification of this cohort's output. It
	# cannot be satisfied by a stale line, because a stale line carries a
	# different tree id.
	$anchor = "settings treeId=$TreeId"
	$doneMarker = '[producer] DONE'
	$sliceToCohort = {
		param($all)
		$idx = -1
		for ($i = 0; $i -lt $all.Count; $i++) {
			if ($all[$i] -and $all[$i].Contains($anchor)) { $idx = $i }
		}
		if ($idx -lt 0) { return @() }
		return @($all[$idx..($all.Count - 1)])
	}
	# SINGLE LINE, deliberately. `az` on Windows resolves to az.cmd, a batch
	# file, and cmd.exe truncates an argument at the first embedded newline.
	# A multi-line here-string query therefore reaches the service as just
	# "ContainerAppConsoleLogs_CL" - a bare table scan with EVERY `| where`
	# clause silently discarded. That failure is invisible: the query
	# succeeds, returns plenty of rows, and the harness happily parses a
	# blend of several different experiments' logs. KQL uses `|` as its
	# operator separator, so one line is fully equivalent and immune.
	#
	# Identify the execution by ContainerGroupName_s ONLY. There is no
	# ExecutionName_s column on this table: a job execution's replica pod
	# surfaces as ContainerGroupName_s = '<execution>-<podsuffix>' (e.g.
	# 'prod-l3dev01-72uclpq-qmnh5'), with ContainerJobName_s carrying the
	# bare job name. Referencing a column that does not exist makes the
	# WHOLE query fail with a SEM0100 semantic error - see the fail-fast
	# guard below for why that mattered so much.
	$kql = "ContainerAppConsoleLogs_CL | where TimeGenerated >= datetime($($SinceUtc.ToString('o'))) | where ContainerGroupName_s startswith '$ExecutionName' | project TimeGenerated, Log_s | order by TimeGenerated asc | limit 100000"
	$deadline = (Get-Date).AddSeconds($MaxWaitSec)
	$best = @()
	$everSucceeded = $false
	while ($true) {
		$json = Invoke-Az @(
			'monitor', 'log-analytics', 'query',
			'--workspace', $Context.workspaceId,
			'--analytics-query', $kql,
			'-o', 'json'
		) -AllowFailure
		# FAIL FAST on a query that never once succeeded. A malformed KQL
		# (a mistyped or non-existent column) returns a semantic error on
		# EVERY poll, and -AllowFailure turns that into an empty result -
		# which is byte-identical to "the logs have not been ingested yet".
		# The harvest then polls to its deadline printing a reassuring
		# "waiting for log ingestion (0 line(s))" while the query could
		# never have worked. That is the same failure shape as the az.cmd
		# truncation above: a broken query presenting as normal operation.
		# Ingestion lag is real, so a transient miss must be tolerated - but
		# a query that has not succeeded ONCE is a bug, not lag, and saying
		# so immediately costs minutes instead of the full MaxWaitSec.
		if ($LASTEXITCODE -ne 0 -and -not $everSucceeded) {
			throw "[aca] Log Analytics query failed on its first attempt and has never succeeded. This is a malformed query, not ingestion lag. Query: $kql"
		}
		if ($LASTEXITCODE -eq 0 -and $json -and $json.Trim()) {
			$everSucceeded = $true
			try {
				$rows = @($json | ConvertFrom-Json)
				$cohort = & $sliceToCohort @($rows | ForEach-Object { $_.Log_s })
				if ($cohort.Count -gt $best.Count) { $best = $cohort }
				# NOT -like. PowerShell wildcards treat [...] as a character
				# class, so '*[producer] DONE*' asks for one character from
				# {p,r,o,d,u,c,e} followed by " DONE". The real line has ']'
				# in that position, so the pattern can never match and the
				# harvest loops until its deadline with every line already
				# in hand. Ordinal Contains has no metacharacters.
				#
				# The search runs over the tree-id-anchored slice, so a
				# previous cohort's DONE can never end this one early.
				if ($cohort | Where-Object { $_ -and $_.Contains($doneMarker) }) { return $cohort }
			} catch { }
		}
		if ((Get-Date) -ge $deadline) {
			Write-Warning "[aca] log harvest for $ExecutionName (tree $TreeId) did not observe a terminal marker within ${MaxWaitSec}s; returning $($best.Count) line(s)"
			return $best
		}
		Write-Host "[aca]   waiting for log ingestion ($($best.Count) line(s) for tree $TreeId so far)" -ForegroundColor DarkGray
		Start-Sleep -Seconds 20
	}
}

function Get-AcaSiloLog {
	<#
	.SYNOPSIS
		Harvest the silo app's console output for a time window.
	.DESCRIPTION
		In cluster ingest mode the measurement engine runs inside the
		producer, so the producer's log carries the per-second `[silo] t=`
		throughput lines and the silos carry none. The silos still run the
		PhaseA diagnostic reporter, so the per-call latency histograms
		(`[phaseA] ... instrument=...`) exist only in THEIR logs.

		Both halves are therefore needed, and the cohort writes them into one
		combined file. That is what lets Layer 3 reuse Read-SiloLogStats
		byte-for-byte instead of growing a parallel parser: identical parsing
		is the strongest available guarantee that a Layer 2 number and a
		Layer 3 number mean the same thing.

		Only `[phaseA]` lines are taken. That is the whole of what a silo
		uniquely contributes here, and restricting to it makes the combined
		log immune to a superseded revision: a silo still running in `tcp`
		ingest mode drives its OWN standalone benchmark and emits the exact
		same `[producer] ...` and `[silo] t=` line shapes, which would
		otherwise be spliced into this cohort's throughput series and silently
		blended into the published number.

		Pass -RevisionName to pin the harvest to the cohort's own silos.
		Because exactly one `az containerapp update` runs per cohort and the
		app is in Single revision mode, the active revision is a one-to-one
		label for this cohort's replica set - a far stronger identification
		than a time window, which a previous cohort's draining container can
		still overlap.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][System.Collections.IDictionary] $Context,
		[Parameter(Mandatory)][datetime] $SinceUtc,
		[Parameter(Mandatory)][datetime] $UntilUtc,
		[string] $RevisionName
	)
	# Single line - see the rationale in Get-AcaJobLog. A multi-line query is
	# truncated to a bare table scan by az.cmd, which is precisely how this
	# harvest once returned three experiments' worth of unfiltered logs.
	$revFilter = if ($RevisionName) { " | where RevisionName_s == '$RevisionName'" } else { '' }
	$kql = "ContainerAppConsoleLogs_CL | where TimeGenerated between (datetime($($SinceUtc.ToString('o'))) .. datetime($($UntilUtc.ToString('o')))) | where ContainerAppName_s == '$($Context.siloApp)'$revFilter | where Log_s startswith '[phaseA]' | project TimeGenerated, Log_s | order by TimeGenerated asc | limit 200000"
	$json = Invoke-Az @(
		'monitor', 'log-analytics', 'query',
		'--workspace', $Context.workspaceId,
		'--analytics-query', $kql,
		'-o', 'json'
	) -AllowFailure
	if ($LASTEXITCODE -ne 0) {
		# Same reasoning as the fail-fast guard in Get-AcaJobLog: a malformed
		# query returns empty, which is indistinguishable from "this cohort
		# produced no [phaseA] lines". Silence here would publish a row with
		# missing per-call quantiles and no explanation.
		throw "[aca] silo log query failed. Query: $kql"
	}
	if (-not $json -or -not $json.Trim()) { return @() }
	try { return @(@($json | ConvertFrom-Json) | ForEach-Object { $_.Log_s }) } catch { return @() }
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
	$deleteArgs = @('group', 'delete', '--name', $rg, '--yes')
	if ($NoWait) { $deleteArgs += '--no-wait' }
	Invoke-Az $deleteArgs -AllowFailure -PassthruOutput

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
