<#
	Storage-side telemetry for Layer 3 cohorts: Azure Monitor metrics of each
	storage account a cohort used, reduced to one '[storage-metrics]' line per
	account and appended to the cohort log.

	Why it exists. Every Layer 3 silo count used to share one storage account
	and the rig recorded nothing about that account, so a flattening scaling
	curve could not be split into "Lattice stopped scaling" and "the account
	stopped scaling". These lines put the account's own view of the cohort -
	transactions, throttling, server versus end-to-end latency - beside the
	throughput it was measured against.

	Pure functions only (no az calls), so Test-Layer3StorageMetrics.ps1 can pin
	the parsing and reduction against literal fixtures. The az fetch lives in
	aca-common.ps1 (Get-AcaStorageMetricsLine). Dot-sourced by aca-common.ps1 and
	performance-report.ps1.
#>

Set-StrictMode -Version Latest

# Transactions ResponseType values, by what they say about the account.
$script:StorageThrottledTypes = @('ServerBusyError', 'ClientThrottlingError')
$script:StorageThrottledSuccessTypes = @('SuccessWithThrottling', 'SuccessWithShortThrottling')
$script:StorageTimeoutTypes = @('ServerTimeoutError', 'ClientTimeoutError')

function ConvertTo-StorageMetricsMinute {
	<#
	.SYNOPSIS
		A metric point's timestamp as a culture-free UTC minute key
		('yyyy-MM-ddTHH:mm'). ConvertFrom-Json may already have turned the
		ISO string into a DateTime, whose default string form is culture
		dependent and would not line up across accounts.
	#>
	[CmdletBinding()] param([AllowNull()] $Value)
	$inv = [System.Globalization.CultureInfo]::InvariantCulture
	$dto = if ($Value -is [datetime]) { [datetimeoffset]$Value.ToUniversalTime() }
		elseif ($Value -is [datetimeoffset]) { $Value }
		else { [datetimeoffset]::Parse([string]$Value, $inv, [System.Globalization.DateTimeStyles]::AssumeUniversal) }
	return $dto.UtcDateTime.ToString('yyyy-MM-ddTHH:mm', $inv)
}

function ConvertFrom-StorageMetricsResponse {
	<#
	.SYNOPSIS
		Reduce `az monitor metrics list -o json` output for one account's
		table service to the figures a cohort reports.
	.PARAMETER TransactionsJson
		Response for `--metrics Transactions --aggregation Total --interval PT1M
		--filter "ResponseType eq '*'"`: one time series per response type.
	.PARAMETER LatencyJson
		Response for `--metrics SuccessServerLatency SuccessE2ELatency
		--aggregation Average Maximum --interval PT1M`.
	.OUTPUTS
		Hashtable: Transactions, Throttled, ThrottledSuccess, Timeouts,
		OtherErrors, PeakTps (busiest minute / 60), Minutes (minutes with any
		transaction), MinuteTotals (UTC minute 'yyyy-MM-ddTHH:mm' to that
		minute's transactions, so accounts can be summed minute by minute),
		ServerLatencyMs and E2ELatencyMs (transaction-free
		minutes excluded; mean of per-minute averages), E2ELatencyMaxMs.
		Latencies are $null when no minute reported one.
	#>
	[CmdletBinding()] param(
		[AllowNull()] [AllowEmptyString()] [string] $TransactionsJson,
		[AllowNull()] [AllowEmptyString()] [string] $LatencyJson
	)

	$result = @{
		Transactions = 0L; Throttled = 0L; ThrottledSuccess = 0L; Timeouts = 0L; OtherErrors = 0L
		PeakTps = 0.0; Minutes = 0; MinuteTotals = @{}; ServerLatencyMs = $null; E2ELatencyMs = $null; E2ELatencyMaxMs = $null
	}
	$perMinute = @{}
	if (-not [string]::IsNullOrWhiteSpace($TransactionsJson)) {
		$tx = $TransactionsJson | ConvertFrom-Json
		foreach ($metric in @($tx.value)) {
			foreach ($series in @($metric.timeseries)) {
				$type = 'Unknown'
				foreach ($md in @($series.metadatavalues)) {
					if ($md -and $md.name -and "$($md.name.value)" -ieq 'responsetype') { $type = [string]$md.value }
				}
				foreach ($point in @($series.data)) {
					if ($null -eq $point -or $null -eq $point.total) { continue }
					$n = [long][math]::Round([double]$point.total)
					if ($n -le 0) { continue }
					$result.Transactions += $n
					$key = ConvertTo-StorageMetricsMinute $point.timeStamp
					$perMinute[$key] = $(if ($perMinute.ContainsKey($key)) { [long]$perMinute[$key] } else { 0L }) + $n
					if ($type -in $script:StorageThrottledTypes) { $result.Throttled += $n }
					elseif ($type -in $script:StorageThrottledSuccessTypes) { $result.ThrottledSuccess += $n }
					elseif ($type -in $script:StorageTimeoutTypes) { $result.Timeouts += $n }
					elseif ($type -ne 'Success') { $result.OtherErrors += $n }
				}
			}
		}
	}
	if ($perMinute.Count -gt 0) {
		$result.Minutes = $perMinute.Count
		$result.PeakTps = [math]::Round((($perMinute.Values | Measure-Object -Maximum).Maximum) / 60.0, 1)
		$result.MinuteTotals = $perMinute
	}

	if (-not [string]::IsNullOrWhiteSpace($LatencyJson)) {
		$lat = $LatencyJson | ConvertFrom-Json
		foreach ($metric in @($lat.value)) {
			$name = [string]$metric.name.value
			$avgs = [System.Collections.Generic.List[double]]::new()
			$max = $null
			foreach ($series in @($metric.timeseries)) {
				foreach ($point in @($series.data)) {
					if ($null -eq $point) { continue }
					# A minute with no transactions reports no latency; it must not
					# drag the mean towards zero.
					if (($point.PSObject.Properties.Name -contains 'average') -and $null -ne $point.average) { $avgs.Add([double]$point.average) }
					if (($point.PSObject.Properties.Name -contains 'maximum') -and $null -ne $point.maximum) {
						if ($null -eq $max -or [double]$point.maximum -gt $max) { $max = [double]$point.maximum }
					}
				}
			}
			$mean = if ($avgs.Count -gt 0) { [math]::Round(($avgs | Measure-Object -Average).Average, 1) } else { $null }
			if ($name -eq 'SuccessServerLatency') { $result.ServerLatencyMs = $mean }
			elseif ($name -eq 'SuccessE2ELatency') {
				$result.E2ELatencyMs = $mean
				if ($null -ne $max) { $result.E2ELatencyMaxMs = [math]::Round($max, 1) }
			}
		}
	}
	return $result
}

function Format-StorageMetricsLine {
	<#
	.SYNOPSIS
		The '[storage-metrics]' cohort-log line for one account.
	.DESCRIPTION
		Role names the account's job in the cohort: 'wal0' is the primary
		account, which also holds the clustering table and grain state, so its
		transactions are not WAL-only; 'wal<i>' is extra WAL account i. A
		latency the account did not report renders as 'n/a'. -Unavailable
		marks an account whose metrics could not be read, so the gap is
		explicit rather than reading as an idle account.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)] [string] $Account,
		[Parameter(Mandatory)] [string] $Role,
		[System.Collections.IDictionary] $Metrics,
		[string] $Unavailable
	)
	if ($Unavailable -or $null -eq $Metrics) {
		$reason = if ($Unavailable) { $Unavailable -replace '\s+', '_' } else { 'no-data' }
		return "[storage-metrics] account=$Account role=$Role unavailable=$reason"
	}
	$inv = [System.Globalization.CultureInfo]::InvariantCulture
	$fmt = { param($v) if ($null -eq $v) { 'n/a' } else { ([double]$v).ToString('0.0', $inv) } }
	$line = ('[storage-metrics] account={0} role={1} transactions={2} throttled={3} throttledSuccess={4} timeouts={5} otherErrors={6} peakTps={7} serverLatencyMs={8} e2eLatencyMs={9} e2eLatencyMaxMs={10} minutes={11}' -f
		$Account, $Role, $Metrics.Transactions, $Metrics.Throttled, $Metrics.ThrottledSuccess, $Metrics.Timeouts,
		$Metrics.OtherErrors, (& $fmt $Metrics.PeakTps), (& $fmt $Metrics.ServerLatencyMs), (& $fmt $Metrics.E2ELatencyMs),
		(& $fmt $Metrics.E2ELatencyMaxMs), $Metrics.Minutes)
	$minutes = $Metrics['MinuteTotals']
	if ($minutes -and $minutes.Count -gt 0) {
		$line += ' minuteTotals=' + ((@($minutes.Keys) | Sort-Object | ForEach-Object { "$_/$($minutes[$_])" }) -join ',')
	}
	return $line
}

function Read-StorageMetricsLines {
	<#
	.SYNOPSIS
		Parse the '[storage-metrics]' lines of a cohort log back into one
		hashtable per account (Account, Role, Unavailable, and the
		Format-StorageMetricsLine figures; 'n/a' reads as $null).
	#>
	[CmdletBinding()] param([AllowNull()] [AllowEmptyCollection()] [string[]] $Lines)

	$accounts = [System.Collections.Generic.List[hashtable]]::new()
	foreach ($line in @($Lines)) {
		if ([string]::IsNullOrEmpty($line) -or -not $line.StartsWith('[storage-metrics] ')) { continue }
		$fields = @{}
		foreach ($m in [regex]::Matches($line, '(\w+)=(\S+)')) { $fields[$m.Groups[1].Value] = $m.Groups[2].Value }
		if (-not $fields.ContainsKey('account')) { continue }
		$num = {
			param($k)
			if (-not $fields.ContainsKey($k) -or $fields[$k] -eq 'n/a') { return $null }
			return [double]::Parse($fields[$k], [System.Globalization.CultureInfo]::InvariantCulture)
		}
		$minuteTotals = $null
		if ($fields.ContainsKey('minuteTotals')) {
			$minuteTotals = @{}
			foreach ($pair in $fields['minuteTotals'].Split(',')) {
				$slash = $pair.LastIndexOf('/')
				if ($slash -gt 0) { $minuteTotals[$pair.Substring(0, $slash)] = [long]$pair.Substring($slash + 1) }
			}
		}
		$accounts.Add(@{
			MinuteTotals     = $minuteTotals
			Account          = $fields['account']
			Role             = $(if ($fields.ContainsKey('role')) { $fields['role'] } else { '' })
			Unavailable      = $(if ($fields.ContainsKey('unavailable')) { $fields['unavailable'] } else { $null })
			Transactions     = & $num 'transactions'
			Throttled        = & $num 'throttled'
			ThrottledSuccess = & $num 'throttledSuccess'
			Timeouts         = & $num 'timeouts'
			OtherErrors      = & $num 'otherErrors'
			PeakTps          = & $num 'peakTps'
			ServerLatencyMs  = & $num 'serverLatencyMs'
			E2ELatencyMs     = & $num 'e2eLatencyMs'
			E2ELatencyMaxMs  = & $num 'e2eLatencyMaxMs'
		})
	}
	return $accounts.ToArray()
}

function Get-StorageMetricsSummary {
	<#
	.SYNOPSIS
		One cohort's storage-side summary across the accounts it used.
	.DESCRIPTION
		ThrottleRatio is throttled (including succeeded-after-throttling)
		transactions over all transactions, across every account; Timeouts
		and TimeoutRatio are the same for ServerTimeoutError and
		ClientTimeoutError transactions. PeakTps is the busiest minute of all
		accounts combined: their per-minute totals summed minute by minute,
		or, for a line that predates minuteTotals, the sum of each account's
		own busiest minute (exact for one account, an upper bound for more).
		PeakAccountTps and the
		latencies are the WORST account's, because one saturated
		account caps the cohort however idle the others are. Returns $null when
		no account reported data, so a cohort without metrics is never read as
		an idle, unthrottled one.
	#>
	[CmdletBinding()] param([AllowNull()] [AllowEmptyCollection()] [object[]] $Accounts)

	$withData = @(@($Accounts) | Where-Object { $_ -and -not $_.Unavailable -and $null -ne $_.Transactions })
	if ($withData.Count -eq 0) { return $null }
	$tx = ($withData | ForEach-Object { [double]$_.Transactions } | Measure-Object -Sum).Sum
	$thr = ($withData | ForEach-Object { [double]$_.Throttled + [double]$_.ThrottledSuccess } | Measure-Object -Sum).Sum
	$tmo = ($withData | ForEach-Object { [double]$_.Timeouts } | Measure-Object -Sum).Sum
	$maxOf = {
		param($key)
		$vals = @($withData | ForEach-Object { $_[$key] } | Where-Object { $null -ne $_ })
		if ($vals.Count -eq 0) { return $null }
		return [double](($vals | Measure-Object -Maximum).Maximum)
	}
	return @{
		Accounts          = $withData.Count
		MissingAccounts   = @(@($Accounts) | Where-Object { $_ }).Count - $withData.Count
		Transactions      = [long]$tx
		ThrottleRatio     = $(if ($tx -gt 0) { [math]::Round($thr / $tx, 4) } else { 0.0 })
		PeakTps           = $(
			if (@($withData | Where-Object { $null -eq $_['MinuteTotals'] }).Count -eq 0) {
				$combined = @{}
				foreach ($a in $withData) { foreach ($k in @($a['MinuteTotals'].Keys)) { $combined[$k] = $(if ($combined.ContainsKey($k)) { [long]$combined[$k] } else { 0L }) + [long]$a['MinuteTotals'][$k] } }
				if ($combined.Count -eq 0) { 0.0 } else { [math]::Round((($combined.Values | Measure-Object -Maximum).Maximum) / 60.0, 1) }
			} else {
				[math]::Round((($withData | ForEach-Object { [double]$_.PeakTps } | Where-Object { $_ } | Measure-Object -Sum).Sum), 1)
			})
		Timeouts          = [long]$tmo
		TimeoutRatio      = $(if ($tx -gt 0) { [math]::Round($tmo / $tx, 6) } else { 0.0 })
		PeakAccountTps    = & $maxOf 'PeakTps'
		ServerLatencyMs   = & $maxOf 'ServerLatencyMs'
		E2ELatencyMs      = & $maxOf 'E2ELatencyMs'
	}
}
