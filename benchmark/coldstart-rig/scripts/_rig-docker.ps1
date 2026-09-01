#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Docker, HTTP and MCP helpers for the isolated cold-start and scale rig.

.DESCRIPTION
	The impure half of the rig. Everything here shells out to Docker or talks
	HTTP, so it is deliberately kept OUT of _rig-helpers.ps1 (which stays pure
	and is what the regression suite exercises).

	Every function that could bind a container, a volume, an image tag or a
	port runs the fail-closed isolation guard FIRST. There is no code path in
	the rig that reaches `docker compose up` without Assert-RigIsolation and
	Assert-RigComposeIsolation having both passed.

	Dot-source it:  . (Join-Path $PSScriptRoot '_rig-docker.ps1')
	It dot-sources _rig-helpers.ps1 itself.
#>

Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot '_rig-helpers.ps1')

<#
.SYNOPSIS
	Runs docker with the supplied arguments and throws on a non-zero exit.
#>
function Invoke-RigDocker {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory, ValueFromRemainingArguments)] [string[]] $DockerArgs,
		[switch] $AllowFailure
	)

	$output = & docker @DockerArgs 2>&1
	$exit = $LASTEXITCODE
	if ($exit -ne 0 -and -not $AllowFailure) {
		throw "docker $($DockerArgs -join ' ') failed with exit code $exit`n$($output -join [Environment]::NewLine)"
	}
	return $output
}

<#
.SYNOPSIS
	Absolute path of the rig's compose file.
#>
function Get-RigComposeFile {
	[CmdletBinding()]
	param([string] $ScriptRoot)

	if (-not $ScriptRoot) { $ScriptRoot = $PSScriptRoot }
	return (Resolve-Path -LiteralPath (Join-Path $ScriptRoot '..' 'docker-compose.rig.yml')).Path
}

<#
.SYNOPSIS
	Root directory for this rig's transient run artefacts (gitignored).
#>
function Get-RigRunRoot {
	[CmdletBinding()]
	param([string] $ScriptRoot)

	if (-not $ScriptRoot) { $ScriptRoot = $PSScriptRoot }
	$root = Join-Path $ScriptRoot '..' '..' '.run' 'coldstart-rig'
	New-Item -ItemType Directory -Force -Path $root | Out-Null
	return (Resolve-Path -LiteralPath $root).Path
}

<#
.SYNOPSIS
	Publishes the rig's compose variables into the current process environment.

.DESCRIPTION
	The compose file reads every isolation-critical value (project, images,
	volumes, host port, workspace) from a variable with a `:?` default, so a
	missing value fails compose loudly rather than silently falling back to
	something live.
#>
function Set-RigComposeEnvironment {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [hashtable] $Config)

	$env:RIG_PROJECT = "$($Config.ProjectName)"
	$env:RIG_MCP_IMAGE = "$($Config.McpImage)"
	$env:RIG_EMBEDDER_IMAGE = "$($Config.EmbedderImage)"
	$env:RIG_WORK_VOLUME = "$($Config.WorkVolume)"
	$env:RIG_HF_VOLUME = "$($Config.HfCacheVolume)"
	$env:RIG_HOST_PORT = "$($Config.HostPort)"
	$env:RIG_WORKSPACE = (Resolve-Path -LiteralPath "$($Config.WorkspaceRoot)").Path
}

<#
.SYNOPSIS
	Runs `docker compose` against the rig's project and compose file.

.DESCRIPTION
	Refuses unless Assert-RigIsolation passes on the configuration. Callers
	that actually START something must additionally have run
	Assert-RigDockerIsolation, which validates the RESOLVED compose document.
#>
function Invoke-RigCompose {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config,
		[Parameter(Mandatory)] [string[]] $ComposeArgs,
		[switch] $AllowFailure
	)

	Assert-RigIsolation -Config $Config | Out-Null
	Set-RigComposeEnvironment -Config $Config

	$composeFile = Get-RigComposeFile
	$all = @('compose', '-p', "$($Config.ProjectName)", '-f', $composeFile) + $ComposeArgs
	return Invoke-RigDocker -DockerArgs $all -AllowFailure:$AllowFailure
}

<#
.SYNOPSIS
	The FULL fail-closed gate: validates the configuration AND the compose
	document Docker actually resolved.

.DESCRIPTION
	This is the function that makes decision D11 structural rather than
	careful. `docker compose config` performs all variable interpolation,
	merges every override, and resolves each mount to a concrete
	type/source/target, so what it prints is what would really be bound. The
	guard reads that resolved document and refuses on a live project name, a
	live volume, a live image tag, a live published port, any build section,
	or any writable bind mount.
#>
function Assert-RigDockerIsolation {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [hashtable] $Config)

	Assert-RigIsolation -Config $Config | Out-Null

	$raw = Invoke-RigCompose -Config $Config -ComposeArgs @('config', '--format', 'json')
	$text = ($raw | Out-String)
	$document = $null
	try { $document = $text | ConvertFrom-Json }
	catch { throw "Rig isolation guard REFUSED to start: the resolved compose document could not be parsed. $($_.Exception.Message)" }

	Assert-RigComposeIsolation -Document $document -Config $Config | Out-Null
	return $document
}

<#
.SYNOPSIS
	Applies the rig's own ADDITIONAL tag to an already-built image.

.DESCRIPTION
	Never builds. Never moves a live tag: the destination is validated by
	Assert-RigIsolation (which requires the rig tag and forbids every live
	image reference), and the source is only ever read.
#>
function Add-RigImageTag {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config,
		[Parameter(Mandatory)] [string] $Source,
		[Parameter(Mandatory)] [string] $Destination
	)

	Assert-RigIsolation -Config $Config | Out-Null

	$normalisedDestination = ConvertTo-RigNormalisedImage -Image $Destination
	$forbidden = @($Config.ForbiddenImages) | ForEach-Object { ConvertTo-RigNormalisedImage -Image $_ }
	if ($forbidden -contains $normalisedDestination) {
		throw "Rig isolation guard REFUSED to tag: '$normalisedDestination' is a LIVE image tag."
	}
	if ((Get-RigImageTag -Image $normalisedDestination) -ne "$($Config.RequiredImageTag)") {
		throw "Rig isolation guard REFUSED to tag: '$normalisedDestination' does not carry the required rig tag '$($Config.RequiredImageTag)'."
	}

	Invoke-RigDocker -DockerArgs @('image', 'inspect', $Source, '--format', '{{.Id}}') | Out-Null
	Invoke-RigDocker -DockerArgs @('tag', $Source, $normalisedDestination) | Out-Null
	return $normalisedDestination
}

<#
.SYNOPSIS
	True when a named Docker volume exists.
#>
function Test-RigVolumeExists {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [string] $Name)

	& docker volume inspect $Name 2>&1 | Out-Null
	return $LASTEXITCODE -eq 0
}

<#
.SYNOPSIS
	Creates a rig volume, refusing any name that fails the isolation guard.
#>
function New-RigVolume {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config,
		[Parameter(Mandatory)] [string] $Name
	)

	$violations = Test-RigVolumeName -Volume $Name -Config $Config -Label 'volume'
	if ($violations.Count -gt 0) {
		throw ("Rig isolation guard REFUSED to create a volume: " + ($violations -join '; ') + '.')
	}
	if (-not (Test-RigVolumeExists -Name $Name)) {
		Invoke-RigDocker -DockerArgs @('volume', 'create', $Name) | Out-Null
	}
	return $Name
}

<#
.SYNOPSIS
	Removes a rig volume, refusing any name that fails the isolation guard.
#>
function Remove-RigVolume {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config,
		[Parameter(Mandatory)] [string] $Name
	)

	$violations = Test-RigVolumeName -Volume $Name -Config $Config -Label 'volume'
	if ($violations.Count -gt 0) {
		throw ("Rig isolation guard REFUSED to remove a volume: " + ($violations -join '; ') + '.')
	}
	if (Test-RigVolumeExists -Name $Name) {
		Invoke-RigDocker -DockerArgs @('volume', 'rm', $Name) | Out-Null
	}
}

<#
.SYNOPSIS
	Clones the pristine master volume onto a freshly recreated working volume.

.DESCRIPTION
	The master is mounted READ-ONLY, so a clone can never mutate the baseline.
	Recreating the destination (rather than overwriting it) means every run
	starts from byte-identical durable state, which is what makes two runs
	comparable. Uses busybox, which is a few megabytes and already local on
	any box that has run this rig once.
#>
function Copy-RigVolume {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config,
		[Parameter(Mandatory)] [string] $Source,
		[Parameter(Mandatory)] [string] $Destination,
		[string] $HelperImage = 'busybox:latest'
	)

	foreach ($name in @($Source, $Destination)) {
		$violations = Test-RigVolumeName -Volume $name -Config $Config -Label 'volume'
		if ($violations.Count -gt 0) {
			throw ("Rig isolation guard REFUSED to clone: " + ($violations -join '; ') + '.')
		}
	}
	if ($Source -eq $Destination) {
		throw 'Rig isolation guard REFUSED to clone: source and destination volumes are the same.'
	}
	if (-not (Test-RigVolumeExists -Name $Source)) {
		throw "Master volume '$Source' does not exist. Run prepare-master.ps1 first."
	}

	Remove-RigVolume -Config $Config -Name $Destination
	New-RigVolume -Config $Config -Name $Destination | Out-Null

	Invoke-RigDocker -DockerArgs @(
		'run', '--rm',
		'-v', "${Source}:/from:ro",
		'-v', "${Destination}:/to",
		$HelperImage,
		'sh', '-c', 'cp -a /from/. /to/'
	) | Out-Null

	return $Destination
}

<#
.SYNOPSIS
	The UTC instant the named container last started, from the Docker daemon.

.DESCRIPTION
	Every measured elapsed time in a run is taken relative to this instant
	rather than to the moment a compose CLI call returned, so CLI overhead
	never lands in the headline number and the three restart scenarios share
	one comparable zero point.
#>
function Get-RigContainerStartedAtUtc {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [string] $Container)

	$raw = (Invoke-RigDocker -DockerArgs @('inspect', '-f', '{{.State.StartedAt}}', $Container) | Out-String).Trim()
	return [datetime]::Parse($raw, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::AdjustToUniversal -bor [System.Globalization.DateTimeStyles]::AssumeUniversal)
}

<#
.SYNOPSIS
	Resolves the container name for a rig compose service.
#>
function Get-RigContainerName {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config,
		[Parameter(Mandatory)] [string] $Service
	)

	$name = (Invoke-RigCompose -Config $Config -ComposeArgs @('ps', '-q', $Service) | Out-String).Trim()
	if ([string]::IsNullOrWhiteSpace($name)) {
		throw "No container is running for rig service '$Service'."
	}
	return ($name -split '\r?\n')[0].Trim()
}

<#
.SYNOPSIS
	Polls an HTTP endpoint until it answers 200, returning seconds elapsed
	since the supplied zero point.

.DESCRIPTION
	Returns $null on timeout rather than throwing, so a scenario that never
	became ready is recorded as a null measurement and is visibly distinct
	from a fast one.
#>
function Wait-RigHttpOk {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $Uri,
		[Parameter(Mandatory)] [datetime] $ZeroUtc,
		[int] $TimeoutSec = 900,
		[int] $IntervalMs = 250
	)

	$deadline = (Get-Date).AddSeconds($TimeoutSec)
	while ((Get-Date) -lt $deadline) {
		try {
			$response = Invoke-WebRequest -Uri $Uri -Method Get -TimeoutSec 10 -SkipHttpErrorCheck -ErrorAction Stop
			if ($response.StatusCode -eq 200) {
				return [Math]::Round(([datetime]::UtcNow - $ZeroUtc).TotalSeconds, 3)
			}
		}
		catch {
			# Connection refused / reset while the listener is still coming up.
		}
		Start-Sleep -Milliseconds $IntervalMs
	}
	return $null
}

<#
.SYNOPSIS
	Calls one MCP tool with a single self-contained POST.

.DESCRIPTION
	The host runs streamable HTTP in STATELESS mode at the route root, so a
	tool call needs no initialize handshake and carries no session id: one
	JSON-RPC request, one response. The response may come back as JSON or as
	a one-event SSE stream, so both are parsed.

	Returns a result object carrying Ok, DurationMs, the decoded JSON-RPC
	payload, and the first text content block (which is where the tool's own
	JSON answer lives).
#>
function Invoke-RigMcpTool {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $BaseUri,
		[Parameter(Mandatory)] [string] $Name,
		[hashtable] $Arguments = @{},
		[int] $TimeoutSec = 300
	)

	$body = @{
		jsonrpc = '2.0'
		id      = 1
		method  = 'tools/call'
		params  = @{ name = $Name; arguments = $Arguments }
	} | ConvertTo-Json -Depth 12 -Compress

	$headers = @{ Accept = 'application/json, text/event-stream' }
	$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
	$ok = $false
	$statusCode = 0
	$payload = $null
	$errorText = $null
	$text = $null

	try {
		$response = Invoke-WebRequest -Uri $BaseUri -Method Post -Body $body `
			-ContentType 'application/json' -Headers $headers `
			-TimeoutSec $TimeoutSec -SkipHttpErrorCheck -ErrorAction Stop
		$statusCode = [int] $response.StatusCode
		$raw = "$($response.Content)"
		$json = ConvertFrom-RigMcpBody -Body $raw
		if ($null -ne $json) {
			$payload = $json
			$resultProperty = $json.PSObject.Properties['result']
			$errorProperty = $json.PSObject.Properties['error']
			if ($null -ne $errorProperty) {
				$errorText = ($errorProperty.Value | ConvertTo-Json -Depth 8 -Compress)
			}
			elseif ($null -ne $resultProperty) {
				$result = $resultProperty.Value
				$isError = $result.PSObject.Properties['isError']
				$text = Get-RigMcpFirstText -Result $result
				$ok = -not ($null -ne $isError -and $isError.Value -eq $true)
			}
		}
		else { $errorText = "unparsable response body: $($raw.Substring(0, [Math]::Min(400, $raw.Length)))" }
	}
	catch {
		$errorText = $_.Exception.Message
	}

	$stopwatch.Stop()
	return [pscustomobject] @{
		Ok         = $ok
		StatusCode = $statusCode
		DurationMs = [Math]::Round($stopwatch.Elapsed.TotalMilliseconds, 1)
		Text       = $text
		Error      = $errorText
		Payload    = $payload
	}
}

<#
.SYNOPSIS
	Decodes an MCP streamable-HTTP response body, accepting either plain JSON
	or a Server-Sent Events stream carrying one `data:` line per event.
#>
function ConvertFrom-RigMcpBody {
	[CmdletBinding()]
	param([AllowNull()] [string] $Body)

	if ([string]::IsNullOrWhiteSpace($Body)) { return $null }
	$trimmed = $Body.TrimStart()
	if ($trimmed.StartsWith('{') -or $trimmed.StartsWith('[')) {
		try { return $Body | ConvertFrom-Json } catch { return $null }
	}

	foreach ($line in ($Body -split '\r?\n')) {
		if (-not $line.StartsWith('data:')) { continue }
		$data = $line.Substring(5).Trim()
		if ([string]::IsNullOrWhiteSpace($data)) { continue }
		try { return $data | ConvertFrom-Json } catch { continue }
	}
	return $null
}

<#
.SYNOPSIS
	Returns the first text content block of an MCP tool result.
#>
function Get-RigMcpFirstText {
	[CmdletBinding()]
	param($Result)

	if ($null -eq $Result) { return $null }
	$content = $Result.PSObject.Properties['content']
	if ($null -eq $content) { return $null }
	foreach ($block in @($content.Value)) {
		if ($null -eq $block) { continue }
		$type = $block.PSObject.Properties['type']
		$textProperty = $block.PSObject.Properties['text']
		if ($null -ne $textProperty -and ($null -eq $type -or $type.Value -eq 'text')) {
			return "$($textProperty.Value)"
		}
	}
	return $null
}

<#
.SYNOPSIS
	Starts a background `docker stats` sampler writing CSV for the rig's containers.
#>
function Start-RigStatsSampler {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string[]] $Containers,
		[Parameter(Mandatory)] [string] $CsvPath
	)

	$arguments = @('stats', '--no-trunc', '--format', '{{.Name}},{{.CPUPerc}},{{.MemUsage}}') + $Containers
	return Start-Process -FilePath 'docker' -ArgumentList $arguments `
		-RedirectStandardOutput $CsvPath -RedirectStandardError ($CsvPath + '.err') `
		-NoNewWindow -PassThru
}

<#
.SYNOPSIS
	Stops a background sampler and reduces its CSV to peak CPU and memory.
#>
function Stop-RigStatsSampler {
	[CmdletBinding()]
	param(
		$Process,
		[Parameter(Mandatory)] [string] $CsvPath
	)

	if ($null -ne $Process -and -not $Process.HasExited) {
		Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
	}
	Start-Sleep -Milliseconds 200
	return Measure-RigStatsCsv -CsvPath $CsvPath
}

<#
.SYNOPSIS
	Reduces a `docker stats` CSV to peak and mean CPU and memory per container.
#>
function Measure-RigStatsCsv {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [string] $CsvPath)

	if (-not (Test-Path -LiteralPath $CsvPath)) { return @() }

	$rows = [System.Collections.Generic.List[object]]::new()
	foreach ($line in (Get-Content -LiteralPath $CsvPath -ErrorAction SilentlyContinue)) {
		# `docker stats` repaints with ANSI cursor codes even without a TTY on
		# some daemons; strip them before parsing.
		$clean = ($line -replace '\x1b\[[0-9;?]*[A-Za-z]', '').Trim()
		if ([string]::IsNullOrWhiteSpace($clean)) { continue }
		$parts = $clean -split ','
		if ($parts.Count -lt 3) { continue }

		$cpu = 0.0
		if (-not [double]::TryParse(($parts[1] -replace '%', '').Trim(), [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref] $cpu)) { continue }
		$memory = ConvertFrom-RigByteSize -Text (($parts[2] -split '/')[0])
		if ($null -eq $memory) { continue }

		$rows.Add([pscustomobject] @{ Container = $parts[0].Trim(); CpuPercent = $cpu; MemoryBytes = $memory })
	}

	$summaries = foreach ($group in ($rows | Group-Object Container | Sort-Object Name)) {
		[pscustomobject] @{
			Container       = $group.Name
			Samples         = $group.Count
			PeakCpuPercent  = [Math]::Round((($group.Group | Measure-Object CpuPercent -Maximum).Maximum), 2)
			MeanCpuPercent  = [Math]::Round((($group.Group | Measure-Object CpuPercent -Average).Average), 2)
			PeakMemoryBytes = [long] (($group.Group | Measure-Object MemoryBytes -Maximum).Maximum)
			MeanMemoryBytes = [long] (($group.Group | Measure-Object MemoryBytes -Average).Average)
		}
	}
	return @($summaries)
}

<#
.SYNOPSIS
	Runs one committed SQL script against a restored copy of the grain-state
	database, offline.

.DESCRIPTION
	Never touches a running box: the database is opened through an
	`immutable=1` URI (which also lets SQLite work over a read-only mount by
	skipping locking and any -wal sidecar), the mount is read-only, and the
	SQL lives in committed files under sql/ so nothing is assembled by string
	concatenation at the shell.

	Source selection matters for speed, not for correctness. A Docker VOLUME
	lives inside the Docker VM's own filesystem and is roughly an order of
	magnitude faster to scan than a host bind mount of the same bytes, so the
	master volume is preferred when it exists; a host staging directory is the
	fallback when all you have is an extracted tarball.

	Returns the raw output lines with blanks removed.
#>
function Invoke-RigSqlite {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config,
		[Parameter(Mandatory)] [string] $SqlName,
		[string] $Volume,
		[string] $StagingPath,
		[string] $SqlDirectory,
		[string] $HelperImage = 'alpine/sqlite:latest'
	)

	if (-not $SqlDirectory) {
		$SqlDirectory = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..' 'sql')).Path
	}
	$sqlFile = Join-Path $SqlDirectory ($SqlName + '.sql')
	if (-not (Test-Path -LiteralPath $sqlFile)) { throw "Census SQL not found: $sqlFile" }

	$dataMount = $null
	if ($Volume) {
		$violations = Test-RigVolumeName -Volume $Volume -Config $Config -Label 'census volume'
		if ($violations.Count -gt 0) {
			throw ("Rig isolation guard REFUSED the census source: " + ($violations -join '; ') + '.')
		}
		$dataMount = "${Volume}:/data:ro"
	}
	elseif ($StagingPath) {
		$resolved = (Resolve-Path -LiteralPath $StagingPath).Path
		$dataMount = "${resolved}:/data:ro"
	}
	else { throw 'Invoke-RigSqlite needs either -Volume or -StagingPath.' }

	$output = Invoke-RigDocker -DockerArgs @(
		'run', '--rm',
		'-v', $dataMount,
		'-v', "${SqlDirectory}:/sql:ro",
		$HelperImage,
		'file:/data/repocontext.db?immutable=1',
		".read /sql/$SqlName.sql"
	)

	return @($output | ForEach-Object { "$_" } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

<#
.SYNOPSIS
	Splits a pipe-delimited census row into its fields.
#>
function ConvertFrom-RigCensusRow {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [string] $Row, [int] $Fields)

	$parts = $Row -split '\|'
	if ($parts.Count -ne $Fields) {
		throw "Census row '$Row' has $($parts.Count) fields; expected $Fields."
	}
	return $parts
}

<#
.SYNOPSIS
	Parses a Docker-formatted byte size ("1.234GiB", "512MiB", "12.3kB").
#>
function ConvertFrom-RigByteSize {
	[CmdletBinding()]
	param([AllowNull()] [string] $Text)

	if ([string]::IsNullOrWhiteSpace($Text)) { return $null }
	$clean = $Text.Trim()
	if ($clean -notmatch '^(?<value>[0-9]*\.?[0-9]+)\s*(?<unit>[A-Za-z]*)$') { return $null }

	$value = 0.0
	if (-not [double]::TryParse($Matches['value'], [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref] $value)) { return $null }

	$multiplier = switch ($Matches['unit'].ToUpperInvariant()) {
		'' { 1 }
		'B' { 1 }
		'KB' { 1000 }
		'KIB' { 1024 }
		'MB' { 1000000 }
		'MIB' { 1048576 }
		'GB' { 1000000000 }
		'GIB' { 1073741824 }
		'TB' { 1000000000000 }
		'TIB' { 1099511627776 }
		default { $null }
	}
	if ($null -eq $multiplier) { return $null }
	return [long] ($value * $multiplier)
}
