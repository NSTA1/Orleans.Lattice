<#
    .SYNOPSIS
        Measures how long an approximate-index build takes to converge, for one
        repository on one container.

    .DESCRIPTION
        Registers a repository over MCP, then polls repocontext_health until the
        ANN plane reports Ready, recording ann.vectorsIndexed as it goes. The
        output is the wall-clock time to converge plus the sample series, which
        is what an A/B of the build path is scored on.

        WHY WALL-CLOCK TO READY AND NOT A STAGE HISTOGRAM. The stage split
        (repocontext.ann.build.stage.duration) only exists on builds that carry
        it, so an A/B whose "before" arm predates it cannot be scored on it. Time
        to converge is reported identically by every build, which makes it the
        only metric both arms can be compared on. Read the stage split
        afterwards to EXPLAIN a difference; do not try to score one with it.

        WHY IT DOES NOT READ /health/ready. That endpoint is the conjunction of
        the lifecycle phase and a demonstrated semantic query, so it can stay 503
        long after the build has converged and can flip to 200 on a single query
        before it has. It answers a different question; this probe asks the build
        about itself.

    .PARAMETER BaseUri
        The MCP endpoint. Defaults to the local deployment's published port.

    .PARAMETER RepoPath
        The in-container path to register, under the mounted workspace root.

    .PARAMETER RepoId
        The repository id. Defaults to the final segment of RepoPath, which is
        what repocontext_add_repo itself derives.

    .PARAMETER MaxWaitMinutes
        Gives up after this long and reports Converged=$false. A timeout is a
        RESULT, not an error: an arm that does not converge is exactly the
        finding an A/B is looking for, so it is recorded rather than thrown.

    .EXAMPLE
        ./Invoke-AnnBuildProbe.ps1 -BaseUri http://localhost:8090 -RepoPath /workspace/testcorpus
#>
param(
    [string] $BaseUri = 'http://localhost:8080',
    [Parameter(Mandatory)][string] $RepoPath,
    [string] $RepoId,
    [int] $TimeoutSeconds = 240,
    [int] $PollSeconds = 10,
    [int] $MaxWaitMinutes = 60,
    [string] $JsonOutputPath
)

$ErrorActionPreference = 'Stop'

. "$PSScriptRoot/_mcpClient.ps1"

if (-not $RepoId) { $RepoId = ($RepoPath.TrimEnd('/') -split '/')[-1] }

Set-McpEndpoint -BaseUri $BaseUri -TimeoutSeconds $TimeoutSeconds -ClientName 'ann-build-probe'

function Get-MemberOrNull {
    param($Object, [string] $Name)
    if ($null -eq $Object) { return $null }
    if ($Object -isnot [psobject]) { return $null }
    if ($Object.PSObject.Properties.Name -contains $Name) { return $Object.$Name }
    return $null
}

Write-Host "ANN build probe"
Write-Host "---------------"
Write-Host ("  endpoint   : {0}" -f $BaseUri)
Write-Host ("  repository : {0} ({1})" -f $RepoId, $RepoPath)

Initialize-McpSession | Out-Null

$register = $null
for ($attempt = 1; $attempt -le 10; $attempt++) {
    $register = Invoke-McpTool -Name 'repocontext_add_repo' -Arguments @{ path = $RepoPath; repoId = $RepoId }
    if ($register.Succeeded) { break }

    # /health/live returns 200 as soon as the process and silo host are alive,
    # which is EARLIER than the MCP surface being able to register a repository.
    # Retrying is what stops a race at container start being scored as a failed
    # arm, which is how the first A/B attempt lost every round.
    Write-Host ("  add_repo attempt {0} failed ({1}); retrying" -f $attempt, $register.Reason)
    Start-Sleep -Seconds 10
}
if (-not $register.Succeeded) {
    throw "repocontext_add_repo failed after retries: $($register.Reason)"
}

$started = Get-Date
$samples = @()
$converged = $false
$deadline = $started.AddMinutes($MaxWaitMinutes)

while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds $PollSeconds

    $health = Invoke-McpTool -Name 'repocontext_health' -Arguments @{ repoId = $RepoId }
    if (-not $health.Succeeded) {
        # A refused health call mid-build is not fatal to the measurement; the
        # next poll re-asks. Recording it keeps the series honest about gaps.
        $samples += [pscustomobject]@{
            ElapsedSeconds = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
            Error          = $health.Reason
        }
        continue
    }

    $repo = Get-MemberOrNull $health.Payload 'repository'
    $ann = Get-MemberOrNull $repo 'ann'
    $ingest = Get-MemberOrNull $repo 'ingest'
    $coverage = Get-MemberOrNull $repo 'vectorCoverage'

    $sample = [pscustomobject]@{
        ElapsedSeconds = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
        AnnPhase       = Get-MemberOrNull $ann 'phase'
        VectorsIndexed = Get-MemberOrNull $ann 'vectorsIndexed'
        VectorsExpected = Get-MemberOrNull $ann 'vectorsExpected'
        CoverageCount  = Get-MemberOrNull $coverage 'count'
        IngestStatus   = Get-MemberOrNull $ingest 'status'
        FilesEmbedded  = Get-MemberOrNull $ingest 'filesEmbedded'
        AnnCanServe    = Get-MemberOrNull $repo 'annCanServe'
        Error          = $null
    }
    $samples += $sample

    Write-Host ("  t+{0,7}s  ann={1,-10} vectors={2,-8} coverage={3,-8} ingest={4}" -f `
        $sample.ElapsedSeconds, $sample.AnnPhase, $sample.VectorsIndexed, $sample.CoverageCount, $sample.IngestStatus)

    # CONVERGENCE IS NOT `phase == Ready` ON ITS OWN, AND ASSUMING IT WAS MADE
    # THIS PROBE MEASURE NOTHING. A build over a corpus that has not been
    # embedded yet has no vectors to take in, so it reaches Ready immediately
    # and trivially: the first run of this probe scored 10.3 s with
    # `vectors=0` while ingest was still Running, and both arms of an A/B would
    # have tied at the cost of starting a container. Convergence is Ready over
    # the WHOLE corpus, so the vector count has to have caught up with the
    # coverage the embedder has produced, and that coverage has to be non-zero.
    $coverage = [int]($sample.CoverageCount ?? 0)
    $indexed = [int]($sample.VectorsIndexed ?? 0)
    if ($sample.AnnPhase -eq 'Ready' -and $coverage -gt 0 -and $indexed -ge $coverage) {
        $converged = $true
        break
    }
}

$elapsed = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
$final = $samples | Where-Object { $null -ne $_.VectorsIndexed } | Select-Object -Last 1
$finalVectors = if ($final) { [int]$final.VectorsIndexed } else { 0 }

$result = [ordered]@{
    BaseUri            = $BaseUri
    RepoId             = $RepoId
    RepoPath           = $RepoPath
    Converged          = $converged
    ElapsedSeconds     = $elapsed
    FinalVectorsIndexed = $finalVectors
    VectorsPerMinute   = if ($elapsed -gt 0) { [math]::Round($finalVectors / ($elapsed / 60), 1) } else { 0 }
    Samples            = $samples
    StartedUtc         = $started.ToUniversalTime().ToString('o')
}

Write-Host ""
Write-Host "RESULT"
Write-Host "------"
Write-Host ("  converged        : {0}" -f $converged)
Write-Host ("  elapsed seconds  : {0}" -f $elapsed)
Write-Host ("  vectors indexed  : {0}" -f $finalVectors)
Write-Host ("  vectors / minute : {0}" -f $result.VectorsPerMinute)

if ($JsonOutputPath) {
    $result | ConvertTo-Json -Depth 8 | Set-Content -Path $JsonOutputPath -Encoding utf8
    Write-Host ("  json             : {0}" -f $JsonOutputPath)
}

if (-not $converged) {
    Write-Host ""
    Write-Host "  NOT CONVERGED within the wait. That is a result, not a harness fault - record it."
    exit 2
}

exit 0
