#Requires -Version 7
<#
.SYNOPSIS
	Provision (or update) the elastic self-hosted GitHub Actions runner pool on
	Azure Container Apps Jobs.

.DESCRIPTION
	Creates one resource group rg-<NamePrefix> holding:

	  vnet-<prefix> 10.44.0.0/16
	    aca-infra 10.44.0.0/23     delegated to Microsoft.App/environments,
	                               egress through nat-<prefix>
	  nat-<prefix> + pip-nat-<prefix>  outbound only (GitHub, NuGet, Maven)
	  id-<prefix>                  user-assigned identity: AcrPull on acr<prefix>
	  acr<prefix>                  registry, admin user disabled; the runner
	                               image is built remotely with `az acr build`
	  law-<prefix>                 Log Analytics for the environment
	  cae-<prefix>                 internal-only workload-profiles environment
	  job-<prefix>                 event-triggered Container Apps Job

	The topology follows benchmark/azure-throughput's -PrivateNetwork shape, for
	subscriptions whose policy blocks public IPs on workloads and requires
	identity-based registry pulls. Nothing here listens: the runners only make
	outbound calls.

	The Job scales from zero. KEDA's github-runner scaler polls the
	repository's queued jobs whose labels match -RunnerLabels and starts one
	execution per queued job. Each execution runs:

	  init container mint-jit  holds the GitHub App key (an ACA secret), mints a
	                           single-use JIT runner config, writes it to an
	                           EmptyDir volume and exits.
	  container runner         starts the runner from that config, runs
	                           exactly one job and exits. It is never given
	                           the App key.

	See tools/ci-runners/README.md for the security model, the GitHub App
	setup and how workflows opt in.

.PARAMETER NamePrefix
	Names every resource (3-9 lowercase letters and digits).

.PARAMETER AppConfigDirectory
	Directory holding app.json ({ id, installation_id, ... }) and
	app-private-key.pem for the GitHub App. Defaults to ~/.lattice-ci-runners.

.PARAMETER SkipImageBuild
	Reuse the image already in the registry.

.EXAMPLE
	./tools/ci-runners/deploy.ps1 -NamePrefix latrun
#>
[CmdletBinding()]
param(
	[ValidatePattern('^[a-z0-9]{3,9}$')]
	[string] $NamePrefix = 'latrun',
	[string] $Location = 'westus3',
	[string] $Repository = 'NSTA1/Orleans.Lattice',
	[string] $RunnerLabels = 'lattice-tlc',
	[string] $AppConfigDirectory = (Join-Path $HOME '.lattice-ci-runners'),
	[ValidateRange(1, 100)] [int] $MaxExecutions = 30,
	[ValidateSet('1.0', '2.0', '3.0', '3.5', '4.0')] [string] $RunnerCpu = '3.5',
	[ValidateSet('2Gi', '4Gi', '6Gi', '7Gi', '8Gi')] [string] $RunnerMemory = '7Gi',
	[ValidateRange(600, 86400)] [int] $ReplicaTimeoutSec = 10800,
	[ValidateRange(10, 300)] [int] $PollingIntervalSec = 20,
	[switch] $SkipImageBuild
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

function Invoke-AzCli {
	param([Parameter(Mandatory)][string[]] $Arguments, [switch] $AllowFailure)
	$out = & az @Arguments 2>&1
	$stdout = @($out | Where-Object { $_ -isnot [System.Management.Automation.ErrorRecord] } | ForEach-Object { "$_" })
	if ($LASTEXITCODE -ne 0 -and -not $AllowFailure) {
		# Never echo a secret argument (the workspace key) into an error.
		$shown = for ($i = 0; $i -lt $Arguments.Count; $i++) {
			if ($i -gt 0 -and $Arguments[$i - 1] -match '-key$|secret') { '***' } else { $Arguments[$i] }
		}
		throw "az $($shown -join ' ') failed:`n$((@($out) | Select-Object -Last 5) -join "`n")"
	}
	if ($LASTEXITCODE -ne 0) { return ((@($out) | ForEach-Object { "$_" }) -join "`n") }
	return ($stdout -join "`n")
}

function Test-AzShow {
	param([Parameter(Mandatory)][string[]] $Arguments)
	& az @Arguments -o none 2>$null | Out-Null
	return $LASTEXITCODE -eq 0
}

function Grant-Role {
	param([string] $PrincipalId, [string] $Role, [string] $Scope)
	$existing = Invoke-AzCli @('role', 'assignment', 'list', '--assignee', $PrincipalId, '--role', $Role,
		'--scope', $Scope, '--query', 'length(@)', '-o', 'tsv') -AllowFailure
	if ($LASTEXITCODE -eq 0 -and "$existing".Trim() -match '^[1-9]') { return }
	for ($attempt = 1; $attempt -le 12; $attempt++) {
		$out = Invoke-AzCli @('role', 'assignment', 'create', '--assignee-object-id', $PrincipalId,
			'--assignee-principal-type', 'ServicePrincipal', '--role', $Role, '--scope', $Scope, '-o', 'none') -AllowFailure
		if ($LASTEXITCODE -eq 0 -or "$out" -match 'RoleAssignmentExists|already exists') { return }
		Write-Host "[runners] role '$Role' not yet assignable (attempt $attempt)" -ForegroundColor DarkYellow
		Start-Sleep -Seconds 10
	}
	throw "Could not assign '$Role' on $Scope."
}

$names = @{
	Rg        = "rg-$NamePrefix"
	Vnet      = "vnet-$NamePrefix"
	Subnet    = 'aca-infra'
	NatPip    = "pip-nat-$NamePrefix"
	Nat       = "nat-$NamePrefix"
	Identity  = "id-$NamePrefix"
	Acr       = "acr$NamePrefix"
	Workspace = "law-$NamePrefix"
	Env       = "cae-$NamePrefix"
	Job       = "job-$NamePrefix"
}

# Tag the image by the hash of everything that goes into it, so a changed
# script always reaches new executions (a reused :latest tag may be served
# from a node's image cache).
$imageInputs = 'Dockerfile', 'mint-jit.py', 'runner-entrypoint.sh' | ForEach-Object {
	[System.IO.File]::ReadAllText((Join-Path $PSScriptRoot $_)).Replace("`r`n", "`n")
}
$sha = [System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes($imageInputs -join "`0"))
$names.Image = "lattice-ci-runner:$([Convert]::ToHexString($sha).Substring(0, 12).ToLowerInvariant())"

# ---------------------------------------------------------------------------
# 0. GitHub App credentials (created once through the manifest flow; see README).
# ---------------------------------------------------------------------------
$appJsonPath = Join-Path $AppConfigDirectory 'app.json'
$appKeyPath = Join-Path $AppConfigDirectory 'app-private-key.pem'
if (-not (Test-Path $appJsonPath) -or -not (Test-Path $appKeyPath)) {
	throw "Missing $appJsonPath or $appKeyPath. Create the GitHub App first (README: 'GitHub App')."
}
$app = Get-Content $appJsonPath -Raw | ConvertFrom-Json
if (-not $app.id -or -not $app.installation_id) { throw "$appJsonPath needs 'id' and 'installation_id'." }
$appKey = Get-Content $appKeyPath -Raw
$owner, $repoName = $Repository.Split('/')

Write-Host "[runners] subscription: $((Invoke-AzCli @('account', 'show', '--query', 'name', '-o', 'tsv')).Trim())" -ForegroundColor Cyan

foreach ($ns in 'Microsoft.App', 'Microsoft.ContainerRegistry', 'Microsoft.OperationalInsights', 'Microsoft.Network', 'Microsoft.ManagedIdentity') {
	$state = (Invoke-AzCli @('provider', 'show', '-n', $ns, '--query', 'registrationState', '-o', 'tsv')).Trim()
	if ($state -ne 'Registered') {
		Write-Host "[runners] registering $ns" -ForegroundColor Cyan
		Invoke-AzCli @('provider', 'register', '-n', $ns, '--wait', '-o', 'none') | Out-Null
	}
}

# ---------------------------------------------------------------------------
# 1. Resource group, NAT egress, VNet, identity.
# ---------------------------------------------------------------------------
Invoke-AzCli @('group', 'create', '-n', $names.Rg, '-l', $Location,
	'--tags', 'purpose=lattice-ci-runners', "repo=$Repository", '-o', 'none') | Out-Null

if (-not (Test-AzShow @('network', 'public-ip', 'show', '-g', $names.Rg, '-n', $names.NatPip))) {
	Write-Host "[runners] creating NAT public IP" -ForegroundColor Cyan
	Invoke-AzCli @('network', 'public-ip', 'create', '-g', $names.Rg, '-n', $names.NatPip, '-l', $Location,
		'--sku', 'Standard', '--allocation-method', 'Static', '-o', 'none') | Out-Null
}
if (-not (Test-AzShow @('network', 'nat', 'gateway', 'show', '-g', $names.Rg, '-n', $names.Nat))) {
	Write-Host "[runners] creating NAT gateway" -ForegroundColor Cyan
	Invoke-AzCli @('network', 'nat', 'gateway', 'create', '-g', $names.Rg, '-n', $names.Nat, '-l', $Location,
		'--public-ip-addresses', $names.NatPip, '--idle-timeout', '10', '-o', 'none') | Out-Null
}
if (-not (Test-AzShow @('network', 'vnet', 'show', '-g', $names.Rg, '-n', $names.Vnet))) {
	Write-Host "[runners] creating VNet" -ForegroundColor Cyan
	Invoke-AzCli @('network', 'vnet', 'create', '-g', $names.Rg, '-n', $names.Vnet, '-l', $Location,
		'--address-prefixes', '10.44.0.0/16', '-o', 'none') | Out-Null
}
if (-not (Test-AzShow @('network', 'vnet', 'subnet', 'show', '-g', $names.Rg, '--vnet-name', $names.Vnet, '-n', $names.Subnet))) {
	Invoke-AzCli @('network', 'vnet', 'subnet', 'create', '-g', $names.Rg, '--vnet-name', $names.Vnet, '-n', $names.Subnet,
		'--address-prefixes', '10.44.0.0/23', '--delegations', 'Microsoft.App/environments',
		'--nat-gateway', $names.Nat, '-o', 'none') | Out-Null
}
$subnetId = (Invoke-AzCli @('network', 'vnet', 'subnet', 'show', '-g', $names.Rg, '--vnet-name', $names.Vnet,
	'-n', $names.Subnet, '--query', 'id', '-o', 'tsv')).Trim()

if (-not (Test-AzShow @('identity', 'show', '-g', $names.Rg, '-n', $names.Identity))) {
	Invoke-AzCli @('identity', 'create', '-g', $names.Rg, '-n', $names.Identity, '-l', $Location, '-o', 'none') | Out-Null
}
$identity = Invoke-AzCli @('identity', 'show', '-g', $names.Rg, '-n', $names.Identity, '-o', 'json') | ConvertFrom-Json

# ---------------------------------------------------------------------------
# 2. Registry (no admin user) and the runner image, built remotely.
# ---------------------------------------------------------------------------
if (-not (Test-AzShow @('acr', 'show', '-g', $names.Rg, '-n', $names.Acr))) {
	Write-Host "[runners] creating container registry" -ForegroundColor Cyan
	Invoke-AzCli @('acr', 'create', '-g', $names.Rg, '-n', $names.Acr, '-l', $Location, '--sku', 'Basic',
		'--admin-enabled', 'false', '-o', 'none') | Out-Null
}
$acr = Invoke-AzCli @('acr', 'show', '-g', $names.Rg, '-n', $names.Acr, '-o', 'json') | ConvertFrom-Json
Grant-Role -PrincipalId $identity.principalId -Role 'AcrPull' -Scope $acr.id
$image = "$($acr.loginServer)/$($names.Image)"

if (-not $SkipImageBuild) {
	Write-Host "[runners] building $image (remote)" -ForegroundColor Cyan
	Push-Location $repoRoot
	try {
		& az acr build --registry $names.Acr --image $names.Image --file tools/ci-runners/Dockerfile `
			--platform linux/amd64 . 2>&1 | ForEach-Object { "$_" } | Select-Object -Last 15
		if ($LASTEXITCODE -ne 0) { throw 'az acr build failed.' }
	} finally { Pop-Location }
}

# ---------------------------------------------------------------------------
# 3. Log Analytics and the internal-only workload-profiles environment.
# ---------------------------------------------------------------------------
if (-not (Test-AzShow @('monitor', 'log-analytics', 'workspace', 'show', '-g', $names.Rg, '--workspace-name', $names.Workspace))) {
	Invoke-AzCli @('monitor', 'log-analytics', 'workspace', 'create', '-g', $names.Rg, '--workspace-name', $names.Workspace,
		'-l', $Location, '--retention-time', '30', '-o', 'none') | Out-Null
}
if (-not (Test-AzShow @('containerapp', 'env', 'show', '-g', $names.Rg, '-n', $names.Env))) {
	$wsId = (Invoke-AzCli @('monitor', 'log-analytics', 'workspace', 'show', '-g', $names.Rg,
		'--workspace-name', $names.Workspace, '--query', 'customerId', '-o', 'tsv')).Trim()
	$wsKey = (Invoke-AzCli @('monitor', 'log-analytics', 'workspace', 'get-shared-keys', '-g', $names.Rg,
		'--workspace-name', $names.Workspace, '--query', 'primarySharedKey', '-o', 'tsv')).Trim()
	Write-Host "[runners] creating Container Apps environment (internal, workload profiles)" -ForegroundColor Cyan
	Invoke-AzCli @('containerapp', 'env', 'create', '-g', $names.Rg, '-n', $names.Env, '-l', $Location,
		'--logs-destination', 'log-analytics', '--logs-workspace-id', $wsId, '--logs-workspace-key', $wsKey,
		'--enable-workload-profiles', 'true', '--infrastructure-subnet-resource-id', $subnetId,
		'--internal-only', 'true', '-o', 'none') | Out-Null
}
$envId = (Invoke-AzCli @('containerapp', 'env', 'show', '-g', $names.Rg, '-n', $names.Env, '--query', 'id', '-o', 'tsv')).Trim()

# ---------------------------------------------------------------------------
# 4. The event-triggered runner Job. YAML rather than flags because the init
#    container, the EmptyDir hand-off and the per-container secret scoping have
#    no CLI flags.
# ---------------------------------------------------------------------------
# A PEM key spans lines, so it goes in as a YAML block literal: a quoted
# scalar would fold its line breaks into spaces.
function ConvertTo-YamlBlock([string] $Value, [int] $Indent) {
	$pad = ' ' * $Indent
	"|`n" + (($Value.Trim() -split "`r?`n" | ForEach-Object { "$pad$_" }) -join "`n")
}

$yaml = @"
location: $Location
identity:
  type: UserAssigned
  userAssignedIdentities:
    $($identity.id): {}
properties:
  environmentId: $envId
  workloadProfileName: Consumption
  configuration:
    triggerType: Event
    replicaTimeout: $ReplicaTimeoutSec
    replicaRetryLimit: 0
    registries:
    - server: $($acr.loginServer)
      identity: $($identity.id)
    secrets:
    - name: github-app-key
      value: $(ConvertTo-YamlBlock $appKey 8)
    eventTriggerConfig:
      parallelism: 1
      replicaCompletionCount: 1
      scale:
        minExecutions: 0
        maxExecutions: $MaxExecutions
        pollingInterval: $PollingIntervalSec
        rules:
        - name: github-runner
          type: github-runner
          metadata:
            owner: $owner
            runnerScope: repo
            repos: $repoName
            labels: $RunnerLabels
            targetWorkflowQueueLength: '1'
            applicationID: '$($app.id)'
            installationID: '$($app.installation_id)'
          auth:
          - secretRef: github-app-key
            triggerParameter: appKey
  template:
    initContainers:
    - name: mint-jit
      image: $image
      command: [ "python3", "/opt/ci-runner/mint-jit.py" ]
      resources:
        cpu: 0.5
        memory: 1Gi
      env:
      - name: GITHUB_APP_ID
        value: '$($app.id)'
      - name: GITHUB_APP_INSTALLATION_ID
        value: '$($app.installation_id)'
      - name: GITHUB_APP_KEY
        secretRef: github-app-key
      - name: GITHUB_REPOSITORY
        value: $Repository
      - name: RUNNER_LABELS
        value: $RunnerLabels
      - name: RUNNER_NAME_PREFIX
        value: $NamePrefix
      volumeMounts:
      - volumeName: jit
        mountPath: /jit
    containers:
    - name: runner
      image: $image
      command: [ "/opt/ci-runner/runner-entrypoint.sh" ]
      resources:
        cpu: $RunnerCpu
        memory: $RunnerMemory
      volumeMounts:
      - volumeName: jit
        mountPath: /jit
    volumes:
    - name: jit
      storageType: EmptyDir
"@

$yamlPath = Join-Path ([System.IO.Path]::GetTempPath()) "job-$NamePrefix-$([guid]::NewGuid().ToString('N')).yaml"
try {
	[System.IO.File]::WriteAllText($yamlPath, $yaml)
	if (Test-AzShow @('containerapp', 'job', 'show', '-g', $names.Rg, '-n', $names.Job)) {
		Write-Host "[runners] updating $($names.Job)" -ForegroundColor Cyan
		Invoke-AzCli @('containerapp', 'job', 'update', '-g', $names.Rg, '-n', $names.Job, '--yaml', $yamlPath, '-o', 'none') | Out-Null
	} else {
		Write-Host "[runners] creating $($names.Job)" -ForegroundColor Cyan
		Invoke-AzCli @('containerapp', 'job', 'create', '-g', $names.Rg, '-n', $names.Job, '--yaml', $yamlPath, '-o', 'none') | Out-Null
	}
} finally {
	Remove-Item $yamlPath -Force -ErrorAction SilentlyContinue
}

Write-Host ''
Write-Host "[runners] ready: $($names.Job) in $($names.Rg) scales 0..$MaxExecutions on queued jobs labelled '$RunnerLabels'." -ForegroundColor Green
Write-Host "[runners] opt a workflow job in with: runs-on: [self-hosted, $($RunnerLabels.Split(',')[0])]"
