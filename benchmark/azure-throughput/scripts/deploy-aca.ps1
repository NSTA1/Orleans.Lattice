<#
.SYNOPSIS
	Provision the multi-silo ("Layer 3") Azure Container Apps rig for the
	azure-throughput benchmark, and build + push its two images.

.DESCRIPTION
	Layer 2 of benchmark/performance-report.ps1 measures a SINGLE silo on one
	VM. Layer 3 measures the same workload against N silos so the report can
	show how throughput scales with silo count and where the knee is.

	The topology is deliberately the smallest thing that answers that question:

	  rg-<prefix>                      (tagged, so teardown can prove ownership)
	    acr<prefix>                    container registry; images are built
	                                   REMOTELY with `az acr build`, so this
	                                   script needs no local Docker daemon
	    st<prefix>                     one storage account: WAL + Orleans
	                                   clustering table + grain storage
	    log-<prefix>                   Log Analytics (required by ACA)
	    env-<prefix>                   Container Apps environment, Consumption
	    silo-<prefix>                  the silo app, scaled to exactly N
	    prod-<prefix>                  the producer, an ACA Job

	Why no VNet. A live two-app spike in this subscription proved that a
	container in one app can open a TCP connection directly to another app's
	replica pod IPs on an arbitrary port, in a DEFAULT Consumption environment
	with vnetConfiguration = null. A listening port answered and a closed port
	returned a TCP RST; only a timeout would have meant the traffic was
	dropped. That is exactly the reachability an Orleans client needs to call
	silo gateways, so VNet injection, subnet delegation and NSG rules are all
	unnecessary here. Adding them would move the environment away from the
	shape actually proven.

	Why the producer is a Job rather than an app. The producer runs to
	completion once per cohort and then must stop. A Job models that directly
	(manual trigger, one replica, completion tracked), whereas an app would
	have to be scaled 1 -> 0 around every cohort and forced onto a new
	revision to re-run.

	Why the silo does not need ingress. Orleans peers and clients address
	silos by the IP:port each silo registers in the clustering table, not
	through the environment's HTTP front door. The spike's target app had no
	ingress block at all and was still reachable on its pod IPs.

.PARAMETER NamePrefix
	Short run prefix, e.g. 'l3ab12cd3'. Every resource is named from it and
	the resource group is tagged with it.

.PARAMETER SiloCount
	Number of silo replicas to run (exactly; min = max, no autoscaling).

.PARAMETER Location
	Azure region. Defaults to westus3, the region the spike was proven in.

.PARAMETER SiloCpu
	vCPU per silo replica. Defaults to 4, matching the Layer 2 VM's
	Standard_D4as_v5 core count so the per-silo CPU budget is identical.

.PARAMETER SiloMemoryGi
	Memory per silo replica. ACA Consumption enforces a strict 1:2 vCPU:GiB
	ratio and caps a replica at 4 vCPU / 8 GiB, so 8 is both the default and
	the maximum. The Layer 2 VM has 16 GiB, so memory is the one dimension
	where Layer 3 cannot reach parity; the report documents that asymmetry
	rather than hiding it.

.PARAMETER SkipImageBuild
	Reuse images already in the registry. Only valid with -ReuseRg.

.PARAMETER ReuseRg
	Provision into an existing resource group from a previous run of this
	script instead of creating one. Used during development to avoid paying
	the image build on every iteration.

.EXAMPLE
	./deploy-aca.ps1 -NamePrefix l3ab12cd3 -SiloCount 2
#>
[CmdletBinding()]
param(
	[Parameter(Mandatory)][string] $NamePrefix,
	[ValidateRange(1, 30)][int] $SiloCount = 2,
	[string] $Location = 'westus3',
	[ValidateRange(1, 4)][double] $SiloCpu = 4,
	[ValidateRange(1, 8)][double] $SiloMemoryGi = 8,
	[switch] $SkipImageBuild,
	[switch] $ReuseRg
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot 'aca-common.ps1')

$names = Get-AcaNames -NamePrefix $NamePrefix
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path

Write-Host "[aca] prefix=$NamePrefix silos=$SiloCount region=$Location cpu=$SiloCpu mem=${SiloMemoryGi}Gi" -ForegroundColor Cyan

# ---------------------------------------------------------------------------
# 1. Resource group.
#
# Tagged at creation with the run prefix. Invoke-AcaTeardown refuses to delete
# a group that does not carry this tag, which is what stops a mistyped prefix
# from deleting an unrelated group that merely happens to be called rg-<that>.
# ---------------------------------------------------------------------------
if ($ReuseRg) {
	Assert-AcaRunGroup -ResourceGroup $names.Rg -NamePrefix $NamePrefix
	Write-Host "[aca] reusing $($names.Rg)" -ForegroundColor Yellow
} else {
	if (Test-AzGroupExists -Name $names.Rg) {
		throw "Resource group $($names.Rg) already exists. Pass -ReuseRg to provision into it, or pick a different -NamePrefix."
	}
	Write-Host "[aca] creating resource group $($names.Rg)" -ForegroundColor Cyan
	Invoke-Az @(
		'group', 'create',
		'--name', $names.Rg,
		'--location', $Location,
		'--tags', "$AcaRunTagName=$NamePrefix", 'purpose=orleans-lattice-benchmark'
	) | Out-Null
}

# ---------------------------------------------------------------------------
# 2. Storage account. One account carries the WAL, the Orleans clustering
#    table and grain storage. Multi-account WAL fan-out is deliberately out of
#    scope for this tier, so a single account's throughput ceiling is part of
#    what the scaling curve measures.
# ---------------------------------------------------------------------------
if (-not (Test-AzResourceExists -Kind 'storage' -Name $names.Storage -ResourceGroup $names.Rg)) {
	Write-Host "[aca] creating storage account $($names.Storage)" -ForegroundColor Cyan
	Invoke-Az @(
		'storage', 'account', 'create',
		'--name', $names.Storage,
		'--resource-group', $names.Rg,
		'--location', $Location,
		'--sku', 'Standard_LRS',
		'--kind', 'StorageV2',
		'--min-tls-version', 'TLS1_2',
		'--allow-blob-public-access', 'false'
	) | Out-Null
}

# Key-based connection string rather than managed identity. A user-assigned
# identity would need an RBAC role assignment whose propagation is eventually
# consistent, which on a short-lived rig shows up as an intermittent 403 on
# first write that is easily misread as a benchmark fault. The account is
# created and destroyed inside one run, so the key never outlives the rig.
$storageKey = (Invoke-Az @(
	'storage', 'account', 'keys', 'list',
	'--account-name', $names.Storage,
	'--resource-group', $names.Rg,
	'--query', '[0].value', '-o', 'tsv'
)).Trim()
$storageConn = "DefaultEndpointsProtocol=https;AccountName=$($names.Storage);AccountKey=$storageKey;EndpointSuffix=core.windows.net"

# ---------------------------------------------------------------------------
# 3. Container registry + remote image build.
# ---------------------------------------------------------------------------
if (-not (Test-AzResourceExists -Kind 'acr' -Name $names.Acr -ResourceGroup $names.Rg)) {
	Write-Host "[aca] creating container registry $($names.Acr)" -ForegroundColor Cyan
	Invoke-Az @(
		'acr', 'create',
		'--name', $names.Acr,
		'--resource-group', $names.Rg,
		'--sku', 'Basic',
		'--location', $Location,
		'--admin-enabled', 'true'
	) | Out-Null
}

$acrServer = (Invoke-Az @('acr', 'show', '--name', $names.Acr, '--query', 'loginServer', '-o', 'tsv')).Trim()
$siloImage = "$acrServer/lattice-bench-silo:$NamePrefix"
$prodImage = "$acrServer/lattice-bench-producer:$NamePrefix"

if (-not $SkipImageBuild) {
	# `az acr build` uploads the build context and builds in the registry's
	# own build service, so this works from any machine with the CLI and no
	# Docker daemon. The context is the REPO ROOT because both Dockerfiles
	# reference src/ and samples/ as well as benchmark/.
	Write-Host "[aca] building silo image (remote, context=$repoRoot)" -ForegroundColor Cyan
	Invoke-Az @(
		'acr', 'build',
		'--registry', $names.Acr,
		'--image', "lattice-bench-silo:$NamePrefix",
		'--file', 'benchmark/azure-throughput/Silo/Dockerfile',
		$repoRoot
	) -PassthruOutput

	Write-Host "[aca] building producer image (remote)" -ForegroundColor Cyan
	Invoke-Az @(
		'acr', 'build',
		'--registry', $names.Acr,
		'--image', "lattice-bench-producer:$NamePrefix",
		'--file', 'benchmark/azure-throughput/Producer/Dockerfile',
		$repoRoot
	) -PassthruOutput
} else {
	Write-Host "[aca] -SkipImageBuild: reusing $siloImage" -ForegroundColor Yellow
}

# ---------------------------------------------------------------------------
# 4. Container Apps environment.
# ---------------------------------------------------------------------------
if (-not (Test-AzResourceExists -Kind 'acaenv' -Name $names.Env -ResourceGroup $names.Rg)) {
	Write-Host "[aca] creating Container Apps environment $($names.Env)" -ForegroundColor Cyan
	# No --internal-only and no VNet arguments: see the header note on why the
	# default Consumption environment is the proven shape.
	Invoke-Az @(
		'containerapp', 'env', 'create',
		'--name', $names.Env,
		'--resource-group', $names.Rg,
		'--location', $Location,
		'--logs-destination', 'log-analytics',
		'--logs-workspace-name', $names.Workspace
	) | Out-Null
}

$envId = (Invoke-Az @(
	'containerapp', 'env', 'show',
	'--name', $names.Env, '--resource-group', $names.Rg,
	'--query', 'id', '-o', 'tsv'
)).Trim()

$acrUser = (Invoke-Az @('acr', 'credential', 'show', '--name', $names.Acr, '--query', 'username', '-o', 'tsv')).Trim()
$acrPass = (Invoke-Az @('acr', 'credential', 'show', '--name', $names.Acr, '--query', 'passwords[0].value', '-o', 'tsv')).Trim()

Write-Host '[aca] provisioning complete' -ForegroundColor Green

# Everything the cohort runner needs to drive this rig, written where both it
# and a human debugging a half-finished run can find it.
$ctx = [ordered]@{
	namePrefix   = $NamePrefix
	location     = $Location
	resourceGroup= $names.Rg
	acr          = $names.Acr
	acrServer    = $acrServer
	acrUser      = $acrUser
	acrPassword  = $acrPass
	storage      = $names.Storage
	storageConn  = $storageConn
	workspace    = $names.Workspace
	envName      = $names.Env
	envId        = $envId
	siloApp      = $names.SiloApp
	producerJob  = $names.ProducerJob
	siloImage    = $siloImage
	producerImage= $prodImage
	siloCpu      = $SiloCpu
	siloMemoryGi = $SiloMemoryGi
	siloCount    = $SiloCount
	createdUtc   = (Get-Date).ToUniversalTime().ToString('o')
}
$ctxPath = Save-AcaContext -Context $ctx
Write-Host "[aca] context: $ctxPath" -ForegroundColor Green
$ctx
