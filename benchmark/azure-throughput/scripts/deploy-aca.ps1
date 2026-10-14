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
	    silo-<prefix>                  the silo app, cohort runner scales it to N
	    prod-<prefix>                  the producer, an ACA Job

	Why no VNet. A live two-app spike in this subscription proved that a
	container in one app can open a TCP connection directly to another app's
	replica pod IPs on an arbitrary port, in a DEFAULT Consumption environment
	with vnetConfiguration = null. A listening port answered and a closed port
	returned a TCP RST; only a timeout would have meant the traffic was
	dropped. That is exactly the reachability an Orleans client needs to call
	silo gateways, so VNet injection, subnet delegation and NSG rules are all
	unnecessary here. Adding them would move the environment away from the
	shape actually proven. The one exception is -PrivateNetwork, below, where
	subscription policy rather than reachability forces a VNet.

	-PrivateNetwork. For subscriptions whose policy disables storage public
	network access and shared-key auth, and blocks public IPs on workloads.
	The topology gains:

	  vnet-<prefix>                    10.43.0.0/16
	    aca-infra 10.43.0.0/23         delegated to Microsoft.App/environments,
	                                   egress through nat-<prefix>
	    pe        10.43.2.0/27         storage private endpoint
	  nat-<prefix> + pip-nat-<prefix>  outbound only (ACR image pull)
	  privatelink.table.core.windows.net  private DNS zone linked to the VNet
	  id-<prefix>                      user-assigned identity: Storage Table
	                                   Data Contributor on st<prefix>, AcrPull
	                                   on acr<prefix>

	The environment becomes an internal-only workload-profiles environment on
	aca-infra (apps still run on the Consumption profile, so the 4 vCPU /
	8 GiB per-replica budget is unchanged). Storage has public access and
	shared keys disabled, so the silo and producer authenticate with the
	identity through BENCH_STORAGE_URI / BENCH_CLUSTERING_TABLE_SERVICE_URI
	and AZURE_CLIENT_ID instead of a connection-string secret, and images are
	pulled with the identity instead of ACR admin credentials. The resource
	group is tagged latticeBenchNetwork=private.

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
	Default silo count recorded in the run context. The app is created
	with a 0..30 replica range; run-cohort-aca.ps1 pins each cohort's
	count when it runs.

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
	Reuse images already in the registry. Intended for -ReuseRg; the
	script does not require that switch, but the images must already exist.

.PARAMETER ReuseRg
	Provision into an existing resource group from a previous run of this
	script instead of creating one. Used during development to avoid paying
	the image build on every iteration.

.PARAMETER PrivateNetwork
	Provision the policy-compliant private topology described above: VNet
	integrated internal environment, storage behind a private endpoint with
	public access and shared keys disabled, managed-identity auth throughout,
	and NAT-gateway egress. Off by default, which keeps the original shape.
	Combining it with -ReuseRg requires the group to have been created with
	-PrivateNetwork too.

.EXAMPLE
	./deploy-aca.ps1 -NamePrefix l3ab12cd3 -SiloCount 2

.EXAMPLE
	./deploy-aca.ps1 -NamePrefix l3ab12cd3 -SiloCount 2 -PrivateNetwork
#>
[CmdletBinding()]
param(
	[Parameter(Mandatory)][string] $NamePrefix,
	[ValidateRange(1, 30)][int] $SiloCount = 2,
	[string] $Location = 'westus3',
	[ValidateRange(1, 4)][double] $SiloCpu = 4,
	[ValidateRange(1, 8)][double] $SiloMemoryGi = 8,
	[switch] $SkipImageBuild,
	[switch] $ReuseRg,
	[switch] $PrivateNetwork
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot 'aca-common.ps1')

$names = Get-AcaNames -NamePrefix $NamePrefix
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path

$networkMode = if ($PrivateNetwork) { 'private' } else { 'public' }
Write-Host "[aca] prefix=$NamePrefix silos=$SiloCount region=$Location cpu=$SiloCpu mem=${SiloMemoryGi}Gi network=$networkMode" -ForegroundColor Cyan

# Private-mode resource names. Derived here rather than in Get-AcaNames so the
# default path's name set, and everything that reads it, is untouched.
$net = [ordered]@{
	Vnet        = "vnet-$NamePrefix"
	InfraSubnet = 'aca-infra'
	PeSubnet    = 'pe'
	NatPip      = "pip-nat-$NamePrefix"
	Nat         = "nat-$NamePrefix"
	Identity    = "id-$NamePrefix"
	TableZone   = 'privatelink.table.core.windows.net'
	TablePe     = "pe-$($names.Storage)-table"
}

function Test-AzShow {
	# Existence probe for the private-mode network resources, which
	# Test-AzResourceExists does not cover. Any non-zero exit means absent.
	param([Parameter(Mandatory)][string[]] $AzArgs)
	Invoke-Az ($AzArgs + @('-o', 'none')) -AllowFailure | Out-Null
	return ($LASTEXITCODE -eq 0)
}

function Grant-AcaRole {
	# Idempotent role assignment for a freshly created identity. A new
	# principal takes a short while to replicate into the directory, and until
	# it does the assignment fails with "principal does not exist", so retry.
	param(
		[Parameter(Mandatory)][string] $PrincipalId,
		[Parameter(Mandatory)][string] $Role,
		[Parameter(Mandatory)][string] $Scope
	)
	$existing = Invoke-Az @(
		'role', 'assignment', 'list',
		'--assignee', $PrincipalId, '--role', $Role, '--scope', $Scope,
		'--query', 'length(@)', '-o', 'tsv'
	) -AllowFailure
	if ($LASTEXITCODE -eq 0 -and "$existing".Trim() -ne '' -and [int]"$existing".Trim() -gt 0) { return $false }
	for ($attempt = 1; $attempt -le 12; $attempt++) {
		$out = Invoke-Az @(
			'role', 'assignment', 'create',
			'--assignee-object-id', $PrincipalId,
			'--assignee-principal-type', 'ServicePrincipal',
			'--role', $Role, '--scope', $Scope, '-o', 'none'
		) -AllowFailure
		if ($LASTEXITCODE -eq 0) { return $true }
		if ("$out" -match 'RoleAssignmentExists|already exists') { return $false }
		Write-Host "[aca] role '$Role' not yet assignable (attempt $attempt): $out" -ForegroundColor DarkYellow
		Start-Sleep -Seconds 10
	}
	throw "Could not assign role '$Role' on $Scope to $PrincipalId."
}

# ---------------------------------------------------------------------------
# 0. Resource providers.
#
# A fresh subscription may not have every namespace the rig uses registered,
# and the first create in that namespace then fails with
# MissingSubscriptionRegistration. Registration is idempotent and only issued
# for a namespace that is not already Registered.
# ---------------------------------------------------------------------------
$providers = @('Microsoft.App', 'Microsoft.ContainerRegistry', 'Microsoft.OperationalInsights', 'Microsoft.Storage')
if ($PrivateNetwork) { $providers += @('Microsoft.Network', 'Microsoft.ManagedIdentity') }
$pending = @()
foreach ($ns in $providers) {
	$state = "$(Invoke-Az @('provider', 'show', '--namespace', $ns, '--query', 'registrationState', '-o', 'tsv') -AllowFailure)".Trim()
	if ($state -eq 'Registered') { continue }
	Write-Host "[aca] registering resource provider $ns (was '$state')" -ForegroundColor Cyan
	Invoke-Az @('provider', 'register', '--namespace', $ns) | Out-Null
	$pending += $ns
}
# Registration of a new namespace (Microsoft.App in particular) can take tens
# of minutes, so the registrations above run concurrently and are awaited here.
$deadline = (Get-Date).AddMinutes(60)
while ($pending.Count -gt 0) {
	$pending = @($pending | Where-Object {
		"$(Invoke-Az @('provider', 'show', '--namespace', $_, '--query', 'registrationState', '-o', 'tsv') -AllowFailure)".Trim() -ne 'Registered'
	})
	if ($pending.Count -eq 0) { break }
	if ((Get-Date) -gt $deadline) { throw "Resource providers still not registered after 60 minutes: $($pending -join ', ')" }
	Write-Host "[aca] waiting for provider registration: $($pending -join ', ')" -ForegroundColor DarkYellow
	Start-Sleep -Seconds 30
}

# ---------------------------------------------------------------------------
# 1. Resource group.
#
# Tagged at creation with the run prefix. Invoke-AcaTeardown refuses to delete
# a group that does not carry this tag, which is what stops a mistyped prefix
# from deleting an unrelated group that merely happens to be called rg-<that>.
# ---------------------------------------------------------------------------
if ($ReuseRg) {
	Assert-AcaRunGroup -ResourceGroup $names.Rg -NamePrefix $NamePrefix
	$existingMode = (Invoke-Az @('group', 'show', '--name', $names.Rg, '--query', 'tags.latticeBenchNetwork', '-o', 'tsv') -AllowFailure)
	if ("$existingMode".Trim() -eq 'private' -and -not $PrivateNetwork) {
		throw "$($names.Rg) was provisioned with -PrivateNetwork; pass -PrivateNetwork to reuse it."
	}
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
		'--tags', "$AcaRunTagName=$NamePrefix", 'purpose=orleans-lattice-benchmark', "latticeBenchNetwork=$networkMode"
	) | Out-Null
}

# ---------------------------------------------------------------------------
# 1b. -PrivateNetwork only: VNet, NAT egress, and the user-assigned identity.
#
# NAT rather than the platform default egress so outbound traffic (ACR image
# pulls) never needs a public IP on a workload; the NAT gateway's own IP is the
# only public address in the group. The infrastructure subnet is a /23
# delegated to Microsoft.App/environments, which is what a workload-profiles
# environment requires and leaves room for a 30-replica sweep.
# ---------------------------------------------------------------------------
$identityId = ''
$identityClientId = ''
$identityPrincipalId = ''
$infraSubnetId = ''
$rolesGranted = $false
if ($PrivateNetwork) {
	if (-not (Test-AzShow @('network', 'public-ip', 'show', '-g', $names.Rg, '-n', $net.NatPip))) {
		Write-Host "[aca] creating NAT public IP $($net.NatPip)" -ForegroundColor Cyan
		Invoke-Az @('network', 'public-ip', 'create', '-g', $names.Rg, '-n', $net.NatPip,
			'-l', $Location, '--sku', 'Standard', '--allocation-method', 'Static', '-o', 'none') | Out-Null
	}
	if (-not (Test-AzShow @('network', 'nat', 'gateway', 'show', '-g', $names.Rg, '-n', $net.Nat))) {
		Write-Host "[aca] creating NAT gateway $($net.Nat)" -ForegroundColor Cyan
		Invoke-Az @('network', 'nat', 'gateway', 'create', '-g', $names.Rg, '-n', $net.Nat,
			'-l', $Location, '--public-ip-addresses', $net.NatPip, '--idle-timeout', '10', '-o', 'none') | Out-Null
	}
	if (-not (Test-AzShow @('network', 'vnet', 'show', '-g', $names.Rg, '-n', $net.Vnet))) {
		Write-Host "[aca] creating VNet $($net.Vnet)" -ForegroundColor Cyan
		Invoke-Az @('network', 'vnet', 'create', '-g', $names.Rg, '-n', $net.Vnet,
			'-l', $Location, '--address-prefixes', '10.43.0.0/16', '-o', 'none') | Out-Null
	}
	if (-not (Test-AzShow @('network', 'vnet', 'subnet', 'show', '-g', $names.Rg, '--vnet-name', $net.Vnet, '-n', $net.InfraSubnet))) {
		Invoke-Az @('network', 'vnet', 'subnet', 'create', '-g', $names.Rg, '--vnet-name', $net.Vnet,
			'-n', $net.InfraSubnet, '--address-prefixes', '10.43.0.0/23',
			'--delegations', 'Microsoft.App/environments', '--nat-gateway', $net.Nat, '-o', 'none') | Out-Null
	}
	if (-not (Test-AzShow @('network', 'vnet', 'subnet', 'show', '-g', $names.Rg, '--vnet-name', $net.Vnet, '-n', $net.PeSubnet))) {
		Invoke-Az @('network', 'vnet', 'subnet', 'create', '-g', $names.Rg, '--vnet-name', $net.Vnet,
			'-n', $net.PeSubnet, '--address-prefixes', '10.43.2.0/27',
			'--private-endpoint-network-policies', 'Disabled', '-o', 'none') | Out-Null
	}
	$infraSubnetId = (Invoke-Az @('network', 'vnet', 'subnet', 'show', '-g', $names.Rg, '--vnet-name', $net.Vnet,
		'-n', $net.InfraSubnet, '--query', 'id', '-o', 'tsv')).Trim()

	if (-not (Test-AzShow @('identity', 'show', '-g', $names.Rg, '-n', $net.Identity))) {
		Write-Host "[aca] creating managed identity $($net.Identity)" -ForegroundColor Cyan
		Invoke-Az @('identity', 'create', '-g', $names.Rg, '-n', $net.Identity, '-l', $Location, '-o', 'none') | Out-Null
	}
	$idJson = Invoke-Az @('identity', 'show', '-g', $names.Rg, '-n', $net.Identity, '-o', 'json') | ConvertFrom-Json
	$identityId = $idJson.id
	$identityClientId = $idJson.clientId
	$identityPrincipalId = $idJson.principalId
}

# ---------------------------------------------------------------------------
# 2. Storage account. One account carries the WAL, the Orleans clustering
#    table and grain storage. Multi-account WAL fan-out is deliberately out of
#    scope for this tier, so a single account's throughput ceiling is part of
#    what the scaling curve measures.
# ---------------------------------------------------------------------------
if (-not (Test-AzResourceExists -Kind 'storage' -Name $names.Storage -ResourceGroup $names.Rg)) {
	Write-Host "[aca] creating storage account $($names.Storage)" -ForegroundColor Cyan
	$storageArgs = @(
		'storage', 'account', 'create',
		'--name', $names.Storage,
		'--resource-group', $names.Rg,
		'--location', $Location,
		'--sku', 'Standard_LRS',
		'--kind', 'StorageV2',
		'--min-tls-version', 'TLS1_2',
		'--allow-blob-public-access', 'false'
	)
	if ($PrivateNetwork) {
		$storageArgs += @('--public-network-access', 'Disabled', '--default-action', 'Deny',
			'--allow-shared-key-access', 'false')
	}
	Invoke-Az $storageArgs | Out-Null
}

$storageConn = ''
$storageTableUri = "https://$($names.Storage).table.core.windows.net"
if ($PrivateNetwork) {
	# Table private endpoint + privatelink DNS. The silo and producer use
	# Table storage only (WAL, clustering, grain state), so no blob or queue
	# endpoint is needed.
	$storageId = (Invoke-Az @('storage', 'account', 'show', '-n', $names.Storage, '-g', $names.Rg, '--query', 'id', '-o', 'tsv')).Trim()
	$vnetId = (Invoke-Az @('network', 'vnet', 'show', '-g', $names.Rg, '-n', $net.Vnet, '--query', 'id', '-o', 'tsv')).Trim()
	if (-not (Test-AzShow @('network', 'private-dns', 'zone', 'show', '-g', $names.Rg, '-n', $net.TableZone))) {
		Invoke-Az @('network', 'private-dns', 'zone', 'create', '-g', $names.Rg, '-n', $net.TableZone, '-o', 'none') | Out-Null
	}
	if (-not (Test-AzShow @('network', 'private-dns', 'link', 'vnet', 'show', '-g', $names.Rg, '-z', $net.TableZone, '-n', "link-$NamePrefix"))) {
		Invoke-Az @('network', 'private-dns', 'link', 'vnet', 'create', '-g', $names.Rg, '-z', $net.TableZone,
			'-n', "link-$NamePrefix", '-v', $vnetId, '-e', 'false', '-o', 'none') | Out-Null
	}
	if (-not (Test-AzShow @('network', 'private-endpoint', 'show', '-g', $names.Rg, '-n', $net.TablePe))) {
		Write-Host "[aca] creating table private endpoint $($net.TablePe)" -ForegroundColor Cyan
		Invoke-Az @('network', 'private-endpoint', 'create', '-g', $names.Rg, '-n', $net.TablePe, '-l', $Location,
			'--vnet-name', $net.Vnet, '--subnet', $net.PeSubnet,
			'--private-connection-resource-id', $storageId, '--group-id', 'table',
			'--connection-name', "$($net.TablePe)-conn", '-o', 'none') | Out-Null
		Invoke-Az @('network', 'private-endpoint', 'dns-zone-group', 'create', '-g', $names.Rg,
			'--endpoint-name', $net.TablePe, '-n', 'default',
			'--private-dns-zone', $net.TableZone, '--zone-name', 'table', '-o', 'none') | Out-Null
	}
	if (Grant-AcaRole -PrincipalId $identityPrincipalId -Role 'Storage Table Data Contributor' -Scope $storageId) { $rolesGranted = $true }
} else {
	# Key-based connection string rather than managed identity. A user-assigned
	# identity would need an RBAC role assignment whose propagation is eventually
	# consistent, which on a short-lived rig shows up as an intermittent 403 on
	# first write that is easily misread as a benchmark fault. The account is
	# created and destroyed inside one run, so the key never outlives the rig.
	# (-PrivateNetwork has no choice: policy disables shared keys there.)
	$storageKey = (Invoke-Az @(
		'storage', 'account', 'keys', 'list',
		'--account-name', $names.Storage,
		'--resource-group', $names.Rg,
		'--query', '[0].value', '-o', 'tsv'
	)).Trim()
	$storageConn = "DefaultEndpointsProtocol=https;AccountName=$($names.Storage);AccountKey=$storageKey;EndpointSuffix=core.windows.net"
}

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
		'--admin-enabled', $(if ($PrivateNetwork) { 'false' } else { 'true' })
	) | Out-Null
}

$acrServer = (Invoke-Az @('acr', 'show', '--name', $names.Acr, '--query', 'loginServer', '-o', 'tsv')).Trim()
if ($PrivateNetwork) {
	# Pull with the identity rather than admin credentials, which tenant policy
	# commonly disables. Granted before the image build so the build time
	# covers RBAC propagation.
	$acrId = (Invoke-Az @('acr', 'show', '--name', $names.Acr, '--query', 'id', '-o', 'tsv')).Trim()
	if (Grant-AcaRole -PrincipalId $identityPrincipalId -Role 'AcrPull' -Scope $acrId) { $rolesGranted = $true }
}
$siloImage = "$acrServer/lattice-bench-silo:$NamePrefix"
$prodImage = "$acrServer/lattice-bench-producer:$NamePrefix"

if (-not $SkipImageBuild) {
	# `az acr build` uploads the build context and builds in the registry's
	# own build service, so this works from any machine with the CLI and no
	# Docker daemon. The context is the REPO ROOT because both Dockerfiles
	# reference src/ and samples/ as well as benchmark/.
	#
	# Push-Location matters: --file is resolved against the CURRENT DIRECTORY
	# rather than against the context argument, so a repo-relative path fails
	# with "Unable to find ..." when this script is invoked from its own
	# scripts/ directory. Running the builds from the repo root makes the two
	# agree.
	Push-Location $repoRoot
	try {
		Write-Host "[aca] building silo image (remote, context=$repoRoot)" -ForegroundColor Cyan
		Invoke-Az @(
			'acr', 'build',
			'--registry', $names.Acr,
			'--image', "lattice-bench-silo:$NamePrefix",
			'--file', 'benchmark/azure-throughput/Silo/Dockerfile',
			'.'
		) -PassthruOutput

		Write-Host '[aca] building producer image (remote)' -ForegroundColor Cyan
		Invoke-Az @(
			'acr', 'build',
			'--registry', $names.Acr,
			'--image', "lattice-bench-producer:$NamePrefix",
			'--file', 'benchmark/azure-throughput/Producer/Dockerfile',
			'.'
		) -PassthruOutput
	}
	finally { Pop-Location }
} else {
	Write-Host "[aca] -SkipImageBuild: reusing $siloImage" -ForegroundColor Yellow
}

# ---------------------------------------------------------------------------
# 4. Log Analytics workspace, then the Container Apps environment.
#
# The workspace is created explicitly rather than left to `containerapp env
# create`: the CLI has no `--logs-workspace-name`, only `--logs-workspace-id`
# (the customer GUID) and `--logs-workspace-key`, so the workspace has to exist
# before the environment can be pointed at it. Creating it here also means the
# same workspace survives a `-ReuseRg` redeploy, so logs from earlier cohorts
# stay queryable.
# ---------------------------------------------------------------------------
if (-not (Test-AzResourceExists -Kind 'workspace' -Name $names.Workspace -ResourceGroup $names.Rg)) {
	Write-Host "[aca] creating Log Analytics workspace $($names.Workspace)" -ForegroundColor Cyan
	Invoke-Az @(
		'monitor', 'log-analytics', 'workspace', 'create',
		'--workspace-name', $names.Workspace,
		'--resource-group', $names.Rg,
		'--location', $Location,
		'-o', 'none'
	) | Out-Null
}

# The workspace GUID, not the ARM id: cohort log harvest queries Log Analytics
# rather than holding a live log stream open for the length of a cohort. A
# stream is fresher but it is a single connection that must survive minutes of
# a saturated cluster, and if it drops mid-cohort the measurement is lost with
# no way to recover it. The workspace already has every line, so a query can be
# retried until the run's terminal marker appears.
$workspaceId = (Invoke-Az @(
	'monitor', 'log-analytics', 'workspace', 'show',
	'--workspace-name', $names.Workspace, '--resource-group', $names.Rg,
	'--query', 'customerId', '-o', 'tsv'
)).Trim()

if (-not (Test-AzResourceExists -Kind 'acaenv' -Name $names.Env -ResourceGroup $names.Rg)) {
	$workspaceKey = (Invoke-Az @(
		'monitor', 'log-analytics', 'workspace', 'get-shared-keys',
		'--workspace-name', $names.Workspace, '--resource-group', $names.Rg,
		'--query', 'primarySharedKey', '-o', 'tsv'
	)).Trim()

	Write-Host "[aca] creating Container Apps environment $($names.Env)" -ForegroundColor Cyan
	# No --internal-only and no VNet arguments by default: see the header note
	# on why the default Consumption environment is the proven shape.
	# -PrivateNetwork instead creates an internal-only workload-profiles
	# environment on the delegated subnet, so it exposes no public IP.
	$envArgs = @(
		'containerapp', 'env', 'create',
		'--name', $names.Env,
		'--resource-group', $names.Rg,
		'--location', $Location,
		'--logs-destination', 'log-analytics',
		'--logs-workspace-id', $workspaceId,
		'--logs-workspace-key', $workspaceKey,
		'-o', 'none'
	)
	if ($PrivateNetwork) {
		$envArgs += @('--enable-workload-profiles', 'true',
			'--infrastructure-subnet-resource-id', $infraSubnetId,
			'--internal-only', 'true')
	}
	Invoke-Az $envArgs | Out-Null
}

$envId = (Invoke-Az @(
	'containerapp', 'env', 'show',
	'--name', $names.Env, '--resource-group', $names.Rg,
	'--query', 'id', '-o', 'tsv'
)).Trim()

$acrUser = ''
$acrPass = ''
if ($PrivateNetwork) {
	$registryArgs = @('--registry-server', $acrServer, '--registry-identity', $identityId)
	$siloAuthArgs = @('--user-assigned', $identityId, '--workload-profile-name', 'Consumption')
	$jobAuthArgs = @('--mi-user-assigned', $identityId, '--workload-profile-name', 'Consumption')
	$siloStorageEnv = @(
		"BENCH_STORAGE_URI=$storageTableUri",
		"BENCH_CLUSTERING_TABLE_SERVICE_URI=$storageTableUri",
		"AZURE_CLIENT_ID=$identityClientId"
	)
	$prodStorageEnv = @(
		"BENCH_CLUSTERING_TABLE_SERVICE_URI=$storageTableUri",
		"AZURE_CLIENT_ID=$identityClientId"
	)
	$secretArgs = @()
	if ($rolesGranted -and $SkipImageBuild) {
		# No image build to absorb RBAC propagation, so give it a fixed window
		# before the first replica tries to pull and write.
		Write-Host '[aca] waiting 60s for new role assignments to propagate' -ForegroundColor Yellow
		Start-Sleep -Seconds 60
	}
} else {
	$acrUser = (Invoke-Az @('acr', 'credential', 'show', '--name', $names.Acr, '--query', 'username', '-o', 'tsv')).Trim()
	$acrPass = (Invoke-Az @('acr', 'credential', 'show', '--name', $names.Acr, '--query', 'passwords[0].value', '-o', 'tsv')).Trim()
	$registryArgs = @('--registry-server', $acrServer, '--registry-username', $acrUser, '--registry-password', $acrPass)
	$siloAuthArgs = @()
	$jobAuthArgs = @()
	$siloStorageEnv = @(
		'BENCH_CLUSTERING_CONNECTION_STRING=secretref:storageconn',
		'BENCH_STORAGE_CONN=secretref:storageconn'
	)
	$prodStorageEnv = $siloStorageEnv
	$secretArgs = @('--secrets', "storageconn=$storageConn")
}

# ---------------------------------------------------------------------------
# 5. The silo app and the producer job.
#
# Created here with a baseline environment and left RESTING AT ZERO REPLICAS.
# Creating a container app is slow; updating one is not, and a sweep changes
# only a handful of env vars per cell. So the shape is created once and the
# cohort runner mutates and scales it, rather than building and destroying an
# app per measurement.
#
# Starting at zero replicas is deliberate and is the whole basis of the "no
# containers left running longer than needed" guarantee: between provisioning
# and the first cohort there is nothing to bill, and a sweep that dies
# half-way leaves the app at whatever count it was last set to rather than at
# a default of 1 that nobody remembers to clear.
# ---------------------------------------------------------------------------
if (-not (Test-AzResourceExists -Kind 'acaapp' -Name $names.SiloApp -ResourceGroup $names.Rg)) {
	Write-Host "[aca] creating silo app $($names.SiloApp) (resting at 0 replicas)" -ForegroundColor Cyan
	# No --ingress: Orleans peers and clients reach a silo on the endpoint it
	# publishes to the clustering table, not through the environment's HTTP
	# front door. The reachability spike confirmed an app with no ingress
	# block is still addressable on its replica pod IPs.
	#
	# Multiple revision mode: in Single mode an update on a parked app
	# reactivates the parked revision against the previous cohort's deleted
	# tables (see Set-AcaSiloMultipleRevisionMode in aca-common.ps1).
	Invoke-Az (@(
		'containerapp', 'create',
		'--name', $names.SiloApp,
		'--resource-group', $names.Rg,
		'--environment', $names.Env,
		'--image', $siloImage,
		'--revisions-mode', 'multiple'
	) + $registryArgs + $siloAuthArgs + @(
		'--cpu', "$SiloCpu",
		'--memory', "${SiloMemoryGi}Gi",
		'--min-replicas', '0',
		'--max-replicas', '30'
	) + $secretArgs + @(
		'--env-vars',
			'BENCH_CLUSTERING=azuretable',
			'BENCH_INGEST_MODE=cluster',
			'BENCH_SHARD_COUNT=0',
			'BENCH_LEAF_STORAGE_KIND=azure',
			'BENCH_TOTAL_DURATION_SEC=0'
	) + $siloStorageEnv + @(
		'-o', 'none'
	)) | Out-Null
} else {
	Write-Host "[aca] silo app $($names.SiloApp) already exists" -ForegroundColor Yellow
}

if (-not (Test-AzResourceExists -Kind 'acajob' -Name $names.ProducerJob -ResourceGroup $names.Rg)) {
	Write-Host "[aca] creating producer job $($names.ProducerJob)" -ForegroundColor Cyan
	# A Job rather than an app: the producer runs a fixed-duration workload
	# and must then stop. A Job models run-to-completion directly, so the
	# runner starts an execution and waits for it to finish, instead of
	# scaling an app 1 -> 0 and forcing a new revision for every cohort.
	Invoke-Az (@(
		'containerapp', 'job', 'create',
		'--name', $names.ProducerJob,
		'--resource-group', $names.Rg,
		'--environment', $names.Env,
		'--image', $prodImage
	) + $registryArgs + $jobAuthArgs + @(
		'--trigger-type', 'Manual',
		'--replica-timeout', '3600',
		'--replica-retry-limit', '0',
		'--parallelism', '1',
		'--replica-completion-count', '1',
		'--cpu', "$SiloCpu",
		'--memory', "${SiloMemoryGi}Gi"
	) + $secretArgs + @(
		'--env-vars',
			'BENCH_PRODUCER_MODE=orleans-client'
	) + $prodStorageEnv + @(
		'-o', 'none'
	)) | Out-Null
} else {
	Write-Host "[aca] producer job $($names.ProducerJob) already exists" -ForegroundColor Yellow
}

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
	storageTableUri = $storageTableUri
	privateNetwork  = [bool]$PrivateNetwork
	identityId      = $identityId
	identityClientId= $identityClientId
	workspace    = $names.Workspace
	workspaceId  = $workspaceId
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
