#Requires -Version 7.0
<#
.SYNOPSIS
    Opinionated one-shot sample that deploys the active-active Orleans.Lattice
    reference estate to three regions (East US 2, West US 3, West Europe) from
    just a deployment name.

.DESCRIPTION
    A thin, zero-decision wrapper around Deploy-ReferenceArchitecture.ps1 for
    demos and evaluation. It does NOT reimplement any deployment logic - it
    derives a full parameter set from the deployment name and forwards to the
    real deployer, which owns every Azure mutation.

    Everything else derives from -DeploymentName:

      * BaseName       = <DeploymentName>            (the resource name stem)
      * ResourceGroup  = rg-<DeploymentName>
      * Location       = eastus2                     (the first region)
      * Regions        = eastus2 (eus2), westus3 (wus3), westeurope (weu)

    Before anything is created it requires an authenticated Azure CLI session
    (it errors out telling you to run 'az login' if none is present), resolves
    the target subscription (the current 'az' context, or -SubscriptionId) and
    its tenant, prints them with the signed-in user, and asks you to confirm.
    Pass -Force to skip the prompt, or -WhatIf to preview without confirming.

    The two secrets the real deployer requires (the cross-region replication key
    and the per-region Grafana admin password) are NOT derived from the name -
    that would be insecure. They are generated with a cryptographic RNG. The
    Grafana admin password is printed ONCE at the end so you can sign in; the
    replication key is never printed (it only rides @secure Bicep parameters).

    The estate is deployed with the public network option and Entra sign-in ON.
    The deploying user is seeded as the sole security administrator - every other
    caller is denied until that admin grants them access through the Explorer
    Access tab. For a private-network estate or a different topology, drive
    Deploy-ReferenceArchitecture.ps1 directly.

    Because Entra sign-in is on, an authenticated 'az' session is required even
    for a -WhatIf preview (the deployer reads the signed-in user to seed the
    security administrator). -WhatIf and -Confirm otherwise flow through to the
    underlying deployer, so
    './deployment-sample.ps1 -DeploymentName demo -WhatIf'
    previews every mutation without touching Azure.

.EXAMPLE
    # Deploy into the current 'az' subscription (you are shown it and asked to confirm).
    ./deployment-sample.ps1 -DeploymentName demo

.EXAMPLE
    # Target a specific subscription and skip the confirmation prompt.
    ./deployment-sample.ps1 -DeploymentName demo -SubscriptionId 00000000-0000-0000-0000-000000000000 -Force

.EXAMPLE
    # Preview every action without mutating Azure.
    ./deployment-sample.ps1 -DeploymentName demo -WhatIf
#>
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingWriteHost', '',
    Justification = 'This is an interactive operator-facing sample CLI; Write-Host renders the banner and the one-time Grafana password as intended console UX, not pipeline output.')]
[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    # The single name every other parameter derives from. Must satisfy the
    # deployer's BaseName contract: 3-16 lowercase alphanumerics (it seeds Azure
    # resource names). Example: 'demo', 'latticeeval'.
    [Parameter(Mandatory)]
    [string]$DeploymentName,

    # The Azure subscription the estate is deployed into. Optional: when omitted
    # the current 'az' context subscription is used (and you are shown it for
    # confirmation before anything is created).
    [string]$SubscriptionId = '',

    # Container image tag stamped on the three host images. Defaults to 'sample';
    # override to pin a specific build.
    [string]$ImageTag = 'sample',

    # Entra tenant the estate signs in against. Optional: when omitted it is
    # resolved from the target subscription. The estate is deployed with Entra
    # sign-in ON, so an authenticated 'az' session is required; the deploying
    # user is seeded as the sole security administrator.
    [string]$EntraTenantId = '',

    # Skip the interactive subscription/tenant confirmation prompt (for
    # non-interactive/automated runs). The context is still printed.
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Validate the one free-form input up front with an actionable message, rather
# than letting the deployer's [ValidatePattern] surface a terse binder error.
# ---------------------------------------------------------------------------
if ($DeploymentName -notmatch '^[a-z0-9]{3,16}$') {
    throw @"
-DeploymentName '$DeploymentName' is not valid.
It must be 3 to 16 lowercase letters or digits (no hyphens, spaces, or capitals),
because every derived Azure resource name is built from it. Examples: 'demo', 'latticeeval'.
"@
}

# ---------------------------------------------------------------------------
# Require an authenticated 'az' session, then resolve the target subscription
# and tenant. The estate is deployed with Entra sign-in ON, so 'az login' is a
# hard prerequisite. When -SubscriptionId is omitted the current context
# subscription is used; -EntraTenantId defaults to that subscription's tenant.
# ---------------------------------------------------------------------------
if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
    $accountJson = az account show --query "{id:id,name:name,tenantId:tenantId,user:user.name}" -o json 2>$null
}
else {
    $accountJson = az account show --subscription $SubscriptionId --query "{id:id,name:name,tenantId:tenantId,user:user.name}" -o json 2>$null
}
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($accountJson)) {
    if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
        throw @"
No authenticated Azure CLI session was found.
Run 'az login' first - this sample deploys with Entra sign-in on and needs an authenticated 'az' context.
"@
    }
    throw @"
Could not read subscription '$SubscriptionId' from the Azure CLI.
Run 'az login', confirm you have access to that subscription, or omit -SubscriptionId to use the current context.
"@
}

$account = $accountJson | ConvertFrom-Json
$SubscriptionId = $account.id
$subscriptionName = $account.name
$signedInUser = $account.user
if ([string]::IsNullOrWhiteSpace($EntraTenantId)) { $EntraTenantId = $account.tenantId }

# ---------------------------------------------------------------------------
# Derive the full parameter set from the deployment name.
# ---------------------------------------------------------------------------
$resourceGroup = "rg-$DeploymentName"
$regions = @(
    @{ regionCode = 'eus2'; location = 'eastus2' },
    @{ regionCode = 'wus3'; location = 'westus3' },
    @{ regionCode = 'weu'; location = 'westeurope' }
)
$primaryLocation = $regions[0].location

# ---------------------------------------------------------------------------
# Generate the two required secrets (never derived from the name). The
# SecureString is built with AppendChar so no plaintext secret is ever handed
# to ConvertTo-SecureString. The Grafana plaintext is retained only to print it
# once at the end so the operator can sign in.
# ---------------------------------------------------------------------------
function Get-RandomSecret {
    param([int]$ByteCount = 24)
    $bytes = [byte[]]::new($ByteCount)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    # URL-safe base64 so the value is a clean single token wherever it is used.
    return [Convert]::ToBase64String($bytes).Replace('+', 'A').Replace('/', 'B').Replace('=', '')
}

function ConvertTo-SecureFromPlain {
    param([string]$Plain)
    $secure = [securestring]::new()
    foreach ($ch in $Plain.ToCharArray()) { $secure.AppendChar($ch) }
    $secure.MakeReadOnly()
    return $secure
}

$replicationPlain = Get-RandomSecret -ByteCount 32
$grafanaPlain = Get-RandomSecret -ByteCount 18
$replicationKey = ConvertTo-SecureFromPlain $replicationPlain
$grafanaPassword = ConvertTo-SecureFromPlain $grafanaPlain

Write-Host ''
Write-Host "Sample deployment '$DeploymentName'" -ForegroundColor Cyan
Write-Host "  Subscription   : $subscriptionName ($SubscriptionId)"
Write-Host "  Tenant         : $EntraTenantId"
Write-Host "  Signed-in user : $signedInUser"
Write-Host "  Resource group : $resourceGroup"
Write-Host "  Base name      : $DeploymentName"
Write-Host "  Regions        : eastus2 (eus2), westus3 (wus3), westeurope (weu)"
Write-Host "  Image tag      : $ImageTag"
Write-Host "  Network option : public   |   Entra sign-in: on"
Write-Host '  Security admin : the deploying user (deny-by-default for everyone else)'
Write-Host ''

# ---------------------------------------------------------------------------
# Confirm the target subscription / tenant before mutating anything. Skipped on
# a -WhatIf preview (nothing is created) and when -Force is passed.
# ---------------------------------------------------------------------------
if (-not $WhatIfPreference -and -not $Force) {
    $answer = Read-Host "Deploy into subscription '$subscriptionName' (tenant $EntraTenantId) as $($signedInUser)? [y/N]"
    if ($answer -notmatch '^(y|yes)$') {
        Write-Host 'Aborted. Pass -SubscriptionId / -EntraTenantId to target a different context, or run az login to switch accounts.'
        return
    }
}

# ---------------------------------------------------------------------------
# Forward to the real deployer. -WhatIf / -Confirm on this wrapper flow through
# so a preview run stays a preview run.
# ---------------------------------------------------------------------------
$deployer = Join-Path $PSScriptRoot 'Deploy-ReferenceArchitecture.ps1'
$deployParams = @{
    SubscriptionId       = $SubscriptionId
    ResourceGroup        = $resourceGroup
    Location             = $primaryLocation
    BaseName             = $DeploymentName
    Regions              = $regions
    ImageTag             = $ImageTag
    ReplicationKey       = $replicationKey
    GrafanaAdminPassword = $grafanaPassword
    EntraEnabled         = $true
    EntraTenantId        = $EntraTenantId
    WhatIf               = $WhatIfPreference
}

& $deployer @deployParams

# ---------------------------------------------------------------------------
# Surface the generated Grafana password once (the real deployer never prints a
# secret). Nothing is emitted on a -WhatIf preview run - no estate was created.
# ---------------------------------------------------------------------------
if (-not $WhatIfPreference) {
    Write-Host ''
    Write-Host 'Generated Grafana admin password (shown once - store it securely):' -ForegroundColor Yellow
    Write-Host "  user 'admin'  password $grafanaPlain"
    Write-Host '  Sign in at the per-region Grafana URLs listed under "Estate endpoints" above.'
    Write-Host 'The cross-region replication key was generated and applied but is not printed.'
}
