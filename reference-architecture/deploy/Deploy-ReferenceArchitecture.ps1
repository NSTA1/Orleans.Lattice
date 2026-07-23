#Requires -Version 7.0
<#
.SYNOPSIS
    Provisions the active-active, cross-region Orleans.Lattice reference estate on
    Azure Container Apps, end to end, from a single parameter set.

.DESCRIPTION
    One idempotent orchestrator for sub-issue F-192 (#1280). Given a region list,
    a subscription / resource group, a network option and image tags, it converges
    the whole estate and prints the resulting endpoints. Re-running it converges
    again - it never duplicates.

    WHAT A REAL RUN DOES (in order):

      1. Selects the subscription and ensures the resource group exists
         (az group create is idempotent).

      2. Deploys bootstrap.bicep - the shared Azure Container Registry ONLY - so
         the three host images can be built BEFORE the Container Apps that pull
         them exist. main.bicep later converges onto this same registry (identical
         name expression, same resource group).

      3. Builds the three host images server-side with 'az acr build' straight
         from the in-folder Dockerfiles
         (hosts/{Silo,Mcp,Explorer}/Dockerfile). No image is ever published to a
         public registry.

      4. PASS 1: deploys main.bicep across all N regions in the module-ordered
         sequence it encodes (compute -> storage -> networking -> observability
         -> Front Door), with the two forward-threaded seams left empty
         (prometheusQueryEndpoint = '', frontDoorId = '') and Entra OFF. Bicep
         detects a compile cycle if those Azure-assigned values are threaded in
         one pass, so they are activated on pass 2.

      5. Deploys entra/entra.bicep (Microsoft Graph extension, GA 2025-07-29):
         app registrations + service principals + FEDERATED IDENTITY CREDENTIALS
         (preferred over client secrets) for the silo facades, the MCP endpoint,
         and the Explorer, the app-to-app app-role grants, AND the silo app's
         Microsoft Graph application-permission consent - all declarative, so no
         imperative admin-consent step runs (the deploying identity must hold a
         privileged directory role for the Graph grant to succeed).

      6. PASS 2: redeploys each region's compute module DIRECTLY (see DESIGN
         NOTES) with the Azure-assigned values now known - the per-region managed
         Prometheus query endpoint (activates the silo KEDA scaler and the MCP
         telemetry tool group), the global Front Door id (activates the
         X-Azure-FDID origin lock), and the SYMMETRIC replication topology
         (reciprocal peer endpoints, the estate-wide wire-merge-mode map, and the
         per-region Key Vault replication-key secret URI), plus the Entra client
         id when Entra is enabled.

      7. Prints the resulting endpoints (the global Front Door hostnames and every
         per-region head FQDN). No secret is ever printed.

    DESIGN NOTES:

      * Symmetric replication. Each region's cluster id is "<baseName>-<regionCode>"
        and its peer endpoint is "https://<siloStateApiFqdn>". For every region the
        script builds the peer list from EVERY OTHER region, so enrollment is fully
        reciprocal across all N regions (asymmetry dead-letters cross-region
        traffic). The wire-merge-mode map (-ReplicationTrees) is applied identically
        estate-wide. The replication key is byte-identical across regions (one Key
        Vault secret per region, same material) and is @secure end to end.

      * Why pass 2 deploys compute DIRECTLY. main.bicep threads a SINGLE
        prometheusQueryEndpoint to every region and does not thread the per-region
        replication seams at all, so the per-region activation cannot go through
        main.bicep. compute.bicep's resource names are pure functions of
        (baseName, regionCode), so a direct per-region deploy converges onto the
        exact same Container Apps that pass 1 created - it is not a second estate.

      * RBAC is declarative and idempotent. Managed-identity data-plane RBAC
        (AcrPull, Storage Table Data Contributor, Key Vault Secrets User) is
        assigned by the Bicep modules; Entra app-to-app RBAC (the Lattice.Access
        app role) is assigned by entra.bicep. The script does not imperatively
        create role assignments, so re-runs never duplicate them.

    SECURITY: -ReplicationKey and -GrafanaAdminPassword are SecureString and are
    threaded to Azure only as @secure() Bicep parameters (never written to disk,
    never logged, never emitted as an output). No client secret is created for any
    Entra app - federated identity credentials replace them.

.NOTES
    THIS SCRIPT DOES NOT RUN ITSELF DURING AUTHORING. It is validated statically
    (Bicep 'az bicep build', PowerShell AST parse, PSScriptAnalyzer). Running it
    for real requires an authenticated 'az' session with rights to create the
    resources, register Entra apps, and (for admin consent) grant tenant-wide
    application permissions.

.EXAMPLE
    $key = Read-Host -AsSecureString 'Replication key'
    $gpw = Read-Host -AsSecureString 'Grafana admin password'
    ./Deploy-ReferenceArchitecture.ps1 `
        -SubscriptionId 00000000-0000-0000-0000-000000000000 `
        -ResourceGroup rg-lattice `
        -Location eastus `
        -BaseName lattice `
        -Regions @(@{ regionCode = 'use'; location = 'eastus' }, @{ regionCode = 'euw'; location = 'westeurope' }) `
        -ImageTag 2025.07.29 `
        -ReplicationTrees 'orders=LwwRegister,inventory=OrSet' `
        -ReplicationKey $key `
        -GrafanaAdminPassword $gpw `
        -EntraEnabled -EntraTenantId 11111111-1111-1111-1111-111111111111

.EXAMPLE
    # Preview every action without mutating Azure.
    ./Deploy-ReferenceArchitecture.ps1 -SubscriptionId ... -ResourceGroup rg-lattice `
        -Location eastus -BaseName lattice -Regions @(@{regionCode='use';location='eastus'}) `
        -ImageTag dev -GrafanaAdminPassword $gpw -ReplicationKey $key -WhatIf
#>
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingWriteHost', '',
    Justification = 'This is an interactive operator-facing deployment CLI; Write-Host renders the phase banners and the final endpoint list as intended console UX, not pipeline output.')]
[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory)]
    [string]$SubscriptionId,

    [Parameter(Mandatory)]
    [string]$ResourceGroup,

    [Parameter(Mandatory)]
    [string]$Location,

    [Parameter(Mandatory)]
    [ValidateLength(3, 16)]
    [ValidatePattern('^[a-z0-9]+$')]
    [string]$BaseName,

    # Each item: @{ regionCode = '<3-6 char code>'; location = '<azure region>' }.
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [object[]]$Regions,

    [Parameter(Mandatory)]
    [string]$ImageTag,

    [ValidateSet('public', 'private')]
    [string]$DeploymentOption = 'public',

    [bool]$ZoneRedundant = $true,

    [string]$SiloImageRepository = 'lattice-silo',
    [string]$McpImageRepository = 'lattice-mcp',
    [string]$ExplorerImageRepository = 'lattice-explorer',

    # regionCode of the single backup-primary region. Defaults to the first region.
    [string]$BackupPrimaryRegionCode,

    # Estate-wide wire-merge-mode map, for example 'orders=LwwRegister,inventory=OrSet'.
    [string]$ReplicationTrees = '',

    [string[]]$IngressAllowedCidrs = @(),

    [ValidateRange(1, 100)]
    [int]$SiloMinReplicas = 1,

    [ValidateRange(1, 100)]
    [int]$SiloMaxReplicas = 10,

    [ValidateSet('Deny', 'Allow')]
    [string]$AuthDefaultEffect = 'Deny',

    [bool]$RequireApiAuthorization = $true,

    # Runtime per-tree replication control plane. The reference estate ships it ON
    # (this switch defaults true): the sys-replication-config tree, the silo
    # replication control gRPC binding, and the MCP lattice_replication_* tools are
    # co-hosted so an operator can manage a tree's replication at runtime. This is
    # SAFE ON by default because the surface is FAIL-CLOSED behind the deny-by-
    # default LatticeOperation.Replication gate - enabling/disabling a tree still
    # requires an explicitly authored Replication grant (not even Admin confers it).
    # Pass -EnableReplicationControl:$false to withhold the surface entirely.
    [bool]$EnableReplicationControl = $true,

    # The read-write Data API (write surface). Enabled by default: the write
    # facade is co-hosted on the silo gRPC endpoint and the MCP head advertises
    # its write tools. Set -EnableDataApi:$false to withhold the write surface;
    # enforcement is otherwise the deny-by-default per-subject access gate.
    [bool]$EnableDataApi = $true,

    # The per-cluster replication key, matched across every region and required by
    # BOTH options (public authenticates replication over public ingress with it;
    # private layers it on the VNet transport as defense in depth). Stable across
    # runs (a re-run with a different key rotates the secret and dead-letters
    # in-flight cross-region traffic until every region converges).
    [securestring]$ReplicationKey,

    # Grafana admin password for every per-region self-hosted Grafana head.
    [Parameter(Mandatory)]
    [securestring]$GrafanaAdminPassword,

    [switch]$EntraEnabled,
    [string]$EntraTenantId = '',
    # Optional pre-existing silo audience app (client) id. When supplied the script
    # does NOT deploy entra/entra.bicep and uses this id as the estate audience.
    [string]$EntraClientId = '',
    # Optional pre-existing Explorer console web-app (client) id. Used only when
    # -EntraClientId is also supplied (entra.bicep is skipped); when entra.bicep is
    # deployed the value is read from its explorerClientId output instead.
    [string]$ExplorerWebClientId = '',
    [string]$EntraAudiences = '',
    # The single Entra security administrator seeded as the sole initial-access
    # principal (the root of trust). Accepts an Entra user object id (GUID) or a
    # UPN / email, which is resolved to its object id. When Entra is enabled and
    # this is left empty, the deploying (currently signed-in) user is used, so
    # only that operator can reach the estate after the first deploy; further
    # administrators are then granted at runtime through the Explorer Access tab.
    [string]$SecurityAdmin = '',
    # Explorer web reply URIs. Defaults are derived from the deployed FQDNs.
    [string[]]$ExplorerRedirectUris = @(),

    # Skip 'az acr build' (images already present in the registry at -ImageTag).
    [switch]$SkipImageBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Repository-relative anchors. The Dockerfiles COPY paths are rooted at the
# reference-architecture folder, so that folder is the acr-build context.
$ScriptRoot = $PSScriptRoot
$RefArchRoot = Split-Path -Parent $ScriptRoot
$BicepRoot = Join-Path $RefArchRoot 'bicep'
$MainTemplate = Join-Path $BicepRoot 'main.bicep'
$BootstrapTemplate = Join-Path $BicepRoot 'bootstrap.bicep'
$ComputeTemplate = Join-Path $BicepRoot 'modules/compute.bicep'
$EntraTemplate = Join-Path $BicepRoot 'entra/entra.bicep'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

function Write-Phase {
    param([string]$Message)
    Write-Host ''
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function ConvertFrom-SecureStringPlain {
    param([securestring]$Secure)
    if ($null -eq $Secure) { return '' }
    $ptr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    try {
        return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
    }
    finally {
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
    }
}

function Invoke-Az {
    <#
        Runs the Azure CLI, fails hard on a non-zero exit, and returns the parsed
        JSON payload (when any). Mutating calls flow through ShouldProcess so
        -WhatIf previews the estate without touching Azure. Secret-bearing
        parameters must be delivered via -StdinInput (an ARM parameters JSON piped
        to `az ... --parameters @-`) so they never appear in the process argument
        list, the shell history, or on disk.
    #>
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [string]$Action,
        [string]$Target,
        [switch]$Mutating,
        [string]$StdinInput,
        [switch]$AllowFailure
    )

    if ($Mutating) {
        $desc = if ($Target) { $Target } else { ($Arguments -join ' ') }
        if (-not $PSCmdlet.ShouldProcess($desc, $Action)) {
            Write-Host "    [WhatIf] would run: az $($Arguments -join ' ')" -ForegroundColor DarkGray
            return $null
        }
    }

    # Capture stdout (the JSON payload) and stderr (warnings such as the Bicep
    # upgrade notice, RP hints) SEPARATELY. Merging them with 2>&1 lets a benign
    # az warning bleed into the JSON stream so ConvertFrom-Json fails and the
    # caller receives a raw string instead of the deployment object. stderr is
    # redirected to a temp file and only surfaced on a non-zero exit.
    $errPath = [System.IO.Path]::GetTempFileName()
    try {
        if ($StdinInput) {
            # Secret-bearing parameters ride stdin (--parameters @-); they are never
            # placed in $Arguments, so nothing secret reaches the process command line.
            $raw = $StdinInput | & az @Arguments 2>$errPath
        }
        else {
            $raw = & az @Arguments 2>$errPath
        }
        $exit = $LASTEXITCODE
        $errText = (Get-Content -Path $errPath -Raw -ErrorAction SilentlyContinue)
    }
    finally {
        Remove-Item -Path $errPath -Force -ErrorAction SilentlyContinue
    }
    if ($exit -ne 0) {
        if ($AllowFailure) {
            Write-Warning "az $($Arguments -join ' ') exited $exit"
            return $null
        }
        # Only the non-secret arguments are shown; stdin is never echoed.
        throw "az $($Arguments -join ' ') failed (exit $exit): $errText"
    }

    $text = ($raw | Out-String).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }
    try { return $text | ConvertFrom-Json } catch { return $text }
}

function New-ParametersFile {
    <#
        Writes an ARM parameters JSON file from a hashtable of NON-SECRET values.
        Returns the path; the caller deletes it in a finally block. Secrets are
        NEVER written here - they are piped to az via stdin (--parameters @-) as
        @secure() params.
    #>
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseShouldProcessForStateChangingFunctions', '',
        Justification = 'Writes a throwaway temp parameters file with no secret content; the actual Azure state change flows through Invoke-Az, which supports ShouldProcess.')]
    [OutputType([string])]
    param([hashtable]$Values)
    $params = @{}
    foreach ($k in $Values.Keys) { $params[$k] = @{ value = $Values[$k] } }
    $doc = [ordered]@{
        '$schema'      = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
        contentVersion = '1.0.0.0'
        parameters     = $params
    }
    $path = Join-Path ([System.IO.Path]::GetTempPath()) ("lattice-deploy-{0}.json" -f ([guid]::NewGuid()))
    $doc | ConvertTo-Json -Depth 30 | Set-Content -Path $path -Encoding utf8
    return $path
}

function Get-ByRegionCode {
    param([object[]]$Items, [string]$RegionCode)
    foreach ($item in $Items) {
        if ($item.regionCode -eq $RegionCode) { return $item }
    }
    return $null
}

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw 'The Azure CLI (az) is required but was not found on PATH.'
}

foreach ($template in @($MainTemplate, $BootstrapTemplate, $ComputeTemplate, $EntraTemplate)) {
    if (-not (Test-Path $template)) { throw "Required template is missing: $template" }
}

foreach ($region in $Regions) {
    if (-not $region.regionCode -or -not $region.location) {
        throw 'Every -Regions entry must have a regionCode and a location, for example @{ regionCode = "use"; location = "eastus" }.'
    }
}

if (-not $BackupPrimaryRegionCode) { $BackupPrimaryRegionCode = $Regions[0].regionCode }
if (-not (Get-ByRegionCode -Items $Regions -RegionCode $BackupPrimaryRegionCode)) {
    throw "-BackupPrimaryRegionCode '$BackupPrimaryRegionCode' does not match any -Regions entry."
}

if ($null -eq $ReplicationKey) {
    throw 'Both deployment options require -ReplicationKey (a SecureString matched across every region). The public option authenticates replication over public ingress with it; the private option layers it on the VNet transport as defense in depth.'
}

if ($EntraEnabled -and [string]::IsNullOrWhiteSpace($EntraTenantId)) {
    throw '-EntraEnabled requires -EntraTenantId.'
}

# Resolve the single security administrator to an Entra object id (the subject
# the silo matches against Auth:BootstrapAdministrators - the oid claim, ordinal).
# When Entra is enabled the estate is deny-by-default with this principal as the
# only seeded administrator; every other caller is refused until this admin grants
# them access through the Explorer Access tab. Empty means "the deploying user".
$securityAdminObjectId = ''
if ($EntraEnabled) {
    $guidRef = [ref]([guid]::Empty)
    if ([string]::IsNullOrWhiteSpace($SecurityAdmin)) {
        Write-Phase 'Resolving the deploying user as the estate security administrator'
        $securityAdminObjectId = az ad signed-in-user show --query id -o tsv
        if ([string]::IsNullOrWhiteSpace($securityAdminObjectId)) {
            throw 'Could not resolve the signed-in user object id; pass -SecurityAdmin explicitly.'
        }
    }
    elseif ([guid]::TryParse($SecurityAdmin, $guidRef)) {
        $securityAdminObjectId = $SecurityAdmin
    }
    else {
        Write-Phase "Resolving security administrator '$SecurityAdmin' to an object id"
        $securityAdminObjectId = az ad user show --id $SecurityAdmin --query id -o tsv
        if ([string]::IsNullOrWhiteSpace($securityAdminObjectId)) {
            throw "Could not resolve -SecurityAdmin '$SecurityAdmin' to an Entra object id."
        }
    }
    Write-Host "  Security administrator object id: $securityAdminObjectId"
}

# Normalise the region list to the shape main.bicep expects.
$regionParam = @($Regions | ForEach-Object { @{ regionCode = $_.regionCode; location = $_.location } })

$replicationKeyPlain = ConvertFrom-SecureStringPlain $ReplicationKey
$grafanaPasswordPlain = ConvertFrom-SecureStringPlain $GrafanaAdminPassword

$tempFiles = New-Object System.Collections.Generic.List[string]

try {
    Write-Phase "Selecting subscription $SubscriptionId"
    Invoke-Az -Arguments @('account', 'set', '--subscription', $SubscriptionId) `
        -Mutating -Action 'Select subscription' -Target $SubscriptionId | Out-Null

    Write-Phase "Ensuring resource group $ResourceGroup ($Location)"
    Invoke-Az -Arguments @('group', 'create', '--name', $ResourceGroup, '--location', $Location, '--output', 'json') `
        -Mutating -Action 'Create resource group' -Target $ResourceGroup | Out-Null

    # -----------------------------------------------------------------------
    # 1. Bootstrap the shared registry so images can be built before compute.
    # -----------------------------------------------------------------------
    Write-Phase 'Bootstrapping the shared container registry'
    $bootstrapParams = New-ParametersFile -Values @{
        baseName         = $BaseName
        registryLocation = $Regions[0].location
    }
    $tempFiles.Add($bootstrapParams)
    $bootstrap = Invoke-Az -Arguments @(
        'deployment', 'group', 'create',
        '--resource-group', $ResourceGroup,
        '--name', 'lattice-bootstrap',
        '--template-file', $BootstrapTemplate,
        '--parameters', "@$bootstrapParams",
        '--output', 'json'
    ) -Mutating -Action 'Deploy registry' -Target 'bootstrap.bicep'

    $acrName = if ($bootstrap) { $bootstrap.properties.outputs.acrName.value } else { '<acr-name>' }
    $acrLoginServer = if ($bootstrap) { $bootstrap.properties.outputs.acrLoginServer.value } else { '<acr-login-server>' }
    Write-Host "    Registry: $acrLoginServer"

    # -----------------------------------------------------------------------
    # 2. Build the three host images server-side (no public publishing).
    # -----------------------------------------------------------------------
    if ($SkipImageBuild) {
        Write-Phase 'Skipping image build (-SkipImageBuild)'
    }
    else {
        Write-Phase "Building host images into $acrName at tag $ImageTag"
        $imageMatrix = @(
            @{ Repo = $SiloImageRepository; Dockerfile = 'hosts/Silo/Dockerfile' }
            @{ Repo = $McpImageRepository; Dockerfile = 'hosts/Mcp/Dockerfile' }
            @{ Repo = $ExplorerImageRepository; Dockerfile = 'hosts/Explorer/Dockerfile' }
        )
        # az acr build resolves --file relative to the current working directory,
        # not the source-location argument, so pin the CWD to the acr-build context
        # (the reference-architecture root) and pass '.' as the source location.
        # This keeps the Dockerfile lookup correct regardless of where the operator
        # invoked the script from.
        Push-Location $RefArchRoot
        try {
            foreach ($image in $imageMatrix) {
                Invoke-Az -Arguments @(
                    'acr', 'build',
                    '--registry', $acrName,
                    '--image', "$($image.Repo):$ImageTag",
                    '--file', $image.Dockerfile,
                    '.'
                ) -Mutating -Action 'Build image' -Target "$($image.Repo):$ImageTag" | Out-Null
            }
        }
        finally {
            Pop-Location
        }
    }

    # -----------------------------------------------------------------------
    # 3. PASS 1 - deploy the estate with the forward-threaded seams empty and
    #    Entra off. Compute -> storage -> networking -> observability -> AFD is
    #    the module order main.bicep encodes; we drive it in one deployment.
    # -----------------------------------------------------------------------
    Write-Phase 'PASS 1: deploying the estate (prometheus + Front Door + Entra seams empty)'
    $pass1Values = @{
        baseName                = $BaseName
        regions                 = $regionParam
        imageTag                = $ImageTag
        siloImageRepository     = $SiloImageRepository
        mcpImageRepository      = $McpImageRepository
        explorerImageRepository = $ExplorerImageRepository
        deploymentOption        = $DeploymentOption
        zoneRedundant           = $ZoneRedundant
        backupPrimaryRegionCode = $BackupPrimaryRegionCode
        ingressAllowedCidrs     = $IngressAllowedCidrs
        siloMinReplicas         = $SiloMinReplicas
        siloMaxReplicas         = $SiloMaxReplicas
        authDefaultEffect       = $AuthDefaultEffect
        requireApiAuthorization = $RequireApiAuthorization
        enableReplicationControl = $EnableReplicationControl
        # Entra is activated on pass 2 (the app registrations are created between
        # the passes), so it stays off here.
        entraEnabled            = $false
        # Forward-threaded seams are empty on pass 1 (compile-cycle avoidance).
        prometheusQueryEndpoint = ''
        frontDoorId             = ''
        # Pin the registry location to match bootstrap.bicep (same region), so the
        # registry resource main.bicep declares converges onto the bootstrapped one
        # instead of attempting an immutable-location change.
        registryLocation        = $Regions[0].location
    }
    $pass1File = New-ParametersFile -Values $pass1Values
    $tempFiles.Add($pass1File)

    # Secret-bearing @secure() params (Grafana admin password and the replication
    # key) are delivered to az through STDIN as an ARM parameters JSON piped via
    # `--parameters @-`. They never appear in the process argument list, the shell
    # history, or on disk (the non-secret temp file above holds only non-secret
    # values). The replication key is written to a per-region Key Vault for BOTH
    # options (public authenticates over public ingress; private layers it on the
    # VNet transport as defense in depth).
    $secretParams = [ordered]@{
        grafanaAdminPassword = @{ value = $grafanaPasswordPlain }
        replicationKey       = @{ value = $replicationKeyPlain }
    }
    $pass1SecretsJson = [ordered]@{
        '$schema'      = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
        contentVersion = '1.0.0.0'
        parameters     = $secretParams
    } | ConvertTo-Json -Depth 30

    $pass1 = Invoke-Az -Arguments @(
        'deployment', 'group', 'create',
        '--resource-group', $ResourceGroup,
        '--name', 'lattice-pass1',
        '--template-file', $MainTemplate,
        '--parameters', "@$pass1File",
        '--parameters', '@-',
        '--output', 'json'
    ) -StdinInput $pass1SecretsJson -Mutating -Action 'Deploy estate (pass 1)' -Target 'main.bicep'

    if (-not $pass1 -and $WhatIfPreference) {
        Write-Host '    [WhatIf] pass 1 skipped; downstream phases have nothing to converge against.' -ForegroundColor DarkGray
        return
    }

    $outputs = $pass1.properties.outputs
    $perRegion = $outputs.perRegion.value
    $perRegionStorage = $outputs.perRegionStorage.value
    $backupBlobEndpoint = $outputs.backupBlobEndpoint.value
    $perRegionObservability = $outputs.perRegionObservability.value
    $perRegionKeyVault = if ($outputs.PSObject.Properties.Name -contains 'perRegionReplicationKeyVault') { $outputs.perRegionReplicationKeyVault.value } else { @() }
    $perRegionNetwork = if ($outputs.PSObject.Properties.Name -contains 'perRegionNetwork') { $outputs.perRegionNetwork.value } else { @() }
    $frontDoorId = $outputs.frontDoorId.value
    $frontDoorEndpoints = $outputs.frontDoorEndpoints.value

    # -----------------------------------------------------------------------
    # 4. Entra - app registrations, SPs, federated identity credentials, RBAC.
    # -----------------------------------------------------------------------
    $entraClientIdResolved = $EntraClientId
    $entraAudiencesResolved = $EntraAudiences
    # The Explorer console signs operators in against its OWN web-app registration
    # (explorerClientId) and requests the silo's delegated user_impersonation scope
    # on-behalf-of them. The scope is a pure function of tenant + base name; the
    # web client id comes from the entra deployment (or the -ExplorerWebClientId
    # param when entra.bicep is skipped).
    $explorerWebClientIdResolved = $ExplorerWebClientId
    $explorerAuthScopeResolved = if ($EntraEnabled) { "api://$EntraTenantId/$BaseName-silo/user_impersonation" } else { '' }
    if ($EntraEnabled -and [string]::IsNullOrWhiteSpace($EntraClientId)) {
        Write-Phase 'Deploying Entra resources (Microsoft Graph extension)'

        $regionManagedIdentities = @($perRegion | ForEach-Object {
                @{ regionCode = $_.regionCode; principalId = $_.managedIdentityPrincipalId }
            })

        # Derive Explorer reply URIs from the deployed hostnames when none given.
        $redirectUris = $ExplorerRedirectUris
        if (-not $redirectUris -or $redirectUris.Count -eq 0) {
            $redirectUris = @()
            if ($frontDoorEndpoints -and $frontDoorEndpoints.PSObject.Properties.Name -contains 'explorer' -and $frontDoorEndpoints.explorer) {
                $redirectUris += "https://$($frontDoorEndpoints.explorer)/signin-oidc"
            }
            foreach ($r in $perRegion) {
                if ($r.explorerFqdn) { $redirectUris += "https://$($r.explorerFqdn)/signin-oidc" }
            }
        }

        $entraValues = @{
            baseName                = $BaseName
            tenantId                = $EntraTenantId
            regionManagedIdentities = $regionManagedIdentities
            explorerRedirectUris    = $redirectUris
        }
        $entraFile = New-ParametersFile -Values $entraValues
        $tempFiles.Add($entraFile)

        $entra = Invoke-Az -Arguments @(
            'deployment', 'group', 'create',
            '--resource-group', $ResourceGroup,
            '--name', 'lattice-entra',
            '--template-file', $EntraTemplate,
            '--parameters', "@$entraFile",
            '--output', 'json'
        ) -Mutating -Action 'Deploy Entra resources' -Target 'entra/entra.bicep'

        if ($entra) {
            $entraClientIdResolved = $entra.properties.outputs.siloClientId.value
            $explorerWebClientIdResolved = $entra.properties.outputs.explorerClientId.value
            if ([string]::IsNullOrWhiteSpace($entraAudiencesResolved)) {
                $entraAudiencesResolved = $entra.properties.outputs.siloAudience.value
            }

            # Tenant admin consent for the silo app's Microsoft Graph application
            # permission is granted declaratively by entra.bicep (an
            # appRoleAssignedTo to the Microsoft Graph service principal), so there
            # is no imperative admin-consent step here. The deploying identity must
            # hold a privileged directory role for that grant to succeed (see the
            # deploy README).
        }
    }
    elseif ($EntraEnabled) {
        Write-Phase "Using supplied Entra client id $EntraClientId (skipping entra.bicep)"
    }

    # -----------------------------------------------------------------------
    # 5. PASS 2 - per-region compute activation: prometheus + Front Door +
    #    SYMMETRIC replication + Entra. Deployed directly per region (see the
    #    DESIGN NOTES in the header) so each region gets its own values.
    # -----------------------------------------------------------------------
    Write-Phase 'PASS 2: activating scaler, Front Door lock, symmetric replication (+ Entra)'
    for ($i = 0; $i -lt $Regions.Count; $i++) {
        $region = $Regions[$i]
        $code = $region.regionCode
        $clusterId = "$BaseName-$code"

        # Symmetric, reciprocal peer list: every OTHER region, keyed by its
        # cluster id and dialed at its silo State/replication ingress FQDN.
        $peerEntries = @()
        foreach ($other in $perRegion) {
            if ($other.regionCode -eq $code) { continue }
            $peerEntries += "$BaseName-$($other.regionCode)=https://$($other.siloStateApiFqdn)"
        }
        $replicationPeers = ($peerEntries -join ',')

        $obs = Get-ByRegionCode -Items $perRegionObservability -RegionCode $code
        $prometheus = if ($obs) { $obs.prometheusQueryEndpoint } else { '' }
        # The MCP cluster-telemetry tools query the same managed Prometheus
        # workspace as the KEDA scaler, authenticating with a rotating
        # managed-identity Entra token (DynamicBearer) - the azure-workload auth
        # mode shipped by #1286. The region managed identity already holds
        # Monitoring Data Reader on the workspace (observability module). When the
        # observability lane is absent the backend is empty and the host leaves
        # the telemetry tool group off.
        $mcpTelemetryAuthMode = if ($prometheus) { 'DynamicBearer' } else { '' }

        $kv = Get-ByRegionCode -Items $perRegionKeyVault -RegionCode $code
        $replicationKeySecretUri = if ($kv) { $kv.replicationKeySecretUri } else { '' }

        $storageRegion = Get-ByRegionCode -Items $perRegionStorage -RegionCode $code
        $walTableEndpoint = if ($storageRegion) { $storageRegion.tableEndpoint } else { '' }
        # Explorer distributed token-cache blob seam (same per-region account as the
        # WAL table, separate blob container + container-scoped RBAC).
        $tokenCacheBlobEndpoint = if ($storageRegion) { $storageRegion.blobEndpoint } else { '' }
        $tokenCacheContainerName = if ($storageRegion) { $storageRegion.tokenCacheContainer } else { 'explorer-token-cache' }

        # Every option is VNet-injected (the environment must be VNet-integrated
        # to be zone-redundant); the subnet exists for both public and private.
        $subnetId = ''
        $net = Get-ByRegionCode -Items $perRegionNetwork -RegionCode $code
        if ($net) { $subnetId = $net.infrastructureSubnetId }

        $computeValues = @{
            location                   = $region.location
            regionCode                 = $code
            baseName                   = $BaseName
            acrLoginServer             = $acrLoginServer
            acrName                    = $acrName
            imageTag                   = $ImageTag
            siloImageRepository        = $SiloImageRepository
            mcpImageRepository         = $McpImageRepository
            explorerImageRepository    = $ExplorerImageRepository
            orleansClusterId           = $clusterId
            orleansServiceId           = $BaseName
            siloMinReplicas            = $SiloMinReplicas
            siloMaxReplicas            = $SiloMaxReplicas
            backupIsPrimary            = ($code -eq $BackupPrimaryRegionCode)
            walTableEndpoint           = $walTableEndpoint
            backupBlobEndpoint         = $backupBlobEndpoint
            authDefaultEffect          = $AuthDefaultEffect
            requireApiAuthorization    = $RequireApiAuthorization
            enableReplicationControl   = $EnableReplicationControl
            dataApiEnabled             = $EnableDataApi
            internalEnvironment        = ($DeploymentOption -eq 'private')
            infrastructureSubnetId     = $subnetId
            # Zone-redundant compute; honoured only when VNet-injected (private).
            zoneRedundant              = $ZoneRedundant
            # Activated seams.
            prometheusQueryEndpoint    = $prometheus
            # MCP cluster-telemetry tools query the managed Prometheus workspace
            # with a rotating managed-identity token (DynamicBearer, #1286). Empty
            # backend leaves the telemetry tool group off.
            mcpTelemetryBackendAddress = $prometheus
            mcpTelemetryAuthMode       = $mcpTelemetryAuthMode
            frontDoorId                = $frontDoorId
            replicationPeers           = $replicationPeers
            replicationTrees           = $ReplicationTrees
            replicationKeySecretUri    = $replicationKeySecretUri
            # Entra.
            entraEnabled               = [bool]$EntraEnabled
            entraTenantId              = $EntraTenantId
            entraClientId              = $entraClientIdResolved
            entraAudiences             = $entraAudiencesResolved
            # Explorer hosted-web OIDC: the console's own web-app client id and the
            # delegated silo scope it requests on-behalf-of the signed-in operator.
            explorerWebClientId        = $explorerWebClientIdResolved
            explorerAuthScope          = $explorerAuthScopeResolved
            # Explorer distributed token cache: keyless blob endpoint + container on
            # the per-region account so operator tokens are shared across warm
            # replicas and survive restart (in-memory fallback when empty).
            tokenCacheBlobEndpoint     = $tokenCacheBlobEndpoint
            tokenCacheContainerName    = $tokenCacheContainerName
            # Public origin operators reach the console at (the global Front Door
            # endpoint) so OIDC sign-in redirect URIs target the public host, not
            # the Front-Door-locked Container Apps origin.
            explorerPublicOrigin       = if ($frontDoorEndpoints -and $frontDoorEndpoints.explorer) { "https://$($frontDoorEndpoints.explorer)" } else { '' }
            # Sole seeded administrator (root of trust); empty when Entra is off.
            bootstrapAdministrators    = $securityAdminObjectId
        }
        $computeFile = New-ParametersFile -Values $computeValues
        $tempFiles.Add($computeFile)

        Write-Host "    Region $code : $($peerEntries.Count) peer(s)"
        Invoke-Az -Arguments @(
            'deployment', 'group', 'create',
            '--resource-group', $ResourceGroup,
            '--name', "lattice-pass2-$code",
            '--template-file', $ComputeTemplate,
            '--parameters', "@$computeFile",
            '--output', 'json'
        ) -Mutating -Action "Activate region $code" -Target "compute.bicep ($code)" | Out-Null
    }

    # -----------------------------------------------------------------------
    # 6. Endpoints - the only thing this script prints (never a secret).
    # -----------------------------------------------------------------------
    Write-Phase 'Estate endpoints'
    if ($frontDoorEndpoints -and $frontDoorEndpoints.PSObject.Properties.Name.Count -gt 0) {
        Write-Host '  Global Front Door:'
        foreach ($name in $frontDoorEndpoints.PSObject.Properties.Name) {
            Write-Host ("    {0,-10} https://{1}" -f $name, $frontDoorEndpoints.$name)
        }
    }
    Write-Host '  Per-region heads:'
    foreach ($r in $perRegion) {
        Write-Host "    [$($r.regionCode)]"
        Write-Host "      silo      https://$($r.siloStateApiFqdn)"
        Write-Host "      mcp       https://$($r.mcpFqdn)"
        Write-Host "      explorer  https://$($r.explorerFqdn)"
    }

    Write-Phase 'Done. The estate has converged.'
}
finally {
    foreach ($file in $tempFiles) {
        Remove-Item -Path $file -ErrorAction SilentlyContinue
    }
    # Best-effort scrub of the plaintext secret locals.
    $replicationKeyPlain = $null
    $grafanaPasswordPlain = $null
    $pass1SecretsJson = $null
    [System.GC]::Collect()
}
