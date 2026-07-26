# Reference-architecture estate deployer

`Deploy-ReferenceArchitecture.ps1` is the single, parameterised, idempotent
orchestrator that stands up the active-active, cross-region Orleans.Lattice
estate on Azure Container Apps end to end, and the Bicep-native Entra module it
drives.

> This folder documents and drives the deploy. The Bicep it deploys lives under
> `../bicep/`. The estate-wide validation runbook is a separate, coordinator-owned
> document.

## What it provisions

From one parameter set, across N regions:

1. **Shared registry** (`../bicep/bootstrap.bicep`) - one Azure Container
   Registry, keyless (no admin user, image pull by managed identity + AcrPull
   RBAC). Deployed first so the host images can be built before the Container Apps
   that pull them exist. `main.bicep` converges onto this same registry.
2. **Host images** - built server-side with `az acr build` straight from
   `../hosts/{Silo,Mcp,Explorer}/Dockerfile`. No image is published to a public
   registry.
3. **The estate** (`../bicep/main.bicep`) - per-region compute, storage,
   networking (both options VNet-injected and zone-redundant; the private option
   adds full-mesh VNet peering), observability, and the global entry point, in the
   module order `main.bicep` encodes. The public option provisions a single global
   Azure Front Door profile; the private option provisions cross-region private DNS
   instead.
4. **Entra** (`../bicep/entra/entra.bicep`, Microsoft Graph extension) - app
   registrations, service principals, federated identity credentials, and the
   app-to-app app-role grants (see below).
5. **Symmetric replication** - reciprocal peer enrollment, the estate-wide wire
   merge mode, and the per-region Key Vault replication-key secret URI, applied to
   every region.
6. **MCP cross-region targeting** - each region's MCP head is given its own region
   id/cluster id plus a route to every peer region's silo (the same per-region
   silo FQDNs replication uses), so one Front Door MCP endpoint can serve
   `lattice_list_regions` and per-call `region`-targeted tool calls across the
   whole estate. Threaded on pass 2 alongside the replication peers.

## Two-pass deployment

`main.bicep` cannot thread two Azure-assigned values in a single pass without
forming a Bicep compile cycle, so the script runs them on a second pass:

- **Managed Prometheus query endpoint** - activates the silo KEDA scaler and the
  MCP cluster-telemetry tools. Each region has its own endpoint. The MCP head
  queries it with a rotating managed-identity Entra token (the `DynamicBearer`
  auth mode shipped by #1286); the region managed identity already holds
  Monitoring Data Reader on the workspace. When the observability lane is absent
  the backend is empty and the host leaves the telemetry tool group off.
- **Front Door id** (public option) - activates the `X-Azure-FDID` origin lock
  on every client-facing head. The private option has no Front Door, so this
  seam is empty and the lock is not wired.

Because the Prometheus endpoint is per-region and the replication seams are not
threaded by `main.bicep` at all, **pass 2 deploys each region's `compute.bicep`
directly**. `compute.bicep`'s resource names are pure functions of
`(baseName, regionCode)`, so the direct deploy converges onto the exact same
Container Apps that pass 1 created - it does not create a second estate. This is
the coordinator-sanctioned "script is the orchestrator, main.bicep is the
all-at-once convenience template" pattern.

## Symmetric replication

- Each region's replication cluster id is `<baseName>-<regionCode>`.
- Its peer endpoint is `https://<siloStateApiFqdn>` (the silo's external gRPC
  ingress, which carries state, auth, and replication).
- For every region the script builds the peer list from **every other region**,
  so enrollment is fully reciprocal across all N regions. Asymmetry dead-letters
  cross-region traffic, so completeness matters.
- The wire merge mode (`-ReplicationTrees`, for example
  `orders=LwwRegister,inventory=OrSet`) is applied identically estate-wide.
- The replication key is byte-identical across regions (one Key Vault secret per
  region, same material). It is a `SecureString` in and an `@secure()` Bicep
  parameter out - never written to disk, never logged, never an output.

## MCP cross-region targeting

The MCP head fronts its co-located silo by default, but the estate is deployed with
one global Front Door in front of every region, so the script also wires each head
to reach its peers directly:

- Each head advertises its own region (`Mcp:RegionId` = the region code,
  `Mcp:ClusterId` = `<baseName>-<regionCode>`), surfaced by `lattice_list_regions`.
- The peer set is exactly the replication peer set: every other region, dialed at
  its DIRECT region-pinned silo gRPC FQDN (`https://<siloStateApiFqdn>`, never the
  anycast Front Door hostname). That single endpoint serves every facade group, so
  a caller can pass an optional `region` on any tool call to pin it to a region.
- `Mcp:VerifyRegionIdentity` is enabled whenever peers exist: before routing to a
  peer the head probes its state facade and compares the reported cluster id to the
  advertised one, rejecting fail-closed a region whose endpoint does not actually
  reach the expected cluster. The peers use direct FQDNs, so the assertion passes.
- The head stamps the shared `X-Azure-FDID` origin-lock header on every cross-region
  call, exactly as it does for its co-located silo, so the peer silo accepts it.

Threaded on pass 2 (the peer FQDNs are only known after pass 1), reusing the same
`perRegion[].siloStateApiFqdn` values the replication peer list is built from.

## Entra design (federated identity, no secrets)

`entra.bicep` creates three app registrations - silo facade, MCP endpoint, and
Explorer - each with a service principal. Instead of client secrets it authors
**federated identity credentials**: one per region managed identity, on the silo,
MCP, and Explorer apps. A workload therefore obtains an app token from its own
managed identity with no secret to store, rotate, or leak.

- The silo facade app declares an application app role, `Lattice.Access`, granted
  to the MCP service principal - the app-to-app (client-credentials) authorization
  edge. The silo app also declares a delegated `user_impersonation` scope; the
  Explorer console signs operators in (OpenID Connect) and calls the facade
  on-behalf-of them, so it is granted that delegated scope (admin-consented
  declaratively via an `oauth2PermissionGrant`) rather than the app role. Least
  privilege either way: a single purpose-named grant per caller.
- The silo app declares the Microsoft Graph `GroupMember.Read.All` **application**
  permission its optional group resolver needs, and `entra.bicep` grants tenant
  admin consent for it **declaratively** - an `appRoleAssignedTo` from the silo
  service principal to the Microsoft Graph service principal's app role, which is
  exactly what `az ad app permission admin-consent` creates. There is therefore
  no imperative consent step. The grant is idempotent, and the deploying identity
  must hold a privileged directory role (for example Privileged Role
  Administrator, or the `AppRoleAssignment.ReadWrite.All` +
  `Application.ReadWrite.All` application permissions) for it to succeed.

No `passwordCredentials` are authored and nothing secret is emitted as an output.
The app (client) ids the module outputs are public identifiers.

### Secret-less Microsoft Graph (managed identity)

The federated credentials provisioned above are consumed directly by the silo
host. When Entra is enabled the silo authenticates its app-only Microsoft Graph
group resolver with the region's user-assigned managed identity (via
`DefaultAzureCredential`, resolved through `AZURE_CLIENT_ID`) against the
federated credential on the app registration - no client secret is stored,
injected, or rotated. Compute sets `Entra__Graph__UseManagedIdentity=true` on the
silo whenever Entra is on. The secret-less `TokenCredential` path in the core
`Orleans.Lattice.Membership.Entra.Graph` package (8.0.1) landed via #1291. A
`Entra:Graph:ClientSecret` is still accepted as a dev / back-compat override and
takes precedence when supplied.

## Initial access: the single security administrator

When Entra is enabled the estate is deny-by-default: no caller can read or write
until a subject is authorized. The deployer seeds exactly one root-of-trust
administrator - the `-SecurityAdmin` (an Entra object id or UPN / email resolved
to its object id), defaulting to the currently signed-in deploying user. That
object id is threaded to every region's silo as `Auth:BootstrapAdministrators`,
matched on the Entra `oid` claim. Only that administrator can reach the estate
after the first deploy; they then grant further operators access at runtime
through the Explorer Access tab, which is itself administrator-gated (every
membership / policy write requires an `Admin` verdict on the authorization tree).

## Running it

Invoke it with a full parameter set (recommended). Splat the parameters and read
the two `SecureString`s so nothing secret is echoed:

```powershell
$key = Read-Host -AsSecureString 'Replication key'
$gpw = Read-Host -AsSecureString 'Grafana admin password'

./Deploy-ReferenceArchitecture.ps1 `
    -SubscriptionId <sub-guid> `
    -ResourceGroup rg-lattice `
    -Location uksouth `
    -BaseName lattice `
    -Regions @(@{ regionCode = 'uks'; location = 'uksouth' }, @{ regionCode = 'wus'; location = 'westus3' }) `
    -ImageTag 2025.07.29 `
    -ReplicationKey $key `
    -GrafanaAdminPassword $gpw `
    -EntraEnabled -EntraTenantId <tenant-guid>
```

Running it with **no arguments** drops into PowerShell's per-parameter prompt,
which can only supply **strings** - it cannot build the `-Regions` hashtables. For
an interactive run, give each region in the compact `regionCode=location` form, one
per line, and a blank line to finish; and type a non-empty `-ImageTag` (a blank
entry is rejected):

```text
Regions[0]: uks=uksouth
Regions[1]: wus=westus3
Regions[2]:
ImageTag: 2025.07.29
```

Add `-WhatIf` to preview every action without mutating Azure.

### Quick start: the three-region sample

For a zero-decision evaluation estate, `deployment-sample.ps1` wraps this
deployer and needs only a deployment name. It fixes the three regions to East US
2, West US 3, and West Europe, derives every other value from the name (base name
= the name, resource group = `rg-<name>`), and generates the replication key and
Grafana admin password for you (the password is printed once at the end, to use
as the `admin` user at the per-region Grafana URLs the deployer lists under its
estate endpoints).
It deploys the public network option with Entra sign-in on.

It first requires an authenticated Azure CLI session (it errors out asking you to
run `az login` if none is present), then resolves the target subscription (the
current `az` context, or `-SubscriptionId`) and its tenant, prints them with the
signed-in user, and asks you to confirm before creating anything:

```powershell
# Deploy into the current 'az' subscription (you are shown it and asked to confirm).
./deployment-sample.ps1 -DeploymentName demo
```

The deployment name must be 3 to 16 lowercase letters or digits. Pass `-Force` to
skip the confirmation prompt, or `-WhatIf` to preview. For any other topology
(private networking, a different region set, a pre-existing Entra app), drive
`Deploy-ReferenceArchitecture.ps1` directly as above.

## Parameters

| Parameter | Required | Notes |
|-----------|----------|-------|
| `-SubscriptionId` | yes | Target subscription. |
| `-ResourceGroup` | yes | Created if absent (idempotent). |
| `-Location` | yes | Resource-group location. |
| `-BaseName` | yes | 3-16 lowercase alphanumerics, shared estate-wide. |
| `-Regions` | yes | One or more regions. Each entry is a hashtable `@{ regionCode = '<2-8 chars>'; location = '<azure region>' }` or the compact string `'regionCode=location'` (for example `'use=eastus'`); the string form is what the interactive prompt accepts. |
| `-ImageTag` | yes | Non-empty tag applied to all three built images (silo / MCP / Explorer). |
| `-DeploymentOption` | no | `public` (default, external ingress + replication key over public ingress) or `private` (internal ingress + VNet peering, replication key layered on as defense in depth). Both are VNet-injected + zone-redundant, and both provision the per-region replication Key Vault. |
| `-ZoneRedundant` | no | `$true` (default) or `$false`. Zone-redundant compute for both options. |
| `-ReplicationTrees` | no | Estate-wide `treeName=MergeMode,...` map. |
| `-BackupPrimaryRegionCode` | no | Defaults to the first region. |
| `-IngressAllowedCidrs` | no | Ingress allow-list (public option). |
| `-ReplicationKey` | yes | `SecureString`, stable across runs. Required by both options. |
| `-GrafanaAdminPassword` | yes | `SecureString`. |
| `-EntraEnabled` / `-EntraTenantId` | Entra | Enable and target tenant. |
| `-EntraClientId` | no | Use a pre-existing audience app instead of deploying `entra.bicep`. |
| `-SecurityAdmin` | no | The single Entra security administrator seeded as the sole initial-access principal (root of trust). Object id (GUID) or UPN / email (resolved to an object id). Defaults to the deploying user when Entra is enabled; add further administrators at runtime via the Explorer Access tab. |
| `-EnableDataApi` | no | `$true` (default) exposes the read-write Data API; `-EnableDataApi:$false` withholds the write surface. |
| `-ExplorerRedirectUris` | no | Defaults derived from the deployed FQDNs. |
| `-SkipImageBuild` | no | Reuse images already present at `-ImageTag`. |
| `-WhatIf` | no | Preview every action without mutating Azure. |

## Idempotency and re-runs

Every step converges:

- `az group create` and the ARM deployments are declarative and idempotent.
- `az acr build` overwrites the same tag.
- RBAC is declarative (Bicep modules assign managed-identity data-plane RBAC;
  `entra.bicep` assigns the app-to-app app role), so re-runs never duplicate role
  assignments.
- Federated identity credentials and app registrations are keyed by stable names,
  so a re-run updates in place.

Supply the **same** `-ReplicationKey` on every run; rotating it dead-letters
in-flight cross-region traffic until all regions converge on the new key.

## Static validation (no Azure required)

This deployer is validated without touching Azure:

```powershell
# Bicep compiles clean (zero warnings). Delete generated JSON after.
az bicep build --file ../bicep/bootstrap.bicep
az bicep build --file ../bicep/entra/entra.bicep

# PowerShell parses with zero errors.
$e = $null
[System.Management.Automation.Language.Parser]::ParseFile(
    "$PWD/Deploy-ReferenceArchitecture.ps1", [ref]$null, [ref]$e); $e.Count

# Lint clean.
Invoke-ScriptAnalyzer -Path ./Deploy-ReferenceArchitecture.ps1
```

`entra.bicep` uses the Microsoft Graph Bicep extension (GA), enabled by the
`bicepconfig.json` colocated in `../bicep/entra/`. It is kept in its own folder so
the extension/experimental config does not apply to `main.bicep` or the ARM
modules, which build with stock Bicep defaults.
