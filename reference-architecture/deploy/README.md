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
   networking, observability, and the single global Azure Front Door profile, in
   the module order `main.bicep` encodes.
4. **Entra** (`../bicep/entra/entra.bicep`, Microsoft Graph extension) - app
   registrations, service principals, federated identity credentials, and the
   app-to-app app-role grants (see below).
5. **Symmetric replication** - reciprocal peer enrollment, the estate-wide wire
   merge mode, and the per-region Key Vault replication-key secret URI, applied to
   every region.

## Two-pass deployment

`main.bicep` cannot thread two Azure-assigned values in a single pass without
forming a Bicep compile cycle, so the script runs them on a second pass:

- **Managed Prometheus query endpoint** - activates the silo KEDA scaler and the
  MCP cluster-telemetry tools. Each region has its own endpoint. The MCP head
  queries it with a rotating managed-identity Entra token (the `DynamicBearer`
  auth mode shipped by #1286); the region managed identity already holds
  Monitoring Data Reader on the workspace. When the observability lane is absent
  the backend is empty and the host leaves the telemetry tool group off.
- **Front Door id** - activates the `X-Azure-FDID` origin lock on every
  client-facing head.

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

## Entra design (federated identity, no secrets)

`entra.bicep` creates three app registrations - silo facade, MCP endpoint, and
Explorer - each with a service principal. Instead of client secrets it authors
**federated identity credentials**: one per region managed identity, on the silo,
MCP, and Explorer apps. A workload therefore obtains an app token from its own
managed identity with no secret to store, rotate, or leak.

- The silo facade app declares an application app role, `Lattice.Access`. The MCP
  and Explorer service principals are granted that role (the app-to-app
  authorization edge) - least privilege, a single purpose-named role assigned to
  exactly the two callers.
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

### Residual follow-up (tracked: #1291)

The federated credentials are provisioned and ready. Wiring the silo host to
acquire its app-only Microsoft Graph token via the federated managed identity
(rather than the `Entra:Graph:ClientSecret` it reads today) needs a secret-less
`TokenCredential` authentication path in the core
`Orleans.Lattice.Membership.Entra.Graph` package, which today supports only
client-secret auth. That core feature is tracked in #1291 and ships as a
released package first (the same release-first pattern used for #1286); the silo
host then consumes it. Until then the group resolver remains opt-in via
`Entra:Graph:ClientSecret`.

## Parameters

| Parameter | Required | Notes |
|-----------|----------|-------|
| `-SubscriptionId` | yes | Target subscription. |
| `-ResourceGroup` | yes | Created if absent (idempotent). |
| `-Location` | yes | Resource-group location. |
| `-BaseName` | yes | 3-16 lowercase alphanumerics, shared estate-wide. |
| `-Regions` | yes | Array of `@{ regionCode = '...'; location = '...' }`. One or many. |
| `-ImageTag` | yes | Tag applied to all three built images. |
| `-DeploymentOption` | no | `public` (default, external ingress + replication key) or `private` (internal ingress + VNet peering). Both are VNet-injected + zone-redundant. |
| `-ZoneRedundant` | no | `$true` (default) or `$false`. Zone-redundant compute for both options. |
| `-ReplicationTrees` | no | Estate-wide `treeName=MergeMode,...` map. |
| `-BackupPrimaryRegionCode` | no | Defaults to the first region. |
| `-IngressAllowedCidrs` | no | Ingress allow-list (public option). |
| `-ReplicationKey` | public option | `SecureString`, stable across runs. |
| `-GrafanaAdminPassword` | yes | `SecureString`. |
| `-EntraEnabled` / `-EntraTenantId` | Entra | Enable and target tenant. |
| `-EntraClientId` | no | Use a pre-existing audience app instead of deploying `entra.bicep`. |
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
