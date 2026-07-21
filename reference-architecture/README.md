# Orleans.Lattice reference architecture: deploy and configuration guide

This is the operator guide for the active-active, cross-region Orleans.Lattice
reference estate on Azure Container Apps (ACA). It covers prerequisites, the
parameter reference, deploy/verify/teardown for both network options, day-2
operations, the optional hardening and upgrade path, and the real-Azure
validation runbook.

For the design, the topology diagrams, the consistency scoping, and the
rationale behind every choice, read the design document first:
[`../reference-architecture.md`](../reference-architecture.md). This guide does
not duplicate the design; it tells you how to run the kit.

## What is in the kit

| Folder | Contents |
|--------|----------|
| [`bicep/`](bicep/) | `main.bicep` orchestrator, the per-concern modules (compute, storage, networking, observability, frontdoor), `bootstrap.bicep` (registry pre-build seam), and `entra/` (the Microsoft Graph extension module + its scoped `bicepconfig.json`). |
| [`hosts/`](hosts/) | The three container host projects - Silo, MCP, and Explorer - each with a chiselled, non-root Dockerfile. They reference the published `Orleans.Lattice` NuGet packages. See [`hosts/README.md`](hosts/README.md) for the full host configuration surface. |
| [`deploy/`](deploy/) | `Deploy-ReferenceArchitecture.ps1`, the single idempotent orchestrator, and [`deploy/README.md`](deploy/README.md) documenting its internals. |
| [`local/`](local/) | A Docker Compose harness that stands the whole estate up on one machine for development. See [`local/README.md`](local/README.md). |

## Prerequisites

- An Azure subscription, and an identity with rights to create resource groups,
  Container Apps, storage accounts, Key Vaults, Azure Monitor workspaces, an
  Azure Container Registry, and an Azure Front Door profile in that subscription.
- For Entra provisioning: rights to create app registrations and service
  principals, and - for the one residual admin-consent step - a
  Privileged Role Administrator (or Global Administrator) to grant tenant-wide
  application permissions.
- Tooling on the operator workstation:
  - PowerShell 7.0 or later.
  - The Azure CLI (`az`) with the `containerapp` extension available, signed in
    (`az login`) to the target tenant.
  - Docker is **not** required on the operator workstation: images are built
    server-side with `az acr build`.

## Quick start

```powershell
$key = Read-Host -AsSecureString 'Replication key'
$gpw = Read-Host -AsSecureString 'Grafana admin password'

./deploy/Deploy-ReferenceArchitecture.ps1 `
    -SubscriptionId 00000000-0000-0000-0000-000000000000 `
    -ResourceGroup rg-lattice `
    -Location eastus `
    -BaseName lattice `
    -Regions @(
        @{ regionCode = 'use'; location = 'eastus' },
        @{ regionCode = 'euw'; location = 'westeurope' }
    ) `
    -ImageTag 2025.07.29 `
    -ReplicationTrees 'orders=LwwRegister,inventory=OrSet' `
    -ReplicationKey $key `
    -GrafanaAdminPassword $gpw
```

One invocation converges the whole estate and prints the resulting endpoints.
Re-running it converges again; it never duplicates resources. Add `-WhatIf` to
preview every action without mutating Azure.

## Parameter reference

### Deployment script

`Deploy-ReferenceArchitecture.ps1` is the operator entry point. Its exhaustive
internals (the two-pass sequence, the secret handling, the idempotency
guarantees) are documented in [`deploy/README.md`](deploy/README.md).

| Parameter | Required | Notes |
|-----------|----------|-------|
| `-SubscriptionId` | yes | Target subscription. |
| `-ResourceGroup` | yes | Created if absent (idempotent). |
| `-Location` | yes | Resource-group location. |
| `-BaseName` | yes | 3-16 lowercase alphanumerics, shared estate-wide. |
| `-Regions` | yes | Array of `@{ regionCode = '...'; location = '...' }`. One or many. |
| `-ImageTag` | yes | Tag applied to all three built images. |
| `-DeploymentOption` | no | `public` (default) or `private`. See below. |
| `-ZoneRedundant` | no | `$true` (default) or `$false`. Zone-redundant compute; applies to both options (both are VNet-injected). |
| `-ReplicationTrees` | no | Estate-wide `treeName=MergeMode,...` map. |
| `-BackupPrimaryRegionCode` | no | Defaults to the first region. |
| `-IngressAllowedCidrs` | no | Ingress allow-list (public option). |
| `-SiloMinReplicas` / `-SiloMaxReplicas` | no | Silo scale floor (default 1) and ceiling (default 10). The floor is never zero. |
| `-AuthDefaultEffect` | no | `Deny` (default, secure) or `Allow` (throwaway dev only). |
| `-RequireApiAuthorization` | no | Default `$true`. |
| `-ReplicationKey` | public option | `SecureString`, byte-identical across every run and region. |
| `-GrafanaAdminPassword` | yes | `SecureString`. |
| `-EntraEnabled` / `-EntraTenantId` | Entra | Enable Entra and target the tenant. |
| `-EntraClientId` | no | Use a pre-existing audience app instead of deploying `entra/entra.bicep`. |
| `-EntraAudiences` | no | Extra accepted token audiences. |
| `-ExplorerRedirectUris` | no | Defaults derived from the deployed FQDNs. |
| `-SkipImageBuild` | no | Reuse images already present at `-ImageTag`. |
| `-WhatIf` | no | Preview every action without mutating Azure. |

### Bicep top-level parameters

`bicep/main.bicep` is the all-at-once template the script drives. The parameters
an operator overrides directly (when deploying the template by hand rather than
through the script) are:

| Parameter | Default | Notes |
|-----------|---------|-------|
| `baseName` | (required) | 3-16 lowercase alphanumerics. |
| `regions` | (required) | Array of `{ regionCode, location }`. |
| `imageTag` | (required) | Host image tag. |
| `deploymentOption` | `public` | `public` or `private`. |
| `zoneRedundant` | `true` | Zone-redundant compute (replicas spread across availability zones). Applies to both options - both are VNet-injected. |
| `siloMinReplicas` / `siloMaxReplicas` | 1 / 10 | Silo autoscale bounds. |
| `backupPrimaryRegionCode` | first region | The single backup-primary region. |
| `replicationKey` | `''` | `@secure()`; the per-cluster replication key (public option). |
| `grafanaAdminPassword` | `''` | `@secure()`; per-region Grafana admin password. |
| `ingressAllowedCidrs` | `[]` | Ingress allow-list (public option). |
| `authDefaultEffect` | `Deny` | Authorization default effect estate-wide. |
| `requireApiAuthorization` | `true` | Whether the facades and MCP require authorization. |
| `entraEnabled` / `entraTenantId` / `entraClientId` / `entraAudiences` | off / `''` | Entra authentication. |
| `prometheusQueryEndpoint` / `frontDoorId` | `''` | Forward-threaded seams; empty on pass 1, activated on pass 2 (compile-cycle avoidance). Managed by the script. |

The per-region module parameters (`bicep/modules/*.bicep`) are internal seams the
orchestrator wires; you do not set them by hand. Each module header documents its
own inputs and outputs.

## Deploy, verify, and teardown

### Public option (default)

The public option exposes each head over ACA external ingress (server TLS,
HTTP/2) fronted by a single global Azure Front Door Standard profile, and stores
the per-cluster replication key in a per-region Key Vault. Deploy it with the
Quick start command above (`-DeploymentOption public`, the default). The
environment is still VNet-injected (each region gets a per-region VNet with a
delegated ACA infrastructure subnet) so it is zone-redundant; it simply keeps an
external ingress and no cross-region VNet peering. Each region therefore consumes
a `/23` infrastructure subnet from a non-overlapping per-region address plan.

### Private option

The private option puts every regional ACA environment on an internal-only,
VNet-integrated ingress with full-mesh global VNet peering, so cross-region
replication travels private address space. Select it with
`-DeploymentOption private`. (Both options are VNet-injected; the private option
adds internal-only ingress plus the peering, on top of the per-region VNets the
public option already provisions.)

```powershell
./deploy/Deploy-ReferenceArchitecture.ps1 `
    -SubscriptionId ... -ResourceGroup rg-lattice-private `
    -Location eastus -BaseName lattice `
    -Regions @(@{ regionCode='use'; location='eastus' }, @{ regionCode='euw'; location='westeurope' }) `
    -ImageTag 2025.07.29 -DeploymentOption private `
    -ReplicationKey $key -GrafanaAdminPassword $gpw
```

Private-option cross-region name resolution requires each region's VNet to be
linked to its peers' ACA managed private DNS zones. Those zones are created in the
platform-managed resource group once each environment exists, so this link is a
manual post-deploy step (the private-option network foundation and its full-mesh
peering live in `bicep/modules/vnet.bicep`); it is the one part of the private
option that is not expressible before the environments are provisioned.

Every managed environment is **zone-redundant** by default (`zoneRedundant`,
default `true`) under both options, because both are VNet-injected. Once the silo
autoscales beyond a single replica those replicas are spread across availability
zones - matching the zone-redundant durability of the WAL storage tier. Set
`zoneRedundant` to `false` to opt an estate back out (for example a single-zone
dev estate).

### Verify

After the script prints the endpoints:

- Open the Explorer Front Door hostname in a browser; sign in (Entra, when
  enabled) and confirm the operator console loads and lists the cluster.
- Point an MCP client at the MCP Front Door hostname and confirm the tool list
  is returned.
- Write a key in one region and read it back from another to confirm
  active-active convergence (see the validation runbook below for the exact
  procedure).

### Teardown

The estate is contained in a single resource group (plus its Entra app
registrations). Tear it down with:

```powershell
az group delete --name rg-lattice --yes
# Remove the three Entra app registrations the kit created (by display name).
# The kit names them "<BaseName> Lattice silo facade", "<BaseName> Lattice MCP
# endpoint", and "<BaseName> Lattice Explorer console" (here BaseName = lattice):
foreach ($app in 'lattice Lattice silo facade','lattice Lattice MCP endpoint','lattice Lattice Explorer console') {
    $id = az ad app list --display-name "$app" --query '[0].appId' -o tsv
    if ($id) { az ad app delete --id $id }
}
```

Deleting the resource group removes the container apps, storage, Key Vaults,
Azure Monitor workspaces, registry, and Front Door profile. The Key Vaults are
soft-delete + purge-protection enabled, so their names are reserved for the
retention window; pass a fresh `-BaseName` (or purge them) to redeploy
immediately under the same names.

## Day-2 operations

### Scaling behaviour

- The **silo** scales on the `lattice.scaling` compute-axis metric through a KEDA
  Prometheus scaler that queries the region's managed Prometheus. The floor is
  pinned at or above one replica (never zero) so the cluster always has a
  membership quorum; the ceiling defaults to ten. A draining replica honours the
  termination grace period so in-flight shard transfers complete or hand off
  before exit.
- The **MCP** and **Explorer** heads scale to zero and wake on HTTP concurrency;
  they are stateless (MCP) or session-isolated (Explorer) admin surfaces and cost
  nothing while idle.

### Backup and restore

- The single backup-primary region's silo runs the backup scheduler and writes
  full and incremental backup chains to the shared global Azure Blob backup sink.
  Standby regions have restore-only (read) access to the sink.
- To restore, follow the `Orleans.Lattice.Backup.AzureBlob` restore procedure
  against the backup container; HLC/LWW causal ordering means a restored value
  never overwrites a causally-newer live value, so a cold restore into a live
  active-active estate is safe.

### Failover and disaster recovery

- Every region is a full read-write peer, so a regional outage is absorbed by the
  surviving regions with no promotion step: Azure Front Door latency-routes
  clients to the nearest healthy region and fails over to the next-nearest.
- The only single-region role is the backup primary. If that region is lost,
  designate a new primary by re-running the deployer with a different
  `-BackupPrimaryRegionCode`; the replication key and data are unaffected.

### Observability

- Each region has a managed Prometheus (Azure Monitor workspace) and a
  self-hosted Grafana head pre-provisioned with the bundled Orleans.Lattice
  dashboards. Reach Grafana at its per-region ingress; sign in with the
  `-GrafanaAdminPassword` you supplied (Prometheus is queried through the
  region's managed identity, no scraped secret).

## Optional hardening and upgrade path

The baseline ships secure but with the Front Door Web Application Firewall (WAF)
**off** and on the Standard SKU. Two opt-in steps harden it further.

### Enable a Front Door WAF custom-rule policy (Standard)

The Standard profile supports custom WAF rules (rate limiting, geo-filtering, IP
allow/deny). Provision a `Microsoft.Network/FrontDoorWebApplicationFirewallPolicies`
policy and attach it to the profile with a `securityPolicies` association over the
endpoint domains. The frontdoor module header carries the exact snippet and the
`enableWaf` seam. Custom rules add no SKU cost but are billed per policy and per
rule evaluation.

### Upgrade Azure Front Door Standard to Premium

Premium adds Azure-managed WAF rule sets (OWASP core + bot protection) and
Private Link private origins. Upgrading:

- Changes the profile SKU from `Standard_AzureFrontDoor` to
  `Premium_AzureFrontDoor` and lets you attach the managed rule sets in addition
  to (or instead of) custom rules.
- Enables Private Link origins, so the heads can be reached privately rather than
  over public ingress. This composes with the **private** network option: with
  Premium + Private Link the client-facing origins never need public ingress at
  all. Note that the private option's internal ingress already keeps
  replication traffic off the public internet; Premium extends that to the
  client-facing path.
- Carries a higher base monthly cost than Standard plus managed-rule request
  charges. Weigh it against the estate's exposure and compliance requirements.

## Local development

To exercise the whole estate on one machine before touching Azure, use the Docker
Compose harness under [`local/`](local/). It runs the three heads against Azurite
and a local Prometheus/Grafana, with the security bypass toggles (Entra off,
plaintext h2c) documented and defaulted for development only. See
[`local/README.md`](local/README.md).

## Real-Azure validation runbook

> Status: this runbook is authored and ready to run. The recorded evidence below
> is left unchecked pending an operator executing it against a live subscription
> (the reference deploy is intentionally not run as part of authoring this kit).
> Fill in the evidence columns after a real run.

Validated topology: two regions (for example `eastus` + `westeurope`), public
network option, Entra enabled.

1. **Deploy.** Run the Quick start command for two regions. Record the printed
   Front Door and per-region head endpoints.
   - Evidence: endpoint list - _pending_.
2. **Reachability.** Open the Explorer Front Door hostname and confirm sign-in
   and the cluster view; call the MCP Front Door hostname and confirm the tool
   list.
   - Evidence: Explorer screenshot / MCP tool list - _pending_.
3. **Active-active convergence.** Write a key through region A's State API and
   read it back through region B's State API; then write the same key
   concurrently in both regions and confirm the CRDT merge result is identical on
   both sides.
   - Evidence: cross-region read + merge result - _pending_.
4. **Autoscale.** Drive load at one region's silo and confirm the KEDA scaler
   raises the replica count above the floor, then scales back down after the load
   stops.
   - Evidence: replica-count timeline / autoscale event - _pending_.
5. **Backup and restore.** Confirm the primary region writes a backup chain to
   the sink, then perform a restore into a standby and confirm the restored value
   is present and causally consistent.
   - Evidence: backup chain listing + restore check - _pending_.
6. **Teardown.** Run the teardown commands and confirm the resource group and the
   three Entra apps are removed.
   - Evidence: empty resource group - _pending_.

### Cost note

The validated two-region public topology's steady-state cost is dominated by the
pinned silo replicas (one per region minimum, scaling on load), the two managed
Prometheus workspaces and Grafana heads, the two Standard storage accounts plus
the shared backup blob account, the two Key Vaults, the shared container
registry, and the single Front Door Standard profile. The scale-to-zero MCP and
Explorer heads add negligible idle cost. Record the actual monthly figure from
Azure Cost Management after the validation run. Enabling Front Door Premium is the
single largest cost lever (see the upgrade path above).
