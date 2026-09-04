# Orleans.Lattice Packages

Orleans.Lattice ships as a small core plus a set of companion packages. Each
companion fills one seam in the core - storage, identity, governance,
replication, administration, observability - and a host takes only the ones it
needs. A deployment that registers none of them runs the core library alone.

Every add-on has its own documentation set, anchored by a package README that
mirrors the repository README (overview, features, quick start, then API /
configuration / architecture references). Most ship as their own NuGet package;
any not yet published to NuGet are marked inline.

Convention: package `foo` has code at `src/foo/`, tests at `test/foo/`, and
documentation at `docs/foo/`. Some rows below are finer-grained than the `src/`
layout, because one source directory can ship several assemblies (the Explorer
is the main example).

For what each capability does, see [FEATURES.md](FEATURES.md).

## Contents

- [Core](#core)
- [APIs](#apis)
- [MCP](#mcp)
- [AI / RepoContext](#ai--repocontext)
- [Explorer (in progress)](#explorer-in-progress)
- [Identity and Security](#identity-and-security)
- [Governance](#governance)
- [Replication](#replication)
- [Storage](#storage)
- [Operations](#operations)

## Core

The core package, plus the companions that extend the data model itself.

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice` | The core platform: the sharded, CRDT-backed B+ tree, the write-ahead log that is its durability boundary, the grain catalogue, and the seams every companion package plugs into. Everything else on this page is optional. | [Docs](docs/lattice/architecture.md) |
| `Orleans.Lattice.GrainIndex` | Typed grain indexing: track an Orleans grain's typed state in a lattice tree and query it with the server-side predicate surface, without hand-maintaining a secondary index. Properties are declared explicitly with `Include`, grains enrol via an `[Indexed]` state facet, a reminder-driven backfill onboards dormant grains, a durable outbox retries failed index writes, and startup rejects a declaration change that would invalidate stored entries. | [README](docs/lattice.grainindex/README.md) |
| `Orleans.Lattice.Vector` | Allocation-lean approximate nearest-neighbour vector index over Lattice-held vectors: an inverted-file core whose query cost is sub-linear in the corpus, persisted on a Lattice tree in bounded chunks with lazy partial load and incremental insert, delete and re-embed maintenance, so a restart reloads the index instead of rebuilding it. Publishes a measured recall target and reports per query whether an approximate or an exact path answered. **Not yet published to NuGet** - build from source today. | [README](docs/lattice.vector/README.md) |

## APIs

The transport-agnostic facade family and its gRPC bindings. A facade is the in-process contract; the matching `.Grpc` package is its wire binding and typed client, for a head that runs outside the cluster.

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Api.Abstractions` | The shared, transport-agnostic API contract: the facade service interfaces - state, data, auth, backup, schema, replication, telemetry, tree administration, and tenant administration - and their request/response DTOs, referenced by the facade implementations, the gRPC bindings, and the MCP server without cross-package internal-visibility grants. | [README](docs/lattice.api.abstractions/README.md) |
| `Orleans.Lattice.Api.State` | Read-only cluster state-API facade: query, observe, and subscribe to trees, structure, entries, change feeds, and metrics. | [README](docs/lattice.api.state/README.md) |
| `Orleans.Lattice.Api.State.Grpc` | The code-first gRPC binding and public client for the read-only state API. | [README](docs/lattice.api.state.grpc/README.md) |
| `Orleans.Lattice.Api.Data` | Write-capable external data-plane facade: point set/delete, point and bounded-range reads, and single- and cross-tree atomic batches for non-.NET clients, each authorized through the core gate. | [README](docs/lattice.api.data/README.md) |
| `Orleans.Lattice.Api.Data.Grpc` | The code-first gRPC binding and public client for the read-write data-plane API. | [README](docs/lattice.api.data.grpc/README.md) |
| `Orleans.Lattice.Api.Auth` | Transport-agnostic control facade for administering membership and policy and explaining authorization decisions. | [README](docs/lattice.api.auth/README.md) |
| `Orleans.Lattice.Api.Auth.Grpc` | The code-first gRPC binding and public client for the authorization control facade. | [README](docs/lattice.api.auth.grpc/README.md) |
| `Orleans.Lattice.Api.Backup` | Transport-agnostic control facade for driving backup capture, restore, catalog listing, chain describe, and retention. | [README](docs/lattice.api.backup/README.md) |
| `Orleans.Lattice.Api.Backup.Grpc` | The code-first gRPC binding and public client for the backup control facade. | [README](docs/lattice.api.backup.grpc/README.md) |
| `Orleans.Lattice.Api.Schema` | Transport-agnostic control facade for managing schema policy, dead letters, versioning, remediation, and compliance audits. | [README](docs/lattice.api.schema/README.md) |
| `Orleans.Lattice.Api.Schema.Grpc` | The code-first gRPC binding and public client for the schema control facade. | [README](docs/lattice.api.schema.grpc/README.md) |
| `Orleans.Lattice.Api.Replication` | Transport-agnostic control facade for runtime per-tree replication configuration: an authorized operator can enable replication for a tree (fixing its wire merge mode), disable it, and inspect the replicated-tree set, authorized fail-closed through the shared access gate. | [README](docs/lattice.api.replication/README.md) |
| `Orleans.Lattice.Api.Replication.Grpc` | The code-first gRPC binding and public client for the runtime replication control facade. | [README](docs/lattice.api.replication.grpc/README.md) |
| `Orleans.Lattice.Api.TreeAdmin` | Transport-agnostic control facade for whole-tree administration, composing the existing single-responsibility facades (it wraps the schema control facade by delegation). Exposes a fail-closed per-operation capability probe plus the whole-tree lifecycle surface: create, inspect, and reconfigure trees; alias resolution; delete, recover, and purge; bulk load; restore and revert; reshard, resize, snapshot; WAL placement audit and movement; materialised-view and tag-index management; shard compaction; and history retention. | [README](docs/lattice.api.treeadmin/README.md) |
| `Orleans.Lattice.Api.TreeAdmin.Grpc` | The code-first gRPC binding and public client for the tree-administration control facade. | [README](docs/lattice.api.treeadmin.grpc/README.md) |
| `Orleans.Lattice.Api.TenantAdmin` | Transport-agnostic operator control facade for tenant administration: create, suspend, resume, and delete tenants (delete cascading the tenant's trees), author per-tenant quotas, and administer per-tenant region residency, plus a fail-closed self-service read surface and an optional tenant-scoped tree-administration surface - all authorized through the shared access gate. | [README](docs/lattice.api.tenantadmin/README.md) |
| `Orleans.Lattice.Api.TenantAdmin.Grpc` | The code-first gRPC binding and public client for the tenant-administration control facade. | [README](docs/lattice.api.tenantadmin.grpc/README.md) |
| `Orleans.Lattice.Api.Telemetry` | Backend-neutral telemetry facade: answers a curated set of named queries over a Prometheus-compatible backend, derives each answer's tenant scope on the server, and enforces a fail-closed metric allow-list on the metric names a query will actually evaluate. Callers name a query id and never supply PromQL. | [Docs](docs/lattice.api.telemetry/README.md) |
| `Orleans.Lattice.Api.Telemetry.Grpc` | gRPC binding for the telemetry facade, for a remote head that cannot enforce tenant scoping locally. References only the shared contract package - a closure asserted over the transitive project graph, package ids, and emitted assembly references - and derives no tenant of its own. | [Docs](docs/lattice.api.telemetry.grpc/README.md) |

## MCP

Model Context Protocol bindings that expose the API facades to AI agents, fail-closed and scoped to the caller's grants.

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Api.Mcp` | Model Context Protocol (MCP) server binding: exposes the transport-agnostic API facades as opt-in, permission-aware MCP tools over an authenticated, fail-closed, default-deny credential bridge, registered with `AddLatticeMcp(...)` and mapped with `MapLatticeMcp()`. | [README](docs/lattice.api.mcp/README.md) |
| `Orleans.Lattice.Api.Mcp.Telemetry` | Opt-in telemetry add-on for the MCP server: exposes cluster OpenTelemetry metrics as MCP tools by proxying a read-only Prometheus/PromQL backend, with a dual-credential trust boundary that stamps the backend credential and never forwards the caller's Lattice credential. | [README](docs/lattice.api.mcp.telemetry/README.md) |
| `Orleans.Lattice.Api.Mcp.Telemetry.Azure` | Azure managed-identity backend-token provider for the MCP telemetry proxy: supplies a rotating Entra (Azure AD) access token so the telemetry tools can query an Azure Monitor managed-Prometheus endpoint, keeping the Azure identity dependency out of the core telemetry package. | [README](docs/lattice.api.mcp.telemetry.azure/README.md) |

## AI / RepoContext

RepoContext, an AI codebase-memory system built entirely on the platform. It is an example application, not part of the platform definition; see the [README](README.md#repocontext-an-example-built-on-the-platform).

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Api.Mcp.RepoContext` | Opt-in MCP tools that give an AI agent durable, conflict-free context and memory about a codebase - repository bootstrap, structural and symbol recall, free-form memories with optional TTL, and exact-kNN semantic search - stored in the CRDT B+ tree and served fail-closed, with a container host for local use. **Not yet published to NuGet** - distributed as a ready-to-run Docker container and consumed from source today; see the [container sample](samples/RepoContextContainer/README.md). | [README](docs/lattice.api.mcp.repocontext/README.md) |
| `Orleans.Lattice.Api.Mcp.RepoContext.Replication` | Opt-in multi-cluster add-on for the repository-context store: `EnableRepoContextMultiCluster(...)` turns on cross-cluster replication for every repository-context tree with the correct per-tree merge mode - the vector-membership presence tree pinned to the add-wins `OrFlag` CRDT so active-active convergence can never silently drop an embedding, the agent-memory tree pinned to `MvRegister` so concurrent cross-cluster memory writes both survive and fold, other trees defaulting to last-writer-wins. A `LATTICE_REPOCONTEXT_INDEXING_ROLE` hub/spoke gate keeps exactly one cluster indexing, and a startup guard rejects an unsafe topology. Takes the `Orleans.Lattice.Replication` dependency so the repo-context core need not. **Not yet published to NuGet** - consumed from source alongside the repository-context package today. | [README](docs/lattice.api.mcp.repocontext.replication/README.md) |

## Explorer (in progress)

**Status: in progress.** The Explorer is under active development. The packages build, are documented and are usable, but the surface area and navigation are still moving, so treat this group as work in flight rather than a stable contract.

The operator console. `Explorer.Web` is the ASP.NET Core head; everything else is a shared library or a plugin that a head opts into one at a time.

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Explorer.Web` | Opt-in, auth-aware web console for a running cluster - a tree browser plus capability-gated Backups and Access admin areas over the gRPC APIs (and a Schema area hidden by default) - embeddable via `AddLatticeExplorerWeb` / `MapLatticeExplorer` or run standalone. Composes the shared explorer libraries (`Explorer.Core`, `.UI`, `.Backup`, `.Access`, `.Schema`) into an ASP.NET Core head. | [README](docs/lattice.explorer/README.md) |
| `Orleans.Lattice.Explorer.Core` | Head-agnostic core of the Explorer: the read-only state-API connection seam, configuration store, session, capability model, and the shared catalog, metrics, topology, data, dead-letter, and history navigation services, depending only on the public read-only state-API gRPC client. | [README](docs/lattice.explorer/running-the-explorer.md) |
| `Orleans.Lattice.Explorer.UI` | Shared Razor component class library for the Explorer: the routable pages, layout, navigation, detail, backup, access, and authentication components (plus packaged static web assets) rendered identically by every explorer head. | [README](docs/lattice.explorer/running-the-explorer.md) |
| `Orleans.Lattice.Explorer.DesignSystem` | The Explorer design system: the design-token layer, the single named breakpoint set (compact / medium / expanded), and the adaptive shell primitives every plugin is styled against. It has no project dependencies, so a plugin can consume it without taking on the Explorer core or any feature package. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.Abstractions` | The Explorer plugin contract: the descriptor, the surface and selection-kind vocabulary, the access-gate seam, and the host context. A plugin declares its identity, its view, the single domain contract that is the whole of its reach, and its gate; the shell needs no per-plugin knowledge. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.Selection` | The shared kernel for per-selection plugins: the view base, the plugin key vocabulary, and the nested-surface registry through which one plugin renders another inline without referencing it. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.Data` | The Data browser plugin: paged key/value browsing for a selected tree or view, with entry detail and tag editing. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.History` | The change-history plugin: the per-key revision timeline, rendered inline from a selected data row rather than as a tab of its own. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.Metrics` | The live-metrics plugin: per-selection counters and rates for a tree or view. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.Topology` | The topology plugin: the shard and node layout for a selected tree. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.TagIndex` | The tag-index plugin: browsing a tag index and its members. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.DeadLetter` | The dead-letter plugin: the rejected-envelope queue for a selected tree, with inspection and remediation. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.Telemetry` | The telemetry plugin: time-series panels over the telemetry facade, mounted both as an operator-facing area and as the metrics section of My Tenant. It renders the tenant scope the server pinned, and says so when a request was narrowed. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.Tenancy` | The shared tenancy seam: the tenant-admin control-API client and domain model that the Tenants and My Tenant plugins both operate against. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.Tenants` | The Tenants plugin, for a platform operator: tenant lifecycle, quota, region authorization, and the initial tenant-admin grant. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Plugins.MyTenant` | The My Tenant plugin, for a tenant administrator: membership and cross-tenant grants, region residency, and usage against quota. Quota is read-only here - setting it is a platform-operator capability. | [Docs](docs/lattice.explorer/writing-a-plugin.md) |
| `Orleans.Lattice.Explorer.Access` | The Access (membership and access-control) management area for the Explorer: bridges the auth-admin control-API gRPC client into the explorer's navigation and capability model, gated behind a capability probe. Companion to `Explorer.Core`. | [README](docs/lattice.explorer/managing-access.md) |
| `Orleans.Lattice.Explorer.Backup` | The Backups management area for the Explorer: bridges the backup control-API gRPC client into the explorer's navigation and capability model, gating the area and its per-scope actions behind a capability probe. Companion to `Explorer.Core`. | [README](docs/lattice.explorer/managing-backups.md) |
| `Orleans.Lattice.Explorer.Schema` | The Schema (enforcement, versioning, remediation, and compliance) management area for the Explorer: bridges the schema control-API gRPC client into the explorer's navigation and capability model, gated behind a capability probe. Companion to `Explorer.Core`. | [README](docs/lattice.explorer/managing-schema.md) |
| `Orleans.Lattice.Explorer.Entra` | Optional Microsoft Entra ID (Azure AD) interactive login provider for the Explorer: an OIDC auth-code + PKCE (or device-code) sign-in that acquires and silently refreshes a bearer token for an auth-enabled State API, keeping the MSAL dependency out of the core explorer. | [README](docs/lattice.explorer.entra/README.md) |
| `Orleans.Lattice.Explorer.Entra.Web` | Hosted-web Microsoft Entra ID (OpenID Connect) sign-in for the Blazor Server Explorer: wires the ASP.NET auth-code + PKCE cookie flow through Microsoft.Identity.Web and exchanges the browser session for a State API bearer token, without any public API change to the released Explorer. | [README](docs/lattice.explorer.entra.web/README.md) |

## Identity and Security

Who the caller is, and what they are allowed to do.

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Auth` | Authorization and enforcement: durable policy store, decision engine, and the fail-closed access gate the data path consults. | [README](docs/lattice.auth/README.md) |
| `Orleans.Lattice.Membership` | Identity directory and credential-to-subject resolution: groups, transitive membership edges, and pluggable authenticators. | [README](docs/lattice.membership/README.md) |
| `Orleans.Lattice.Membership.Oidc` | Generic, discovery-document-driven OpenID Connect credential authenticator for the membership layer (Okta, Auth0, Keycloak, Ping, Google). | [README](docs/lattice.membership.oidc/README.md) |
| `Orleans.Lattice.Membership.Entra` | Microsoft Entra ID (Azure AD) credential authenticator for the membership layer. | [README](docs/lattice.membership.entra/README.md) |
| `Orleans.Lattice.Membership.Entra.Graph` | Microsoft Graph-backed group-overflow resolver for the Entra authenticator (for subjects whose group claims exceed the token) and the Graph-backed identity directory that the Explorer Access area searches and validates against. | [README](docs/lattice.membership.entra.graph/README.md) |

## Governance

Policy over the shape of stored data, and over the boundaries between tenants.

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Schema` | Opt-in schema enforcement and versioning companion over the opaque-`byte[]` core: per-tree write validation with dead-letter diversion of non-compliant replicated or restored items, and self-describing value versioning with read-time upcasting. | [README](docs/lattice.schema/README.md) |
| `Orleans.Lattice.Tenancy` | Opt-in multi-tenancy across a single-cluster or multi-cluster deployment: keyspace-partitioned tenants under a `t/{tenant}/` prefix, a tenant registry with a create / suspend / resume / delete lifecycle, per-tenant quotas, usage metering (folded across clusters), and rate limiting under a cluster-converged or per-cluster enforcement scope, and optional per-tenant region residency - layered on the core through null seams so a host without it is byte-for-byte unchanged. | [README](docs/lattice.tenancy/README.md) |

## Replication

Cross-cluster active-active replication and its transport.

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Replication` | Cross-cluster active-active replication: producer, WAL, shipper, apply, bootstrap, and anti-entropy. | [README](docs/lattice.replication/README.md) |
| `Orleans.Lattice.Replication.Grpc` | The canonical gRPC push-transport binding for replication. | [README](docs/lattice.replication.grpc/README.md) |

## Storage

Durability backends behind the storage seams. The core ships an in-memory write-ahead log; these replace it for production.

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Storage.AzureTable` | The durable Azure Table Storage write-ahead-log backend. | [README](docs/lattice.storage.azuretable/README.md) |
| `Orleans.Lattice.Storage.File` | A durable local-disk write-ahead-log backend: an append-and-fsync log per shard with crash-safe reconciliation and background compaction that rewrites the log to reclaim trimmed space, using the same per-entry record payload encoding as the Azure Table backend. Intended for single-node and containerized deployments. **Not yet published to NuGet** - build from source today. | [README](docs/lattice.storage.file/README.md) |
| `Orleans.Lattice.Backup.AzureBlob` | The durable Azure Blob Storage sink backend for backup artifacts and manifests. | [README](docs/lattice.backup.azureblob/README.md) |
| `Orleans.Lattice.Caching.AzureBlob` | A durable Azure Blob Storage `IDistributedCache` for the family, backing the hosted-web Explorer's distributed token cache on a multi-replica host. | [README](docs/lattice.caching.azureblob/README.md) |

## Operations

Backup, autoscaling, and dashboards.

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Backup` | Causally consistent backup and restore: full and incremental capture, scheduling and chain retention, an optional cross-tree causal fence, and a fail-closed permission model over a pluggable sink. | [README](docs/lattice.backup/README.md) |
| `Orleans.Lattice.Scaling` | Cluster-aggregate autoscaling signal: a compute-axis replica-demand scalar for KEDA plus an advisory, signal-only storage-axis WAL rebalance recommendation, served over an HTTP endpoint and an ASP.NET Core health check. | [README](docs/lattice.scaling/README.md) |
| `Orleans.Lattice.Dashboards` | Bundled Grafana dashboards and provisioning templates for the `orleans.lattice` and `orleans.lattice.replication` meters. | [README](docs/lattice.dashboards/README.md) |

## Related

- [README](README.md) - what the platform is, the deployment journey, and the architecture seams.
- [FEATURES.md](FEATURES.md) - the full capability catalogue.
- [reference-architecture.md](reference-architecture.md) - the active-active, cross-region deployment blueprint and its deployment kit.
- [docs/RELEASING.md](docs/RELEASING.md) - the per-package tag-and-publish protocol.