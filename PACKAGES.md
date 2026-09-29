# Orleans.Lattice Packages

Orleans.Lattice ships as a small core plus a set of companion packages. Each
companion fills one seam in the core - storage, identity, governance,
replication, administration, observability - and a host takes only the ones it
needs. A deployment that registers none of them runs the core library alone.

Every add-on has its own documentation set, anchored by a package README in the
standard project layout (what it is, core properties, features, quick start,
then API / configuration / architecture references). Most ship as their own NuGet package.
The `NuGet` column carries a version badge for the currently published version,
linked to the package on nuget.org; a package that has not shipped a release yet
reads `Unreleased` there and is built from source.

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

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice)](https://www.nuget.org/packages/Orleans.Lattice) | The core platform: the sharded, CRDT-backed B+ tree, the write-ahead log that is its durability boundary, the grain catalogue, and the seams every companion package plugs into. Everything else on this page is optional. | [Docs](docs/lattice/architecture.md) |
| `Orleans.Lattice.GrainIndex` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.GrainIndex)](https://www.nuget.org/packages/Orleans.Lattice.GrainIndex) | Typed grain indexing: track an Orleans grain's typed state in a lattice tree and query it with the server-side predicate surface, without hand-maintaining a secondary index. Properties are declared explicitly with `Include`, grains enrol via an `[Indexed]` state facet, a reminder-driven backfill onboards dormant grains, a durable outbox retries failed index writes, and startup rejects a declaration change that would invalidate stored entries. | [README](docs/lattice.grainindex/README.md) |
| `Orleans.Lattice.Vector` | Unreleased | Allocation-lean approximate nearest-neighbour vector index over Lattice-held vectors: an inverted-file core whose query cost is sub-linear in the corpus, persisted on a Lattice tree in bounded chunks with lazy partial load and incremental insert, delete and re-embed maintenance, so a restart reloads the index instead of rebuilding it; an interrupted build resumes from its last durable checkpoint, and a corrupt or version-incompatible index fails its verified load and is rebuilt rather than served. Publishes a measured recall target and reports per query whether an approximate or an exact path answered. **Not yet published to NuGet** - build from source today. | [README](docs/lattice.vector/README.md) |

## APIs

The transport-agnostic facade family and its gRPC bindings. A facade is the in-process contract; the matching `.Grpc` package is its wire binding and typed client, for a head that runs outside the cluster.

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice.Api.Abstractions` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Abstractions)](https://www.nuget.org/packages/Orleans.Lattice.Api.Abstractions) | The shared, transport-agnostic API contract: the facade service interfaces - state, data, auth, backup, schema, replication, telemetry, tree administration, tenant administration, and installable-app control - the region-discovery contract, and their request/response DTOs, referenced by the facade implementations, the gRPC bindings, and the MCP server without cross-package internal-visibility grants. | [README](docs/lattice.api.abstractions/README.md) |
| `Orleans.Lattice.Api.State` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.State)](https://www.nuget.org/packages/Orleans.Lattice.Api.State) | Read-only cluster state-API facade: query, observe, and subscribe to trees, structure, entries, change feeds, and metrics. | [README](docs/lattice.api.state/README.md) |
| `Orleans.Lattice.Api.State.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.State.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Api.State.Grpc) | The code-first gRPC binding and public client for the read-only state API. | [README](docs/lattice.api.state.grpc/README.md) |
| `Orleans.Lattice.Api.Data` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Data)](https://www.nuget.org/packages/Orleans.Lattice.Api.Data) | Write-capable external data-plane facade for non-.NET clients: point set/delete, non-atomic bulk upsert, point and bounded-range reads, bounded range deletes, single- and cross-tree atomic batches, and the typed CRDT write and read verbs (counters, sets, flags, registers, maps, sequences, and version vectors), each authorized through the core gate. | [README](docs/lattice.api.data/README.md) |
| `Orleans.Lattice.Api.Data.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Data.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Api.Data.Grpc) | The code-first gRPC binding and public client for the read-write data-plane API. | [README](docs/lattice.api.data.grpc/README.md) |
| `Orleans.Lattice.Api.Auth` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Auth)](https://www.nuget.org/packages/Orleans.Lattice.Api.Auth) | Transport-agnostic control facade for administering membership and policy and explaining authorization decisions. | [README](docs/lattice.api.auth/README.md) |
| `Orleans.Lattice.Api.Auth.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Auth.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Api.Auth.Grpc) | The code-first gRPC binding and public client for the authorization control facade. | [README](docs/lattice.api.auth.grpc/README.md) |
| `Orleans.Lattice.Api.Backup` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Backup)](https://www.nuget.org/packages/Orleans.Lattice.Api.Backup) | Transport-agnostic control facade for driving backup capture (full, incremental, and backup sets), recurring schedules and per-scope status, restore and revert, cold restore, catalog rebuild from the sink and catalog scrub against it, catalog listing and inventory, chain describe, artifact export, deletion, and backup health monitoring. | [README](docs/lattice.api.backup/README.md) |
| `Orleans.Lattice.Api.Backup.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Backup.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Api.Backup.Grpc) | The code-first gRPC binding and public client for the backup control facade. | [README](docs/lattice.api.backup.grpc/README.md) |
| `Orleans.Lattice.Api.Schema` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Schema)](https://www.nuget.org/packages/Orleans.Lattice.Api.Schema) | Transport-agnostic control facade for managing schema policy, dead letters, versioning, remediation, and compliance audits. | [README](docs/lattice.api.schema/README.md) |
| `Orleans.Lattice.Api.Schema.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Schema.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Api.Schema.Grpc) | The code-first gRPC binding and public client for the schema control facade. | [README](docs/lattice.api.schema.grpc/README.md) |
| `Orleans.Lattice.Api.Replication` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Replication)](https://www.nuget.org/packages/Orleans.Lattice.Api.Replication) | Transport-agnostic control facade for runtime per-tree replication configuration: an authorized operator can enable replication for a tree (fixing its wire merge mode), disable it, and inspect the replicated-tree set, authorized fail-closed through the shared access gate. | [README](docs/lattice.api.replication/README.md) |
| `Orleans.Lattice.Api.Replication.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Replication.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Api.Replication.Grpc) | The code-first gRPC binding and public client for the runtime replication control facade. | [README](docs/lattice.api.replication.grpc/README.md) |
| `Orleans.Lattice.Api.TreeAdmin` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.TreeAdmin)](https://www.nuget.org/packages/Orleans.Lattice.Api.TreeAdmin) | Transport-agnostic control facade for whole-tree administration, composing the existing single-responsibility facades (it wraps the schema control facade by delegation). Exposes a fail-closed per-operation capability probe plus the whole-tree lifecycle surface: create, inspect, and reconfigure trees; alias resolution and assignment; delete, recover, and purge; bulk load; restore and revert; reshard, resize, snapshot; WAL placement audit and movement; materialised-view and tag-index management; shard compaction; history retention; and orphaned-leaf audit and repair. | [README](docs/lattice.api.treeadmin/README.md) |
| `Orleans.Lattice.Api.TreeAdmin.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.TreeAdmin.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Api.TreeAdmin.Grpc) | The code-first gRPC binding and public client for the tree-administration control facade. | [README](docs/lattice.api.treeadmin.grpc/README.md) |
| `Orleans.Lattice.Api.TenantAdmin` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.TenantAdmin)](https://www.nuget.org/packages/Orleans.Lattice.Api.TenantAdmin) | Transport-agnostic operator control facade for tenant administration: create, suspend, resume, and delete tenants (delete cascading the tenant's trees), author per-tenant quotas and read usage against them, administer admin subjects, cross-tenant grants, and per-tenant region residency, plus a fail-closed self-service read surface and an optional tenant-scoped tree-administration surface - all authorized through the shared access gate. | [README](docs/lattice.api.tenantadmin/README.md) |
| `Orleans.Lattice.Api.TenantAdmin.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.TenantAdmin.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Api.TenantAdmin.Grpc) | The code-first gRPC binding and public client for the tenant-administration control facade. | [README](docs/lattice.api.tenantadmin.grpc/README.md) |
| `Orleans.Lattice.Api.Apps` | Unreleased | Transport-agnostic control facade for installable apps: install, enable, disable, uninstall, list and describe apps and manage each install's version-pinned consent, one generic contract dispatching by app slug. Every verb requires the scopeless `AppInstall` capability through the shared access gate, and responses and errors never expose composed physical tree ids. | [README](docs/lattice.api.apps/README.md) |
| `Orleans.Lattice.Api.Apps.Grpc` | Unreleased | The code-first gRPC binding and public client for the app control facade, default-deny with server-classified operations. | [README](docs/lattice.api.apps.grpc/README.md) |
| `Orleans.Lattice.Api.Telemetry` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Telemetry)](https://www.nuget.org/packages/Orleans.Lattice.Api.Telemetry) | Backend-neutral telemetry facade: answers a curated set of named queries over a Prometheus-compatible backend, derives each answer's tenant scope on the server, and, when a metric allow-list is configured, enforces it fail-closed on the metric names a query will actually evaluate. Callers name a query id and never supply PromQL. | [Docs](docs/lattice.api.telemetry/README.md) |
| `Orleans.Lattice.Api.Telemetry.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Telemetry.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Api.Telemetry.Grpc) | gRPC binding for the telemetry facade, for a remote head that cannot enforce tenant scoping locally. References only the shared contract package - a closure asserted over the transitive project graph, package ids, and emitted assembly references - and derives no tenant of its own. | [Docs](docs/lattice.api.telemetry.grpc/README.md) |

## MCP

Model Context Protocol bindings that expose the API facades to AI agents, fail-closed and scoped to the caller's grants.

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice.Api.Mcp` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Mcp)](https://www.nuget.org/packages/Orleans.Lattice.Api.Mcp) | Model Context Protocol (MCP) server binding: exposes the transport-agnostic API facades as opt-in, permission-aware MCP tools over an authenticated, fail-closed, default-deny credential bridge, registered with `AddLatticeMcp(...)` and mapped with `MapLatticeMcp()`. | [README](docs/lattice.api.mcp/README.md) |
| `Orleans.Lattice.Api.Mcp.Apps` | Unreleased | Opt-in MCP surface for installable apps: every enabled app's tools on the single MCP endpoint, namespaced `{slug}_{tool}`, paired exactly with the app's manifest and gated per caller by the grants its roles compile to, without changing the facade groups or `lattice_capabilities`. | [README](docs/lattice.api.mcp.apps/README.md) |
| `Orleans.Lattice.Api.Mcp.Telemetry` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Mcp.Telemetry)](https://www.nuget.org/packages/Orleans.Lattice.Api.Mcp.Telemetry) | Opt-in telemetry add-on for the MCP server: exposes cluster OpenTelemetry metrics as MCP tools by proxying a read-only Prometheus/PromQL backend, with a dual-credential trust boundary that stamps the backend credential and never forwards the caller's Lattice credential. | [README](docs/lattice.api.mcp.telemetry/README.md) |
| `Orleans.Lattice.Api.Mcp.Telemetry.Azure` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Api.Mcp.Telemetry.Azure)](https://www.nuget.org/packages/Orleans.Lattice.Api.Mcp.Telemetry.Azure) | Azure managed-identity backend-token provider for the telemetry facade's Prometheus proxy: supplies a rotating Entra (Azure AD) access token so any telemetry binding - the MCP telemetry tools, or a client head hosting the facade - can query an Azure Monitor managed-Prometheus endpoint, keeping the Azure identity dependency out of the core telemetry package and taking no dependency on the MCP server. | [README](docs/lattice.api.mcp.telemetry.azure/README.md) |

## AI / RepoContext

RepoContext, an AI codebase-memory system built entirely on the platform. It is an example application, not part of the platform definition; see the [README](README.md#repocontext-an-example-built-on-the-platform).

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice.Api.Mcp.RepoContext` | Unreleased | Opt-in MCP tools that give an AI agent durable, conflict-free context and memory about a codebase - repository bootstrap, structural and symbol recall, free-form memories with optional TTL, and semantic search (approximate nearest-neighbour by default, or an exact scan when configured) - stored in the CRDT B+ tree and served fail-closed, with a container host for local use. **Not yet published to NuGet** - distributed as a ready-to-run Docker container and consumed from source today; see the [container sample](samples/RepoContextContainer/README.md). | [README](docs/lattice.api.mcp.repocontext/README.md) |
| `Orleans.Lattice.Api.Mcp.RepoContext.Replication` | Unreleased | Opt-in multi-cluster add-on for the repository-context store: `EnableRepoContextMultiCluster(...)` turns on cross-cluster replication for every repository-context tree with the correct per-tree merge mode - the vector-membership presence tree pinned to the add-wins `OrFlag` CRDT so active-active convergence can never silently drop an embedding, the agent-memory tree pinned to `MvRegister` so concurrent cross-cluster memory writes both survive and fold, other trees defaulting to last-writer-wins. A `LATTICE_REPOCONTEXT_INDEXING_ROLE` hub/spoke switch confines indexing to the cluster started as the hub - start exactly one, because an unset or unrecognised value resolves to `hub` and nothing detects a second - and a startup guard rejects a per-tree merge mode that would imply active-active indexing or drop concurrent memory writes. Takes the `Orleans.Lattice.Replication` dependency so the repo-context core need not. **Not yet published to NuGet** - consumed from source alongside the repository-context package today. | [README](docs/lattice.api.mcp.repocontext.replication/README.md) |

## Explorer (in progress)

**Status: in progress.** The Explorer is under active development. The packages build, are documented and are usable, but the surface area and navigation are still moving, so treat this group as work in flight rather than a stable contract.

The operator console. `Explorer.Web` is the ASP.NET Core head; `Explorer.UI` is the whole console with every area compiled in, `Explorer.Core` its service layer, and `Explorer.AppKit` the kit a Lattice App UI runs on inside its frame. The retired Explorer package ids are listed in [docs/RELEASING.md](docs/RELEASING.md#retired-packages).

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice.Explorer.Web` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Explorer.Web)](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Web) | Opt-in, auth-aware web console for a running cluster, embeddable via `AddLatticeExplorerWeb` / `MapLatticeExplorer` or run standalone. Composes `Explorer.Core` and `Explorer.UI` into a Blazor Server head behind static security headers, and maps the sandboxed Lattice App frame's bootstrap route. | [README](docs/lattice.explorer/README.md) |
| `Orleans.Lattice.Explorer.Core` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Explorer.Core)](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Core) | Head-agnostic core of the Explorer: the read-only state-API connection seam, configuration store, session and preferences, sign-in and credential storage, tenant view, and the catalog, metrics, data, dead-letter and history services, depending only on the public read-only state-API gRPC client. | [README](docs/lattice.explorer/running-the-explorer.md) |
| `Orleans.Lattice.Explorer.UI` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Explorer.UI)](https://www.nuget.org/packages/Orleans.Lattice.Explorer.UI) | The Explorer UI, a Razor class library: the navigation and session chrome, the compiled-in Data, Apps, Access, Schema, Tenancy, Replication, Backups, Telemetry and Cluster areas, the order-diagram design system, the credential-aware transport adapters, and the Lattice App frame host and bridge broker. It has no extension point. | [README](docs/lattice.explorer/running-the-explorer.md) |
| `Orleans.Lattice.Explorer.AppKit` | Unreleased | The static assets that run inside a Lattice App's sandboxed frame: the app-agnostic bootstrap document and loader, the in-frame `lattice` API, the kit stylesheet and fonts, the frame protocol schema, and the `AppKitProtocol` constants. | [README](docs/lattice.explorer/README.md) |
| `Orleans.Lattice.Explorer.Entra` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Explorer.Entra)](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Entra) | Optional Microsoft Entra ID (Azure AD) interactive login provider for the Explorer: an OIDC auth-code + PKCE (or device-code) sign-in that acquires and silently refreshes a bearer token for an auth-enabled State API, keeping the MSAL dependency out of the core explorer. | [README](docs/lattice.explorer.entra/README.md) |
| `Orleans.Lattice.Explorer.Entra.Web` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Explorer.Entra.Web)](https://www.nuget.org/packages/Orleans.Lattice.Explorer.Entra.Web) | Hosted-web Microsoft Entra ID (OpenID Connect) sign-in for the Blazor Server Explorer: wires the ASP.NET auth-code + PKCE cookie flow through Microsoft.Identity.Web and exchanges the browser session for a State API bearer token, without any public API change to the released Explorer. | [README](docs/lattice.explorer.entra.web/README.md) |

## Identity and Security

Who the caller is, and what they are allowed to do.

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice.Auth` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Auth)](https://www.nuget.org/packages/Orleans.Lattice.Auth) | Authorization and enforcement: durable policy store, decision engine, and the fail-closed access gate the data path consults. | [README](docs/lattice.auth/README.md) |
| `Orleans.Lattice.Membership` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Membership)](https://www.nuget.org/packages/Orleans.Lattice.Membership) | Identity directory and credential-to-subject resolution: groups, transitive membership edges, and pluggable authenticators. | [README](docs/lattice.membership/README.md) |
| `Orleans.Lattice.Membership.Oidc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Membership.Oidc)](https://www.nuget.org/packages/Orleans.Lattice.Membership.Oidc) | Generic, discovery-document-driven OpenID Connect credential authenticator for the membership layer (Okta, Auth0, Keycloak, Ping, Google). | [README](docs/lattice.membership.oidc/README.md) |
| `Orleans.Lattice.Membership.Entra` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Membership.Entra)](https://www.nuget.org/packages/Orleans.Lattice.Membership.Entra) | Microsoft Entra ID (Azure AD) credential authenticator for the membership layer. | [README](docs/lattice.membership.entra/README.md) |
| `Orleans.Lattice.Membership.Entra.Graph` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Membership.Entra.Graph)](https://www.nuget.org/packages/Orleans.Lattice.Membership.Entra.Graph) | Microsoft Graph-backed group-overflow resolver for the Entra authenticator (for subjects whose group claims exceed the token) and the Graph-backed identity directory that the Explorer Access area searches and validates against. | [README](docs/lattice.membership.entra.graph/README.md) |

## Governance

Policy over the shape of stored data, over the boundaries between tenants, and over what an installed app may do.

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice.Schema` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Schema)](https://www.nuget.org/packages/Orleans.Lattice.Schema) | Opt-in schema enforcement and versioning companion over the opaque-`byte[]` core: per-tree write validation that rejects a non-compliant local write and, under opt-in strict ingest, dead-letters a non-compliant replicated or restored item, and self-describing value versioning with read-time upcasting. | [README](docs/lattice.schema/README.md) |
| `Orleans.Lattice.Tenancy` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Tenancy)](https://www.nuget.org/packages/Orleans.Lattice.Tenancy) | Opt-in multi-tenancy across a single-cluster or multi-cluster deployment: keyspace-partitioned tenants under a `t/{tenant}/` prefix, a tenant registry with a create / suspend / resume / delete lifecycle, per-tenant quotas admitted against a cluster-converged or per-cluster usage aggregate, usage metering (folded across clusters), rate limiting enforced per cluster, and optional per-tenant region residency - layered on the core through null seams so a host without it is byte-for-byte unchanged. | [README](docs/lattice.tenancy/README.md) |
| `Orleans.Lattice.Apps` | Unreleased | Opt-in installable apps: an embedded manifest declares an app's trees, roles, replication intent, change-feed subscriptions and MCP tools; install records group role bindings and a version-pinned capability ceiling, and activation compiles the roles into ordinary authorization rules and provisions `a/{app}/{tree}` trees, per tenant when tenancy is on. | [README](docs/lattice.apps/README.md) |

## Replication

Cross-cluster active-active replication and its transport.

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice.Replication` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Replication)](https://www.nuget.org/packages/Orleans.Lattice.Replication) | Cross-cluster active-active replication: producer, WAL, shipper, apply, bootstrap, and anti-entropy. | [README](docs/lattice.replication/README.md) |
| `Orleans.Lattice.Replication.Grpc` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Replication.Grpc)](https://www.nuget.org/packages/Orleans.Lattice.Replication.Grpc) | The canonical gRPC transport binding for replication: the live push transport and the bootstrap snapshot transport, with shared-secret authentication. | [README](docs/lattice.replication.grpc/README.md) |

## Storage

Durability backends behind the storage seams. The core ships an in-memory write-ahead log, which the two write-ahead-log backends replace for production; the other two back the backup sink and a distributed cache.

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice.Storage.AzureTable` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Storage.AzureTable)](https://www.nuget.org/packages/Orleans.Lattice.Storage.AzureTable) | The durable Azure Table Storage write-ahead-log backend. | [README](docs/lattice.storage.azuretable/README.md) |
| `Orleans.Lattice.Storage.File` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Storage.File)](https://www.nuget.org/packages/Orleans.Lattice.Storage.File) | A durable local-disk write-ahead-log backend: an append-and-fsync log per shard with crash-safe reconciliation and background compaction that rewrites the log to reclaim trimmed space, using the same per-entry record payload encoding as the Azure Table backend. Intended for single-node and containerized deployments. | [README](docs/lattice.storage.file/README.md) |
| `Orleans.Lattice.Backup.AzureBlob` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Backup.AzureBlob)](https://www.nuget.org/packages/Orleans.Lattice.Backup.AzureBlob) | The durable Azure Blob Storage sink backend for backup artifacts and manifests. | [README](docs/lattice.backup.azureblob/README.md) |
| `Orleans.Lattice.Caching.AzureBlob` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Caching.AzureBlob)](https://www.nuget.org/packages/Orleans.Lattice.Caching.AzureBlob) | A durable Azure Blob Storage `IDistributedCache` for the family, backing the hosted-web Explorer's distributed token cache on a multi-replica host. | [README](docs/lattice.caching.azureblob/README.md) |

## Operations

Backup, autoscaling, and dashboards.

| Package | NuGet | Description | Docs |
|---|---|---|---|
| `Orleans.Lattice.Backup` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Backup)](https://www.nuget.org/packages/Orleans.Lattice.Backup) | Causally consistent backup and restore: full and incremental capture, scheduling and chain retention, an optional cross-tree causal fence, and a fail-closed permission model over a pluggable sink. | [README](docs/lattice.backup/README.md) |
| `Orleans.Lattice.Scaling` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Scaling)](https://www.nuget.org/packages/Orleans.Lattice.Scaling) | Cluster-aggregate autoscaling signal: a compute-axis replica-demand scalar for KEDA plus an advisory, signal-only storage-axis WAL rebalance recommendation, served over an HTTP endpoint and an ASP.NET Core health check. | [README](docs/lattice.scaling/README.md) |
| `Orleans.Lattice.Dashboards` | [![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice.Dashboards)](https://www.nuget.org/packages/Orleans.Lattice.Dashboards) | Bundled Grafana dashboards and provisioning templates for the `orleans.lattice`, `orleans.lattice.replication`, `orleans.lattice.replication.grpc`, `orleans.lattice.auth`, `orleans.lattice.membership`, `orleans.lattice.backup`, `orleans.lattice.scaling`, and `orleans.lattice.tenancy` meters. | [README](docs/lattice.dashboards/README.md) |

## Related

- [README](README.md) - what the platform is, the deployment journey, and the architecture seams.
- [FEATURES.md](FEATURES.md) - the full capability catalogue.
- [reference-architecture.md](reference-architecture.md) - the active-active, cross-region deployment blueprint and its deployment kit.
- [docs/RELEASING.md](docs/RELEASING.md) - the per-package tag-and-publish protocol.