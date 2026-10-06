# Orleans.Lattice.Api.Replication API reference

The package exposes the registration entry points below (extension methods on the public static `LatticeApiReplicationServiceCollectionExtensions` class) and the public options types `LatticeApiReplicationOptions` and `LatticeReplicationStatusOptions`. The control contract `ILatticeReplicationControl`, the read-only peer-status contract `ILatticeReplicationStatus`, and the model records they return are defined in the shared [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) package.

## Registration

| Member | Signature | Purpose |
|---|---|---|
| `AddLatticeReplicationApi` | `ISiloBuilder AddLatticeReplicationApi(this ISiloBuilder builder, Action<LatticeApiReplicationOptions>? configure = null)` | Registers the replication control facade on the silo. Must be called after `AddLatticeReplication(..., enableRuntimeConfig: true)`; calling it first throws at registration with an actionable message. |
| `AddLatticeReplicationStatusApi` | `ISiloBuilder AddLatticeReplicationStatusApi(this ISiloBuilder builder, Action<LatticeReplicationStatusOptions>? configure = null)` | Registers the read-only peer-status facade (`ILatticeReplicationStatus`) on the silo and validates `LatticeReplicationStatusOptions`. Must be called after `AddLatticeReplication(...)`, which records the per-link telemetry it reads; calling it first throws `InvalidOperationException` at registration. Independent of `AddLatticeReplicationApi`, so it needs no runtime config authority. Repeated calls are idempotent for the structural wiring and still layer any supplied options delegate. |

## Facade

`ILatticeReplicationControl` (defined in `Orleans.Lattice.Api.Abstractions`) is the single control surface every transport binding adapts over.
Every `treeId` these operations accept is a **tenant-local name**: the facade resolves it to its effective, tenant-scoped id through `ITenantContextResolver.ResolveEffectiveTreeIdAsync` at the entry point and uses that one id for **both** the authorization check and the operation, so a verb can never authorize one tree and act on another. With the tenancy add-on absent - or registered, but with no active tenant asserted, which resolves the default tenant - the bare name is returned unchanged, so behaviour is byte-for-byte as before. Under an asserted active tenant an unqualified name is scoped into that tenant's `t/{tenant}/{name}` namespace, and an already-qualified `t/` id or a `_lattice_` system-tree name passes through unchanged (a well-formed foreign `t/{other}/{name}` is left to the tenancy access gate to adjudicate). The call fails closed with a `LatticeTenantAccessDeniedException` when the asserted tenant fails validation against the caller's own membership, or when it names a `sys-` tree or a malformed `t/` id that belongs to no tenant. See [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md). Results carry the effective id rather than echoing the caller's name: under an asserted non-default tenant the `TreeId` of an enable or disable result is the tenant-scoped `t/{tenant}/{name}` id, and the config report lists every tree the caller is authorized to manage by its effective id. The peer status report (`ILatticeReplicationStatus.GetPeerStatusAsync`) names each link's tree by the same effective id, so a `ReplicationPeerStatusEntry.TreeId` joins to the `ReplicationTreeConfigEntry.TreeId` of the same tree; its tree filter is a tenant-local name, scoped the same way.

| Operation | Signature | Notes |
|---|---|---|
| Enable replication | `Task<ReplicationEnableResult> EnableReplicationAsync(string treeId, LatticeMergeMode mode, string? bootstrapSourceClusterId = null, CancellationToken cancellationToken = default)` | Authorizes the tree fail-closed, then enables it under the fixed `mode`. Rejects an in-place mode change on an already-enabled tree. When `bootstrapSourceClusterId` is supplied and the tree already holds data, requests a snapshot bootstrap; an enable that finds the tree already enabled under the same mode returns `AlreadyEnabled = true` and requests none. |
| Disable replication | `Task<ReplicationDisableResult> DisableReplicationAsync(string treeId, CancellationToken cancellationToken = default)` | Authorizes the tree fail-closed, then disables its runtime enrollment without purging peer data. Idempotent. |
| Get replication config | `Task<ReplicationConfigReport> GetReplicationConfigAsync(CancellationToken cancellationToken = default)` | Returns a permission-scoped report; trees the caller may not manage are omitted rather than throwing. |
| Decommission peer | `Task<ReplicationDecommissionPeerResult> DecommissionPeerAsync(string peerClusterId, CancellationToken cancellationToken = default)` | Authorizes against a cluster-wide replication-administration capability (there is no single tree to scope the check to), then removes `peerClusterId`'s durable enrollment from every registered tree and releases any origin-side cross-tree decision hold waiting on its acknowledgement ([#4684](https://github.com/NSTA1/Orleans.Lattice/issues/4684), [#4723](https://github.com/NSTA1/Orleans.Lattice/issues/4723)). Refuses with `LatticeReplicationPeerStillConfiguredException` while the peer is still present in `ReplicationPeers` - a configured peer's acknowledgement is still awaited via live topology regardless of enrollment, so decommissioning it first would silently fail to release anything. A re-added peer re-enrolls from a fresh bootstrap; decommission is not the same operation as a detach, which keeps the hold (see [Architecture](architecture.md)). Idempotent for an already-decommissioned peer. |

## Peer status facade

`ILatticeReplicationStatus` (defined in `Orleans.Lattice.Api.Abstractions`) is a separate, read-only contract, registered by `AddLatticeReplicationStatusApi`. `ILatticeReplicationControl` does not include it.

| Operation | Signature | Notes |
|---|---|---|
| Get peer status | `Task<ReplicationPeerStatusPage> GetPeerStatusAsync(ReplicationPeerStatusQuery query, CancellationToken cancellationToken = default)` | Reads one page of per-link status for the whole local cluster, ordered by tree id, then peer region id, then direction. Page through the report by passing each page's `ContinuationToken` back in the next query until it is `null`; a page can hold fewer rows than the page size and still carry a token. Reports only the trees the caller holds `LatticeOperation.Replication` over, and a tree filter the caller may not manage yields an empty page. |

## Model types

All model records live in `Orleans.Lattice.Api.Abstractions` (namespace `Orleans.Lattice.Api.Replication`) and are Orleans-serializable with stable aliases.

### `ReplicationEnableResult`

| Member | Type | Meaning |
|---|---|---|
| `TreeId` | `string` | The tree that was enabled. |
| `Mode` | `LatticeMergeMode` | The merge mode now fixed for the tree. |
| `AlreadyEnabled` | `bool` | `true` when the tree was already enabled under the same mode (idempotent enable). |
| `BootstrapRequested` | `bool` | `true` when a snapshot bootstrap was requested for a non-empty tree. The request goes through the operator re-seed seam (`ILatticeReplicationAdmin.RequestSnapshotAsync`), so it is still reported `true` when that seam's `OperatorReseedMinInterval` rate limit declines to start a bootstrap because one was honoured for the same tree and source cluster within the interval. |

### `ReplicationDisableResult`

| Member | Type | Meaning |
|---|---|---|
| `TreeId` | `string` | The tree that was disabled. |
| `AlreadyDisabled` | `bool` | `true` when the tree was already disabled or was never configured (idempotent disable). |

### `ReplicationConfigReport`

| Member | Type | Meaning |
|---|---|---|
| `Trees` | `IReadOnlyList<ReplicationTreeConfigEntry>` | The per-tree entries the caller is authorized to see. `ReplicationConfigReport.Empty` is the canonical empty report. |

### `ReplicationDecommissionPeerResult`

| Member | Type | Meaning |
|---|---|---|
| `PeerClusterId` | `string` | The decommissioned peer cluster id. |
| `TreeCount` | `int` | The number of registered trees whose durable enrollment the peer was removed from. |
| `AlreadyDecommissioned` | `bool` | `true` when the peer was already decommissioned and the call was an idempotent no-op. |

### `ReplicationTreeConfigEntry`

| Member | Type | Meaning |
|---|---|---|
| `TreeId` | `string` | The enrolled tree. |
| `Enabled` | `bool` | Whether the tree is enrolled: the runtime enablement flag when the runtime entry is in force, and always `true` when the static declaration is - the static map is a floor, so a runtime disable does not remove it. An ambiguous entry reports its runtime flag, because ambiguity wins over the static declaration. |
| `Mode` | `LatticeMergeMode?` | The merge mode in force (for a disabled tree known only to the runtime config, the mode its entry retains), or `null` when no mode has been assigned or the mode is ambiguous (see `Ambiguous`). |
| `Ambiguous` | `bool` | `true` when concurrent mode writes naming different modes have not yet been resolved; concurrent writes of the same mode are not ambiguous. Resolution then fails closed - no mode is picked and `Mode` is `null` - but shipping does not pause. Ambiguity wins over a static declaration. |
| `Source` | `ReplicationEnrollmentSource` | Which enrollment source put the entry in force. Defaults to `Runtime`. |

### `ReplicationEnrollmentSource`

| Value | Meaning |
|---|---|
| `Runtime` | Only the runtime config tree declares the tree, and its entry is in force. The default, so an entry received from a peer predating this member reads as the runtime enrollment the report has always described. |
| `Static` | The static deployment-time replicated-tree map puts the tree in force: either the runtime config tree has no entry for it, or its entry yields no enabled unambiguous mode and the resolver falls back to the static declaration. A runtime disable therefore does not change the mode such a tree resolves to; the deployment configuration does. |
| `RuntimeAndStatic` | Both sources declare the tree and the runtime entry is in force, so the reported mode is the runtime-fixed mode - or none, when that entry's mode is ambiguous. |

### `ReplicationPeerStatusQuery`

| Member | Type | Meaning |
|---|---|---|
| `TreeId` | `string?` | When set, only links of this tree are reported. A tenant-local name, scoped to the caller's tenant as the enrolment verbs scope theirs; an id the report itself returned selects the same tree. `null` or empty reports every tree the caller may see. |
| `PeerRegionId` | `string?` | When set, only links to or from this peer region (cluster id) are reported. `null` or empty reports every peer. |
| `PageSize` | `int` | The maximum number of rows in the page. `0` selects `DefaultPageSize` (100), a value above `MaxPageSize` (1000) is clamped to it, and a negative value is rejected. |
| `ContinuationToken` | `string?` | The previous page's opaque `ContinuationToken`, or `null` for the first page. It is only meaningful with the same filters it was issued under. |

`ReplicationPeerStatusQuery.All` is the query for the first page of every link the caller may see, at the default page size, and `ResolvePageSize()` returns the page size a query applies.

### `ReplicationPeerStatusPage`

| Member | Type | Meaning |
|---|---|---|
| `LocalRegionId` | `string` | The id (cluster id) of the region that produced the page, so a caller can place itself among the peers it reports. |
| `Peers` | `IReadOnlyList<ReplicationPeerStatusEntry>` | The link rows on this page, ordered by tree id, then peer region id, then direction. |
| `ContinuationToken` | `string?` | The token that resumes the report after the last row of this page, or `null` when there is nothing further to read. Pass it back unaltered. |

`ReplicationPeerStatusPage.Empty(localRegionId)` creates an empty, final page.

### `ReplicationPeerStatusEntry`

| Member | Type | Meaning |
|---|---|---|
| `TreeId` | `string` | The effective tree id: the bare name for a default-tenant tree, and the tenant-qualified `t/{tenant}/{name}` id for a tree of an asserted, non-default tenant. It is the id `ReplicationTreeConfigEntry.TreeId` carries for the same tree. |
| `PeerRegionId` | `string` | The id (cluster id) of the peer region at the other end of the link. |
| `Direction` | `ReplicationLinkDirection` | Which way the link carries entries, relative to the local region. |
| `EntriesBehind` | `long` | WAL entries the local region has yet to ship to the peer. Outbound links only; always `0` on an inbound link. |
| `BytesBehind` | `long` | Payload bytes the local region has yet to ship to the peer. Outbound links only; always `0` on an inbound link. |
| `ConsecutiveErrors` | `long` | Consecutive failed contact attempts since the last success: failed shipments on an outbound link, failed applies of the peer's entries on an inbound link. |
| `TimeSinceLastContact` | `TimeSpan?` | Time since the last successful contact in this direction, or `null` when there has never been one. The liveness probe refreshes an idle outbound link; an inbound link is refreshed only when the peer's entries are applied, so an idle peer's inbound link ages without being unhealthy. |
| `InFlight` | `long` | Batches shipped to the peer and not yet acknowledged. Outbound links only; always `0` on an inbound link. |
| `Health` | `ReplicationLinkHealth` | The health derived from the fields above against the `LatticeReplicationStatusOptions` thresholds (see [Configuration](configuration.md#latticereplicationstatusoptions)). |

### `ReplicationLinkDirection`

| Value | Meaning |
|---|---|
| `Outbound` | The local region ships the tree's entries to the peer. |
| `Inbound` | The local region applies the tree's entries authored by the peer. |

### `ReplicationLinkHealth`

| Value | Meaning |
|---|---|
| `Unknown` | Not enough is known to judge the link: it has never made a successful contact and no threshold has been crossed. Also the value an entry from a peer that predates the field decodes to. |
| `Healthy` | Every signal is within its lagging threshold. |
| `Lagging` | At least one signal is past its lagging threshold and none is past its stalled threshold. |
| `Stalled` | At least one signal is past its stalled threshold, or the sender has taken the peer off the log after a write-ahead-log trim lost records it never shipped and is waiting for the peer to re-seed ([#4534](https://github.com/NSTA1/Orleans.Lattice/issues/4534)); such a peer receives no saga records until it does. |

## Exceptions

| Exception | Raised when |
|---|---|
| `LatticeAuthorizationDeniedException` | The caller is not authorized for the `LatticeOperation.Replication` capability on the target tree. |
| `ArgumentException` | `treeId` is null or empty, or a peer-status query's `ContinuationToken` is malformed (including a token of an earlier format version). |
| `ArgumentNullException` | `GetPeerStatusAsync` is passed a null `query`. |
| `ArgumentOutOfRangeException` | A peer-status query's `PageSize` is negative. |
| `LatticeTenantAccessDeniedException` | The tenancy add-on is registered and the request asserts an active tenant the caller may not act as (an anonymous caller never can), or, under an asserted tenant, names a `sys-` tree or a malformed `t/` id, so the tenant-local `treeId` cannot be resolved. `GetPeerStatusAsync` checks the caller's tenant on every call, with or without a tree filter, and resolves its tree filter the same way. (Defined in `Orleans.Lattice`.) |
| `LatticeReplicationModeChangeRejectedException` | An enable would change the merge mode of an already-enabled tree, or targets an enabled tree whose mode is currently ambiguous. Carries `TreeId`, `CurrentMode`, `RequestedMode`, and `CurrentModeAmbiguous`. (Defined in `Orleans.Lattice.Replication`.) |
| `LatticeReplicationPreconditionFailedException` | A runtime precondition for authoring the change was not met: no local replica id is configured - the config entry's flag dots are stamped with it, so both an enable and the disable of an enabled tree need one - or a flag-based merge mode is requested without one. (Defined in `Orleans.Lattice.Replication`.) |
| `InvalidOperationException` | An enable that requests a snapshot bootstrap finds a bootstrap for the same tree already in progress from a different source cluster. The enable itself has already been written to the config tree by the time this is raised. |
| `LatticeReplicationPeerStillConfiguredException` | `DecommissionPeerAsync` is called while the peer is still present in `ReplicationPeers`. (`InvalidOperationException`-derived; defined in `Orleans.Lattice.Replication`.) |

## See also

- [Configuration](configuration.md) - the `LatticeApiReplicationOptions` and `LatticeReplicationStatusOptions` properties.
- [Architecture](architecture.md) - how each control operation composes authorization and engine delegation, and how the peer-status read path reads, authorizes, and pages.
