---
agent_spec: "docs/agents/api/mcp.json"
---

# Public API

The complete public surface of `Orleans.Lattice.Api.Mcp`: the registration extension methods, the options types, the security seams a host can replace, the discovery records, the structured tool-result records, and the metrics class. Everything lives in the `Orleans.Lattice.Api.Mcp` namespace. The narrative guides explain how the pieces fit together - [Setup](setup.md) for registration and options, [Tools](tools.md) for the tool catalogue, [Security](security.md) for the fail-closed posture, and [Remote hosting](remote.md) for the out-of-silo topology. Every option property, default and validation rule is tabulated in [Configuration](configuration.md); the request pipeline is described in [Architecture](architecture.md).

## Registration

Every registration method extends `IServiceCollection` except `MapLatticeMcp`, which extends `IEndpointRouteBuilder`. The server registers no tools on its own; each module is opted in by its own call, and a module's destructive verbs stay hidden until its flag is set.

| Method | Declared on | What it registers |
|---|---|---|
| `AddLatticeMcp(Action<LatticeApiMcpOptions>? configure = null)` | `LatticeMcpServiceCollectionExtensions` | The MCP server over the streamable-HTTP transport with no tools, the default-deny authorizer, the credential and active-tenant bridges, the HTTP context accessor, permission-scoped discovery, region routing, origin validation and the protected-resource-metadata challenge. Idempotent. |
| `MapLatticeMcp()` | `LatticeMcpServiceCollectionExtensions` | Maps the transport at `TransportPattern`, requires authorization when `RequireAuthorization` is `true`, enforces the origin check at the endpoint when `RequireAllowedOrigin` is `true`, and serves the anonymous OAuth metadata document when `ProtectedResourceMetadata` is set. Returns the endpoint's `IEndpointConventionBuilder`. |
| `AddStateTools()` | `LatticeMcpStateToolsServiceCollectionExtensions` | The `lattice_state_*` tools. |
| `AddDataTools(bool enableWrites = false)` | `LatticeMcpDataToolsServiceCollectionExtensions` | The `lattice_data_*` tools; the destructive write tools only when `enableWrites` is `true`. |
| `AddBackupTools(bool enableControl = false)` | `LatticeMcpBackupServiceCollectionExtensions` | The `lattice_backup_*` inspect tools; the control tools (capture, incremental capture, restore, revert, delete) only when `enableControl` is `true`. |
| `AddAuthTools(bool enableAdministration = false)` | `LatticeMcpServiceCollectionExtensions` | The `lattice_auth_*` introspection tools; the mutating administration verbs only when `enableAdministration` is `true`. |
| `AddReplicationTools(bool enableControl = false)` | `LatticeMcpReplicationServiceCollectionExtensions` | The `lattice_replication_*` inspect tool; the enable and disable control tools only when `enableControl` is `true`. |
| `AddTreeAdminTools(bool enableSchemaControl = false, bool enableLifecycle = false)` | `LatticeMcpTreeAdminServiceCollectionExtensions` | The `lattice_treeadmin_*` read-only diagnostics, operation, schema-inspection and lifecycle-read tools; the schema-management mutations behind `enableSchemaControl`, and the tree lifecycle and control mutations behind `enableLifecycle`. |
| `AddTenantSelfAwarenessTools()` | `LatticeMcpTenantSelfAwarenessServiceCollectionExtensions` | The read-only tenant self-awareness tools (`lattice_tenant_current`, `lattice_tenant_list`, `lattice_tenant_get`). |
| `AddTenantAdminTools(bool enableControl = false)` | `LatticeMcpTenantAdminServiceCollectionExtensions` | The tenant-administration group. With `enableControl` `true` it contributes the tenant lifecycle, region-residency and delegated tenant access tools; with the `false` default the group is registered for discovery but contributes no tools. |
| `AddLatticeMcpRemote(Action<LatticeApiMcpRemoteOptions> configure)` | `LatticeMcpRemoteServiceCollectionExtensions` | The remote-host topology: calls `AddLatticeMcp()`, then wires a gRPC-backed facade adapter and its tool module for every group with a configured endpoint, the credential-forwarding interceptor and the multi-region router. |
| `AddLatticeMcpManagedIdentityAdministrator(Action<LatticeApiMcpManagedIdentityAdministratorOptions> configure)` | `LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions` | A self-refreshing managed-identity administrator credential for remote discovery, replacing the static `AdministratorCredential` source regardless of registration order. |

Each module needs its facade registered on the silo first; the list is in [Setup](setup.md#prerequisites), and the tools each module contributes are catalogued in [Tools](tools.md). Two further tool modules ship as companion packages: [`Orleans.Lattice.Api.Mcp.Telemetry`](../lattice.api.mcp.telemetry/api.md) (`AddTelemetryTools`) and [`Orleans.Lattice.Api.Mcp.RepoContext`](../lattice.api.mcp.repocontext/api.md) (`AddRepoContextTools`); [`Orleans.Lattice.Api.Mcp.Apps`](../lattice.api.mcp.apps/README.md) adds installable-app tools to the same endpoint.

## Options types

| Type | Bound by | Reference |
|---|---|---|
| `LatticeApiMcpOptions` | `AddLatticeMcp` | [Front-door options](configuration.md#latticeapimcpoptions) |
| `LatticeApiMcpProtectedResourceMetadata` | `LatticeApiMcpOptions.ProtectedResourceMetadata` | [OAuth protected-resource metadata](configuration.md#latticeapimcpprotectedresourcemetadata) |
| `LatticeApiMcpRemoteOptions` | `AddLatticeMcpRemote` | [Remote-host options](configuration.md#latticeapimcpremoteoptions) |
| `LatticeApiMcpRemoteRegionOptions` | `LatticeApiMcpRemoteOptions.Regions` | [Peer regions](configuration.md#latticeapimcpremoteregionoptions) |
| `LatticeApiMcpRemoteEndpoint` | each per-group endpoint property | [Remote endpoints](configuration.md#latticeapimcpremoteendpoint) |
| `LatticeApiMcpManagedIdentityAdministratorOptions` | `AddLatticeMcpManagedIdentityAdministrator` | [Managed-identity administrator](configuration.md#latticeapimcpmanagedidentityadministratoroptions) |

## Security seams

`AddLatticeMcp` registers a default for each of these with `TryAdd`, so a host substitutes its own implementation by registering it in the service collection. See [Security](security.md) for the behaviour each one guarantees.

| Type | Role |
|---|---|
| `ILatticeApiMcpAuthorizer` | The coarse gate consulted, through one lock-step check, both when a tool is advertised and when it is invoked: `Task<bool> IsAuthorizedAsync(LatticeApiMcpAuthorizationContext, CancellationToken)`. |
| `DenyAllMcpAuthorizer` | The registered default: refuses every facade tool, so the surface stays closed until a host opts in. |
| `AllowAllMcpAuthorizer` | The opt-in "defer to the access gate" implementation: admits every tool and leaves the decision to the per-tree / per-key access gate. |
| `LatticeApiMcpAuthorizationContext` | The read-only struct passed to the authorizer: `Call` is the ASP.NET Core `HttpContext` serving the request, and `ToolName` is the tool being invoked, or `null` for a decision not scoped to one tool. |
| `ILatticeApiMcpCredentialBridge` | `LatticeCredential? Resolve(HttpContext context)`: lifts the authenticated caller onto the ambient Lattice credential for each tool invocation. The default resolves a credential only for an authenticated principal. |
| `ILatticeApiMcpActiveTenantBridge` | `TenantId? Resolve(HttpContext context)`: lifts the caller's asserted active tenant (an assertion the tenancy add-on re-validates downstream) onto the ambient active-tenant scope. |

## Discovery records

| Type | Returned by | Shape |
|---|---|---|
| `LatticeApiMcpCapabilities` | `lattice_capabilities` | The resolved `SubjectId` (or `null`), whether the caller is `Authenticated`, the connected silo's `ClusterId` and `ServiceId`, and one `LatticeApiMcpGroupCapability` per group in `Groups`. |
| `LatticeApiMcpGroupCapability` | an element of `LatticeApiMcpCapabilities.Groups` | The `Group`, whether it is `Available` to this caller, and the remote `Endpoint` it is served from (`null` when co-hosted). |
| `LatticeApiMcpGroup` | the `Group` value | `State`, `Data`, `Backup`, `Auth`, `Telemetry`, `Replication`, `TreeAdmin`, `RepoContext`, `TenantAdmin`. |
| `LatticeApiMcpRegionsToolResult` | `lattice_list_regions` | The `CurrentRegion` id and the `Regions` list of `LatticeRegionDescriptor` entries (a type from `Orleans.Lattice.Api.Abstractions`). See [Region targeting](tools.md#region-targeting). |

## Tool result and argument records

These records are MCP protocol payloads: the MCP SDK serializes them to the tool's JSON structured content with `System.Text.Json`, they carry no Orleans serialization attributes, and their `byte[]` members travel as base64 strings. Tools not listed here return records owned by the facade package they adapt, or a projection whose JSON shape [Tools](tools.md) describes per tool.

| Record | Used by |
|---|---|
| `DataGetToolResult` | `lattice_data_get` |
| `DataRangePageToolResult` | `lattice_data_read_range` (its `Entries` are `DataEntryDto`) |
| `DataSetToolResult` | `lattice_data_set` |
| `DataSetManyToolResult` | `lattice_data_set_many` |
| `DataAtomicBatchToolResult` | `lattice_data_set_many_atomic` |
| `DataCrossTreeBatchToolResult` | `lattice_data_set_many_atomic_cross_tree` |
| `DataDeleteToolResult` | `lattice_data_delete` |
| `DataRangeDeleteToolResult` | `lattice_data_delete_range` |
| `DataEntryDto` | A key / value entry: a returned range-read entry and an upsert leg of a write batch. |
| `DataTreeBatchDto` | An input argument of `lattice_data_set_many_atomic_cross_tree`: one tree's upserts and deletes; each `TreeId` in a batch must be distinct and non-empty. |
| `CrdtWriteToolResult` | Every typed CRDT write tool (`lattice_data_pncounter`, `lattice_data_gcounter`, `lattice_data_orset`, `lattice_data_gset`, `lattice_data_orflag`, `lattice_data_rwflag`, `lattice_data_rwset`, `lattice_data_ormap`, `lattice_data_sequence`, `lattice_data_mvregister_set`, `lattice_data_maxregister_set`, `lattice_data_minregister_set`, `lattice_data_version_vector_tick`) |
| `CrdtCounterToolResult` | `lattice_data_pncounter_get`, `lattice_data_gcounter_get` |
| `CrdtElementsToolResult` | `lattice_data_orset_get`, `lattice_data_gset_get`, `lattice_data_rwset_get`, `lattice_data_sequence_get`, `lattice_data_mvregister_get`, `lattice_data_maxregister_get`, `lattice_data_minregister_get` |
| `CrdtFlagToolResult` | `lattice_data_orflag_get`, `lattice_data_rwflag_get` |
| `CrdtMapToolResult` | `lattice_data_ormap_get` |
| `CrdtVersionVectorToolResult` | `lattice_data_version_vector_get` |
| `CrdtCounterOp` | The operation argument of `lattice_data_pncounter`: `Increment`, `Decrement`. |
| `CrdtSetOp` | The operation argument of `lattice_data_orset`: `Add`, `Remove`. |
| `CrdtFlagOp` | The operation argument of `lattice_data_orflag` and `lattice_data_rwflag`: `Enable`, `Disable`. |
| `CrdtRwSetOp` | The operation argument of `lattice_data_rwset`: `Add`, `Remove`. |
| `CrdtSequenceOp` | The operation argument of `lattice_data_sequence`: `InsertAt`, `RemoveAt`. |
| `CrdtMapOp` | The operation argument of `lattice_data_ormap`: `Set`, `Remove`. |
| `PhysicalShardCountResult` | `lattice_state_get_physical_shard_count`; `TreeExists` is `false` (and `PhysicalShardCount` `null`) for an unknown tree. |
| `ScanCancellationResult` | `lattice_state_cancel_scan` |
| `AuthGroupMembersResult` | `lattice_auth_list_group_members` |
| `AuthSubjectGroupsResult` | `lattice_auth_list_subject_groups` |
| `SchemaDeadLetterListResult` | `lattice_treeadmin_schema_list_dead_letters` (its `Entries` reuse the schema facade's `LatticeSchemaDeadLetterEntry`) |

## Metrics

`LatticeApiMcpMetrics` owns the `orleans.lattice.api.mcp` meter (`MeterName`) and one counter, `ToolClientErrors`, published as `orleans.lattice.api.mcp.tool.client_errors` (`ToolClientErrorsName`, unit `{error}`). It counts tool calls answered as the caller's mistake rather than thrown, tagged `tool` (`TagTool`) and `reason` (`TagReason`), whose values are `invalid_argument`, `unknown_argument`, `rejected_content` and `not_found` (`ReasonInvalidArgument`, `ReasonUnknownArgument`, `ReasonRejectedContent`, `ReasonNotFound`). See [Client errors](tools.md#client-errors-are-answered-not-logged-as-faults).

## Related

- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Setup](setup.md), [Tools](tools.md), [Security](security.md), [Remote hosting](remote.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Api.Mcp.AllowAllMcpAuthorizer`

[Source](../../src/lattice.api.mcp/Security/ILatticeApiMcpAuthorizer.cs) (line 59).

`public sealed class AllowAllMcpAuthorizer : ILatticeApiMcpAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeApiMcpAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Mcp.AuthGroupMembersResult`

[Source](../../src/lattice.api.mcp/Tools/Auth/AuthToolResults.cs) (line 21).

`public sealed record AuthGroupMembersResult`

- `public required string GroupId { get; init; }`
- `public required IReadOnlyList<string> Members { get; init; }`

### `Orleans.Lattice.Api.Mcp.AuthSubjectGroupsResult`

[Source](../../src/lattice.api.mcp/Tools/Auth/AuthToolResults.cs) (line 48).

`public sealed record AuthSubjectGroupsResult`

- `public required string MemberId { get; init; }`
- `public required IReadOnlyList<string> Groups { get; init; }`

### `Orleans.Lattice.Api.Mcp.CrdtCounterOp`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtCounterOp.cs) (line 4).

`public enum CrdtCounterOp`

- `Increment`
- `Decrement`

### `Orleans.Lattice.Api.Mcp.CrdtCounterToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtCounterToolResult.cs) (line 8).

`public sealed record CrdtCounterToolResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public required long Value { get; init; }`

### `Orleans.Lattice.Api.Mcp.CrdtElementsToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtElementsToolResult.cs) (line 11).

`public sealed record CrdtElementsToolResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public required IReadOnlyList<byte[]> Elements { get; init; }`
- `public bool Equals(CrdtElementsToolResult? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Mcp.CrdtFlagOp`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtFlagOp.cs) (line 4).

`public enum CrdtFlagOp`

- `Enable`
- `Disable`

### `Orleans.Lattice.Api.Mcp.CrdtFlagToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtFlagToolResult.cs) (line 7).

`public sealed record CrdtFlagToolResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public required bool Enabled { get; init; }`

### `Orleans.Lattice.Api.Mcp.CrdtMapOp`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtMapOp.cs) (line 4).

`public enum CrdtMapOp`

- `Set`
- `Remove`

### `Orleans.Lattice.Api.Mcp.CrdtMapToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtMapToolResult.cs) (line 10).

`public sealed record CrdtMapToolResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public required IReadOnlyDictionary<string, IReadOnlyList<byte[]>> Fields { get; init; }`
- `public bool Equals(CrdtMapToolResult? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Mcp.CrdtRwSetOp`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtRwSetOp.cs) (line 4).

`public enum CrdtRwSetOp`

- `Add`
- `Remove`

### `Orleans.Lattice.Api.Mcp.CrdtSequenceOp`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtSequenceOp.cs) (line 4).

`public enum CrdtSequenceOp`

- `InsertAt`
- `RemoveAt`

### `Orleans.Lattice.Api.Mcp.CrdtSetOp`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtSetOp.cs) (line 4).

`public enum CrdtSetOp`

- `Add`
- `Remove`

### `Orleans.Lattice.Api.Mcp.CrdtVersionVectorToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtVersionVectorToolResult.cs) (line 8).

`public sealed record CrdtVersionVectorToolResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public required IReadOnlyDictionary<string, string> Entries { get; init; }`

### `Orleans.Lattice.Api.Mcp.CrdtWriteToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/CrdtWriteToolResult.cs) (line 9).

`public sealed record CrdtWriteToolResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public bool Committed { get; init; }`

### `Orleans.Lattice.Api.Mcp.DataAtomicBatchToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/DataAtomicBatchToolResult.cs) (line 9).

`public sealed record DataAtomicBatchToolResult`

- `public required string TreeId { get; init; }`
- `public required string OperationId { get; init; }`
- `public bool Committed { get; init; }`

### `Orleans.Lattice.Api.Mcp.DataCrossTreeBatchToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/DataCrossTreeBatchToolResult.cs) (line 10).

`public sealed record DataCrossTreeBatchToolResult`

- `public required string OperationId { get; init; }`
- `public required string Outcome { get; init; }`
- `public bool Committed { get; init; }`

### `Orleans.Lattice.Api.Mcp.DataDeleteToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/DataDeleteToolResult.cs) (line 9).

`public sealed record DataDeleteToolResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public bool Deleted { get; init; }`

### `Orleans.Lattice.Api.Mcp.DataEntryDto`

[Source](../../src/lattice.api.mcp/Tools/Data/DataEntryDto.cs) (line 11).

`public sealed record DataEntryDto`

- `public required string Key { get; init; }`
- `public byte[] Value { get; init; }`
- `public bool Equals(DataEntryDto? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Mcp.DataGetToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/DataGetToolResult.cs) (line 10).

`public sealed record DataGetToolResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public bool Found { get; init; }`
- `public byte[] Value { get; init; }`
- `public string? MergeMode { get; init; }`
- `public bool Raw { get; init; }`
- `public bool Equals(DataGetToolResult? other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Api.Mcp.DataRangeDeleteToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/DataRangeDeleteToolResult.cs) (line 9).

`public sealed record DataRangeDeleteToolResult`

- `public required string TreeId { get; init; }`
- `public required int DeletedCount { get; init; }`

### `Orleans.Lattice.Api.Mcp.DataRangePageToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/DataRangePageToolResult.cs) (line 9).

`public sealed record DataRangePageToolResult`

- `public required string TreeId { get; init; }`
- `public IReadOnlyList<DataEntryDto> Entries { get; init; }`
- `public string? ContinuationToken { get; init; }`

### `Orleans.Lattice.Api.Mcp.DataSetManyToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/DataSetManyToolResult.cs) (line 10).

`public sealed record DataSetManyToolResult`

- `public required string TreeId { get; init; }`
- `public required int Count { get; init; }`
- `public bool Committed { get; init; }`

### `Orleans.Lattice.Api.Mcp.DataSetToolResult`

[Source](../../src/lattice.api.mcp/Tools/Data/DataSetToolResult.cs) (line 9).

`public sealed record DataSetToolResult`

- `public required string TreeId { get; init; }`
- `public required string Key { get; init; }`
- `public bool Committed { get; init; }`

### `Orleans.Lattice.Api.Mcp.DataTreeBatchDto`

[Source](../../src/lattice.api.mcp/Tools/Data/DataTreeBatchDto.cs) (line 10).

`public sealed record DataTreeBatchDto`

- `public required string TreeId { get; init; }`
- `public IReadOnlyList<DataEntryDto> Upserts { get; init; }`
- `public IReadOnlyList<string> DeleteKeys { get; init; }`

### `Orleans.Lattice.Api.Mcp.DenyAllMcpAuthorizer`

[Source](../../src/lattice.api.mcp/Security/ILatticeApiMcpAuthorizer.cs) (line 41).

`public sealed class DenyAllMcpAuthorizer : ILatticeApiMcpAuthorizer`

- `public Task<bool> IsAuthorizedAsync(LatticeApiMcpAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Mcp.ILatticeApiMcpActiveTenantBridge`

[Source](../../src/lattice.api.mcp/Security/ILatticeApiMcpActiveTenantBridge.cs) (line 37).

`public interface ILatticeApiMcpActiveTenantBridge`

- `TenantId? Resolve(HttpContext context)`

### `Orleans.Lattice.Api.Mcp.ILatticeApiMcpAuthorizer`

[Source](../../src/lattice.api.mcp/Security/ILatticeApiMcpAuthorizer.cs) (line 16).

`public interface ILatticeApiMcpAuthorizer`

- `Task<bool> IsAuthorizedAsync(LatticeApiMcpAuthorizationContext authorizationContext, CancellationToken cancellationToken)`

### `Orleans.Lattice.Api.Mcp.ILatticeApiMcpCredentialBridge`

[Source](../../src/lattice.api.mcp/Security/ILatticeApiMcpCredentialBridge.cs) (line 30).

`public interface ILatticeApiMcpCredentialBridge`

- `LatticeCredential? Resolve(HttpContext context)`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpAuthorizationContext`

[Source](../../src/lattice.api.mcp/Security/LatticeApiMcpAuthorizationContext.cs) (line 12).

`public readonly struct LatticeApiMcpAuthorizationContext`

- `public LatticeApiMcpAuthorizationContext(HttpContext call, string? toolName = null)`
- `public HttpContext Call { get; }`
- `public string? ToolName { get; }`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpCapabilities`

[Source](../../src/lattice.api.mcp/Discovery/LatticeApiMcpCapabilities.cs) (line 15).

`public sealed record LatticeApiMcpCapabilities`

- `public string? SubjectId { get; init; }`
- `public required bool Authenticated { get; init; }`
- `public string ClusterId { get; init; }`
- `public string ServiceId { get; init; }`
- `public IReadOnlyList<LatticeApiMcpGroupCapability> Groups { get; init; }`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpGroup`

[Source](../../src/lattice.api.mcp/Discovery/LatticeApiMcpGroup.cs) (line 11).

`public enum LatticeApiMcpGroup`

- `State`
- `Data`
- `Backup`
- `Auth`
- `Telemetry`
- `Replication`
- `TreeAdmin`
- `RepoContext`
- `TenantAdmin`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpGroupCapability`

[Source](../../src/lattice.api.mcp/Discovery/LatticeApiMcpGroupCapability.cs) (line 14).

`public sealed record LatticeApiMcpGroupCapability`

- `public required LatticeApiMcpGroup Group { get; init; }`
- `public required bool Available { get; init; }`
- `public string? Endpoint { get; init; }`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpManagedIdentityAdministratorOptions`

[Source](../../src/lattice.api.mcp/Remote/LatticeApiMcpManagedIdentityAdministratorOptions.cs) (line 16).

`public sealed class LatticeApiMcpManagedIdentityAdministratorOptions`

- `public TokenCredential? Credential { get; set; }`
- `public string Scope { get; set; }`
- `public TimeSpan RefreshSkew { get; set; }`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpMetrics`

[Source](../../src/lattice.api.mcp/LatticeApiMcpMetrics.cs) (line 11).

`public static class LatticeApiMcpMetrics`

- `public const string MeterName`
- `public const string TagTool`
- `public const string TagReason`
- `public const string ReasonInvalidArgument`
- `public const string ReasonUnknownArgument`
- `public const string ReasonRejectedContent`
- `public const string ReasonNotFound`
- `public const string ToolClientErrorsName`
- `public static readonly Meter Meter`
- `public static readonly Counter<long> ToolClientErrors`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpOptions`

[Source](../../src/lattice.api.mcp/LatticeApiMcpOptions.cs) (line 22).

`public sealed class LatticeApiMcpOptions`

- `public bool RequireAuthorization { get; set; }`
- `public string TransportPattern { get; set; }`
- `public bool Stateless { get; set; }`
- `public bool RequireAllowedOrigin { get; set; }`
- `public IList<string> AllowedOrigins { get; }`
- `public bool AllowLoopbackOrigins { get; set; }`
- `public string CredentialHeaderName { get; set; }`
- `public string CredentialScheme { get; set; }`
- `public string ActiveTenantHeaderName { get; set; }`
- `public bool EnableStateTools { get; set; }`
- `public bool EnableDataTools { get; set; }`
- `public bool EnableBackupTools { get; set; }`
- `public bool EnableBackupControlTools { get; set; }`
- `public bool EnableAuthTools { get; set; }`
- `public bool EnableAuthAdministration { get; set; }`
- `public bool EnableReplicationTools { get; set; }`
- `public bool EnableReplicationControlTools { get; set; }`
- `public bool EnableTreeAdminSchemaControlTools { get; set; }`
- `public bool EnableTreeAdminLifecycleTools { get; set; }`
- `public LatticeApiMcpProtectedResourceMetadata? ProtectedResourceMetadata { get; set; }`
- `public bool EnableTenantAdminTools { get; set; }`
- `public bool EnableTenantAdminControlTools { get; set; }`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpProtectedResourceMetadata`

[Source](../../src/lattice.api.mcp/LatticeApiMcpProtectedResourceMetadata.cs) (line 31).

`public sealed class LatticeApiMcpProtectedResourceMetadata`

- `public const string DefaultWellKnownPath`
- `public Uri? Resource { get; set; }`
- `public IList<Uri> AuthorizationServers { get; }`
- `public IList<string> ScopesSupported { get; }`
- `public IList<string> BearerMethodsSupported { get; }`
- `public string WellKnownPath { get; set; }`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpRegionsToolResult`

[Source](../../src/lattice.api.mcp/Tools/Region/LatticeApiMcpRegionsToolResult.cs) (line 18).

`public sealed record LatticeApiMcpRegionsToolResult`

- `public required string CurrentRegion { get; init; }`
- `public IReadOnlyList<LatticeRegionDescriptor> Regions { get; init; }`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpRemoteEndpoint`

[Source](../../src/lattice.api.mcp/Remote/LatticeApiMcpRemoteEndpoint.cs) (line 22).

`public sealed class LatticeApiMcpRemoteEndpoint`

- `public required string Endpoint { get; init; }`
- `public CallInvoker? CallInvoker { get; init; }`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpRemoteOptions`

[Source](../../src/lattice.api.mcp/Remote/LatticeApiMcpRemoteOptions.cs) (line 35).

`public sealed class LatticeApiMcpRemoteOptions`

- `public const string DefaultRegionId`
- `public string RegionId { get; set; }`
- `public string? ClusterId { get; set; }`
- `public IList<LatticeApiMcpRemoteRegionOptions> Regions { get; }`
- `public bool VerifyRegionIdentity { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? State { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Data { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Auth { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Backup { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Replication { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? TreeAdmin { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? TenantAdmin { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Telemetry { get; set; }`
- `public string CredentialHeaderName { get; set; }`
- `public string CredentialScheme { get; set; }`
- `public string ActiveTenantHeaderName { get; set; }`
- `public LatticeCredential? AdministratorCredential { get; set; }`
- `public bool EnableDataWrites { get; set; }`
- `public bool EnableBackupControl { get; set; }`
- `public bool EnableAuthAdministration { get; set; }`
- `public bool EnableReplicationControl { get; set; }`
- `public bool EnableSchemaControl { get; set; }`
- `public bool EnableLifecycleControl { get; set; }`
- `public bool EnableTenantControl { get; set; }`

### `Orleans.Lattice.Api.Mcp.LatticeApiMcpRemoteRegionOptions`

[Source](../../src/lattice.api.mcp/Remote/LatticeApiMcpRemoteRegionOptions.cs) (line 18).

`public sealed class LatticeApiMcpRemoteRegionOptions`

- `public required string RegionId { get; set; }`
- `public string? ClusterId { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? State { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Data { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Auth { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Backup { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Replication { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? TreeAdmin { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? TenantAdmin { get; set; }`
- `public LatticeApiMcpRemoteEndpoint? Telemetry { get; set; }`

### `Orleans.Lattice.Api.Mcp.LatticeMcpBackupServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/Tools/Backup/LatticeMcpBackupServiceCollectionExtensions.cs) (line 33).

`public static class LatticeMcpBackupServiceCollectionExtensions`

- `public static IServiceCollection AddBackupTools(this IServiceCollection services, bool enableControl = false)`

### `Orleans.Lattice.Api.Mcp.LatticeMcpDataToolsServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/LatticeMcpDataToolsServiceCollectionExtensions.cs) (line 27).

`public static class LatticeMcpDataToolsServiceCollectionExtensions`

- `public static IServiceCollection AddDataTools(this IServiceCollection services, bool enableWrites = false)`

### `Orleans.Lattice.Api.Mcp.LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/Remote/LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions.cs) (line 15).

`public static class LatticeMcpManagedIdentityAdministratorServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeMcpManagedIdentityAdministrator(this IServiceCollection services, Action<LatticeApiMcpManagedIdentityAdministratorOptions> configure)`

### `Orleans.Lattice.Api.Mcp.LatticeMcpRemoteServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/Remote/LatticeMcpRemoteServiceCollectionExtensions.cs) (line 34).

`public static class LatticeMcpRemoteServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeMcpRemote(this IServiceCollection services, Action<LatticeApiMcpRemoteOptions> configure)`

### `Orleans.Lattice.Api.Mcp.LatticeMcpReplicationServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/Tools/Replication/LatticeMcpReplicationServiceCollectionExtensions.cs) (line 35).

`public static class LatticeMcpReplicationServiceCollectionExtensions`

- `public static IServiceCollection AddReplicationTools(this IServiceCollection services, bool enableControl = false)`

### `Orleans.Lattice.Api.Mcp.LatticeMcpServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/LatticeMcpServiceCollectionExtensions.cs) (line 37), [partial](../../src/lattice.api.mcp/LatticeMcpServiceCollectionExtensions.ProtectedResourceMetadata.cs) (line 19), [partial](../../src/lattice.api.mcp/Tools/Auth/LatticeMcpServiceCollectionExtensions.Auth.cs) (line 11).

`public static partial class LatticeMcpServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeMcp(this IServiceCollection services, Action<LatticeApiMcpOptions>? configure = null)`
- `public static IEndpointConventionBuilder MapLatticeMcp(this IEndpointRouteBuilder endpoints)`
- `public static IServiceCollection AddAuthTools(this IServiceCollection services, bool enableAdministration = false)`

### `Orleans.Lattice.Api.Mcp.LatticeMcpStateToolsServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/Tools/State/LatticeMcpStateToolsServiceCollectionExtensions.cs) (line 26).

`public static class LatticeMcpStateToolsServiceCollectionExtensions`

- `public static IServiceCollection AddStateTools(this IServiceCollection services)`

### `Orleans.Lattice.Api.Mcp.LatticeMcpTenantAdminServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/Tools/TenantAdmin/LatticeMcpTenantAdminServiceCollectionExtensions.cs) (line 45).

`public static class LatticeMcpTenantAdminServiceCollectionExtensions`

- `public static IServiceCollection AddTenantAdminTools(this IServiceCollection services, bool enableControl = false)`

### `Orleans.Lattice.Api.Mcp.LatticeMcpTenantSelfAwarenessServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/Tools/TenantSelfAwareness/LatticeMcpTenantSelfAwarenessServiceCollectionExtensions.cs) (line 38).

`public static class LatticeMcpTenantSelfAwarenessServiceCollectionExtensions`

- `public static IServiceCollection AddTenantSelfAwarenessTools(this IServiceCollection services)`

### `Orleans.Lattice.Api.Mcp.LatticeMcpTreeAdminServiceCollectionExtensions`

[Source](../../src/lattice.api.mcp/Tools/TreeAdmin/LatticeMcpTreeAdminServiceCollectionExtensions.cs) (line 40).

`public static class LatticeMcpTreeAdminServiceCollectionExtensions`

- `public static IServiceCollection AddTreeAdminTools(this IServiceCollection services, bool enableSchemaControl = false, bool enableLifecycle = false)`

### `Orleans.Lattice.Api.Mcp.PhysicalShardCountResult`

[Source](../../src/lattice.api.mcp/Tools/State/StateToolResults.cs) (line 14).

`public sealed record PhysicalShardCountResult`

- `public required string TreeId { get; init; }`
- `public int? PhysicalShardCount { get; init; }`
- `public bool TreeExists { get; }`

### `Orleans.Lattice.Api.Mcp.ScanCancellationResult`

[Source](../../src/lattice.api.mcp/Tools/State/StateToolResults.cs) (line 43).

`public sealed record ScanCancellationResult`

- `public required string TreeId { get; init; }`
- `public bool Acknowledged { get; init; }`

### `Orleans.Lattice.Api.Mcp.SchemaDeadLetterListResult`

[Source](../../src/lattice.api.mcp/Tools/TreeAdmin/TreeAdminSchemaToolResults.cs) (line 26).

`public sealed record SchemaDeadLetterListResult`

- `public required string TreeId { get; init; }`
- `public required IReadOnlyList<LatticeSchemaDeadLetterEntry> Entries { get; init; }`
