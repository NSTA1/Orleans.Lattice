---
agent_spec: "docs/agents/api/state.json"
---

# gRPC Contract

`Orleans.Lattice.Api.State.Grpc` is a **code-first** gRPC binding. There is no hand-written `.proto`: the service and its methods are defined in C#, and its request / response messages are Orleans-serialized C# records. Most RPCs reuse the facade DTOs directly; RPCs that need a transport-specific envelope wrap the facade arguments or results in binding-owned records. The server binds the methods; the public `LatticeStateApiGrpcClient` calls them; both sides share identical marshallers, so the wire format stays in lock-step by construction.

## Service

The service name on the wire is `orleans.lattice.api.state` (so each method's full path is `/orleans.lattice.api.state/<Rpc>`). It exposes unary and server-streaming RPCs for the remotely supported read-only facade operations, plus an unauthenticated `GetAuthScheme` advertisement RPC. In-process-only summary helpers on the facade are not gRPC RPCs.

| RPC | Kind | Request | Response | Surface |
|---|---|---|---|---|
| `ListTrees` | unary | `CatalogRequest` | `TreeCatalogPage` | [Discovery](surfaces.md#discovery) |
| `ListViews` | unary | `CatalogRequest` | `ViewCatalogPage` | [Discovery](surfaces.md#discovery) |
| `ListTagIndexes` | unary | `CatalogRequest` | `TagIndexCatalogPage` | [Discovery](surfaces.md#discovery) |
| `ListTagValues` | unary | `CatalogRequest` | `TagValueCatalogPage` | [Discovery](surfaces.md#discovery) |
| `ListCoveredTrees` | unary | `CatalogRequest` | `CoveredTreeCatalogPage` | [Discovery](surfaces.md#discovery) |
| `ListIndexTags` | unary | `CatalogRequest` | `TagValueCatalogPage` | [Discovery](surfaces.md#discovery) |
| `ScanTagMembers` | unary | `TagMemberScanRequest` | `TagMemberScanPage` | [Discovery](surfaces.md#discovery) |
| `GetTreeStructure` | unary | `StructureRequest` | `StructureResponse` | [Structure](surfaces.md#structure) |
| `ScanEntries` | unary | `EntryScanRequest` | `EntryScanResponse` | [Entries](surfaces.md#entries) |
| `GetEntry` | unary | `EntryGetRequest` | `EntryGetResponse` | [Entries](surfaces.md#entries) |
| `GetEntryHistory` | unary | `EntryHistoryRequest` | `EntryHistoryResponse` | [Change history](surfaces.md#change-history) |
| `CancelScan` | unary | `EntryScanCancelRequest` | `EntryScanCancelResponse` | [Entries](surfaces.md#entries) |
| `GetMetricsSnapshot` | unary | `TreeMetricsRequest` | `TreeMetricsSnapshot` | [Metrics](surfaces.md#metrics) |
| `GetClusterInfo` | unary | `ClusterInfoRequest` | `ClusterInfo` | [Cluster info](surfaces.md#cluster-info) |
| `GetAuthScheme` | unary | `AuthSchemeAdvertisementRequest` | `AuthSchemeAdvertisement` | [Security](security.md) |
| `GetDeadLetterCount` | unary | `DeadLetterCountRequest` | `DeadLetterCountResponse` | [Dead letters](surfaces.md#dead-letters) |
| `ListDeadLetters` | unary | `DeadLetterQueueRequest` | `DeadLetterQueuePage` | [Dead letters](surfaces.md#dead-letters) |
| `ObserveChanges` | server-streaming | `StateObserveRequest` | `StateChangeNotification` | [Change observation](surfaces.md#change-observation) |
| `ObserveMetrics` | server-streaming | `TreeMetricsRequest` | `TreeMetricsSnapshot` | [Metrics](surfaces.md#metrics) |

`CancelScan` is a best-effort, idempotent cleanup verb: it releases the server-side snapshot cursor named by a scan continuation token, deleting the frozen per-shard baselines it captured when the scan opened, unregistering it from the write-ahead-log cursor registry, and clearing its persisted state promptly, rather than waiting for the cursor's idle TTL to do the same. That registration does not hold back WAL trimming: a snapshot cursor registers at a zero position with no blocked floor, which WAL garbage collection skips, so cancelling reclaims baseline storage and cursor state rather than WAL. A client that abandons a multi-page scan before draining it (refresh, re-filter, navigate away) should call it. Cancelling an empty token, or one that names an unknown, already-drained, or already-closed cursor, is a tolerated no-op; the empty `EntryScanCancelResponse` is a bare acknowledgement. A cancel naming a materialised-view (`view-*`) tree is a no-op too: the facade treats a view tree as reserved and returns without closing anything, so an abandoned view scan's cursor is released only by its idle TTL (`LatticeOptions.CursorIdleTtl`).

`GetAuthScheme` is the one **unauthenticated** RPC: it returns the endpoint's advertised authentication schemes (an ordered set of `AuthSchemeDescriptor`) so a client can discover how to sign in before it holds any credential. Each descriptor carries a required `SchemeId` (the stable id a client matches to a login provider, for example `basic` or `entra`), a `DisplayName` (empty by default), and `Parameters` - the public values the provider needs to run the challenge (empty by default). The authorization interceptor exempts this RPC; every other RPC is authorized.

## Status mapping

The service maps facade outcomes onto explicit gRPC statuses rather than letting them fall through to a generic fault. The unary query RPCs share one mapping; `GetMetricsSnapshot` and the two streaming RPCs map a narrower set:

| Exception | gRPC status | Applies to |
|---|---|---|
| `ArgumentException` | `InvalidArgument` | Every RPC: a malformed request, including a continuation or resume token that is malformed or names an unknown, drained, or closed cursor. |
| `LatticeTenantAccessDeniedException` | `PermissionDenied` | Every RPC: fail-closed tenant resolution refused the call. |
| `LatticeAuthorizationDeniedException` | `PermissionDenied` | Every RPC: the core access gate refused the caller the read - for example a `GetMetricsSnapshot` or `ObserveMetrics` sample, which is a whole-tree aggregate, under a grant narrowed to part of the tree. It is not logged as a server fault. |
| `LatticeSaturatedException` | `ResourceExhausted` | The unary query RPCs other than `GetMetricsSnapshot`: the tree is WAL-saturated and shed the operation (for example a snapshot scan open); back off and retry. |
| `LatticeTreeBootstrappingException` | `Unavailable` | The unary query RPCs other than `GetMetricsSnapshot`: the tree's replica is being bootstrapped from a snapshot and its reads are fenced until the import completes; back off and retry. |
| `LatticeStateCursorExpiredException` | `FailedPrecondition` | `ObserveChanges`: the resume token fell outside the write-ahead-log retention window. |
| `KeyNotFoundException` | `NotFound` | `ObserveChanges`: an unknown, system, or unreadable tree. The unary RPCs do not map it, so there it arrives as `Internal`. |
| `OperationCanceledException` | `Cancelled` | The unary RPCs. On `ObserveChanges` and `ObserveMetrics` a cancellation ends the stream cleanly instead. |
| anything else | `Internal` | Logged server-side and returned with a generic message, without echoing the exception text. |

The authorization interceptor's refusal is a `PermissionDenied` as well.

## Messages

Every request and response is a `[GenerateSerializer]` record with a stable `[Alias]` and sequential `[Id]`s, exactly like the core library's wire types. Some records are the public facade DTOs from `Orleans.Lattice.Api.Abstractions`; binding-owned envelopes such as `DeadLetterCountRequest` / `DeadLetterCountResponse` wrap scalar facade arguments and results so the wire contract can grow additively. The binding-owned records carry the `olag.` aliases held in the public `GrpcStateTypeAliases` class; the facade DTOs keep their `ApiStateTypeAliases` aliases. The marshallers serialize them with the Orleans binary serializer, so the client and the server must share an Orleans serializer registration (`AddSerializer()`). The records are public; the service implementation and method binding are internal.

Because the messages are Orleans records rather than protobuf messages, the contract preserves the facade model's fidelity - nullable continuation tokens, `TimeSpan` sample intervals, predicate trees, and value-length metadata all round-trip without a lossy `.proto` projection.

## The public client

`LatticeStateApiGrpcClient` is the public typed client for the binding. It wraps a gRPC `CallInvoker` and exposes one method per RPC:

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();

using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);
```

The client carries **no transport policy of its own**. Address, TLS, retries, deadlines, and call credentials live on the `CallInvoker` / `GrpcChannel` the caller supplies; the client only adds the per-RPC marshalling. Unary RPCs return a `Task<TResponse>`; the streaming RPCs return an `IAsyncEnumerable<TResponse>` you consume with `await foreach`.

## Server options

`LatticeStateApiGrpcOptions` is populated by `AddLatticeStateApiGrpc(configure)` and controls the server-side binding: `RequireAuthorization`, `CredentialHeaderName`, `CredentialScheme`, `ActiveTenantHeaderName` and `AdvertisedAuthSchemes`. Types, defaults and semantics are in the [gRPC binding configuration](../lattice.api.state.grpc/configuration.md); every public type of the binding is listed in its [public API](../lattice.api.state.grpc/api.md), and the request path through the interceptor and service is described in its [architecture](../lattice.api.state.grpc/architecture.md).

See [Client](client.md) for the full set of calls and [Surfaces](surfaces.md) for what each request and response carries.
