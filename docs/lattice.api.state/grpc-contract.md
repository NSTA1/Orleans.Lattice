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

`CancelScan` is a best-effort, idempotent cleanup verb: it releases the server-side snapshot cursor named by a scan continuation token, freeing its WAL-retention pin and per-shard baseline promptly rather than waiting for the cursor's idle TTL. A client that abandons a multi-page scan before draining it (refresh, re-filter, navigate away) should call it. Cancelling an empty token, or one that names an unknown, already-drained, or already-closed cursor, is a tolerated no-op; the empty `EntryScanCancelResponse` is a bare acknowledgement.

`GetAuthScheme` is the one **unauthenticated** RPC: it returns the endpoint's advertised authentication schemes (an ordered set of `AuthSchemeDescriptor`) so a client can discover how to sign in before it holds any credential. The authorization interceptor exempts it; every other RPC is authorized.

## Messages

Every request and response is a `[GenerateSerializer]` record with a stable `[Alias]` and sequential `[Id]`s, exactly like the core library's wire types. Some records are the public facade DTOs from `Orleans.Lattice.Api.Abstractions`; binding-owned envelopes such as `DeadLetterCountRequest` / `DeadLetterCountResponse` wrap scalar facade arguments and results so the wire contract can grow additively. The marshallers serialize them with the Orleans binary serializer, so the client and the server must share an Orleans serializer registration (`AddSerializer()`). The records are public; the service implementation and method binding are internal.

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

`LatticeStateApiGrpcOptions` is populated by `AddLatticeStateApiGrpc(configure)` and controls the server-side binding.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Enforce `ILatticeStateApiAuthorizer` on inbound protected state-API calls. The default is fail-closed; set to `false` only behind an outer authentication boundary. |
| `CredentialHeaderName` | `string` | `"authorization"` | Request-header (gRPC metadata) name carrying the caller credential token to bridge into the ambient Lattice credential. |
| `CredentialScheme` | `string` | `"Bearer"` | Authentication scheme stamped on the bridged `LatticeCredential`; a matching scheme prefix on the header value is stripped before the token is used. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor>` | Empty `List<AuthSchemeDescriptor>` | Public auth-scheme descriptors advertised by the unauthenticated `GetAuthScheme` RPC, in preference order. |

See [Client](client.md) for the full set of calls and [Surfaces](surfaces.md) for what each request and response carries.
