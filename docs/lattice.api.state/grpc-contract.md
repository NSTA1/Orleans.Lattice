# gRPC Contract

`Orleans.Lattice.Api.State.Grpc` is a **code-first** gRPC binding. There is no hand-written `.proto`: the service, its methods, and its messages are defined in C#, and every message is one of the package's Orleans-serialized request/response records. The server binds the methods; the public `LatticeStateApiGrpcClient` calls them; both sides share the identical marshallers, so the wire format stays in lock-step by construction.

## Service

The service name on the wire is `orleans.lattice.api.state` (so each method's full path is `/orleans.lattice.api.state/<Rpc>`). It exposes twelve RPCs: ten unary and two server-streaming. Each maps one-to-one onto a facade verb.

| RPC | Kind | Request | Response | Surface |
|---|---|---|---|---|
| `ListTrees` | unary | `CatalogRequest` | `TreeCatalogPage` | [Discovery](surfaces.md#discovery) |
| `ListViews` | unary | `CatalogRequest` | `ViewCatalogPage` | [Discovery](surfaces.md#discovery) |
| `ListTagIndexes` | unary | `CatalogRequest` | `TagIndexCatalogPage` | [Discovery](surfaces.md#discovery) |
| `ListTagValues` | unary | `CatalogRequest` | `TagValueCatalogPage` | [Discovery](surfaces.md#discovery) |
| `GetTreeStructure` | unary | `StructureRequest` | `StructureResponse` | [Structure](surfaces.md#structure) |
| `ScanEntries` | unary | `EntryScanRequest` | `EntryScanResponse` | [Entries](surfaces.md#entries) |
| `GetEntry` | unary | `EntryGetRequest` | `EntryGetResponse` | [Entries](surfaces.md#entries) |
| `CancelScan` | unary | `EntryScanCancelRequest` | `EntryScanCancelResponse` | [Entries](surfaces.md#entries) |
| `GetMetricsSnapshot` | unary | `TreeMetricsRequest` | `TreeMetricsSnapshot` | [Metrics](surfaces.md#metrics) |
| `GetClusterInfo` | unary | `ClusterInfoRequest` | `ClusterInfo` | Cluster info |
| `ObserveChanges` | server-streaming | `StateObserveRequest` | `StateChangeNotification` | [Change observation](surfaces.md#change-observation) |
| `ObserveMetrics` | server-streaming | `TreeMetricsRequest` | `TreeMetricsSnapshot` | [Metrics](surfaces.md#metrics) |

`CancelScan` is a best-effort, idempotent cleanup verb: it releases the server-side snapshot cursor named by a scan continuation token, freeing its WAL-retention pin and per-shard baseline promptly rather than waiting for the cursor's idle TTL. A client that abandons a multi-page scan before draining it (refresh, re-filter, navigate away) should call it. Cancelling an empty token, or one that names an unknown, already-drained, or already-closed cursor, is a tolerated no-op; the empty `EntryScanCancelResponse` is a bare acknowledgement.

## Messages

Every request and response is a `[GenerateSerializer]` record with a stable `[Alias]` and sequential `[Id]`s, exactly like the core library's wire types. The marshallers serialize them with the Orleans binary serializer, so the client and the server must share an Orleans serializer registration (`AddSerializer()`). The records are public; the service, the marshallers, and the method definitions are internal.

Because the messages are Orleans records rather than protobuf messages, they carry the full fidelity of the facade model - nullable continuation tokens, `TimeSpan` sample intervals, predicate trees, and value-length metadata all round-trip without a lossy `.proto` projection.

## The public client

`LatticeStateApiGrpcClient` is the only public type in the binding. It wraps a gRPC `CallInvoker` and the internal method definitions, exposing one method per RPC:

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();

using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);
```

The client carries **no transport policy of its own**. Address, TLS, retries, deadlines, and call credentials live on the `CallInvoker` / `GrpcChannel` the caller supplies; the client only adds the per-RPC marshalling. Unary RPCs return a `Task<TResponse>`; the two streaming RPCs return an `IAsyncEnumerable<TResponse>` you consume with `await foreach`.

See [Client](client.md) for the full set of calls and [Surfaces](surfaces.md) for what each request and response carries.
