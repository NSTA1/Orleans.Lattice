# Client

`LatticeStateApiGrpcClient` is the public, strongly-typed client for the state-API gRPC surface. It is the consumer half of the [gRPC contract](grpc-contract.md): one method per RPC, over the same Orleans-serialized records the server binds.

## Building a client

`Create` takes a gRPC `CallInvoker` and a service provider that has Orleans serialization registered. The service provider supplies the per-message serializers, so its registration must match the server's (`AddSerializer()`).

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();

using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);
```

The client adds **only** the per-RPC marshalling. Address, TLS, retries, deadlines, and call credentials all live on the `GrpcChannel` / `CallInvoker` you pass in, so apply your transport and auth policy there:

```csharp verify
using Grpc.Net.Client;
using Grpc.Core;

var credentials = CallCredentials.FromInterceptor((_, metadata) =>
{
    metadata.Add("authorization", "Bearer <token>");
    return Task.CompletedTask;
});

using var channel = GrpcChannel.ForAddress("https://cluster.example:5001", new GrpcChannelOptions
{
    Credentials = ChannelCredentials.Create(ChannelCredentials.SecureSsl, credentials),
});
```

## Calling the surface

The unary RPCs return a `Task<TResponse>`; the streaming RPCs return an `IAsyncEnumerable<TResponse>` consumed with `await foreach`. The full set, with the request and response each carries, is in [Surfaces](surfaces.md):

- `ListTreesAsync` / `ListViewsAsync` / `ListTagIndexesAsync` / `ListTagValuesAsync` - paged discovery catalog.
- `GetTreeStructureAsync` - shard-root node graph.
- `ScanEntriesAsync` / `GetEntryAsync` - snapshot-isolated entry inspection.
- `GetEntryHistoryAsync` - per-key change-history timeline.
- `CancelScanAsync` - release a server-side scan cursor early.
- `GetMetricsSnapshotAsync` - one-shot metrics.
- `GetClusterInfoAsync` - connected-cluster identity (cluster id, service id).
- `ObserveChangesAsync` - server-streamed live mutations.
- `ObserveMetricsAsync` - server-streamed live metric deltas.

Cancel a streaming call through its `CancellationToken` to unsubscribe; the server tears the subscription down when the stream ends.

## In-process reuse

The facade is transport-agnostic, so a consumer co-located in the silo - for example a future MCP bridge - does not need a network hop. Co-host the gRPC service in the silo process (`AddLatticeStateApiGrpc` + `MapLatticeStateApiGrpc`) and dial it over an in-process / loopback channel with the same `LatticeStateApiGrpcClient`. The client code is identical to the remote case; only the channel address changes. This is exactly what the package's MCP-reuse parity test exercises: the in-process path and the gRPC path return the same records for the same requests.

The [`StateExplorer`](../../samples/StateExplorer) sample demonstrates the full journey - silo plus gRPC host in one process, a client dialing it over a loopback channel, and a walk through discovery, structure, scan, and a live change tail.

## Next

- [Surfaces](surfaces.md) - each request and response in detail.
- [Security](security.md) - the channel-side credentials the authorizer validates.
