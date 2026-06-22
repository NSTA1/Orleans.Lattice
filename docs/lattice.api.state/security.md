# Security

The state API is a read-only surface, but read-only is not the same as public. Tree ids, key ranges, value previews, and live mutation feeds are sensitive, so the gRPC binding **fails closed**: every call is authorized, and the default posture denies everything.

## The authorization seam

`ILatticeStateApiAuthorizer` is the single per-call authorization seam. The binding installs an interceptor that calls it on every unary and streaming RPC before the request reaches the service. The package ships two reference implementations:

- `DenyAllStateApiAuthorizer` - rejects every call. This is the **default**, registered with `TryAdd` so it only applies when you have not registered your own.
- `AllowAllStateApiAuthorizer` - accepts every call. Use it only behind an already-authenticated outer boundary (a service mesh, a gateway, or mutual-TLS termination that has already established trust).

`AddLatticeStateApiGrpc` registers `DenyAllStateApiAuthorizer` via `TryAddSingleton`, so a custom authorizer registered before or after it wins.

## Default-deny posture

With `AddLatticeStateApiGrpc` and nothing else, `LatticeStateApiGrpcOptions.RequireAuthorization` is at its default and the default-deny authorizer is in place, so the endpoint rejects all traffic. You open it one of two ways:

Register a real authorizer that validates the caller (a token, a client certificate, a claim):

```csharp verify
var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeStateApiAuthorizer, AllowAllStateApiAuthorizer>();
```

Or, when an outer boundary already guards the endpoint, turn enforcement off explicitly:

```csharp verify
var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = false);
```

The key property is that **neither happens by accident**. An operator who forgets to configure authorization gets a closed door, not an open one.

## Transport

The client carries no transport policy: TLS, deadlines, retries, and call credentials all live on the `GrpcChannel` / `CallInvoker` the caller supplies (see [Client](client.md)). In production, terminate TLS at the channel and authenticate the caller through the authorizer.

For local development and the [`StateExplorer`](../../samples/StateExplorer) sample, the surface runs over HTTP/2 without TLS (h2c) to stay dependency-free - acceptable on a loopback address, not in production.

## Next

- [Client](client.md) - configuring the channel and credentials.
- [Setup](setup.md) - where authorization registration sits in the wiring.
