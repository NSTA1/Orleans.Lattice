# Setup

`Orleans.Lattice.Api.State` layers on top of an existing `Orleans.Lattice` silo. There are three registration steps, two of which are optional depending on whether you expose the surface remotely.

## 1. Register the facade on the silo

`AddLatticeStateApi` registers the read-only facade (`ILatticeStateQuery`, `ILatticeStateObserver`, `ILatticeStateMetricsObserver`) and the shared metrics sampler. It must be called **after** `AddLattice`, because it resolves the same per-tree options the core library registers.

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo =>
{
    silo
        .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
        .AddLatticeStateApi();
});
```

With only this step the facade runs **in-process**, with no transport hop. The facade interfaces (`ILatticeStateQuery` and friends) live in the shared `Orleans.Lattice.Api.Abstractions` contract package and are `public`, so a co-located consumer reuses them in one of two ways: either it references that contract package and resolves them from DI directly (the path the co-hosted `Orleans.Lattice.Api.Mcp` server takes), or it co-hosts the gRPC binding in the same process and dials it over a loopback channel, which still avoids a network hop. See [Client](client.md#in-process-reuse) for that path.

## 2. Add the gRPC binding

To expose the surface to remote consumers, register the code-first gRPC binding. `AddLatticeStateApiGrpc` wires the gRPC service, the marshallers, the default-deny authorizer, and the authorization interceptor. It is idempotent.

```csharp verify
var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = true);
```

The binding **fails closed**. With `RequireAuthorization` left at its default and no authorizer registered, the default `DenyAllStateApiAuthorizer` rejects every protected call; only the `GetAuthScheme` advertisement RPC is unauthenticated. Register a real authorizer (or, behind an already-authenticated outer boundary, an `AllowAllStateApiAuthorizer`) before the endpoint serves traffic - see [Security](security.md).

```csharp verify
var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeStateApiGrpc();
builder.Services.AddSingleton<ILatticeStateApiAuthorizer, AllowAllStateApiAuthorizer>();
```

## 3. Map the endpoint routes

`MapLatticeStateApiGrpc` registers the gRPC service routes on the ASP.NET Core endpoint table. Call it on the built application.

```csharp verify
var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeStateApiGrpc();

var app = builder.Build();
app.MapLatticeStateApiGrpc();
```

## Putting it together

A minimal silo that hosts both the lattice and its remote state API in one process:

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo =>
{
    silo
        .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
        .AddLatticeStateApi();
});

builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeStateApiAuthorizer, AllowAllStateApiAuthorizer>();

var app = builder.Build();
app.MapLatticeStateApiGrpc();
```

The [`StateExplorer`](../../samples/StateExplorer) sample shows this wiring as a runnable program, including the client side.

## Next

- [gRPC Contract](grpc-contract.md) - the service, RPCs, and wire records the binding exposes.
- [Security](security.md) - configuring the authorization seam and the transport.
- [Client](client.md) - consuming the surface remotely or in-process.
