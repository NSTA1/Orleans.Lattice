# Orleans.Lattice.Api.State.Grpc

gRPC transport binding for [`Orleans.Lattice.Api.State`](https://www.nuget.org/packages/Orleans.Lattice.Api.State) - the optional read-only cluster-state API for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

It exposes the transport-agnostic state facade as a small set of unary gRPC RPCs over a code-first, Orleans-serialized contract (no `.proto` / `protoc` toolchain), reusing the same versioned serialization the rest of Lattice uses:

- `ListTrees` / `ListViews` - paged discovery of trees and materialised views.
- `GetTreeStructure` - the bounded, depth-limited structural node graph of a tree.
- `ScanEntries` - snapshot-isolated, key-ordered, paged entry inspection.
- `GetEntry` - the full record for a single key.

## Usage

Co-host the binding with the facade on a silo, then map the endpoints:

```csharp
builder.Services.AddLatticeStateApiGrpc();
builder.Services.AddSingleton<ILatticeStateApiAuthorizer, MyTokenAuthorizer>();

// ... in the endpoint composition:
app.MapLatticeStateApiGrpc();
```

## Security

The surface exposes potentially sensitive structural and entry-level data, so it ships **default-deny**: with the built-in `DenyAllStateApiAuthorizer` and `LatticeStateApiGrpcOptions.RequireAuthorization = true`, every call is rejected with `PERMISSION_DENIED` until a host opts in - either by registering a permissive `ILatticeStateApiAuthorizer` (the bundled `AllowAllStateApiAuthorizer` for trusted-network deployments, or a custom token/secret check) or by setting `RequireAuthorization = false` when an outer authentication boundary already guards the endpoint.

## Contract versioning

The wire messages are Orleans-serialized records with additive-only fields. New fields get new ids; aliases and field numbers are never renumbered, so a newer response decodes cleanly under an older client and vice-versa.
