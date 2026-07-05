# Orleans.Lattice.Api.Data.Grpc

Code-first gRPC transport binding for
[Orleans.Lattice.Api.Data](https://www.nuget.org/packages/Orleans.Lattice.Api.Data).
Projects the write-capable external data-plane facade onto a flat set of unary
gRPC RPCs so non-.NET clients can set, delete, atomically batch (single-tree and
cross-tree), point-read, and bounded-range-read tree entries.

## RPCs

| RPC | Facade method |
|-----|---------------|
| `Set` | `SetAsync` |
| `Delete` | `DeleteAsync` |
| `SetManyAtomic` | `SetManyAtomicAsync` |
| `SetManyAtomicCrossTree` | `SetManyAtomicCrossTreeAsync` |
| `Get` | `GetAsync` |
| `ReadRange` | `ReadRangeAsync` |

## Security

- **Opt-in and absent by default.** Nothing is registered unless the host calls
  `AddLatticeDataApiGrpc()` and `MapLatticeDataApiGrpc()`.
- **Default-deny transport gate.** The `ILatticeDataApiAuthorizer` coarse gate
  defaults to `DenyAllDataApiAuthorizer`; every call is rejected with
  `PermissionDenied` until the host registers a permissive authorizer or turns
  `RequireAuthorization` off.
- **Identity bridge.** A header-based `ILatticeDataApiCredentialBridge` (default
  header `authorization`, `Bearer` scheme stripped) lifts the caller credential
  onto the ambient context so the gated `ILattice` surface enforces per-tree /
  per-key authorization.
- **Gate-denial mapping.** A `LatticeAuthorizationDeniedException` from the data
  plane maps to gRPC `PermissionDenied`, carrying only the non-sensitive tree /
  operation / subject / reason fields as trailers - never a value.

## Registration

```csharp
builder.Services.AddLatticeDataApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeDataApiAuthorizer, MyAuthorizer>();
// ...
app.MapLatticeDataApiGrpc();
```
