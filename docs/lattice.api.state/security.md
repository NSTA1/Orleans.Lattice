# Security

The state API is a read-only surface, but read-only is not the same as public. Tree ids, key ranges, value previews, and live mutation feeds are sensitive, so the gRPC binding **fails closed**: every call is authorized, and the default posture denies everything.

## The authorization seam

`ILatticeStateApiAuthorizer` is the single per-call authorization seam. The binding installs an interceptor that calls it on every unary and streaming RPC before the request reaches the service. The package ships two reference implementations:

- `DenyAllStateApiAuthorizer` - rejects every call. This is the **default**, registered with `TryAdd` so it only applies when you have not registered your own.
- `AllowAllStateApiAuthorizer` - accepts every call. Use it only behind an already-authenticated outer boundary (a service mesh, a gateway, or mutual-TLS termination that has already established trust).

`AddLatticeStateApiGrpc` registers `DenyAllStateApiAuthorizer` via `TryAddSingleton`, so a custom authorizer registered before or after it wins.

The seam describes each call faithfully: the `LatticeStateApiAuthorizationContext` carries the specific `LatticeStateApiOperation` (every RPC maps to its own member) and the `TargetTreeId` the call acts on, so a policy can scope by operation and by tree. A method the binding does not recognise maps to `LatticeStateApiOperation.Unknown` rather than a benign default, so a deny-by-default policy refuses anything unmapped instead of letting it pass as a catalog read.

The three index-wide tag-browsing operations (`ListCoveredTrees`, `ListIndexTags`, `ScanTagMembers`) span every tree a tag index covers, so they present a `null` `TargetTreeId` - they authorize at the cluster / index level like `ListTrees` and `ListViews`, not per subject tree. A policy that grants tag-index browsing therefore grants it across all covered trees; scope it by operation (deny these three) rather than by target tree if a tenant must not see cross-tree membership. The subject-tree-scoped `ListTagValues` (which does carry a `TargetTreeId`) remains available for per-tree tag enumeration.

### Turnkey credential authorizer

`AddEnvVarCredentialAuthorizer` registers a reference `EnvVarCredentialAuthorizer` that validates an inbound `authorization: Basic base64(user:pass)` header against environment-variable-backed PBKDF2-SHA256 password hashes, with a per-username failed-attempt lockout. Because it reads a `Basic` credential off the wire, it **must run behind TLS** - terminate TLS at the channel (or an outer boundary) so the credential is never sent in clear text. It is an authentication front door, not a per-tree authorization policy: it does not consult the call's operation or target tree.

## Visibility boundaries

Silo-internal **system trees** (the reserved `_lattice_*` prefix) are hidden from every public surface. The read facade refuses them (`GetEntry`, `ScanEntries`, `GetTreeStructure`, `GetEntryHistory`) and the change feed (`ObserveChanges`) refuses a subscription to one, so internal WAL keys, change kinds, and HLC timestamps never leak through the API. Materialised-view (`view-*`) trees stay readable and observable, mirroring the read paths.

`CatalogRequest.IncludeSystemTrees` is an **operator-convenience filter, not a security boundary**: it only adds reserved trees to a `ListTrees` catalog listing for diagnostics. It does not unlock reading or observing their contents - those paths reject system trees regardless of the flag - and it must not be relied on to gate access.

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
