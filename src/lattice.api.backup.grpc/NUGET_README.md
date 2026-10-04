# Orleans.Lattice.Api.Backup.Grpc

Optional, opt-in **gRPC transport binding** for
[Orleans.Lattice.Api.Backup](https://github.com/NSTA1/Orleans.Lattice) - the
transport-agnostic backup / restore control facade.

It exposes the facade as a code-first, Orleans-serialized gRPC service and ships
a strongly-typed `LatticeBackupApiGrpcClient` that re-exposes the remote-safe
subset of the control surface over the wire: capturing full, incremental, and
backup-set backups, listing the catalog (paged unary and bounded-memory
server-streaming), describing a restore chain, deleting a backup, accept-then-poll
backup and restore operations with progress and cancellation, reverting,
streaming a backup's artifacts back chunk-wise, scheduling, scope status,
capability probing, backup health, and tracked catalog rebuild / scrub. Inventory
and the blocking catalog rebuild / scrub methods stay in-process only. The blocking create, restore, and health-check client methods are deprecated and
will be removed in the next major version; migrate at
https://nsta1.github.io/Orleans.Lattice/docs/lattice.api.backup/operations.html#migrating-from-the-blocking-verbs. Every wire message rides the Orleans
serializer, so the contract stays versioned and additive-only.

Wiring is two calls on the co-hosting silo:

```csharp
builder.Services.AddLatticeBackupApiGrpc(o => o.RequireAuthorization = true);
app.MapLatticeBackupApiGrpc();
```

The binding is **default-deny**: until the host registers a permissive
`ILatticeBackupApiAuthorizer` (or turns enforcement off behind an outer
authentication boundary), every call is rejected. The unauthenticated
`GetAuthScheme` discovery RPC is the single exemption, so a client can learn how
to sign in before it holds a credential.
