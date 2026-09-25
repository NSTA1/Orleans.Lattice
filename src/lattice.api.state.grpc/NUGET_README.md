# Orleans.Lattice.Api.State.Grpc

gRPC transport binding for [`Orleans.Lattice.Api.State`](https://www.nuget.org/packages/Orleans.Lattice.Api.State) - the optional read-only cluster-state API for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

It exposes the transport-agnostic state facade as seventeen unary RPCs and two server-streaming RPCs over a code-first, Orleans-serialized contract (no `.proto` / `protoc` toolchain), reusing the same versioned serialization the rest of Lattice uses:

- `ListTrees` / `ListViews` / `ListTagIndexes` / `ListTagValues` / `ListCoveredTrees` / `ListIndexTags` / `ScanTagMembers` - paged discovery of trees, materialised views, and tag indexes.
- `GetTreeStructure` - the bounded, depth-limited structural node graph of a tree.
- `ScanEntries` / `CancelScan` - key-ordered, paged entry inspection (snapshot-isolated by default, or a cheaper live cursor), and early release of an abandoned scan cursor.
- `GetEntry` / `GetEntryHistory` - the full record for a single key, and its change-history timeline.
- `GetDeadLetterCount` / `ListDeadLetters` - read-only strict-mode dead-letter inspection.
- `GetMetricsSnapshot` / `GetClusterInfo` - a one-shot metrics snapshot and the connected cluster's identity.
- `ObserveChanges` / `ObserveMetrics` (server-streaming) - a live mutation feed and a delta-coalesced live metrics feed.
- `GetAuthScheme` - the unauthenticated auth-scheme advertisement.

## Usage

Co-host the binding with the facade on a silo, then map the endpoints:

```csharp
builder.Services.AddLatticeStateApiGrpc();
builder.Services.AddSingleton<ILatticeStateApiAuthorizer, MyTokenAuthorizer>();

// ... in the endpoint composition:
app.MapLatticeStateApiGrpc();
```

## Security

The surface exposes potentially sensitive structural and entry-level data, so it ships **default-deny**: with the built-in `DenyAllStateApiAuthorizer` and `LatticeStateApiGrpcOptions.RequireAuthorization = true`, every protected call (all but the unauthenticated `GetAuthScheme` advertisement) is rejected with `PERMISSION_DENIED` until a host opts in - either by registering a permissive `ILatticeStateApiAuthorizer` (the bundled `AllowAllStateApiAuthorizer` for trusted-network deployments, or a custom token/secret check) or by setting `RequireAuthorization = false` when an outer authentication boundary already guards the endpoint.

## Contract versioning

The wire messages are Orleans-serialized records with additive-only fields. New fields get new ids; aliases and field numbers are never renumbered, so a newer response decodes cleanly under an older client and vice-versa.
