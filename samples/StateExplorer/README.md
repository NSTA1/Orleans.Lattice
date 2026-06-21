# State Explorer sample

A console tree-explorer for the optional `Orleans.Lattice.Api.State` add-on. It
co-hosts a single-silo Orleans cluster with the read-only state-API gRPC
surface, then connects to it over a real gRPC channel - exactly as a standalone
dashboard or CLI would - and walks the full explorer journey:

1. **Discover** the trees registered in the cluster (`ListTreesAsync`).
2. **Structure** - render a tree's shard-root node graph (`GetTreeStructureAsync`).
3. **Scan** a key range under a snapshot-isolated cursor (`ScanEntriesAsync`).
4. **Tail** live changes: subscribe, write, and watch the mutation surface
   (`ObserveChangesAsync`).

Everything runs in one process for convenience, but the client talks to the
server strictly over gRPC using only the package's public surface
(`LatticeStateApiGrpcClient` plus the wire DTOs), so `Program.cs` doubles as a
copy-paste reference for wiring a real external client.

## Run it

```
dotnet run --project samples/StateExplorer/StateExplorer.csproj
```

The sample seeds a small demo tree, prints each stage of the journey, then
exits. It listens on `http://localhost:5199` over HTTP/2 without TLS (h2c) to
stay dependency-free; a real deployment would terminate TLS and register an
`ILatticeStateApiAuthorizer` instead of disabling authorization.

## What to look at

- `Program.cs` - the silo + gRPC host wiring (`AddLatticeStateApi` /
  `AddLatticeStateApiGrpc` / `MapLatticeStateApiGrpc`) and the client journey.
- The package docs under [`docs/lattice.api.state/`](../../docs/lattice.api.state/README.md)
  cover the surfaces, the security posture, and the efficiency guarantees in
  depth.
