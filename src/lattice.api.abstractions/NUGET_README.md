# Orleans.Lattice.Api.Abstractions

Shared **contract** package for the
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice) API facades. It
holds the transport-agnostic service interfaces and their request / response
models, and nothing else - no implementation, no registration, no background
work.

## Design

The API facades (`Orleans.Lattice.Api.State`, `.Api.Data`, `.Api.Auth`,
`.Api.Backup`) each expose a single transport-agnostic service surface that a
transport binding projects onto a wire protocol. Two families of package
consume those surfaces: the code-first gRPC bindings and the
`Orleans.Lattice.Api.Mcp` server.

This package is the seam between them. It carries:

- **The service interfaces** - `ILatticeStateQuery`, `ILatticeStateObserver`,
  `ILatticeStateMetricsObserver`, `ILatticeDataApi`, `ILatticeAuthAdmin`,
  `ILatticeBackupControl`, and `ILatticeSchemaControl`.
- **Their request / response models** - the results, pages, records, and
  requests those interfaces exchange, with their stable Orleans serialization
  aliases.

The facade packages reference this package and implement the interfaces; the
binding packages reference this package and consume them. Publishing the
contract as a real, versioned public surface keeps `internal` meaning
"safe to change" inside each facade, and lets a binding evolve against a
stable contract rather than another package's internals.

## Usage

You do not register anything from this package directly. Register a facade
(which implements these contracts) and a binding (which consumes them):

```csharp
siloBuilder
    .AddLattice(/* ... */)
    .AddLatticeStateApi()      // implements ILatticeStateQuery
    .AddLatticeStateApiGrpc(); // binds a gRPC surface over it
```
