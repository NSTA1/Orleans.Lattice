# Orleans.Lattice.Api.Replication configuration

The package has one public options type, `LatticeApiReplicationOptions`, bound through `AddLatticeReplicationApi(configure)` and resolvable via `IOptions<LatticeApiReplicationOptions>`.

## `LatticeApiReplicationOptions`

The facade currently exposes no tunable knobs. The type is the stable registration front door: it lets later work add configuration without changing the `AddLatticeReplicationApi` signature. Register the facade with no options today:

```csharp
siloBuilder
    .AddLatticeReplication(/* ... */)
    .ReplicateLatticeReplicationConfig()
    .AddLatticeReplicationApi();
```

## What is configured elsewhere

This facade drives the replication config authority but does not re-expose its configuration.

- The **static seed / fallback** replicated-tree set and the local cluster identity are configured on [`Orleans.Lattice.Replication`](../lattice.replication/configuration.md) through `LatticeReplicationOptions` (`ReplicatedTrees`, `ClusterId`).
- Which trees are **runtime-enabled** is not configuration at all: it is authored through this facade at runtime and distributed as the `sys-replication-config` tree. See [runtime replication configuration](../lattice.replication/runtime-config.md).
- Transport concerns - authorization enforcement, credential headers, TLS, advertised auth schemes - live on the [gRPC binding](../lattice.api.replication.grpc/configuration.md), not here.
