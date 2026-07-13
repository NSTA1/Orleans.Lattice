# Orleans.Lattice.Scaling

Opt-in autoscaling signal for Orleans.Lattice: a read-only, cluster-aggregate,
two-axis (compute and storage) pressure snapshot that an external autoscaler can
scrape to size the silo pool.

> Status: package skeleton. The facade currently returns a well-formed zero/stub
> signal so downstream work (the compute collector, the storage axis, and the
> HTTP endpoint) can build against a stable surface. This document is a scaffold;
> the coordinator authors the full guide.

## Overview

The signal has two axes:

- Compute axis (`ComputePressure`): normalised activation, host-resource, and
  WAL-dispatch pressure, plus the worst-case WAL saturation state.
- Storage axis (`StoragePressure`): whether retained WAL bytes crossed a
  threshold, the aggregate retained bytes, a per-catalogue-key breakdown
  (`WalAccountPressure`), and an optional rebalance suggestion
  (`WalRebalanceRecommendation`).

Both axes roll up into a single `ScalingSignal` carrying a scale demand
(`ScaleValue`, in replica-units), a concrete `RecommendedReplicas` count, a
human-readable `Reason`, and a `SampledAt` timestamp.

## Public surface

- `ILatticeScalingSignal` - the read-only facade; call `GetScalingSignalAsync`.
- `ScalingSignal`, `ComputePressure`, `StoragePressure`, `WalAccountPressure`,
  `WalRebalanceRecommendation` - the snapshot DTOs.
- `LatticeScalingSignalOptions` - configuration (endpoint path, replica floor).
- `LatticeScalingServiceCollectionExtensions.AddLatticeScalingSignal` - the
  opt-in silo registration.

## Getting started

Registration and usage examples are authored with the full guide once live
collection lands. For now, call `AddLatticeScalingSignal` on the silo builder to
register the facade, then resolve `ILatticeScalingSignal` from the container.

## Related

- Package overview: `src/lattice.scaling/NUGET_README.md`.
- Tracking: epic F-183 and issues #1185 through #1188 on GitHub.
