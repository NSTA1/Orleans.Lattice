# Orleans.Lattice.Scaling

Opt-in autoscaling signal for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

This package exposes a read-only, cluster-aggregate, two-axis (compute and
storage) pressure snapshot that an external autoscaler can scrape to size the
silo pool. It is additive and off by default: nothing changes until you call
`AddLatticeScalingSignal` on your silo builder.

## How it works

A hosted collector samples cluster compute and storage pressure live, reduces the
compute axis to a replica-demand scalar (scale-out reacts immediately; a falling
scalar decays through an exponentially-weighted moving average and a scale-in
gate), and caches a two-axis `ScalingSignal` snapshot for cheap scrape-path
reads. The storage axis is reported unsmoothed and never feeds the scalar.
`GetScalingSignalAsync` returns the most recent snapshot; until the first
collection completes it reports `Reason = "warming up"`. Alongside the signal
the package ships a health
check (`AddLatticeScalingHealthCheck`) and an HTTP scrape endpoint
(`MapLatticeScalingSignal`) so an external autoscaler can consume it directly.

The public surface - `ILatticeScalingSignal`; the `ScalingSignal`,
`ComputePressure`, `StoragePressure`, `WalAccountPressure`, and
`WalRebalanceRecommendation` snapshots and the `WalPressureClassification` enum;
`LatticeScalingSignalOptions` and `LatticeScalingHealthCheckOptions`; the
registration and endpoint extension classes; `LatticeScalingMetrics`; and
`ScalingTypeAliases` - is stable for downstream integration.

## Getting started

Register the signal on your silo with `AddLatticeScalingSignal`, then resolve
`ILatticeScalingSignal` and call `GetScalingSignalAsync`. See the
[Orleans.Lattice.Scaling docs](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.scaling/README.md)
for details.

## License

MIT. See the repository root for the full license text.
