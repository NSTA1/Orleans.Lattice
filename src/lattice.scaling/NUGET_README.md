# Orleans.Lattice.Scaling

Opt-in autoscaling signal for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

This package exposes a read-only, cluster-aggregate, two-axis (compute and
storage) pressure snapshot that an external autoscaler can scrape to size the
silo pool. It is additive and off by default: nothing changes until you call
`AddLatticeScalingSignal` on your silo builder.

## How it works

A hosted collector samples cluster compute and storage pressure live, smooths
each axis with an exponentially-weighted moving average, and caches a two-axis
`ScalingSignal` snapshot for cheap scrape-path reads. `GetScalingSignalAsync`
returns the most recent snapshot; until the first collection completes it
reports `Reason = "warming up"`. Alongside the signal the package ships a health
check (`AddLatticeScalingHealthCheck`) and an HTTP scrape endpoint
(`MapLatticeScalingSignal`) so an external autoscaler can consume it directly.

The public surface (`ILatticeScalingSignal`, `ScalingSignal`, `ComputePressure`,
`StoragePressure`, `WalAccountPressure`, `WalRebalanceRecommendation`,
`LatticeScalingSignalOptions`) is stable for downstream integration.

## Getting started

Register the signal on your silo with `AddLatticeScalingSignal`, then resolve
`ILatticeScalingSignal` and call `GetScalingSignalAsync`. See the
[Orleans.Lattice.Scaling docs](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.scaling/README.md)
for details.

## License

MIT. See the repository root for the full license text.
