# Orleans.Lattice.Scaling

Opt-in autoscaling signal for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

This package exposes a read-only, cluster-aggregate, two-axis (compute and
storage) pressure snapshot that an external autoscaler can scrape to size the
silo pool. It is additive and off by default: nothing changes until you call
`AddLatticeScalingSignal` on your silo builder.

## Status

This is the package skeleton. The facade returns a well-formed zero/stub signal
(`Reason = "not yet collecting"`); live pressure collection, the storage axis,
and the HTTP endpoint are added by follow-up work. The public surface
(`ILatticeScalingSignal`, `ScalingSignal`, `ComputePressure`, `StoragePressure`,
`WalAccountPressure`, `WalRebalanceRecommendation`, `LatticeScalingSignalOptions`)
is stable for downstream integration.

## Getting started

Register the signal on your silo with `AddLatticeScalingSignal`, then resolve
`ILatticeScalingSignal` and call `GetScalingSignalAsync`. See the
[Orleans.Lattice.Scaling docs](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.scaling/README.md)
for details.

## License

MIT. See the repository root for the full license text.
