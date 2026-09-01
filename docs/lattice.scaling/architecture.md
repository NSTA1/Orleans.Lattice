# Architecture

How the autoscaling signal is collected, aggregated, smoothed, and gated, and why
the storage axis can never move a replica.

## Shape

```
             per silo, every SampleInterval
  +---------------------------------------------------+
  |  compute-pressure collector  storage-pressure collector
  |     |                              |
  |     v                              v
  |  ComputePressure               StoragePressure
  |     \                              /
  |      \                            /
  |       v                          v
  |            scaling-signal computer
  |          (max-dimension, EWMA, scale-in gate, floor)
  |                     |
  |                     v
  |               ScalingSignal  (cached)
  +---------------------------------------------------+
                        |
        per scrape (cheap, cached read)
                        v
     ILatticeScalingSignal / HTTP endpoint / health check
```

The live facade (`ILatticeScalingSignal`) runs as an `IHostedService`. On a timer
(`SampleInterval`) it samples both collectors, folds them through the
scaling-signal computer, and caches the resulting `ScalingSignal`. Every scrape -
whether from the HTTP endpoint, the health check, or a direct
`GetScalingSignalAsync` call - reads the cached snapshot, so scrapes are cheap and
never fan out to the cluster.

## Compute axis

The compute-pressure collector produces a cluster-aggregate `ComputePressure` with
three normalised dimensions, each `0.0` (idle) to `1.0` (saturated):

- **Activation** - the per-silo grain-activation working set from Orleans
  `SiloRuntimeStatistics.ActivationCount` (read via the management grain),
  normalised against `ActivationWorkingSetTarget`.
- **Resource** - the worst-case of CPU and available-memory headroom across the
  silo pool, from Orleans `EnvironmentStatistics` (cgroup-aware).
- **WAL dispatch** - how close the WAL append-dispatch pipeline is to its
  admission ceiling, derived from the WAL saturation signal.

It also carries the worst-case `WalSaturationState` across every tree and
partition, so a hard `Saturated` state can gate scale-in and drive the health
check independently of the ratios.

## Aggregating to a scalar

The scaling-signal computer reduces the compute axis to one replica-demand scalar:

1. **Dominant dimension, not sum.** The raw scalar is
   `max(activation, resource, walDispatch) * replicaCount`. Taking the maximum
   (not the sum) keeps the bottleneck unambiguous: the scalar reflects the single
   most-constrained dimension, so one saturated dimension is enough to justify
   scale-out without three half-loaded dimensions masquerading as one hot one.
   Multiplying by the current replica count expresses demand in replica-units -
   if every replica is at pressure `p`, the cluster needs about `p * replicas`
   replicas' worth of capacity.
2. **Asymmetric smoothing.** Scale-out is fast-attack: when the raw scalar rises
   the computer snaps to it immediately and re-baselines the EWMA. Scale-in is
   slow-release: a falling scalar is only allowed to descend through an
   exponentially-weighted moving average with half-life `EwmaHalfLife`. This
   makes the signal quick to ask for capacity and reluctant to give it back.
3. **Gated scale-in.** Even a smoothed decline is only published once every
   scale-in precondition has held continuously for `ScaleInGateWindow`: all three
   compute dimensions below their scale-in thresholds, WAL `Healthy`, and no
   shard split in flight. Any break resets the window. Until the gate opens the
   computer holds the previous scalar.
4. **Floor.** `RecommendedReplicas` is `max(MinReplicas, ceil(finalScalar))`, so
   the recommendation never drops below the configured minimum.

`ScaleValue` carries the smoothed, gated scalar an autoscaler should act on;
`RawScaleValue` exposes the un-smoothed instantaneous demand for observability;
`Reason` names the dominant dimension and the decision (warming up, scaling out,
holding, or scaling in).

## Cluster-aggregate answering

The signal is a *cluster* answer, not a per-silo one: the compute collector reads
cluster-wide runtime statistics through the management grain, so any silo that
serves a scrape returns the same aggregate view. This is why a KEDA rule can
point at any replica's endpoint and read a coherent whole-cluster `scaleValue`.

## Storage axis, and the invariant

The storage-pressure collector produces `StoragePressure` independently: an
over-threshold flag, aggregate retained WAL bytes, a per-account
`WalAccountPressure` breakdown, and an optional `WalRebalanceRecommendation`. See
[storage pressure](storage-pressure.md) for the classification and remediation
detail.

**The storage axis never contributes to `ScaleValue`.** The computer takes the
storage pressure as an input and carries it through onto the published signal
untouched - the scalar is a pure function of the compute axis. This is a
deliberate invariant: WAL storage saturation is a per-account throughput or
capacity problem that more silos cannot fix, so folding it into the replica
demand would cause runaway compute scale-out against a storage bottleneck. The
invariant is locked down by a dedicated regression test
(`StorageNeverScalesComputeTests`) that drives the storage axis to its extremes
and asserts `ScaleValue` is unchanged.

## Health check

`AddLatticeScalingHealthCheck` projects the cached signal onto a single
`HealthStatus`: the worst compute dimension against a tiered bound, the discrete
WAL-saturation classification, and the storage over-threshold flag (which
contributes at most `Degraded`, honouring the invariant). See
[configuration](configuration.md#latticescalinghealthcheckoptions).

## Split-aware scale-in

The scale-in gate is split-aware: while any adaptive shard split is in flight
anywhere in the cluster, scale-in is suppressed. Relocating load off a silo while
a shard is mid-split risks stranding the split's in-flight work, so the gate
holds the previous scalar until every split completes and the window has elapsed
again. Only scale-**in** is affected - scale-out is never influenced by split
activity.

The signal comes from the cluster's split-admission singleton, read once per
sample tick through `ILatticeAdmin.GetSplitActivityAsync`. Each per-tree
autonomic monitor publishes its authoritative in-flight count (derived from shard
`IsSplitting` status) to that singleton, so the query costs one call and never
fans out across trees or shards. Publication is edge-triggered - a tree reports
only while it actually has splits in flight, plus one call to clear its footprint
when they finish - so an idle cluster adds no traffic. Footprints carry a
time-to-live, so a silo lost mid-split has its share reclaimed on expiry rather
than suppressing scale-in indefinitely.

Because the count is sampled once per monitor pass, it is a lower bound that
trails reality by at most one `HotShardSampleInterval`; splits a monitor triggers
are published in the same pass that starts them, so the gate never misses a split
it caused. A deployment with autonomic splitting disabled always reports zero.

Degradation is deliberately **fail-open**: if the admin surface is unreachable,
or the package is hosted outside a silo, the probe reports "no split in flight"
rather than throwing. Reporting the opposite would be fail-closed, but a
persistently unreachable admin surface would then suppress scale-in forever -
turning a small, self-correcting risk (a silo drained mid-split, which costs that
split some rework but no correctness) into an unbounded cost ceiling. The failure
is logged so the degradation is visible.

Set `SplitAwareScaleIn` to `false` to make the axis inert - appropriate for a
deployment with autonomic splitting disabled, where the query would be pure
overhead. The gate then relies on the WAL-healthy and all-dimensions-low
preconditions plus the window alone, exactly as it did before this signal
existed.

## Current limitations

- **Storage sampling path.** The default storage source reads retained bytes and
  WAL placement through the public `ILatticeAdmin` surface
  (`GetTotalStorageUsageAsync` and `GetWalPlacementAsync`), which activates each
  shard root but never walks leaves, rather than a WAL-only per-tree accessor
  (which is internal to the core package). Per-partition retained bytes are
  approximated by dividing a tree's retained bytes across its partitions. This is
  accurate enough for an advisory signal; it is isolated behind an internal
  source seam so it can be swapped for a WAL-only accessor without touching the
  collector.
