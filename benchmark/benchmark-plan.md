# Benchmark Plan: Load-testing Orleans.Lattice with the Vehicle Fleet Simulator

This document captures the plan for using the Vehicle Fleet Simulator as a sustained, realistic
workload generator against [`Orleans.Lattice`](file:///C:/dev/lattice) and its companion
`Orleans.Lattice.Replication` package. The simulator produces a large population of independent,
long-lived vehicle grains, each emitting telemetry at a steady cadence. That shape — many
key-disjoint producers writing small payloads at a fixed rate — is a close match for the workloads
Lattice is designed to absorb (sorted distributed B+ tree, per-tree change feed, per-peer HLC
replication cursor), and lets us exercise both the core primitive and the replication engine under
the same harness. The offered load is deliberately treated as a tunable: the goal is to find knees,
saturation points, and failure modes, not to hit a fixed throughput number. Each scenario below is
an independent experiment with its own success signal; runs are intended to be reproducible from
the `LoadHarness` project plus configuration of the `ITelemetrySink` implementation that targets
Lattice.

## Benchmarks

- [ ] **B-01: Simulator baseline (no Lattice).**
  Run the simulator end-to-end against the existing Orleans stream sink with replication and
  Lattice fully out of the picture. Captures host CPU, ThreadPool, GC, per-tick grain latency, and
  tick-skew (how far behind 1 Hz each `VehicleGrain` actually runs). Establishes the upper bound
  the producer side can sustain on the test hardware, so later runs can distinguish "Lattice is
  slow" from "the simulator is saturated."

- [ ] **B-02: `ILattice` micro-benchmark from `LoadHarness`.**
  Bypass the simulator entirely. Sweep concurrency × key cardinality × value size against
  `SetAsync`, `GetAsync`, and `EntriesAsync` directly. Compare results to
  `docs/lattice/benchmarks.md`. Characterizes the primitive in isolation and gives a reference
  curve for interpreting later end-to-end runs.

- [ ] **B-03: Current-state tree, replication off.**
  Wire a `LatticeSink` that maps `key = vehicleId.ToString()` and
  `value = serialize(VehicleSnapshot)` against a single tree, single cluster, replication
  disabled. Each tick is one `SetAsync`. Measures Lattice's steady-state write throughput and
  latency under uniform key distribution (Guid hashing) at the simulator's offered load.

- [ ] **B-04: Current-state tree, replication on, single peer.**
  Same wiring as B-03 with `Orleans.Lattice.Replication` enabled and one downstream cluster. Track
  `IMutationObserver` overhead (compare write-path p99 vs. B-03), WAL append rate, ship-loop
  throughput, ack RTT, and per-peer HLC cursor lag (`hlc.now − cursor`). Validates the roadmap's
  sub-second flush-latency claim and the F-035 "zero-cost when no observer registered" guarantee.

- [ ] **B-05: Skewed-key variant to force adaptive shard splits.**
  Re-run B-03 with keys prefixed by a deliberately oversubscribed bucket
  (e.g. `region/vehicleId` with one region holding the majority of the fleet) so a single shard
  goes hot. Watch for F-011 autonomic splits firing online, and confirm reads/writes/scans remain
  consistent across the split (the property the chaos suite asserts). Without skew, default
  `ShardCount = 64` plus Guid hashing keeps load uniform and the split monitor never engages.

- [ ] **B-06: Replication backpressure and catch-up.**
  Building on B-04, pause the receiving cluster for a controlled interval while the simulator
  keeps writing, then resume. Measure WAL growth during the pause, time-to-converge after resume,
  and that the per-peer cursor advances strictly on ack. Exercises cursor durability and the
  janitor's GC predicate (R-061).

- [ ] **B-07: Receiver crash mid-stream.**
  Building on B-04, hard-kill the receiver silo during steady-state replication. Verifies
  idempotent replay from the durable HLC cursor and that no replog entries are lost or
  double-applied.

- [ ] **B-08: Two-cluster bidirectional replication.**
  Split the fleet across two clusters, each replicating to the other. Probes `OriginClusterId`
  cycle-break (F-036) — the design item the upstream sample explicitly got wrong — by confirming
  writes do not echo back to their origin and HLC cursors stabilize on both sides.

- [ ] **B-09: Per-key replication filter cost.**
  Re-run B-04 with a non-trivial per-key filter (R-012) on the producer side. Measures the
  inline filter's contribution to write-path latency. If the core observer-latency histogram
  (G-013) is shipped, capture it; if not, this run motivates landing it.

- [ ] **B-10: Event-log tree with TTL (separate run).**
  Alternative key shape: `key = vehicleId/yyyyMMddTHHmmss.fff`, `value = VehicleTelemetryEvent`,
  with a TTL of e.g. 1 hour via the F-016 `SetAsync(ttl)` overload. Stresses ordered scans
  (`ScanKeysAsync` / `EntriesAsync`), continuous tombstone compaction, and the read-path
  expiry filter. Run independently of throughput experiments — compaction will distort the
  latency tail and conflate signals if mixed with B-03/B-04.

- [ ] **B-11: Streaming bulk-load ingest variant.**
  For the event-log shape (B-10), feed the simulator's output through `BulkLoadAsync` streaming
  mode instead of per-event `SetAsync`. Measures per-shard parallel flushing and gives a second
  data point for high-cardinality append workloads versus the point-write path.

- [ ] **B-12: Observer-off vs. observer-on delta.**
  Controlled A/B of identical simulator load with `IMutationObserver` unregistered vs. registered
  (no-op). Isolates observer-dispatch cost on the hot write path. Pairs with B-04 / B-09 to
  attribute latency between dispatch overhead, filter cost, and downstream replication work.

## Cross-cutting requirements

These apply to every benchmark above and should be verified before kicking off a run.

- OpenTelemetry wired to the `orleans.lattice` meter (shard counters, leaf-latency histograms,
  cache hit/miss, replication WAL/HWM gauges).
- Warm-up window excluded from measurement: `AddVehicleBatch` fan-out at full fleet size takes
  meaningful time; capture steady state only.
- Telemetry sink writes happen **off** the `VehicleGrain` turn, so grain-tick latency is
  attributable to the simulator and not to Lattice.
- `FleetGrain.GetFleetStats` continues to use the in-grain aggregator — never back it with a
  Lattice scatter-gather scan, which would dominate the measurement.
- For any cross-cluster scenario (B-04, B-06, B-07, B-08, B-09), use at least two physical hosts;
  single-box replication runs are smoke tests only.

## Simulator Integration

This section documents the concrete attach points the simulator exposes so each scenario above
can be wired up without modifying grain code. The seam is intentionally narrow: a single DI
interface, registered once per silo. Tests in `VehicleFleetSimulator.Tests` lock in the contracts
described below (`NullTelemetrySinkTests`, `FanOutTelemetrySinkRoutingTests`,
`TelemetrySinkSwappabilityTests`) — any change here that breaks these contracts will fail CI.

### 1. The `ITelemetrySink` seam

`VehicleFleetSimulator.Abstractions.ITelemetrySink` is the only path `VehicleGrain` writes
telemetry through. Two methods, both fire-and-forget at the producer:

```csharp
ValueTask PublishTelemetryAsync(VehicleTelemetryEvent telemetry, CancellationToken ct = default);
ValueTask PublishEventAsync(VehicleEvent vehicleEvent, CancellationToken ct = default);
```

The interface is deliberately minimal: no batching API, no flush hook, no completion callback. A
`LatticeSink` implementation is expected to do its own batching, off-turn dispatch, and failure
handling, because:

- Returning `ValueTask` lets the sink complete synchronously when the work is purely "enqueue to a
  bounded channel" — the steady-state hot path stays allocation-free.
- The producer must not be blocked by a slow downstream. A correct sink MUST NOT couple its
  own latency to the `VehicleGrain` turn (see §4 below).

### 2. Sink selection at silo startup

Sinks are registered as a DI singleton in the silo's service collection. There is no
configuration-driven sink switching at runtime; each benchmark run is started with the sink
registration appropriate to the scenario.

| Scenario | Registration |
|---|---|
| B-01 producer baseline | `services.AddSingleton<ITelemetrySink, NullTelemetrySink>(_ => NullTelemetrySink.Instance);` |
| Default app run, B-02 (n/a — harness path) | `services.AddSingleton<ITelemetrySink, FanOutTelemetrySink>();` |
| B-03 onward (Lattice) | `services.AddSingleton<ITelemetrySink, LatticeSink>();` |
| B-12 observer-off control | `NullTelemetrySink` for the off run; `LatticeSink` for the on run |

`Program.cs` in `VehicleFleetSimulator.Silo` is the single registration point. The replacement
must be exclusive — registering a second `ITelemetrySink` does not chain (verified by
`TelemetrySinkSwappabilityTests.The_default_FanOutTelemetrySink_is_overridden_not_chained`).

### 3. The `LatticeSink` shape (informative)

The eventual `LatticeSink` should live in a new project (proposed:
`VehicleFleetSimulator.Grains.Lattice`) so the `Orleans.Lattice` package dependency is opt-in and
doesn't land in non-Lattice deployments. The minimum surface:

```csharp
public sealed class LatticeSink : ITelemetrySink, IAsyncDisposable
{
    public LatticeSink(IClusterClient clusterClient, IOptions<LatticeSinkOptions> options, ILogger<LatticeSink> logger);
    // PublishTelemetryAsync: write Channel<VehicleTelemetryEvent>.Writer.TryWrite, return synchronously.
    // Background Task: drain the channel in batches, call ILattice.SetAsync per entry (or a typed
    // helper); on transient failures, surface via metrics, never throw out of the producer path.
    // DisposeAsync: complete the channel, await the drain task, flush metrics.
}

public sealed class LatticeSinkOptions
{
    public string TreeId { get; set; } = "vehicle-fleet";
    public int BatchSize { get; set; } = 256;
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromMilliseconds(50);
    public KeyShape KeyShape { get; set; } = KeyShape.CurrentStateByVehicleId;
    public Func<VehicleTelemetryEvent, byte[]>? Serializer { get; set; } // default: System.Text.Json
}
```

Key shape is the central knob and maps directly to the benchmark scenarios:

| `KeyShape` | Key | Scenarios |
|---|---|---|
| `CurrentStateByVehicleId` | `vehicleId.ToString("N")` | B-03, B-04, B-06, B-07, B-08, B-09, B-12 |
| `RegionPrefixedVehicleId` | `region/vehicleId` (skewed region distribution) | B-05 |
| `EventLogTimestamped` | `vehicleId/{Timestamp:O}` with TTL | B-10, B-11 |

The sink encapsulates the key-shape choice so `VehicleGrain` remains key-agnostic.

### 4. Off-turn dispatch requirement

`VehicleGrain.TickAsync` is a turn-based grain method. A sink that calls `ILattice.SetAsync`
inline will couple grain-tick latency to Lattice's write latency, contaminating every scenario.
Implementations MUST:

- Enqueue into a bounded `Channel<T>` (or equivalent) inside `PublishTelemetryAsync`.
- Drain the channel from a long-running background `Task` started in the sink's constructor (or
  on first publish), not from the grain turn.
- Apply backpressure by either bounding the channel and recording drops via a metric, or by
  using `BoundedChannelFullMode.Wait` with a hard timeout. Silently blocking the producer is a
  benchmark contamination bug — surface it loudly.
- Handle `IClusterClient` / Lattice-side faults entirely inside the drain loop. The producer
  side never observes them.

### 5. Replication wiring (B-04 onward)

`Orleans.Lattice.Replication` is configured at the silo level, independently of the sink. Per
the package's roadmap:

- Replication is per-tree opt-in. The `LatticeSink` decides which `TreeId` it writes to, and the
  silo opts that tree into replication via `Orleans.Lattice.Replication`'s configuration surface.
- The `OriginClusterId` MUST be unique per cluster in the deployment topology — the
  `IConfiguration` value `Orleans:ClusterId` already used by `Program.cs` is a natural source.
- Per-key replication filters (R-012) are configured against the replicator, not the sink.
  Scenario B-09 enables a non-trivial filter; sink code is unchanged.
- Scenario B-08 (bidirectional) requires both clusters to register the replicator with each
  other as peers and to advertise distinct `OriginClusterId`s, otherwise echo cycles will form.

### 6. Lifecycle and graceful shutdown

The silo's hosted lifetime governs sink shutdown. `LatticeSink : IAsyncDisposable` must be
registered such that `DisposeAsync` runs before the cluster client disposes:

- Register the sink as both `ITelemetrySink` and a hosted service (or via `AddSingleton` with
  the silo's `IHostApplicationLifetime` to drain on `ApplicationStopping`).
- On shutdown: complete the channel writer, await the drain task with a bounded timeout, then
  flush metrics. Pending writes that cannot drain within the timeout are recorded as
  `vehicle_fleet_simulator.sink.dropped_on_shutdown` so post-run analysis can detect truncation.

### 7. Metrics wiring

Each benchmark scenario interprets two meter sources side-by-side:

- `orleans.lattice` — Lattice's published `System.Diagnostics.Metrics` meter (shard counters,
  leaf-latency histograms, cache hit/miss; replication WAL append, HWM, ack RTT).
- `vehicle_fleet_simulator.sink.*` — sink-side counters and histograms the implementation should
  publish: `published`, `dropped`, `queue_depth`, `flush_duration_ms`, `flush_batch_size`,
  `inline_publish_duration_ms` (target: bimodal at ~0 and ~channel-write cost).

Both meters should be registered with the same OpenTelemetry exporter so latency attribution is
visible in a single dashboard. The silo's existing OpenTelemetry registration (if any) is the
attach point; otherwise a `services.AddOpenTelemetry().WithMetrics(...)` block in
`Program.cs` adds it for benchmark runs.

### 8. Test coverage that protects this contract

The following tests in `VehicleFleetSimulator.Tests` must continue to pass for any future change
to the simulator integration:

- `NullTelemetrySinkTests` — guarantees the producer-baseline sink is a true no-op, completes
  synchronously, and tolerates a representative burst without throwing.
- `FanOutTelemetrySinkRoutingTests.Telemetry_lands_on_the_shard_chosen_by_ShardForVehicle` —
  guarantees the default sink's per-vehicle shard mapping has not regressed (so the existing
  `FleetStreamHub` consumers continue to work in non-Lattice runs).
- `FanOutTelemetrySinkRoutingTests.Events_always_land_on_shard_zero` — guarantees discrete
  events do not leak across shards.
- `TelemetrySinkSwappabilityTests.A_custom_sink_registered_in_DI_receives_every_vehicle_tick` —
  guarantees the swap mechanism works end-to-end through `VehicleGrain`.
- `TelemetrySinkSwappabilityTests.The_default_FanOutTelemetrySink_is_overridden_not_chained` —
  guarantees a custom sink replaces, not augments, the fan-out path. Without this, B-03/B-04
  would silently double-write and produce misleading numbers.
