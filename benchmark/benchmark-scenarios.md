# Benchmark Scenarios: Load-testing Orleans.Lattice with the Vehicle Fleet Simulator

This document is the authoritative scenario catalogue for the
`Orleans.Lattice` benchmark suite and its companion
`Orleans.Lattice.Replication` package. The Vehicle Fleet Simulator (under
`samples/VehicleFleetSimulator/`) is used unmodified as a sustained, realistic
workload generator: a large population of independent, long-lived vehicle
grains, each emitting telemetry at a steady cadence. That shape — many
key-disjoint producers writing small payloads at a fixed rate — is a close
match for the workloads Lattice is designed to absorb (sorted distributed B+
tree, per-tree change feed, per-peer HLC replication cursor), and lets us
exercise both the core primitive and the replication engine under the same
harness. The offered load is deliberately treated as a tunable: the goal is
to find knees, saturation points, and failure modes, not to hit a fixed
throughput number.

Each scenario below is an independent experiment with its own success
signal; runs are reproducible end-to-end via `benchmark.ps1 <scenario-id>`,
which reads `benchmark/scenarios/<scenario-id>.env` to configure the silo
and the harness. The micro-benchmark scenario (`microbench`) drives
`ILattice` directly through a BenchmarkDotNet harness in
`benchmark/host/Bench.Microbench/` and does not stand up the docker stack.

## Benchmarks

- [x] **microbench: `ILattice` micro-benchmark from `Bench.Microbench`.**
  Bypass the simulator entirely. Sweep concurrency × key cardinality × value
  size against `SetAsync`, `GetAsync`, `SetManyAsync`, and a 70r/30w mix
  directly. Compare results to `docs/lattice/benchmarks.md`. Characterizes
  the primitive in isolation and gives a reference curve for interpreting
  later end-to-end runs.

- [x] **current-state-no-replication: Current-state tree, replication off.**
  Wire a `LatticeSink` that maps `key = vehicleId.ToString("N")` and
  `value = serialize(VehicleSnapshot)` against a single tree, single cluster,
  replication disabled. Each tick is one `SetAsync`. Measures Lattice''s
  steady-state write throughput and latency under uniform key distribution
  (Guid hashing) at the simulator''s offered load.

- [x] **current-state-single-peer: Current-state tree, replication on, single peer.**
  Same wiring as `current-state-no-replication` with
  `Orleans.Lattice.Replication` enabled and one downstream cluster. Track
  `IMutationObserver` overhead (compare write-path p99 vs.
  `current-state-no-replication`), WAL append rate, ship-loop throughput,
  ack RTT, and per-peer HLC cursor lag (`hlc.now - cursor`). Validates the
  roadmap''s sub-second flush-latency claim and the F-035 "zero-cost when no
  observer registered" guarantee.

- [x] **skewed-key-shard-splits: Skewed-key variant to force adaptive shard splits.**
  Re-run `current-state-no-replication` with keys prefixed by a deliberately
  oversubscribed bucket (`region/vehicleId` with one region holding the
  majority of the fleet) so a single shard goes hot. Watch for F-011
  autonomic splits firing online, and confirm reads/writes/scans remain
  consistent across the split (the property the chaos suite asserts).
  Without skew, default `ShardCount = 64` plus Guid hashing keeps load
  uniform and the split monitor never engages.

- [x] **replication-backpressure: Replication backpressure and catch-up.**
  Building on `current-state-single-peer`, pause the receiving cluster for a
  controlled interval while the simulator keeps writing, then resume.
  Measure WAL growth during the pause, time-to-converge after resume, and
  that the per-peer cursor advances strictly on ack. Exercises cursor
  durability and the janitor''s GC predicate (R-061).

- [x] **receiver-crash: Receiver crash mid-stream.**
  Building on `current-state-single-peer`, hard-kill the receiver silo
  during steady-state replication. Verifies idempotent replay from the
  durable HLC cursor and that no replog entries are lost or double-applied.

- [x] **bidirectional-replication: Two-cluster bidirectional replication.**
  Split the fleet across two clusters, each replicating to the other.
  Probes `OriginClusterId` cycle-break (F-036) by confirming writes do not
  echo back to their origin and HLC cursors stabilize on both sides.

- [x] **replication-key-filter: Per-key replication filter cost.**
  Re-run `current-state-single-peer` with a non-trivial per-key filter
  (R-012) on the producer side. Measures the inline filter''s contribution
  to write-path latency. If the core observer-latency histogram (G-013) is
  shipped, capture it; if not, this run motivates landing it.

- [x] **event-log-with-ttl: Event-log tree with TTL (separate run).**
  Alternative key shape: `key = vehicleId/yyyyMMddTHHmmss.fff`,
  `value = VehicleTelemetryEvent`, with a TTL of e.g. 1 hour via the F-016
  `SetAsync(ttl)` overload. Stresses ordered scans (`ScanKeysAsync` /
  `EntriesAsync`), continuous tombstone compaction, and the read-path
  expiry filter. Run independently of throughput experiments — compaction
  will distort the latency tail and conflate signals if mixed with
  `current-state-no-replication` / `current-state-single-peer`.

- [x] **observer-no-peer: Observer-off vs. observer-on delta.**
  Controlled A/B of identical simulator load with `IMutationObserver`
  unregistered vs. registered (no-op). Isolates observer-dispatch cost on
  the hot write path. Pairs with `current-state-single-peer` /
  `replication-key-filter` to attribute latency between dispatch overhead,
  filter cost, and downstream replication work.

- [x] **read-heavy-random: 95:5 read:write, random keys.**
  Same write topology as `current-state-no-replication` plus a read-driver
  (in `Bench.Sink/LatticeReadDriver`) issuing `GetAsync` at a steady rate
  against random keys discovered via cursor pagination. Read:write ratio
  approximates 95:5, mirroring YCSB workload B. Measures cache
  effectiveness and `Get` fast-path tail latency under random access.

- [x] **read-heavy-ordered: 95:5 read:write, sequential keyspace walk.**
  Same write topology as `read-heavy-random` but the read-driver walks the
  keyspace sequentially via `ScanKeysAsync`. Captures the cache/prefetch
  signal that random access cannot — sequential reads should hit the leaf
  block cache more often, so the gap between this run and
  `read-heavy-random` is itself a regression metric.

- [x] **read-write-mix-random: 50:50 read/write, random keys.**
  Balanced YCSB-A-shape mix. Drives a fixed-rate `SetAsync` from the
  simulator and an equal-rate `GetAsync` from the read-driver against
  random keys. Stresses the contention between the read fast path and the
  commit path on the same shard, which neither pure-write nor pure-read
  scenarios isolate.

- [x] **read-write-mix-ordered: 50:50 read/write, sequential walks.**
  Same shape as `read-write-mix-random` but the read-driver walks
  sequentially. Pairs with `read-write-mix-random` the same way the
  read-heavy variants pair: the gap between random and ordered is the
  cache-effectiveness signal under contention.

## Cross-cutting requirements

These apply to every benchmark above and should be verified before kicking off a run.

- OpenTelemetry wired to the `orleans.lattice` meter (shard counters,
  leaf-latency histograms, cache hit/miss, replication WAL/HWM gauges) and
  the `vehicle_fleet_simulator.*` meters (`vehicle_fleet_simulator.sink`
  for the LatticeSink, `vehicle_fleet_simulator.read_driver` for the
  read-driver).
- Warm-up window excluded from measurement: `AddVehicleBatch` fan-out at
  full fleet size takes meaningful time; capture steady state only.
  Configured via `BENCH_WARMUP_SECONDS` in each `.env`.
- Telemetry sink writes happen **off** the `VehicleGrain` turn, so
  grain-tick latency is attributable to the simulator and not to Lattice
  (see §4 below).
- `FleetGrain.GetFleetStats` continues to use the in-grain aggregator —
  never back it with a Lattice scatter-gather scan, which would dominate
  the measurement.
- For any cross-cluster scenario (`current-state-single-peer`,
  `replication-backpressure`, `receiver-crash`,
  `bidirectional-replication`, `replication-key-filter`), use at least two
  physical hosts; single-box replication runs are smoke tests only.

## Simulator Integration

This section documents the concrete attach points the simulator exposes so
each scenario above can be wired up without modifying grain code. The seam
is intentionally narrow: a single DI interface, registered once per silo.
Tests in `VehicleFleetSimulator.Tests` lock in the contracts described
below (`NullTelemetrySinkTests`, `FanOutTelemetrySinkRoutingTests`,
`TelemetrySinkSwappabilityTests`) — any change here that breaks these
contracts will fail CI.

### 1. The `ITelemetrySink` seam

`VehicleFleetSimulator.Abstractions.ITelemetrySink` is the only path
`VehicleGrain` writes telemetry through. Two methods, both fire-and-forget
at the producer:

```csharp
ValueTask PublishTelemetryAsync(VehicleTelemetryEvent telemetry, CancellationToken ct = default);
ValueTask PublishEventAsync(VehicleEvent vehicleEvent, CancellationToken ct = default);
```

The interface is deliberately minimal: no batching API, no flush hook, no
completion callback. A `LatticeSink` implementation is expected to do its
own batching, off-turn dispatch, and failure handling, because:

- Returning `ValueTask` lets the sink complete synchronously when the work
  is purely "enqueue to a bounded channel" — the steady-state hot path
  stays allocation-free.
- The producer must not be blocked by a slow downstream. A correct sink
  MUST NOT couple its own latency to the `VehicleGrain` turn (see §4
  below).

### 2. Sink selection at silo startup

Sinks are registered as a DI singleton in the silo''s service collection.
There is no configuration-driven sink switching at runtime; the benchmark
silo (`benchmark/host/Bench.Silo/`) reads `BENCH_TELEMETRY_SINK` from the
`.env` and registers exactly one sink at startup.

| `BENCH_TELEMETRY_SINK` | Registration                                                                                | Used by |
|---|---|---|
| `null`   | `services.AddSingleton<ITelemetrySink, NullTelemetrySink>(_ => NullTelemetrySink.Instance);` | (rare; observer-off A/B half) |
| `fanout` | `services.AddSingleton<ITelemetrySink, FanOutTelemetrySink>();`                              | non-Lattice control runs       |
| `lattice`| `services.AddSingleton<ITelemetrySink, LatticeSink>();` (extension: `AddLatticeSink`)        | every Lattice scenario above   |

`Program.cs` in `benchmark/host/Bench.Silo/` is the single registration
point. The replacement must be exclusive — registering a second
`ITelemetrySink` does not chain (verified by
`TelemetrySinkSwappabilityTests.The_default_FanOutTelemetrySink_is_overridden_not_chained`).

### 3. The `LatticeSink` shape

`LatticeSink` lives in `benchmark/host/Bench.Sink/` so the
`Orleans.Lattice` package dependency is opt-in and doesn''t land in
non-Lattice deployments of the simulator. The minimum surface:

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
| `CurrentStateByVehicleId` | `vehicleId.ToString("N")` | `current-state-no-replication`, `current-state-single-peer`, `replication-backpressure`, `receiver-crash`, `bidirectional-replication`, `replication-key-filter`, `observer-no-peer`, `read-heavy-random`, `read-heavy-ordered`, `read-write-mix-random`, `read-write-mix-ordered` |
| `RegionPrefixedVehicleId` | `region/vehicleId` (skewed region distribution) | `skewed-key-shard-splits` |
| `EventLogTimestamped` | `vehicleId/{Timestamp:O}` with TTL | `event-log-with-ttl` |

The sink encapsulates the key-shape choice so `VehicleGrain` remains
key-agnostic.

### 4. Off-turn dispatch requirement

`VehicleGrain.TickAsync` is a turn-based grain method. A sink that calls
`ILattice.SetAsync` inline will couple grain-tick latency to Lattice''s
write latency, contaminating every scenario. Implementations MUST:

- Enqueue into a bounded `Channel<T>` (or equivalent) inside
  `PublishTelemetryAsync`.
- Drain the channel from a long-running background `Task` started in the
  sink''s constructor (or on first publish), not from the grain turn.
- Apply backpressure by either bounding the channel and recording drops
  via a metric, or by using `BoundedChannelFullMode.Wait` with a hard
  timeout. Silently blocking the producer is a benchmark contamination
  bug — surface it loudly.
- Handle `IClusterClient` / Lattice-side faults entirely inside the drain
  loop. The producer side never observes them.

### 5. Replication wiring (`current-state-single-peer` onward)

`Orleans.Lattice.Replication` is configured at the silo level, independently
of the sink. Per the package''s roadmap:

- Replication is per-tree opt-in. The `LatticeSink` decides which `TreeId`
  it writes to, and the silo opts that tree into replication via
  `Orleans.Lattice.Replication`''s configuration surface.
- The `OriginClusterId` MUST be unique per cluster in the deployment
  topology — the `IConfiguration` value `Orleans:ClusterId` already used
  by `Program.cs` is a natural source.
- Per-key replication filters (R-012) are configured against the
  replicator, not the sink. Scenario `replication-key-filter` enables a
  non-trivial filter; sink code is unchanged.
- Scenario `bidirectional-replication` requires both clusters to register
  the replicator with each other as peers and to advertise distinct
  `OriginClusterId`s, otherwise echo cycles will form.

### 6. Lifecycle and graceful shutdown

The silo''s hosted lifetime governs sink shutdown.
`LatticeSink : IAsyncDisposable` must be registered such that
`DisposeAsync` runs before the cluster client disposes:

- Register the sink as both `ITelemetrySink` and a hosted service (or via
  `AddSingleton` with the silo''s `IHostApplicationLifetime` to drain on
  `ApplicationStopping`).
- On shutdown: complete the channel writer, await the drain task with a
  bounded timeout, then flush metrics. Pending writes that cannot drain
  within the timeout are recorded as
  `vehicle_fleet_simulator.sink.dropped_on_shutdown` so post-run analysis
  can detect truncation.

### 7. Read-driver wiring (read-heavy and read/write-mix scenarios)

Read-dominant scenarios add a `LatticeReadDriver`
(`benchmark/host/Bench.Sink/`) registered as a hosted service alongside
the sink. The read-driver:

- Issues `GetAsync` (random) or `ScanKeysAsync` (ordered) against the same
  `TreeId` the `LatticeSink` writes to, at a fixed
  `BENCH_READ_RATE_PER_SECOND`.
- Bounds concurrency via `BENCH_READ_CONCURRENCY` so the offered read
  load is reproducible.
- Publishes the `vehicle_fleet_simulator.read_driver` meter
  (`reads_total`, `duration_ms` histogram, `errors_total`,
  `inflight` UpDownCounter). The benchmark script promotes the histogram
  to `bench_vehicle_fleet_simulator_read_driver_*` aliases so the
  history dashboards bind to short, stable names.

The read-driver is a separate component from the sink so the four read
scenarios can reuse `current-state-no-replication`''s exact write
configuration without forking the sink contract.

### 8. Metrics wiring

Each benchmark scenario interprets three meter sources side-by-side:

- `orleans.lattice` — Lattice''s published `System.Diagnostics.Metrics`
  meter (shard counters, leaf-latency histograms, cache hit/miss;
  replication WAL append, HWM, ack RTT — the replication histograms ride
  the same meter via `orleans.lattice.replication`).
- `vehicle_fleet_simulator.sink` — sink-side counters and histograms:
  `published`, `dropped`, `queue_depth`, `flush_duration_ms`,
  `flush_batch_size`, `inline_publish_duration_ms` (target: bimodal at ~0
  and ~channel-write cost).
- `vehicle_fleet_simulator.read_driver` — read-driver-side counters and
  histograms (only emitted by read-heavy and read/write-mix scenarios):
  `reads_total`, `duration_ms`, `errors_total`, `inflight`.

All three meters are registered with the same OpenTelemetry exporter in
`benchmark/host/Bench.Silo/Program.cs` so latency attribution is visible
in a single dashboard.

### 9. Test coverage that protects this contract

The following tests in `VehicleFleetSimulator.Tests` must continue to pass
for any future change to the simulator integration:

- `NullTelemetrySinkTests` — guarantees the producer-baseline sink is a
  true no-op, completes synchronously, and tolerates a representative
  burst without throwing.
- `FanOutTelemetrySinkRoutingTests.Telemetry_lands_on_the_shard_chosen_by_ShardForVehicle` —
  guarantees the default sink''s per-vehicle shard mapping has not
  regressed (so the existing `FleetStreamHub` consumers continue to work
  in non-Lattice runs).
- `FanOutTelemetrySinkRoutingTests.Events_always_land_on_shard_zero` —
  guarantees discrete events do not leak across shards.
- `TelemetrySinkSwappabilityTests.A_custom_sink_registered_in_DI_receives_every_vehicle_tick` —
  guarantees the swap mechanism works end-to-end through `VehicleGrain`.
- `TelemetrySinkSwappabilityTests.The_default_FanOutTelemetrySink_is_overridden_not_chained` —
  guarantees a custom sink replaces, not augments, the fan-out path.
  Without this, `current-state-no-replication` /
  `current-state-single-peer` would silently double-write and produce
  misleading numbers.