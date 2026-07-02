# Metrics

## What it shows

Orleans.Lattice publishes rich `System.Diagnostics.Metrics` instruments (counters,
histograms, and observable gauges) on a single meter named `orleans.lattice`. This
sample attaches a `MeterListener` to that meter, drives a handful of writes, reads,
and deletes, and then prints every instrument it observed along with the number of
measurements and their running total. In a real deployment you would wire this meter
into OpenTelemetry / Prometheus instead of a listener, but the listener makes the
raw instrument surface directly observable with no external collector.

## Run it

```
dotnet run --project samples/Metrics
```

## Expected output

Counter totals (for example `orleans.lattice.shard.writes`) are exact and reflect
the operations this sample performs. Histogram and gauge `total` sums are timing- and
scheduler-dependent, so their values vary from run to run. The exact set of
instruments can also shift slightly as background maintenance fires.

```
Silo starting... ready.
Listening on meter 'orleans.lattice'.

Driving 5 writes, 5 reads, 2 deletes...

Recorded Lattice instrument measurements:
  instrument                                    measurements         total
  orleans.lattice.cache.hits                              33         33.00
  orleans.lattice.cache.misses                             3          3.00
  orleans.lattice.exists.duration                          6         20.91
  orleans.lattice.get.duration                            30        125.85
  orleans.lattice.get.stage.duration                      60        125.42
  orleans.lattice.leaf.commit.duration                    32        318.77
  orleans.lattice.leaf.commit.in_flight                    6          0.00
  orleans.lattice.leaf.scan.duration                      64        140.27
  orleans.lattice.leaf.tombstones.created                  2          2.00
  orleans.lattice.leaf.write.duration                    138       1422.11
  orleans.lattice.materialiser.drain_lag                   5          0.00
  orleans.lattice.set.duration                             6       1523.03
  orleans.lattice.set.stage.duration                      18       1516.64
  orleans.lattice.shard.reads                            100        100.00
  orleans.lattice.shard.writes                             8          8.00
  orleans.lattice.wal.append.batch_bytes                   8       1046.00
  orleans.lattice.wal.append.batch_entries                 8          8.00
  orleans.lattice.wal.append.in_flight                     8          0.00
  orleans.lattice.wal.append.provider.duration             8         31.52
  orleans.lattice.wal.append.queue_depth                   8          8.00
  orleans.lattice.wal.append.turn_wait                     8        164.80
  orleans.lattice.wal.saturation.state                     2          0.00
  orleans.lattice.wal.shard.dispatch.duration              8        257.88
  orleans.lattice.wal.shard.dispatch.entries               8          8.00
  orleans.lattice.wal.shard.pending_segments               8          8.00
  orleans.lattice.wal.shard.start_flush.calls              8          8.00
  orleans.lattice.wal.writer.append.admission_wait            8          0.04
  orleans.lattice.wal.writer.append.dispatched             8          8.00
  orleans.lattice.wal.writer.partition.pending_appends            8          0.00

29 distinct instrument(s) recorded.
(Counter totals are exact; histogram/gauge 'total' sums vary per run.)
```

## When to use

- You want production observability: export the `orleans.lattice` meter through
  OpenTelemetry to Prometheus, Azure Monitor, or any OTLP backend.
- You need to alert on WAL saturation, cache hit ratio, tombstone growth, or
  per-shard latency without adding bespoke logging.

## When not to use

- Do not use a `MeterListener` as shown here as your production telemetry path - it
  is a demonstration device. Use the OpenTelemetry `Meter` exporters instead.
- Do not treat individual histogram/gauge totals from a single short run as stable
  benchmarks; they reflect one process's scheduling and warm-up.

## Feature doc

See [../../docs/lattice/metrics.md](../../docs/lattice/metrics.md).
