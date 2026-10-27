---
agent_spec: "docs/agents/deployment.yaml"
---

# Performance methodology: Layer 2 (one silo on Azure Tables)

This page explains how the Layer 2 table in
[Performance: single silo](performance-single-silo.md) is produced and how to
read it. The results themselves live on that page. The in-process cost per
call is [Layer 1](performance-methodology-layer1.md), on a VM of the same
size and region, and the same workloads on 1 to 8 silos are
[Layer 3](performance-methodology-layer3.md).

## What it measures

Layer 2 measures what one silo sustains end to end against a real Azure
Tables Standard storage account: every write goes through the durable
write-ahead log (WAL) before it is applied, with real Azure round trips.

- **Topology.** One Azure VM runs two systemd services: a producer that
  streams synthetic vehicle telemetry over loopback TCP, and a silo that
  batches what it receives and calls the `ILattice` operation under test.
  The silo reaches storage with the VM's managed identity. See the topology
  in `benchmark/azure-throughput/README.md`.
- **Offered load** is vehicles x ticks per second. Each vehicle produces one
  key per tick, so 4,000 vehicles at 5 Hz offers 20,000 keys per second.
- **Configuration.** The silo runs the library's shipping defaults for
  [`WalPartitions`](configuration.md#walpartitions) and
  `WalMaxPendingBatches`; the values are recorded in the table's metadata.

## Offered load per workload

Every workload runs for 45 seconds at a fixed offered load that is never
raised during a run:

| Workload | Vehicles x Hz | Offered |
|----------|--------------:|--------:|
| `GetAsync`, `GetManyAsync` | 4,000 x 5 | 20,000 keys/s |
| `SetManyAsync` | 1,200 x 5 | 6,000 keys/s |
| `SetAsync`, with or without a view | 200 x 5 | 1,000 keys/s |
| `SetManyAtomicAsync`, 64 keys | 100 x 5 | 500 keys/s |
| `BeginAtomicWrite` cross-tree, 64 keys | 150 x 5 | 750 keys/s |
| `SetManyAtomicAsync`, 2 keys | 20 x 5 | 100 keys/s |
| `BeginAtomicWrite` cross-tree, 2 keys | 8 x 5 | 40 keys/s |

Each write load is fixed, and lower than the read load, so that every run
completes its load reproducibly. A write row is therefore a sustained rate at
its stated load, not that call's ceiling, and write rows offered different
loads must not be compared by throughput. Compare call shapes by their latency
columns instead.

## The read keyspace

Before a read workload starts, the silo writes one key per vehicle (4,000
keys of 245 bytes), and the harness refuses any read run whose log does not
show that seed covering the whole fleet. The producer then reads the same
4,000 keys on every tick, so the read rows measure a small, hot working set
held in memory: the read-through leaf cache, or the leaf's resident state,
answers every call, and no read reaches storage. A workload whose keys do not
stay in memory pays for grain activation and a storage read on a miss, and
reads more slowly. With
[`OptimisticShardRootPointReads`](configuration.md#optimisticshardrootpointreads)
on (the default), a validated `GetAsync` is served from the primary leaf's
resident state without consulting the cache.

The read latencies are what an `ILattice` caller waits for, cache included,
because the cache is part of the production read path. Two consequences:

- **A workload with a low cache hit ratio will see different numbers.** The
  duration histogram cannot tell hits from misses, so read `get.duration`
  beside the `cache.hits` and `cache.misses` counters, and beside
  `shard_root.optimistic_read.outcomes`, whose `validated` arm counts the point
  reads the cache never sees. See [Caching](caching.md).
- **`GetWithVersionAsync` bypasses the cache**, because the version it returns
  must come from the primary leaf. `get_with_version.duration` therefore runs
  higher than `get.duration` for the same keys; the reverse on a dashboard is a
  regression signal.

## How a published figure is produced

`benchmark/performance-report.ps1 -Layer 2` provisions a fresh VM, runs each
workload three times (three *cohorts*), and deletes the VM.

- **Grading.** After each cohort the harness grades the silo log: `WEDGE`
  when the silo never reported its final line, left work unresolved, or kept
  draining after the producer stopped; `FAILED` when writes failed;
  `DEGRADED` when stall-watchdog alarms, WAL warnings or exception lines
  attributed to the cohort appeared; otherwise `HEALTHY`. Only `HEALTHY`
  cohorts contribute to a cell, and the line under the table states how many
  did.
- **Throughput** is the median, across cohorts, of each cohort's steady-state
  mean: the silo's per-second completion rate from the first non-zero second
  at or after 15 seconds to the last non-zero second, with stalled seconds
  inside that window counted as zero.
- **`>=` cells.** A row whose cohorts completed at least 90% of their offered
  load was keeping up with it, so it measured the offered load rather than the
  silo's limit. It is published as a lower bound, marked `>=`.
- **Latency.** The silo reports each operation's duration quantiles every 10
  seconds, from a 4,096-sample reservoir. A cohort's p50, p75, p90 and p99
  merge the windows inside its steady-state window, weighted by call count;
  the cell is the median across cohorts. The instruments are the caller-visible
  durations: `get.duration`, `get_many.duration`, `set.duration`,
  `set_many.duration`, and, for every atomic row, `saga.broadcast.duration`,
  which is the saga's terminal broadcast phase rather than its whole commit.

## Keys, entities and transactions

Three per-second units appear in performance discussions and are easy to
mix up:

- **Keys per second** is what every throughput cell reports: one key is one
  `(string, byte[])` entry from the caller's point of view.
- **Entities per second** is the Azure side of the same thing: the library
  writes one Azure Tables entity per key, so for these workloads the two are
  equal.
- **Transactions per second** is what Azure Tables' scalability targets are
  counted in. One entity-group transaction carries up to 100 entities in one
  partition, so a 4,096-key `SetManyAsync` call is about 41 transactions,
  while a `SetAsync` is one transaction per key.

## Re-running it

```powershell
pwsh benchmark/performance-report.ps1 -Layer 2
```

Add `-PrivateNetwork` in a subscription whose policy forbids public IPs on
VMs; the harness then reaches the VM through a Bastion tunnel. `-DryRun`
re-renders the table from the most recent saved run without touching Azure.
See [Benchmarks](benchmarks.md) for prerequisites.
