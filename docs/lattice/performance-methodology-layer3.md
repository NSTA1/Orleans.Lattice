---
agent_spec: "docs/agents/deployment.yaml"
---

# Performance methodology: Layer 3 (scaling from one to eight silos)

This page explains how the grid, charts and storage table in
[Performance: multi-silo scaling](performance-multi-silo.md) are produced, and
what bounds them. The results themselves live on that page.

## What it measures

Layer 3 runs the same workloads as the single-silo
[Layer 2](performance-methodology-layer2.md) table against a
cluster of 1, 2, 4, 6 and 8 silos, one tree (two for the cross-tree rows) and
one Azure Storage account, and records one cell per workload and silo count.
The question it answers is what adding hosts buys: the shape of each curve,
and where it stops climbing.

**Read the curve, not the absolute numbers.** Several choices hold the
topology constant so that the silo count is the only variable: one shared
storage account, a fixed 64-shard tree, Azure Container Apps' memory limit, and
a client with bounded concurrency. Each of them also caps the absolute figures,
so a deployment tuned for throughput rather than for comparability can beat
them.

## The rig

Each rig is one resource group created by
`benchmark/azure-throughput/scripts/deploy-aca.ps1`:

- **Silos.** A Container App whose replicas are the silos, each 4 vCPU and
  8 GiB (the Consumption profile's limit).
- **Client.** A Container Apps job that joins the cluster as an Orleans
  client through the shared clustering table and calls the silo gateways
  directly. It opens four connections per silo by default, so calls spread
  across every gateway.
- **Storage.** One account holds the WAL, grain state and the clustering
  table. `-Layer3WalAccounts N` spreads the tree's WAL partitions across N
  accounts instead.
- **Logs.** Producer and silo logs go to Log Analytics, where the harness
  reads them after each run.

A diagram is in `benchmark/azure-throughput/README.md`.

## How a cell is measured

A *cohort* is one 45-second measured run at one silo count. For each cohort
the harness:

1. Parks the silos at zero replicas and deletes every table except the
   clustering table, so no cohort inherits trees, registry rows or WAL backlog
   from an earlier one.
2. Scales the silo app to N replicas, waits for every replica and for the
   previous revision to retire, and lets cluster membership settle.
3. Starts the client job, which warms the tree, seeds the keyspace for the
   read workloads, and drives the measurement window.
4. Parks the silos, harvests the logs and the storage account's Azure Monitor
   metrics, and grades the cohort.

**Offered load grows with the cluster.** Each workload has a per-silo offered
load, and the cell is offered that load times N. A flat curve therefore means
the extra hosts absorbed nothing, not that the load ran out.

**Every figure is the cluster's limit, not the load offered.** If a cell's
first cohort completes at least 90% of what it was offered, the cluster has
not reached its limit. The harness discards that cohort, doubles the per-silo
load and tries again, up to three times. The load a workload settles on is
carried to the larger silo counts, so demand never falls as the cluster
grows. A cell still keeping up after the last doubling is published as a
lower bound, marked `>=`. So is a cell whose client could not generate the
load it was asked for: that measures the client, not the cluster.

**Each cell is the median of three cohorts** at the settled load, or of two
where one was excluded; the metadata's `cohortN` gives the range.

**The client's concurrency is sized per workload.** The client keeps a fixed
number of calls in flight per silo, recorded as `flushConcurrency` in the
table's metadata:

- **Batched workloads:** 8 per silo, since one call carries 4,096 keys.
- **`GetAsync`:** 16 slots per silo, each fanned out into 16 individual
  calls, so 256 calls per silo.
- **Point writes:** 64 slots per silo, fanned out the same way.
- **Atomic sagas:** 64 sagas per silo.

Calls in flight therefore grow in proportion to the silo count, keeping the
demand per silo constant.

## How a cohort is graded

A cohort is excluded from its cell, graded `WEDGE`, only when:

- the client never finished;
- no measurement window completed any work;
- completions stopped for a minute while work was in flight; or
- a multi-account cohort's WAL placement was not confirmed.

Otherwise it is `HEALTHY` and its numbers are kept, including failed
operations and any backlog still queued when the run ended: a cell at the
cluster's limit is exactly what this layer exists to measure.

**A read cohort whose keyspace was never seeded is re-run**, up to twice,
rather than published as a measurement of an empty tree. The client seeds one
key per vehicle, so the keyspace grows with the offered load and the silo
count, and the seed's time budget grows with it: 300 seconds, or one second
per 1,000 keys when that is longer.

**The read keyspace is bulk-loaded.** Every cohort starts from an empty tree,
so the client seeds it with Lattice's resumable bulk load
(`ILattice.BulkAppendChunkAsync`, in ascending-key chunks of 16,384) rather
than with batched writes. A bulk load packs every leaf to capacity and writes
about one batched WAL append per leaf, with no leaf splits, so the reads run
against a fully packed tree. The single-silo layers seed their 4,000 keys with
one `SetManyAsync` call; at that size no shard holds more than one leaf, so
both seeds build the same tree.

**Failures are counted, not hidden.** The engine retries a WAL
back-pressure refusal on its own back-off, and its final line reports
`satRetries`, `satRecovered` and `satExhausted`. Other failures are Azure
Tables operations that did not report success, almost always server-side
timeouts ("Operation could not be completed within the specified time") in
every cohort checked, with
a few `already exists` conflicts from a transaction retried after it had in
fact landed. A point workload counts a failure per key; a batched workload
counts the whole 4,096-key batch.

## How to read the grid

- **Throughput** is completed operations divided by the time the cluster was
  active, counted at the client: the work that succeeded, over the time it
  took. It cannot exceed the offered load.
- **Speedup vs 1 silo** is the cell's throughput over the same workload's
  one-silo throughput. **Per-silo efficiency** is that speedup divided by N.
  Perfect scaling is a speedup of N at 100% efficiency; the silo count after
  which efficiency falls away sharply is the knee.
- **The one-silo cell is a different machine.** At one silo every grain call
  stays in the process. From two silos on, most hops cross the network, so the
  step from one to two carries a one-off latency cost that later steps do not
  repeat. Reading speedup from two silos as well as from one separates that
  cost from how the cluster scales.
- **Latency is measured in the silos, not at the client.** Each silo logs its
  duration quantiles every 10 seconds. A cell's p50 and p99 merge every window
  of every silo, weighted by call count, by treating each window as a
  piecewise-linear distribution through its logged quantiles, so they
  estimate the cluster-wide distribution rather than computing it exactly. They
  exclude the client-to-gateway hop. For the atomic rows the instrument is the
  saga's terminal broadcast phase (`saga.broadcast.duration`), not its whole
  commit.
- **Storage-side columns** come from each storage account's Azure Monitor
  metrics over the cohort:
  - **Timed-out transactions:** the transactions answered
    `ServerTimeoutError` or `ClientTimeoutError` on every account the cohort
    used, with their share of all its transactions. The count and the share
    are each the median across the cell's cohorts.
  - **Throttling:** transactions answered `ServerBusy` or `ClientThrottling`
    are reported in a sentence under the table, taken from the worst cohort
    of any cell, so a single throttled cohort is not hidden by a median.
  - **Storage transactions:** the busiest minute of the cohort, in
    transactions per second, with every WAL account's transactions added
    together minute by minute. Divide by the WAL account count for the load
    on each account.
  - **Server and end-to-end latency:** `SuccessServerLatency` and
    `SuccessE2ELatency` for the worst account. The gap between them is time
    outside the storage service, on the network and in client-side queueing.

  The first account also holds the clustering table and grain state, so its
  transactions are not WAL-only.

## What bounds these numbers

**The one-silo cell is not a stand-in for Layer 2.** [Layer 2](performance-methodology-layer2.md) runs most rows
at a fixed load to measure the cost per call; Layer 3 raises the load until
the cluster stops keeping up. Most one-silo cells here therefore sit well
above the Layer 2 figure for the same workload.

**The throughput basis differs from Layer 2's, deliberately.** Layer 2
averages the silo's per-second completion rate. That works when work retires
smoothly, but the Layer 3 client submits large batches, so a whole batch
retires within one second and the seconds between are zero. A per-second
mean is then quantised by batch retirement, and the distortion varies with
the silo count, which would bend the very curve being measured. Layer 3's
completed-work basis is the more conservative of the two.

**One storage account backs each rig.** Every silo's WAL writes and grain
state land in one account, so a flattening write curve can be measuring the
account rather than the cluster. The storage-side table shows what the
account reported. There is no equivalent fan-out for grain state.

**The shard count is fixed at 64** in every cell, so a change in the curve is
a change in host count, not in shard granularity. At eight silos that is eight
shard roots per silo.

**The client's concurrency is bounded.** Doubling the offered load raises the
rate the client offers, not the calls it keeps in flight, so a cell where the
client was at its limit measures the cluster's response time under that
concurrency, not an open-loop ceiling.

**Silo memory is half Layer 2's.** The Layer 2 host has 4 vCPU and 16 GiB; a
Container Apps replica is capped at 4 vCPU and 8 GiB. CPU matches; memory
mostly affects cache-resident reads, whose absolute figures are
correspondingly conservative.

**Every call crosses the network.** The Layer 2 producer feeds its silo over
loopback and the silo calls `ILattice` in-process. The Layer 3 client is a
separate container, so every call pays a network hop that Layer 2 does not.

**The replay admission queue is widened.** A cohort opens a cold tree and
warms every shard root at once, which the replay admission gate's default
queue depth refuses on a 4-vCPU replica. The harness raises
`LatticeOptions.WalReplayPermitQueueDepthPerPermit` on these silos to 64,
against a library default of 4. The single-silo layers do not.

**Two back-pressure budgets are finite.** `LatticeOptions.SetManyFanOutBudget` and
`LatticeOptions.WalAdmissionSaturationCallBudget` ship finite (30 s and 15 s)
and the harness keeps those defaults (`-SetManyFanOutBudgetSec`,
`-WalAdmissionCallBudgetSec`; pass `0` to measure them unbounded).

## Re-running it

`pwsh benchmark/performance-report.ps1 -Layer 3` provisions a rig, sweeps it
and deletes it; `-NamePrefix <prefix>` names that rig. To sweep a rig you
deployed yourself, deploy it and pass `-ReuseAca`:

```powershell
pwsh benchmark/azure-throughput/scripts/deploy-aca.ps1 -NamePrefix l3ab12cd3
pwsh benchmark/performance-report.ps1 -Layer 3 -ReuseAca l3ab12cd3
```

Use a prefix of three to nine lowercase letters and digits, and run both
scripts from the same checkout: the rig's context file is written under
`benchmark/.run/aca/`. A rig passed with `-ReuseAca` is never deleted; remove
it with `az group delete --name rg-<prefix> --yes`. A reused rig keeps the
images it was built with, so after a code change run `deploy-aca.ps1 -ReuseRg`
again before sweeping.

The switches that matter most:

- `-Layer3Rigs <K>` runs the sweep on K rigs in parallel, each with its own
  storage. The harness splits the work itself: by workload, so no curve mixes
  rigs, or, with more rigs than workloads, by giving each rig a contiguous run
  of one workload's silo counts (the metadata's `splitCurves` key names those
  curves). Check the region's quota: K rigs can hold K times eight 4-vCPU
  replicas at once.
- `-Workloads` and `-SiloCounts` restrict the sweep. `-Workloads` takes one
  comma-separated string; to pass a list of silo counts use
  `pwsh -Command "& ./benchmark/performance-report.ps1 -Layer 3 -SiloCounts 6,8"`.
- `-Resume` skips the cells a rig's saved state already holds, so a sweep can
  be stopped and continued on the same rig; with a larger `-N` it tops cells
  up with extra cohorts.
- `-Layer3WalAccounts <N>` spreads each cohort's WAL across N storage accounts.
- `-SaturationRatio` (default 0.9) and `-MaxRungEscalations` (default 3)
  control when a cell's load is doubled and how many times.
- `-Layer3ClientsPerSilo` (default 4) sets the client connections per silo;
  raise it when a cell is graded producer-bound.
- `-Layer3OverlapHarvest` harvests a cohort's logs in the background while the
  next cohort runs, for cohorts whose result cannot change what the sweep does
  next.
- `-DryRun` re-renders the results page from the most recent saved run
  without touching Azure.

See [Benchmarks](benchmarks.md) for prerequisites, and the rig's own
`benchmark/azure-throughput/README.md`.
