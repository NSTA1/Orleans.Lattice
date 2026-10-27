---
agent_spec: "docs/agents/deployment.yaml"
---

# Performance methodology: Layer 1 (in-process cost per call)

This page explains how the Layer 1 table in
[Performance: single silo](performance-single-silo.md) is produced and how to
read it. The results themselves live on that page.

## What it measures

Layer 1 measures the cost of one call to each `ILattice` method with
scheduling, the network and durable storage taken out of the picture. It is
the algorithmic floor of each call: what the library itself costs on one
thread, before Orleans messaging and Azure round trips are added.

- **Harness.** A BenchmarkDotNet suite (`LatticeMicroBenchmarks`, in
  `benchmark/host/Bench.Microbench`) creates the grain layer in-process
  against an in-memory storage provider and runs each operation on a single
  thread, using BenchmarkDotNet's in-process toolchain. There is no Orleans
  RPC, no network and no Azure I/O on this path.
- **Keys are written before they are read.** The read benchmarks look up keys
  the setup stored, so they time real hits.
- **Reported per call:** p50, p75, p90 and p99 latency, and bytes allocated
  (BenchmarkDotNet's memory diagnoser).
- **Per-thread call rate** is derived, not measured: `1 / p50`, multiplied by
  the keys per call, and reported in keys per second so batched and single-key
  calls compare directly.

## How a published figure is produced

`benchmark/performance-report.ps1 -Layer 1` provisions a fresh Azure VM, runs
the suite three times (three *cohorts*), publishes each cell as the median of
the three per-cohort values, and deletes the VM. The VM is the same size and
region as the [Layer 2](performance-methodology-layer2.md) host, so a difference between hosts cannot be mistaken
for a difference between workloads.

The default fidelity, `quick`, is BenchmarkDotNet's short run: one launch,
three warm-up iterations and three measured iterations per benchmark. The
table's hidden metadata block records the host size, .NET version,
fidelity, toolchain, cohort count, measurement date and commit; the line
under the table summarises it.

## Reading the numbers

- **One thread, not a silo.** The per-thread rate is what one thread achieves
  running the call back to back with nothing else to do. A multi-core silo
  runs many such threads, but it also pays for grain messaging, the
  write-ahead log and storage, which this path skips. The sustained rate a
  silo actually delivers is the Layer 2 table
  ([how it is measured](performance-methodology-layer2.md)), and it is much
  lower.
- **Allocations barely vary.** In the latest run 29 of the 30 allocation
  figures were identical in all three cohorts, so a change in that column is
  almost always a change in the code path rather than noise. That makes it the
  most reliable column for spotting a regression between two publishes.
- **Latency varies more.** In the same run the three cohorts' p50s agreed
  within 3% for the median benchmark and within 12% at worst; p99s move more,
  and a fresh VM on a later date adds its own difference. Compare latencies
  across publishes only when the change is large or the allocations moved
  with it.

## Re-running it

```powershell
pwsh benchmark/performance-report.ps1 -Layer 1
```

Add `-PrivateNetwork` in a subscription whose policy forbids public IPs on
VMs. `-Fidelity full` runs BenchmarkDotNet's default job for tighter
quantiles at about three times the duration. `-DryRun` re-renders the table
from the most recent saved run without touching Azure. See
[Benchmarks](benchmarks.md) for prerequisites, and
`benchmark/azure-throughput/README.md` for the rig itself.
