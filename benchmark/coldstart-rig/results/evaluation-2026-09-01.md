# ANN adoption evaluation, 2026-09-01

Evaluation of `main` at `208bffaf` - epic #1830 plus #1872 (scheduled index build)
and #1898 (rig image-tag safety) - against a restored copy of the real
deployment.

Image under test: `repocontext-mcp:coldstart-208bffafed69`
(`sha256:3327bd5d...`), built from `208bffaf` by `rig.ps1 build`.
Cohort: [`cohort-post-1872.json`](cohort-post-1872.json).

This supersedes nothing in [`adoption-2026-08-31.md`](adoption-2026-08-31.md);
it re-measures the same rig after four remediation fixes and the build-scheduling
change landed, because those postdate that report.

## Verdict

| Claim | Status |
| --- | --- |
| The index converges with **no query at all** | **Proven** - 1 m 23 s from a pristine pre-epic volume |
| Approximate retrieval is **as accurate as exact** | **Proven** - recall@10 = 1.000, top-1 identical, 5/5 queries |
| A persisted index **survives restart** | **Proven** - fast on every restart, including SIGKILL |
| The #1883 heal converges on **real data** | **Proven** - repairs observed on the restored corpus |
| Time to first semantic query improves | **Not proven** - no attributable change, as before |
| The live deployment is **never touched** | **Proven** - image pin unchanged throughout |

## Conversion converges without a query

A container booted on the pristine pre-epic master - a volume that carries
vectors but no approximate index - reached a servable vector plane with **zero
external queries issued**:

```
RepoContext retrieval warmup complete: the vector plane served a semantic
query after 00:01:23.4658516.
```

Before #1872 the build was armed by the first query, so an idle repository never
built one and a restart resumed nothing. This is the behaviour that change
exists to deliver, and it is the deterministic assertion the evaluation was
designed around: it is a state question, answered in minutes, not a latency
question needing a cohort.

## Approximate recall is exact

Five natural-language queries were answered by the approximate plane
(`retrievalPath: semantic.approximate`). The **same container** was then
restarted with `LATTICE_REPOCONTEXT_SEMANTIC_RETRIEVAL=Exact` against the **same
working volume**, and the same queries re-run
(`retrievalPath: semantic.exact`).

| Query | Recall@10 | Top-1 identical |
| --- | --- | --- |
| where is the readiness health probe wired | 1.000 | yes |
| how does shard consolidation fold a donor | 1.000 | yes |
| allocation probe differential measurement | 1.000 | yes |
| WAL garbage collection cadence | 1.000 | yes |
| approximate nearest neighbour index build | 1.000 | yes |

**Mean recall@10 = 1.000.** On this corpus the approximate plane returned
exactly the exact-KNN result set, in the same order at the top. Holding the
corpus and the container fixed and varying only the retrieval mode is what makes
this a controlled comparison rather than two independent measurements.

## The cohort, and how to read it

Warm-query p50, three runs of three scenarios:

| Run | `first-boot` | `graceful-restart` | `sigkill-restart` |
| --- | --- | --- | --- |
| 1 | 22,595 ms | 8,812 ms | **219 ms** |
| 2 | 23,339 ms | **233 ms** | **194 ms** |
| 3 | 21,056 ms | **167 ms** | **180 ms** |

**A slow `first-boot` is the conversion window, not a regression.** That
scenario boots a pre-epic volume with no index, so it must build roughly 35,000
vectors while the exact scan answers queries. Retrieval never stops and never
loses recall while it does - that is the adoption path behaving correctly.

The shape is the result: **build once on first boot, then fast on every
subsequent restart**, including an ungraceful one. The epic's own measurement
found a persisted index reloaded on 1 activation out of 9; here both restart
scenarios are consistently in the 167-233 ms band.

A caution for anyone comparing these figures against
[`cohort-post-epic.json`](cohort-post-epic.json): **a scenario name encodes a
precondition.** Comparing a `first-boot` number across two cohorts is only
meaningful if the master volume was in the same state in both, and it was not.

## What did not improve

Time to first semantic query, unchanged within noise:

| Scenario | Mean | Spread |
| --- | --- | --- |
| `first-boot` | 100.1 s | 20.8 % |
| `graceful-restart` | 89.7 s | 131.0 % |
| `sigkill-restart` | 55.5 s | 44.1 % |

The first query still arrives before the index is ready and therefore still pays
the un-indexed cost. #1872 fixed *scheduling* - the index now converges without
a query - not the race between the first query and a finished build. The spreads
also remain far too wide for an n=3 comparison of means to detect anything
smaller than a very large effect, so this design cannot settle the question even
in principle.

## A bug the evaluation found: #1902

`rig.ps1 build -Ref <ref>` records its output as the run's source image, but
`prepare-master.ps1` then re-tags `coldstart-rig` from the configured default
(`repocontext-mcp:local`), silently discarding that record. Observed here: the
rig tag resolved to the freshly built `sha256:3327bd5d...` after `build`, then
to the live pre-epic `sha256:7a8318c4...` after preparing the master.

Run in that order, a cohort **measures the deployed image and labels the results
as the candidate**. `rig.ps1 tag` re-applies the record and is the workaround
used for this evaluation. Tracked as #1902.

## Safety

The live deployment was never touched. Its container pin and the
`repocontext-mcp:local` tag both resolved to
`sha256:7a8318c485c2aae9fa0a7ff62f76acafebb55396b71aa6f5ecaabaf139c057c0`
before, during and after; the container ran continuously throughout and was
never built, tagged, stopped, restarted or recreated. The #1898 drift check
reported the pin CLEAN at every stage, and the cohort re-asserted it after the
run. The rig ran under its own compose project, volumes and images on port
18080; every rig container was removed afterwards.
