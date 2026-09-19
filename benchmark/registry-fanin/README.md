# registry-fanin

A measurement rig for the `LatticeRegistryGrain` cold-start fan-in question.

It delivers an **instrument**, a **sidecar driver**, and **measurement
scripts**. It deliberately contains **no fix and no asserting test fixture** -
the behaviour under investigation is unresolved, so a fixture written now would
land red in CI and assert a conclusion the measurements do not yet support.

## The question

The live estate produced 103 `System.TimeoutException`, all on the single
activation `latticeregistry/_lattice_trees`, all within 1.2% of the exact 30 s
deadline (so they were **never served**, not served slowly), all on read
members, from per-tree background services, inside a two-minute window about
five minutes after start - and then **zero for the following 46 minutes**.

The proposed scaling law was (number of trees) x (per-tree background services)
fanning in onto one activation.

## Three findings that shape how this rig must be used

These are recorded here because each one silently invalidates an obvious way of
running the measurement, and each cost real time to find.

### 1. The background services are reminder-birthed, not boot-started

`LatticeGrain` registers an Orleans **reminder** per tree, and all three of the
live storm's top callers register with the same one-minute due time:

| grain | site | due |
|---|---|---|
| `HotShardMonitorGrain` | `:226-229` | 1 min |
| `ShardHealingOrchestratorGrain` | `:96-99` | 1 min |
| `ViewMaintainerGrain` | `:170-173` | 1 min |

Those three were 87 of the 103 live timeouts. Reminders are **persistent**, so
they survive into the next process lifetime and re-fire after a cold start with
nothing touching the tree.

Two consequences, and both invalidate an obvious measurement:

- **A short window cannot see the burst.** The reminder service must start, load
  the table, and reach a due tick. A 120 s window against a K=20 estate reported
  the six services at a count of **5** - the host's own trees - and zero
  timeouts. That reads exactly like "the rig cannot reproduce the storm". It was
  a window-length artefact. The same estate measured over a longer window
  reported **45** at K=40, precisely one per tree.
- **A steady-state probe cannot see it either, at any K.** Once the birth burst
  has passed the reminders spread across their period and the fan-in is gone.
  Measuring a warm container samples the one regime that never failed. A flat
  result from such a probe is a true statement about a warm container and says
  nothing about cold start.

### 2. `reset` destroys the carrier; use `down` / `up`

The reminder table lives in the volume. `rig.ps1 reset` wipes it, so a cold
start after a reset measures an estate with **no background services at all**.
That produces a clean, confident, wrong negative.

Build the estate, then `down` / `up`. `run-birth-curve.ps1` does this and never
resets between the build and the cold start.

### 3. `RegisterAsync` is cheap; tree creation is not a registry operation

Creating a tree costs about 1.2 s, and that is `ILattice.SetAsync` - shard-root
and leaf activation and storage. `ILatticeRegistry.RegisterAsync` itself is
~5 ms client against ~4.9 ms server, with no queueing gap, and a 64x offered
concurrency sweep costs only 2.7x latency.

Attributing the 1.2 s to the registry is easy to do by accident: measure
`create` end to end and read the first latency in the report. Separate the
members.

Also note `RegisterAsync` is **idempotent** - re-registering an existing id
short-circuits in single-digit milliseconds without touching the write path. Any
registration measurement must use a fresh `--prefix`, or it measures the
short-circuit and reports a healthy number for work that never happened.

## Measurement method - non-negotiable

- **Bucket by timestamp from the log, never by counter scrape.** A single scrape
  of a cumulative counter reads as an active fault; a 90 s delta of the same
  counter reads as healthy. Both single-shot readings are wrong, in opposite
  directions. Only the timestamped distribution is truthful.
- **Re-baseline every counter after each restart.** Counter readings are scoped
  to a process lifetime and must never be tabled across a restart boundary.
  Every reading records `StartedAt` so this stays checkable.
- **`RestartCount=0` does not prove no restart occurred.** A compose recreation
  yields a fresh container with `RestartCount=0` and a new `StartedAt`. Compare
  `StartedAt` against the reading time.
- **n >= 3 per cell.** Observed storm magnitude already varies 103 vs 18 across
  two runs of the same configuration - a factor of 5.7. One sample cannot
  distinguish a real effect from that dispersion.
- **Report the distribution, not the peak.**
- **Record host load at both ends of every window.** Co-tenancy must be observed,
  not assumed absent.

## Interleaving is read from source, never inferred

`RegistryCallCensus.NonInterleavedOperations` (`get_all_tree_ids`, `register`,
`unregister`) is the authority, and the collector parses it from the source file
rather than restating it.

This matters because the in-flight counter is **global across arms**. It answers
"how many registry calls of any kind were in the grain body when this one was
admitted" - the fan-in width - and **not** "was this member interleaving". A
non-interleaved member routinely reports a width above zero: an
`[AlwaysInterleave]` read awaiting its downstream hop is still in flight, and
Orleans may start a new turn when the running turn yields. Measured directly,
`register` reported a mean width of **1.44** while non-interleaved throughout.

A test of the form `width > 0 therefore it interleaved` therefore fails in the
**permissive** direction, licensing exactly the attribution the list exists to
forbid. The parse is strict - an unresolved identifier, an empty result, or a
missing file all throw - because a collector that cannot establish which members
interleave must fail rather than fall back to a default that would be
indistinguishable from a correct read in the output.

## Layout

| path | what it is |
|---|---|
| `Driver/` | sidecar console driver; joins the cluster as an Orleans client and addresses `ILattice` / `ILatticeRegistry` directly |
| `scripts/rig.ps1` | container lifecycle: `build` `up` `ready` `driver` `down` `reset` `status` |
| `scripts/_fanin-helpers.ps1` | timeout census, counter baselining, restart detection, isolation guard, Prometheus parse, host load, interleaving parse |
| `scripts/collect-window.ps1` | the measurement proper: per-arm service time, fan-in width, attribution validity, timeout census, host load |
| `scripts/run-cell.ps1` | one cell = K x depth x cold start x window |
| `scripts/run-breadth.ps1` | steady-state enumeration vs K - **superseded and unrun**; see Finding 2, the per-tree services are reminder-birthed so a steady-state sweep samples the wrong regime |
| `scripts/run-birth-curve.ps1` | **cold-start birth curve** - the experiment that targets the storm regime |
| `scripts/run-host-pressure.ps1` | cold start at fixed K while a throwaway burner contends for the host |
| `scripts/Test-FanInHelpers.ps1` | unit tests for the helpers (41) |

## Driver

```powershell
./scripts/rig.ps1 driver -DriverArgs "create --trees 40 --leaves-per-tree 1"
./scripts/rig.ps1 driver -DriverArgs "probe --trees 40 --rate 100 --duration 240 --enumerate-pct 10"
./scripts/rig.ps1 driver -DriverArgs "teardown --trees 40"
```

Verbs: `create`, `populate`, `probe`, `census`, `list`, `teardown`. The driver
emits client-side timing independent of every server instrument - per-call
latency, deadline exceptions, and peak in-flight concurrency.

Teardown is `DeleteTreeAsync` then `UnregisterAsync`.

## Isolation

Every script calls `Assert-FanInIsolation` before touching anything. The
protected container `repocontextcontainer-repocontext-1` and its volume
`repocontextcontainer_repocontext-data` are named in the forbidden lists and the
guard refuses to run if the rig's own project, volume, or image tag does not
match its required prefix. The rig runs against a **separate, throwaway**
container with its own volume, built from the same image.

## Results so far

### Steady state, warm container

A true statement about the regime that never failed:

| K (total trees) | `GetAllTreeIdsAsync` P50 | mean | max |
|---|---|---|---|
| 5 | 2.06 ms | 2.53 ms | 25.7 ms |
| 25 | 2.00 ms | 2.64 ms | 22.5 ms |
| 165 | 2.10 ms | 2.84 ms | 141.6 ms |

Flat across a 33x range, and 165 is over 8x the live estate. Under sustained
load the non-interleaved scan does show a heavy tail (P99 360 ms, max 1358 ms)
against point reads at P99 ~3.5 ms - two orders of separation, three orders
short of the 30 s deadline.

Depth at 163,860 entries / >=1,281 leaves / 448 MB (~7% of the live estate)
produced zero deadlines on every arm.

### Cold start, reminder table preserved - the storm regime

Six cold starts, all with the per-tree background services confirmed birthed at
exactly one row per tree before the measurement window closed.

| run | K | trees total | plateau (reg calls / 30 s) | timeouts | neighbour CPU at end |
|---|---|---|---|---|---|
| `birth-K80` | 80 | 85 | 1087 | **28** | **5.96 cores, 11.89/12 GiB** |
| `birth-K80-r2` | 80 | 85 | 1021 | 0 | 1.51 cores, 11.43 GiB |
| `birth-K80-r3` | 80 | 85 | 1035 | 0 | 1.85 cores, 10.89 GiB |
| `birth-K80-fresh2` | 80 | 85 | 1132 | 0 | 0.56 cores, 10.28 GiB |
| `birth-K20-control` | 20 | 25 | ~350 | 0 | - |
| `pressure-K20-r1` | 20 | 25 | ~320 | 0 | 8.4-12.8 cores (induced) |
| `pressure-K80-heavy` | 80 | 85 | 1196 | 0 | **14.6-15.6 cores (induced)** |

Read that table before reading any conclusion drawn from a single run.

### Finding 4: at constant K and constant offered load, the storm appeared once in four

The four K=80 rows differ in offered registry load by less than 10%
(1021-1132 calls per 30 s, measured per sample, not assumed). Three produced
zero timeouts. One produced 28.

`birth-K80-fresh2` repeats `birth-K80`'s procedure exactly - same K, same fresh
volume, same immediate cold start after the creation burst - and is clean, which
disposes of the obvious "a freshly created estate storms" explanation. It also
had zero client-side deadlines during creation, where `birth-K80` had 48.

So tree count did not predict the storm, and neither did fresh-versus-settled.
The variable that separated the one storming run from the other three was
recorded only incidentally: a **neighbouring container** was burning 5.96 CPU
cores and sitting at 99.1% of its 12 GiB memory cap, while the rig's own silo
was idle at 0.27 cores. `run-birth-curve.ps1` now records host CPU per sample
(`HostCores`, `SelfCores`, `NeighbourCores`) rather than once at the end,
because that is the variable that actually moved.

**This does not confirm host contention as the mechanism.** `run-host-pressure.ps1`
manufactured 7.9 burner cores plus three I/O writers at K=20 and produced zero
timeouts, with the rig's silo still at 0.13-0.64 cores - the host had headroom
at 16 cores, so the silo was never genuinely starved. Finding 7 closes that gap
and the answer is still negative.

What can be said without qualification is the negative: **the
`(trees) x (per-tree services)` scaling law is not supported by this rig.** At
4.25x the live tree count and ~2x the live registry load, three of four cold
starts were entirely clean.

### Finding 7: scale and host CPU saturation, jointly, do not reproduce it either

Finding 4's pressure arm left one gap: 7.9 burner cores on a 16-core host is not
saturation, so a clean result there proves only that an unsaturated host is
harmless. `pressure-K80-heavy` closes it. It holds K=80 - the same scale as the
one storming run - and drives the burner to **1476% CPU**, putting the host at
**14.6-15.6 of 16 cores for the entire fifteen-minute window**:

| | `birth-K80` (stormed) | `pressure-K80-heavy` |
|---|---|---|
| K / trees total | 80 / 85 | 80 / 85 |
| host CPU | not saturated | **15.55 cores, 97% of 16** |
| rig silo CPU | 0.27 cores | 0.16-0.98 cores |
| registry calls / 30 s | 1087 | 1196 |
| timeouts | **28** | **0** |

The silo was left 0.2-1.0 cores and served a *higher* registry rate than the
storming run, with no timeout at any sample. So CPU starvation is not the
mechanism either, and the storm is now unreproduced across **seven** controlled
cells varying tree count 4x and host CPU 27x.

One difference between the storming run and every cell since remains untested:
its neighbour was at 99.1% of a 12 GiB memory cap, not merely busy on CPU. On
this host's WSL2 backend, memory pressure squeezes the VM page cache and shows
up as disk I/O latency rather than as CPU. **That test is deliberately not run
here**: inducing host-wide memory pressure would put the protected
`repocontextcontainer-repocontext` container at risk, which the isolation rule
forbids without qualification. It is recorded as an open lead, not as a result.

### Finding 5: when it does storm, it is served slowly, not never served

The storm census from `birth-K80`, after the attribution fix below:

| | live estate | `birth-K80` |
|---|---|---|
| total | 103 | 28 |
| on `latticeregistry/_lattice_trees` | 103 | 26 |
| never served (no status clause) | 103 | 2 |
| served slowly | 0 | 26 |
| members | `ResolveAsync` 49, `GetEntryAsync` 46, `GetShardMapAsync` 8 | `RegisterAsync` 10, `GetAllTreeIdsAsync` 8, `GetEntryAsync` 4, `ResolveAsync` 4 |
| interleaved members | 100% | 29% |
| window | ~2 min, ~5 min after start | t+70 s to t+252 s, then zero |

These are **different failures**, and the difference is the Phase 2
discriminator:

- Live is 100% never-served on members that all carry `[AlwaysInterleave]`. An
  interleaved call cannot queue behind another interleaved call, so the only
  thing that can block all of them at once is **activation**, which is not
  interleavable. That is consistent with activation-blocking.
- The rig's storm is 93% served-slowly and 64% on **non-interleaved** members
  (`RegisterAsync`, `GetAllTreeIdsAsync`), with the grain's own diagnostics
  reporting `NumRunning=3 NonReentrancyQueueSize=4`. That is **turn-token
  contention**, a different mechanism with a different remedy.

So the rig has not reproduced the live storm. It has produced an adjacent
saturation that is distinguishable from it, and the instrument is what makes
them distinguishable.

### Finding 6: the timeout parser attributed every storm to its caller

Worth recording because it nearly inverted the result. Orleans renders a request
as `[<silo> <source>]->[<silo> <target>]`, so source and target are
structurally identical and are told apart only by which side of the arrow they
fall on. The original `Get-FanInTimeoutTarget` matched the first `type/key` pair
anywhere in the body - the **source** - and so reported a storm that was
entirely on `latticeregistry/_lattice_trees` as a spread across
`sys.client/...`, `hotshardmonitor/...` and `shardhealingorchestrator/...`. The
first reading of `birth-K80` was therefore "these are not registry timeouts",
which is the opposite of the truth.

The unit fixture did not catch it because the fixture was **synthetic and
abbreviated**: its bodies had no `]->[` separator at all, so it exercised a
shape the parser never meets. The fixture bodies are now copied verbatim from a
real container log, and the member pattern no longer requires an `Async` suffix
(`IRemindable.ReceiveReminder` has none, so every collateral reminder-tick
timeout previously parsed as an empty member and vanished from the census).

A parser that guesses is worse than one that fails: the guess is confident,
plausible, and wrong. `Get-FanInTimeoutTarget` now returns empty fields when it
cannot find a request descriptor.

