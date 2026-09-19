# registry-fanin

A measurement rig for the `LatticeRegistryGrain` cold-start fan-in question.

It delivers an **instrument**, a **sidecar driver**, and **measurement
scripts**. It deliberately contains **no fix and no asserting test fixture** -
the behaviour under investigation is unresolved, so a fixture written now would
land red in CI and assert a conclusion the measurements do not yet support.

## Read this first: the rig does not reproduce the production storm

**If you came here expecting a storm, you will not get one, and that is the
finding rather than a failure of the rig.** Seven controlled cold-start cells,
varying tree count 4x and host CPU 27x, produced **one** storm between them - at
4.25x the live tree count and ~2x the live registry load, three of four K=80
cold starts were entirely clean. The proposed
`(trees) x (per-tree background services)` scaling law **is not supported**.
Full cells and dispersion are in [Results](#results-so-far); do not re-run them
expecting a different answer without first reading Findings 4 and 5.

More useful than the negative: the one storm the rig **did** produce is a
**different failure** from the production one, and there is a cheap test that
tells them apart.

### The signature test - use this before attributing any registry storm

Two orthogonal readings classify a timeout population. Take both from the log,
never from a counter scrape.

|  | **never served** | **served slowly** |
|---|---|---|
| how to read it | timeout clusters at the exact deadline; no `Status:`/`Diagnostics:` clause in the body | timeout carries a diagnostics clause; census duration tail approaches the deadline with a matching call count |
| means | the call was never admitted to the body | the call was admitted and the time went inside it |

|  | **on interleaved members** | **on non-interleaved members** |
|---|---|---|
| how to read it | member carries `[AlwaysInterleave]` - read from source, never inferred | member does not |
| means | turn-token contention is **excluded** - an interleaved call cannot queue behind another interleaved call | turn-token contention is available as an explanation |

Crossing them:

| | production storm | this rig's storm (`birth-K80`) |
|---|---|---|
| total / on the registry | 103 / 103 | 28 / 26 |
| **never served** | **103** | 2 |
| **served slowly** | 0 | **26** |
| members | `ResolveAsync` 49, `GetEntryAsync` 46, `GetShardMapAsync` 8 | `RegisterAsync` 10, `GetAllTreeIdsAsync` 8, `GetEntryAsync` 4, `ResolveAsync` 4 |
| **on interleaved members** | **100%** | 29% |
| grain diagnostics | - | `NumRunning=3 NonReentrancyQueueSize=4` |
| mechanism | **not** turn-token contention | **turn-token contention** |

The production storm is 100% never-served on members that **all** carry
`[AlwaysInterleave]` (`ResolveAsync` `ILatticeRegistry.cs:246`, `GetEntryAsync`
`:129`, `GetShardMapAsync` `:259`). This also explains, without needing any
further conjecture, why PR #3183's interleaving attributes were present in the
binary that produced the 103-timeout storm and did not prevent it.

The rig's storm is 93% served-slowly and 64% on non-interleaved members
(`GetAllTreeIdsAsync` `:169`, `RegisterAsync` `:096`). Different mechanism,
different remedy.

**Consequence for anyone writing the fix: a remedy validated against this rig
would be validated against the wrong failure.**

### What the never-served-on-interleaved reading does and does not license

It **excludes** turn-token contention. It does **not** leave activation-blocking
as the only survivor. Any mechanism that stalls the whole process stalls
interleaved calls too, because it sits beneath the scheduler rather than inside
it - a blocking gen2 GC pause being the realistic one. Activation-blocking and a
process-wide stall fit this signature **equally well**, and separating them is
not reachable from this rig at all:

- the census records from inside the grain body, so a process-wide stall
  suspends its clock along with everything else and is **elided** from the
  histogram rather than recorded in it;
- the distinguishing evidence is runtime counters on the affected process -
  `dotnet_gc_pause_time_total` against wall clock,
  `dotnet_gc_collections_total{gen2}`, and managed heap and committed size
  against the process memory cap.

An earlier revision of this file asserted activation-blocking as the sole
possibility. That was too strong and is corrected here.

## The question

The live estate produced 103 `System.TimeoutException`, all on the single
activation `latticeregistry/_lattice_trees`, all within 1.2% of the exact 30 s
deadline (so they were **never served**, not served slowly), all on read
members, from per-tree background services, inside a two-minute window about
five minutes after start - and then **zero for the following 46 minutes**.

The proposed scaling law was (number of trees) x (per-tree background services)
fanning in onto one activation.

## The instrument that was specified, and why it cannot exist

The brief asked for a `state_name="lattice-registry"` series in
`orleans_storage_read_latency_count`, so that the registry's activation-time
state load would be observable. **That series cannot exist, and its absence is
correct rather than a gap.** Orleans takes `state_name` from a grain's
`[PersistentState]` declaration. `LatticeRegistryGrain` declares none - its
primary constructor at `LatticeRegistryGrain.cs:27` carries no such attribute.
It is a POCO grain with no persistent state and no activation-time state load at
all, reading everything through the backing `_lattice_trees` Lattice tree
instead.

Absence from source plus absence from the metrics endpoint are two readings of
the same absence, so the claim is instead settled against the **siblings**, which
makes it two-sided:

| grain | `[PersistentState]` | series present |
|---|---|---|
| `TxRegistryGrain` (`:46`) | yes, `TxRegistryState` | yes - `state_name="tx-registry"`, 19 reads / 21 writes |
| `ViewRegistryGrain` (`:13`) | yes, `ViewRegistryState` | yes - `state_name="view-registry"`, 1 read |
| `LatticeRegistryGrain` (`:27`) | **no** | **absent** |

Two grains that declare it appear; the one that does not, does not. The
instrument is working and reporting a true negative. Manufacturing the specified
series would have meant giving the grain state it does not have, in order to
measure a load that does not happen.

What ships instead measures the thing that actually governs the activation
window: per-member service time, fan-in width, and which members are
non-interleaved (read from source, never inferred). See
[`RegistryCallCensus.cs`](../../src/lattice/BPlusTree/Grains/RegistryCallCensus.cs),
whose XML doc also records the instrument's own blind spot - a process-wide
stall suspends the census's clock along with everything else, so it is elided
from the histogram rather than recorded in it.

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
| `scripts/run-breadth.ps1` | steady-state enumeration vs K - **superseded and unrun**; see finding 1 above, the per-tree services are reminder-birthed so a steady-state sweep samples the wrong regime |
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
at 16 cores, so the silo was never genuinely starved. Finding 5 closes that gap
and the answer is still negative.

What can be said without qualification is the negative: **the
`(trees) x (per-tree services)` scaling law is not supported by this rig.** At
4.25x the live tree count and ~2x the live registry load, three of four cold
starts were entirely clean.

### Finding 5: scale and host CPU saturation, jointly, do not reproduce it either

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

### Finding 6: when it does storm, it is served slowly, not never served

**The classification table is [above the fold](#the-signature-test---use-this-before-attributing-any-registry-storm)
and is the reusable artefact; this section records only what is not there.**

Timing, which the summary table omits:

| | live estate | `birth-K80` |
|---|---|---|
| window | ~2 min, ~5 min after start | t+70 s to t+252 s, then zero |

Both are bounded windows followed by flat zero, which is the one respect in
which the rig genuinely resembles the live estate. It is also the weakest of the
available similarities, and on its own it misled the first reading of this rig -
a bounded window is what *any* transient produces, so it does not discriminate
between mechanisms and should not be cited as if it does.

The discriminating readings are never-served-vs-served-slowly and
interleaved-vs-not, and they say the two storms are **different failures**:
turn-token contention here, something that is not turn-token contention there.

An earlier revision of this section concluded from the live estate's
never-served-on-interleaved population that "the only thing that can block all
of them at once is activation". **That was too strong.** It correctly excludes
turn-token contention, but activation is not the only mechanism beneath it - any
process-wide stall blocks interleaved and non-interleaved members alike. See
[what that reading does and does not license](#what-the-never-served-on-interleaved-reading-does-and-does-not-license).

So the rig has not reproduced the live storm. It has produced an adjacent
saturation that is distinguishable from it, and the instrument is what makes
them distinguishable.

### Finding 7: the timeout parser attributed every storm to its caller

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

## Arms deliberately not run

Recorded so a future session does not rebuild them believing they are gaps.

**Sustained-load steady-state hold (originally contention arm (b)).** This was
to hold load for 30 minutes without a restart, testing whether the
"saturates at cold start only, healthy thereafter" characterisation is true at
all. **Retired: the live estate has already run a better version of it.** Four
hours of uninterrupted steady state on the protected container - 243,934
registry calls at 17.01 calls/s across one activation, 46 faulted (0.019%), and
**zero** timeouts, against real trees, real views, real materialiser pins and
`txregistry` traffic this driver never generates. A synthetic 30-minute hold
would be a weaker instance of an experiment that has run at longer duration and
higher fidelity. Note the denominator: that is not "zero because idle".

**Host memory pressure.** The one condition separating the single storming run
from every clean cell since is that its neighbour sat at 99.1% of a 12 GiB
memory cap, not merely busy on CPU. Inducing that would endanger the protected
container, which the isolation rule forbids without qualification. It does not
need inducing in any case - the protected container is itself the instrument,
and its own runtime counters are the place to look. Open lead, not a result.

