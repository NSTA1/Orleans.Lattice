---
agent_spec: "docs/agents/api/lattice.json"
---

# WAL saturation back-pressure signal

This document is the design reference for the per-tree saturation signal exposed by `IWalSaturationSignal`, `IWalSaturationObserver`, and `WalSaturationStateChange`. It complements the call-site reference in [`api.md`](api.md#wal-saturation-back-pressure) and the operational sizing context in [`wal-tuning.md`](wal-tuning.md#when-lifting-the-cap-stops-helping).

## Motivation

The WAL write path on a single silo has two implicit ceilings:

1. The writer-side admission semaphore, capped per partition at `LatticeOptions.WalMaxPendingBatches` (default 16). When the cap is reached, new `AppendAsync` callers park on the semaphore until a peer dispatch releases its slot.
2. The downstream shard activation and its storage provider. A wedged shard or a saturating storage account holds dispatches in flight long enough that the admission semaphore stays at cap, and parked callers eventually trip `WalAppendDispatchTimeout` (default 30 seconds) and surface a `TimeoutException` to the foreground commit path.

Before this signal existed, the only way a caller knew the silo was approaching either ceiling was to observe the failure tail - a `TimeoutException` from `SetAsync` / `SetManyAsync`. By the time the failure surfaced, the silo had already accumulated hundreds of in-flight transactions, many of which would surface as `failed=N` on a benchmark cohort or as `UnobservedTaskException` in production apps whose dispatchers had forgotten to await.

The saturation signal exposes the writer-side admission gate's pressure as a typed, observable, per-tree state so callers can throttle their offered load *before* the failure tail surfaces. It does **not** change the underlying mechanics: `WalMaxPendingBatches` still caps the in-flight depth, and `WalAppendDispatchTimeout` still bounds individual dispatches. The signal makes the existing pressure visible.

## State contract

| State | Meaning | Caller action |
|-------|---------|---------------|
| `Healthy` | Admission depth well under cap, no recent dispatch-timeout trips, no recent provider-side commit failures. | Continue dispatching at full rate. |
| `Throttled` | Admission depth at or above `WalSaturationThrottledRatio` (default 0.75) of the cap on at least one partition, **or** a sustained materialiser drain-lag condition (on by default), **or** (when enabled) a sustained durable pin-write-latency condition. | Slow down the offered rate. Continue dispatching - new appends will land, each append to a `Throttled` partition paced by `WalThrottledAdmissionPace` (default 25 ms) at the writer's admission gate. |
| `Saturated` | Recent dispatch-timeout trip rate at or above `WalSaturationDispatchTimeoutThreshold` (default 1) in a single sample window, **or** recent provider-side commit failure rate at or above `WalSaturationProviderFailureRateThreshold` (default 1) in a single sample window, **or** (when enabled) the flush-latency classifier input has observed at least one provider-flush longer than `WalSaturationFlushLatencyThreshold` in each of the last `WalSaturationFlushLatencySampleWindows` sample windows in a row, **or** - only when `WalSaturationAcuteOnly` is `false` - an admission semaphore at cap with parked callers. With the default `WalSaturationAcuteOnly = true` an at-cap partition reads `Throttled` instead. | Pause new appends until the state returns to `Healthy`. Continuing to dispatch parks each append at the writer's admission gate for up to `WalAdmissionSaturationWaitBudget` (default 5 s) and refuses it with `LatticeSaturatedException` if its partition is still `Saturated` when that budget runs out (with the gate disabled, a caller parked on a full admission semaphore can instead fault with `TimeoutException` after `WalAppendDispatchTimeout`), rather than improving throughput. |

States are totally ordered (`Healthy < Throttled < Saturated`), and the tree's state rolls up every partition and shard of that tree: the admission-depth inputs read its worst partition, and the trip-count inputs sum each window's trips across its shards. The state space is open for additive extension - future minor releases may introduce intermediate or recovery states (for example a `Recovering` state when pressure has just dropped) without breaking subscribers that switch on the three documented values.

### Throttled regime stability (recovery window)

The per-tick `max(depth_ratio)` across the tree's WAL partitions is structurally bursty: one partition fills to cap, drains entirely in the next tick, the next partition fills. The cross-partition `max` consequently oscillates between `~1.0` (one partition at cap) and `~0.0` (the partition just emptied, the next one not yet filling) inside a single sampler period. Without smoothing, the classifier would see `Saturated` then `Healthy` in alternating ticks and the advisory `Throttled` band (the inclusive range `[WalSaturationThrottledRatio, 1.0)` of `max(depth_ratio)`) is never observed as a stable regime. (With the default `WalSaturationAcuteOnly = true` an at-cap partition reads `Throttled`, so this depth-driven oscillation arises only when that option is `false`; the window still smooths recovery after the acute causes that raise `Saturated`.)

The classifier applies an **upgrade rule** to make `Throttled` observable across the burst cycle: when the current-tick classification is `Healthy` but the tree was observed at `Saturated` within the past `LatticeOptions.WalSaturationRecoveryWindow` (default 1 second), the classifier upgrades the tree to `Throttled` instead. The upgrade rule preserves three invariants:

- **`Healthy -> Saturated` latency is unchanged.** `Saturated` still fires on the current tick's saturating condition, so the public saturation-signal surface's bound (transition latency under one `WalSaturationSampleInterval`) still holds.
- **Recovery eligibility follows the window.** Once a finite recovery window elapses and the current sample would otherwise classify the tree `Healthy`, the upgrade no longer applies. Pending `WaitForHealthyAsync` waiters are then released on eligible sampling ticks, at most `WalSaturationRecoveryReleaseBatch` per tick (see [Paced recovery release](#paced-recovery-release)). This is not a wall-clock admission or append-completion bound: sampling must run, and other pressure inputs can keep the tree non-Healthy.
- **Per-tree independence is preserved.** A tree that has never been observed `Saturated` is never upgraded, regardless of any other tree's regime.

Two sentinels disable or invert the upgrade:

- `WalSaturationRecoveryWindow = TimeSpan.Zero`: the upgrade is disabled entirely. The classifier behaves the way the sampler shipped originally - the per-tick depth observation drives the regime directly. Use this when the workload's WAL drain pattern is non-bursty (single-partition trees, or workloads where every partition tracks closely in lockstep) and the upgrade introduces no benefit.
- `WalSaturationRecoveryWindow = Timeout.InfiniteTimeSpan`: the upgrade is sticky. Once `Saturated` has been observed, every subsequent `Healthy`-classified tick is upgraded to `Throttled` forever. Useful for tests that want a deterministic sticky-Throttled floor without arming wall-clock dependencies, and for defensive production deployments that prefer the saturation regime to be sticky.

### Paced recovery release

Recovery is not only a question of *when* a partition is declared `Healthy` but of *how many* parked callers that observation releases back to the admission path. `WaitForHealthyAsync` completes a saturation-signal wait; it does not reserve a slot in the writer's separate admission semaphore or complete a WAL append. Releasing every signal waiter at once can produce a herd that refills the pipeline and renews pressure. With `WalSaturationAcuteOnly = false`, an at-cap partition reads `Saturated`, so repeated recovery/re-saturation cycles can make the gate itself amplify contention. This historical failure mechanism motivates paced release; it is not a proof that every unpaced execution makes no progress.

`WalSaturationRecoveryReleaseBatch` (default `16`) bounds that release burst: a recovered partition releases at most this many parked signal waiters per sampler tick. Three properties define the pacing:

- **Oldest-first.** The longest-parked live signal waiters are released first. This orders that signal queue, not newcomers that bypass the queue or the separate admission semaphore; it does not guarantee service before a caller's wait budget expires.
- **Level-triggered.** Every eligible sampling tick releases a further batch, not just the recovery transition. Under continued qualifying samples, a fixed finite backlog therefore drains; an edge-triggered release would leave the residue parked until another transition. A new saturation episode can suspend release again.
- **Per partition.** Each partition meters its own backlog, so draining one does not consume another's quota.

Under the default `WalSaturationAcuteOnly = true` the writer's parked callers wait only for their partition to leave `Saturated`, so the same metered release runs on every tick that reads the partition `Throttled` or `Healthy`. The public tree-scoped `WaitForHealthyAsync` is metered the same way, per tree, on every tick that reads the tree `Healthy`.

The default release batch equals the default `WalMaxPendingBatches`: at most one default pipeline-fill's worth of signal waiters is released per eligible tick, or about 80 signal wait completions per second at an uninterrupted default 200 ms sample cadence and a nonempty queue. This is a release-rate calculation, not a throughput or admission guarantee. Set the batch to `0` to release every parked waiter at once (the original behaviour).

Progress after release is conditional on semaphore capacity, storage completion and caller scheduling. Fairness alone supplies no finite latency bound, and an unbounded offered load does not imply an eventually permanent `Healthy` state. See [Verified scheduling and progress](verified-scheduling.md) for the separation between gate-release, capacity-conditioned admission and storage progress.

### Acute-only classification (on by default)

Historically an admission semaphore at its cap was a `Saturated` input. That made ordinary pipelining - a partition running at the cap it was sized for - close the writer's admission gate, and a parked caller then waited for `Healthy`, which the recovery window and the paced release both defer, while a newcomer arriving during the same `Throttled` window passed straight through.

`WalSaturationAcuteOnly` (default `true`) narrows `Saturated` to acute causes: dispatch-timeout trips, provider failures, and sustained flush latency. A partition at its cap falls through to the depth-ratio test and reads `Throttled` (the semaphore still enforces the cap itself), and a caller parked at the gate resumes as soon as its partition leaves `Saturated`. Set it to `false` to restore the historical classification, in which an at-cap partition reads `Saturated` to every consumer of the signal. See [`WalSaturationAcuteOnly`](configuration.md#walsaturationacuteonly).

### Flush-latency classifier input (opt-in)

The original three Saturated inputs (admission depth at cap, dispatch-timeout trips, provider-failure trips; the first of these now counts only when `WalSaturationAcuteOnly` is `false`) all require the WAL writer to have *already* shed work: callers parked on the admission semaphore, dispatch tasks tripped on their per-partition timeout, or the storage provider returned an error. The classifier therefore has a **small-batch blind spot** - a workload whose every flush calls into the provider just *slowly* (provider getting close to capacity, but not yet erroring or throttling enough to back up admission depth) can sail past all three inputs and never register as `Saturated`, even when steady-state flush latency has crept up by an order of magnitude.

`WalSaturationFlushLatencyThreshold` (default `null`, meaning disabled) and `WalSaturationFlushLatencySampleWindows` (default `3`) close that gap. When the threshold is set:

- Each WAL partition counts a per-(tree, partition) trip every time a provider flush's wall-clock latency meets or exceeds the threshold, from the same measurement that feeds `wal.append.provider.duration` on dashboards, so there is no second measurement path.
- On every sample tick the classifier reads the delta from the prior tick. A tree with a non-zero delta in the current window has the per-tree consecutive-window counter incremented; a tree with a zero delta has the counter reset.
- When the counter reaches `WalSaturationFlushLatencySampleWindows`, the classifier upgrades the tree to `Saturated`. The "sustained over N windows in a row" gate eliminates noisy single-flush blips (a one-off GC pause, a leader-election spike) while still firing well before the writer would otherwise back up to one of the existing inputs.

Sizing guidance: the threshold should be 5-10x the steady-state p99 of `wal.append.provider.duration` for the deployment's normal workload. The sample-window count should be at least 3 so a single noisy tick cannot flip the regime. For most Azure-Tables backed deployments a 500 ms - 1 s threshold over 3 windows (600 ms wall-clock at the default 200 ms sample interval) is a reasonable starting point.

The input is purely additive: leaving `WalSaturationFlushLatencyThreshold` at its default `null` means the WAL partition skips the trip-counter increment entirely and the classifier behaves exactly as it shipped before the input was introduced.

### Materialiser drain-lag classifier input (on by default)

The flush-latency input watches how fast the WAL *writes*; the drain-lag input measures whether consumers keep up. Drain age is the age of the oldest unread WAL offset, measured from its local append time against each fresh consumer's inclusive per-partition checkpoint, not from mutation HLCs. A caught-up checkpoint reads zero even when the leaf's applied HLC is hours old. Single and batch appends retain at most 8,192 offset timestamps per tree/partition. After restart, remote appends or eviction leave gaps: the sampler warns and uses only the age of an observed unread append as a lower bound; a wholly unknown age emits no sample, never a fabricated zero or an HLC fallback. Custom consumers must report offsets with `IWalCursorRegistry.ReportCheckpointAsync`. Custom registries must store those offsets and implement `GetRegisteredTreeNamesAsync` to discover cursor-only trees after restart; null explicitly means unavailable discovery and produces a warning, not an empty-population success. `WalSaturationMaterialiserLagThreshold` remains a `TimeSpan` (default 30 seconds), and `orleans.lattice.materialiser.drain_lag` remains in milliseconds. After `WalSaturationMaterialiserLagSampleWindows` consecutive breaches (default 3), the tree is held at `Throttled`, never `Saturated`. Causal-vector, blocked-floor and durable block-pin guards are unchanged. `orleans.lattice.materialiser.lagging_consumers` counts over-threshold consumers from the same snapshot.

`WalDrainLagConsumerFreshness` (default 5 minutes) keeps a cold leaf from holding a live tree throttled: a consumer whose latest report is older than the window is left out of the lag minimum (while still pinning the WAL GC trim floor), and so is a leaf whose cursor has neither advanced within the window nor sits inside it. A never-checkpointed leaf's block pin is not treated as lag. Set the threshold to `null` to disable the input, and the freshness window to `TimeSpan.Zero` to restore the historical all-consumers minimum.

While a tree's lag is over the threshold the sampler names up to three eligible consumers in descending unread-append age order, in Warning logs rather than metric tags, sharing an observation id and timestamp. Each line includes the consumer id, applied HLC, report age and last HLC advance age. The first observation occurs on the crossing, then at most once per global `WalDrainLagHolderLogInterval` (default 10 minutes); `null` logs only crossings. Recovery resets the interval. See [holder observations](metrics.md#drain-lag-holder-observations).

### Durable-floor classifier input (opt-in)

The flush-latency input above closes the blind spot on the WAL *write* path. A second, structurally different blind spot sits on the WAL *retention* path, and issue #2012 is what exposed it.

Every classifier input described so far is derived from in-memory state. That is also true of the materialiser drain-lag input, which compares the WAL head against the in-memory leaf cursor registry. But the floor the WAL garbage collector actually trims against is not the in-memory registry - it is the **durable** materialiser pin store. Those two can diverge completely: leaves keep reporting, the in-memory registry keeps advancing, drain lag reads zero, and every input reports `Healthy`, while the durable pin store is wedged and the trim floor has not moved in hours. The WAL grows without bound and nothing in the signal says so. That is precisely what happened in issue #2012, where a pin shard's non-reentrancy queue reached 165 deep and single writes ran for 43 seconds with the saturation signal reading `Healthy` throughout.

`WalSaturationMaterialiserPinLatencyThreshold` (default `null`, meaning disabled) and `WalSaturationMaterialiserPinLatencySampleWindows` (default `3`) close that gap:

- The reporting leaf measures each **durable** pin write at its own call site and counts a per-(tree, partition) trip when the write meets or exceeds the threshold, or faults. Measuring at the call site rather than inside the pin grain is deliberate: it captures the time a report spent queued ahead of the pin-store activation, which is both what the reporting leaf actually experiences and the component of the delay that made #2012 pathological. It is also what makes the signal work at all - a pin activation lives on one silo while its reporting leaves are cluster-wide, so only the caller-side measurement is visible to the reporting silo's sampler.
- On every sample tick the classifier reads the delta from the prior tick, incrementing a per-tree consecutive-window counter on a non-zero delta and resetting it on a zero delta - the same shape as the flush-latency input.
- When the counter reaches `WalSaturationMaterialiserPinLatencySampleWindows`, the classifier holds the tree at `Throttled`.

**This input holds at `Throttled` and never escalates to `Saturated`.** That bound is load-bearing, not conservatism. `Saturated` engages the writer-side admission gate, which fast-fails callers with `LatticeSaturatedException` once `WalAdmissionSaturationWaitBudget` expires. A stalled pin store is a retention-floor maintenance problem, not an inability to accept writes, so escalating would convert slow WAL trimming into user-visible write failures and make the very incident the input exists to detect strictly worse. Slowing producers is the correct response: it gives the pin store room to drain. The materialiser drain-lag input is bounded at `Throttled` for the same reason.

Sizing guidance: set the threshold well above the duration of a healthy durable pin write, so healthy traffic stays quiet. `orleans.lattice.materialiser.pin.durable_write_latency` records only the writes that met the threshold or faulted (and nothing while the input is disabled), so it shows the trips rather than a steady-state baseline to size from. If the input fires persistently, the corrective knob is [`WalMaterialiserPinBuckets`](configuration.md#walmaterialiserpinbuckets), the minimum number of independently persisted slots each pin shard splits its pins across (a shard also widens itself once a slot outgrows its byte budget), so raising it shrinks the durable blob each pin write rewrites; `orleans.lattice.materialiser.pin.reports_shed` reports how much steady-state pin traffic the reporters are already dropping to protect the store.

The input is purely additive: left at its default `null`, the reporting leaf skips the increment entirely and the classifier behaves exactly as it did before the input was introduced.

### Cause attribution

Two inputs now classify to `Throttled` for reasons unrelated to admission depth, so `WalSaturationStateChange` carries a `Cause` discriminator (`WalSaturationCause`) naming which input drove the classification: `None`, `DispatchTimeouts`, `ProviderFailures`, `FlushLatency`, `AdmissionDepth`, `MaterialiserDrainLag`, or `MaterialiserPinLatency`. The same cause is published, spelled `dispatch_timeouts`, `provider_failures`, `flush_latency`, `admission_depth`, `materialiser_drain_lag`, `materialiser_pin_latency` or `none`, as the `cause` tag on the `orleans.lattice.wal.saturation.transitions` counter (the state gauge carries only the `tree` tag and the derived `tenant` label), so a dashboard can distinguish "producers are outrunning the WAL" from "the WAL cannot retire what it already has" without correlating across panels.

Sampled cause attribution follows the classifier's priority: `DispatchTimeouts`, `ProviderFailures`, `FlushLatency`, `AdmissionDepth`, `MaterialiserDrainLag`, then `MaterialiserPinLatency`, considering the inputs applicable to the raw classified state. It is computed *before* the recovery-window floor described above. A tree held at `Throttled` only by that floor therefore reports `None`; the cause identifies a sampled input, not the recovery hold itself.

Refusal provenance is separate. `LatticeSaturatedException` carries `TreeId` and `SaturationSource`, identifying the refusing seam, but has no `Cause` field. A refusal's source does not identify the sampled pressure cause; that attribution belongs to `WalSaturationStateChange` and transition telemetry.

`Cause` is an additive field on an existing serialized type and the enum is open for extension: subscribers that do not read it are unaffected, and one that switches on it should treat an unrecognised value as `None`.

## Resolution and scope

- **Per-tree.** A multi-tree silo does not lump every tree's pressure together. A `Saturated` tree A does not affect tree B's signal. `IWalSaturationSignal.GetAggregateState()` exists for callers that want a single global signal across every observed tree (a TCP listener that fronts every tree at once, for example) and returns the worst case across the per-tree views.
- **Per-partition, for the writer.** Alongside each tree's roll-up the sampler publishes a verdict for every WAL partition it observed (issue #3348), re-running the classifier with that partition's own admission depth, so tree-wide causes still reach every partition while the admission-depth inputs stay partition-local. The writer's admission gate and throttle pacing act on that per-partition verdict; the public getters report the tree-wide roll-up.
- **Keyed by the id the WAL is written under.** The sampler records every verdict under the tree id its writer-side trackers carry, which is the physical tree's id once the tree is [aliased](tree-registry.md#tree-aliasing) to another physical tree. A tree's alias is set or changed only by an operator's set-alias (`ILatticeTreeAdmin.SetTreeAliasAsync`), a shadow-cutover restore or its revert, a resize or its undo, and a schema remediation; a reshard leaves it unchanged. The writer's admission gate and throttle pace look the verdict up under that same id, and `WalSaturationStateChange.TreeId` carries it. `GetCurrentState`, `WaitForHealthyAsync` and `ApplyBackPressureAsync` look up exactly the id they are passed; they do not resolve aliases. The plain writer, [saga quiesce gate](#saga-side-quiesce-gate-atomic-write-saga) and a view maintainer obeying `LatticeViewOptions.ObeySourceBackpressure` use the bound physical source identity. The saga checks its captured WAL partitions, falling back to the physical tree verdict when partition-scoped lookup or a usable snapshot is unavailable. The [snapshot-open shed](#snapshot-cursor-open-shedding) and a replication receiver's flow control (`WalSaturationReceiverFlowControlPolicy`) instead query the supplied logical name. An unobserved alias reads `Healthy` and is not shielded by the target physical copy's signal; this does not make every aliased consumer unshielded. The `orleans.lattice.wal.saturation.state` gauge and the transitions counter report a physical copy that a resize, a restore or a remediation created under the logical id it was created for, while an independent tree that an operator aliased a tree onto keeps its own id (see [The `tree` dimension across aliasing](metrics.md#the-tree-dimension-across-aliasing)).
- **Per-silo.** The signal is scoped to a single silo process. Each silo's WAL writer owns the admission gate for the traffic it dispatches, and the sampler reads only its own process's partition trackers and trip counters (process-wide state, so silos hosted in one process - an in-process test cluster - share it). A multi-silo cluster's aggregate health is a dashboard concern (take the maximum of the `orleans.lattice.wal.saturation.state` observable gauge across silos - its value is the regime's ordinal, so the maximum is the worst silo's regime), not a runtime one.
- **Per-tick.** A silo-scoped hosted sampler runs a sampling and publication pass, then waits `LatticeOptions.WalSaturationSampleInterval` (default 200 ms) before the next pass. This is a configured inter-sample delay, not a wall-time deadline. Runtime scheduling, sampling work and awaited observers can delay later samples and transition delivery; neither one interval nor one second is an unconditional latency bound. Classifier sample windows, the recovery hold and paced waiter release still apply.
- **Classified with the default options.** The sampler reads its classifier settings - `WalSaturationThrottledRatio`, the dispatch-timeout and provider-failure thresholds, the drain-lag threshold and consumer freshness, every input's sample-window count, whether the flush-latency and pin-latency inputs are on, the recovery window, the release batch and `WalSaturationAcuteOnly` - from the default (unnamed) options, so a per-tree override of any of them does not change how the sampler classifies that tree. The writer reads its own gate and pacing settings per tree. See [Configuration](configuration.md) for each option's scope.

## Idle cost

The sampler is the only piece of the signal that runs unconditionally:

- **When no callers are subscribed** (no observers, no polling getters, no awaiters): the sampler still ticks, but per-tick work is a few `ConcurrentDictionary` enumerations plus per-tree state arithmetic and, while the drain-lag input is on, one in-memory cursor-registry read and one `orleans.lattice.materialiser.drain_lag` recording per tree this process has routed a write for. On a silo with no tree traffic since the process started the loop's work is a no-op - the dictionaries are empty.
- **When polling getters are called**: one `ConcurrentDictionary.TryGetValue` returning an `enum` for `GetCurrentState`. No allocation, no grain call. `GetAggregateState` instead walks the states of every tree this silo has observed, stopping at the first `Saturated` one, still with no grain call.
- **When `WaitForHealthyAsync` is called on an already-Healthy tree**: returns `Task.CompletedTask` synchronously. No allocation.
- **When `WaitForHealthyAsync` is called on a non-Healthy tree**: one `TaskCompletionSource` plus an optional `CancellationTokenRegistration`. The TCS settles on a sample tick that observes the tree at `Healthy`; each such tick releases at most `WalSaturationRecoveryReleaseBatch` waiters for the tree, oldest first, so a large parked population drains over several ticks.
- **When observers are registered**: one `ValueTask` per transition per observer. Transitions are rare (one per regime change), not per-call.
- **On the `SetAsync` / `SetManyAsync` hot path**: a `Healthy` partition reads the admission and pacing dictionaries without awaiting. `Throttled` adds `WalThrottledAdmissionPace`; `Saturated` parks at the admission gate. Successful append acknowledgements record bounded per-offset append times for drain-age sampling; dispatch-timeout and provider-failure counters change only on those failures.

## Choosing a shape

The three surfaces are designed to compose. Pick the one that matches the consumer's natural control flow.

| Consumer shape | Recommended surface |
|----------------|---------------------|
| A TCP read loop that wants to check before each `ReadAsync` | Polling: `signal.GetCurrentState(treeId)`. Cost is a single dictionary lookup per check. |
| A producer whose mainline needs to "pause until the silo recovers" before continuing | Await: `await signal.WaitForHealthyAsync(treeId, ct)`. Synchronous fast-path when already `Healthy`. |
| A control plane / circuit breaker / sidecar that reacts to *transitions* as events | Push: `IWalSaturationObserver` registered in DI. Single callback per regime change. |
| A Grafana dashboard or alert | Metrics: `orleans.lattice.wal.saturation.state` (gauge) + `orleans.lattice.wal.saturation.transitions` (counter). |
| A producer that wants the canonical per-call response to every state without writing the switch itself | Helper: `await signal.ApplyBackPressureAsync(treeId, ct)` - a no-op on `Healthy`, a short delay on `Throttled` (`WalSaturationSignalExtensions.DefaultThrottledDelay`, 1 ms, unless a delay is passed), a `WaitForHealthyAsync` park on `Saturated`. See the [call-site reference](api.md#canonical-per-call-back-pressure-helper-applybackpressureasync). |

A host may use all three at once without drift - they all read from the same per-tree state cache populated by the sampler.

Observers are awaited one at a time, in registration order, inside the sampler loop, so an observer that blocks delays the next sample and every transition after it; hand long work to a queue drained elsewhere. An exception an observer throws is logged as a warning and suppressed, and the remaining observers still run. Each `WalSaturationStateChange` carries the tree id, the previous and new states, the `Cause`, the `ObservedAt` instant and two best-effort attribution slots, neither filtered by `Cause`: `AttributedPartition`, the partition with the highest admission depth in the window (`null` when every partition was idle), and `AttributedShard`, a shard that recorded a dispatch-timeout, provider-failure, flush-latency or pin-latency trip in the window (`null` when none did).

## Strategy is the caller's

The signal carries no strategy. A caller seeing `Saturated` may:

- Pause TCP reads (the canonical bench pattern - the kernel TCP window naturally back-pressures the producer).
- Shed offered traffic at a load balancer.
- Write the offered request to a side buffer / outbox / queue.
- Reject the request to the upstream caller with a typed back-pressure error.
- Slow the producer down to a heartbeat dispatch rate while keeping the connection open.

The library is agnostic. The surface is the **signal**; the strategy is the application's.

## Relationship to other surfaces

- **`LatticeOptions.WalMaxPendingBatches`.** The admission semaphore the signal reads from. This is the underlying cap; the signal makes it observable. Lifting the cap reduces how often the signal fires but does not change its contract.
- **`LatticeOptions.WalAppendDispatchTimeout`.** The dispatch deadline whose trips feed the second source of the `Saturated` classification. A non-zero dispatch-timeout trip rate is the failure-tail surface; the saturation signal turns it into a leading-edge surface.
- **`LatticeOptions.WalSaturationProviderFailureRateThreshold`.** The third Saturated input, added so the signal also covers the regime where the downstream storage provider's commit calls return quickly (so neither the admission depth nor the dispatch deadline crosses the threshold) but terminally fail at a high rate - the canonical pattern on the Azure Tables single-account 409-Conflict burst. Counts the exceptions that escape the writer's outbound appends to a WAL partition other than its own dispatch-deadline timeout, any cancellation and a shutdown refusal (`LatticeShuttingDownException`), per `(tree, partition)`, per sample window. Set to `0` to disable the trigger entirely.
- **`LatticeOptions.WalAdmissionSaturationWaitBudget` and [`LatticeSaturatedException`](api.md#saturation-back-pressure---latticesaturatedexception).** The library-side consumer of the signal. The WAL writer's pre-admission gate consults the verdict for the append's WAL partition before each admission acquire; on `Saturated` it parks up to the configured budget (default 5 s) - resuming, under the default `WalSaturationAcuteOnly = true`, as soon as the partition leaves `Saturated` - and, on expiry, refuses the dispatch with the typed exception. The atomic-write saga's quiesce gate uses its own bounded quiesce wait before each batched dispatch, refusing with the same exception on expiry rather than re-entering RowKeys into a still-throttled storage account. Both gates make the *runtime* leading-edge surface load-bearing: callers see typed back-pressure in budget time instead of parking on the admission semaphore until `WalAppendDispatchTimeout` (default 30 s). Set `WalAdmissionSaturationWaitBudget = TimeSpan.Zero` to opt out of the writer-side gate; the saga quiesce gate remains active when a usable signal is registered and uses its captured physical identity and routed partitions, including on aliased trees (see [Resolution and scope](#resolution-and-scope)).
- **`LatticeShuttingDownException`.** Typed back-pressure exception the silo throws when an operation cannot complete because the WAL writer is draining for host shutdown. Distinct from `LatticeSaturatedException` (the runtime-leading-edge surface) - the shutdown exception is the terminal-refusal surface that fires when the silo has already decided to stop accepting traffic. Callers that observe this exception should abandon the operation rather than retry it; see the [API Reference - Shutdown back-pressure](api.md#shutdown-back-pressure---latticeshuttingdownexception) for the caller contract.
- **WAL writer-side drain on host shutdown** ([wal-tuning.md - bounded shutdown](wal-tuning.md#bounded-shutdown-when-the-writer-is-wedged)). The drain closes the shutdown half of the saturation problem - parked callers are released within bounded time of SIGTERM. The saturation signal closes the runtime half - callers can stop offering load before parking becomes the dominant regime.
- **`LatticeOptions.WalThrottledAdmissionPace`.** The writer's pacing delay on a `Throttled` partition; see [Writer-side throttle pacing](#writer-side-throttle-pacing).
- **`LatticeMetrics.WalAppendDispatchTimeouts`, `WalAppendAdmissionTimeouts`, `WalAppendAdmissionSaturationRefusals`, `WalAppendAdmissionWait`.** The existing instruments that surface the underlying signals. The saturation gauge / counter sit one layer up: they classify the regime rather than counting the individual events. `WalAppendAdmissionSaturationRefusals` is the counter that distinguishes the writer-side saturation-budget refusal path from the dispatch-deadline and drain-release paths. `LatticeMetrics.SaturationRefusals` (`orleans.lattice.saturation.refusals`) counts those writer-side refusals again, alongside every other refusing seam's, tagged by `source` - see [Caller-side recovery shape](#caller-side-recovery-shape).

## Library-side consumers: admission gate and saga quiesce gate

Before the consumer-coverage gates landed, the signal was a *publish-only* surface: the sampler observed the writer's state and emitted the regime to observers, but no in-library hot path consumed the signal to refuse work. Under the canonical Azure Tables single-account 409-Conflict regime that meant the sampler raised `Saturated` many times before the first observable failure, while every new dispatch still admitted into the per-partition semaphore and parked at the cap for the full `WalAppendDispatchTimeout` (default 30 seconds) before surfacing as a generic `TimeoutException` - the saturation signal was correct but operationally inert.

The consumer-coverage gap is closed by wiring two library-side gates that consult the signal directly, plus a throttle pace on the writer's local admission path (a third, read-side consumer - [snapshot-cursor open shedding](#snapshot-cursor-open-shedding) - is described after them):

### Writer-side admission gate

Except for terminal-only relief described below, before each append acquires its partition's admission slot, the writer reads the saturation verdict for **that WAL partition** rather than the tree-wide roll-up (issue #3348), so one partition at its cap cannot refuse appends routed to its idle siblings; genuinely tree-wide causes still reach every partition's verdict. On `Healthy` / `Throttled` the gate check is a single concurrent-dictionary lookup and the caller proceeds towards the semaphore with no allocation - a `Throttled` partition first pays the [throttle pace](#writer-side-throttle-pacing). On `Saturated` the writer parks for at most `LatticeOptions.WalAdmissionSaturationWaitBudget` (default 5 s), or for whatever remains of the enclosing call's `WalAdmissionSaturationCallBudget` if that is smaller (the call budget is unbounded by default, so by default only the per-append budget applies). With `WalSaturationAcuteOnly` at its default `true` the caller resumes as soon as its partition leaves `Saturated`; with it `false` the caller waits for `Healthy`. If the budget expires with the partition still `Saturated` the writer throws `LatticeSaturatedException` so the caller sees the back-pressure as a typed refusal in budget time. The cumulative refusal rate lands on the `orleans.lattice.wal.writer.append.admission_saturation_refusals` counter (tagged `tree`, `partition`), distinct from the admission-deadline counter (`admission_timeouts`, an admission wait that outlived `WalAppendDispatchTimeout`) and the drain-release counter (`drain.releases`). A custom `IWalSaturationSignal` that cannot answer per partition falls back to the tree-wide verdict, and a caller parked at the gate then waits for `Healthy`.

A borderline-recovery race (the wait expires AND the partition recovered between the wait expiring and the re-check firing) is suppressed: the writer re-reads the partition's verdict once after budget expiry and proceeds without refusal when it is no longer `Saturated`.

**Terminal-only relief.** A point append or an entire caller batch containing only `TxCommit` / `TxAbort` records bypasses the saturation-signal gate and throttle pace, so transaction terminal work is not blocked by the pressure signal it can help relieve (issue #4986). The writer classifies the whole caller batch before partition fan-out. A mixed caller batch does not receive this exemption, even if one of its partition buckets contains only terminal records. This is only a signal-gate and pacing bypass: admission capacity, storage acknowledgement, call deadlines, cancellation and shutdown/drain bounds remain in force.

### Writer-side throttle pacing

Except for terminal-only relief, before each append admits into its partition's semaphore, the writer also paces on the partition's verdict. On `Throttled` it waits a single `LatticeOptions.WalThrottledAdmissionPace` (default 25 ms) and then proceeds; on `Healthy` the check is one concurrent-dictionary lookup with no await, and on `Saturated` the pace is skipped because the admission gate already governs the dispatch. The pace never faults a caller with back-pressure and never escalates to `LatticeSaturatedException`; only the caller's own cancellation propagates out of it. It is what gives the `Throttled`-only inputs - materialiser drain lag and durable pin latency - an effect on a single silo's local write path, where no replication sender exists to drip-feed. A host-shutdown drain cuts a pending pace short. Set it to `TimeSpan.Zero` to opt out of local pacing.

### Saga-side quiesce gate (atomic-write saga)

Before each batched `SetManyAsync` dispatch the saga calls a private `QuiesceOnSaturatedAsync` helper. Prepare persists one routing snapshot: physical tree id, shard-map version, touched shard indices and the distinct WAL partitions to which the entries route. When the per-partition saturation signal is available, the saga checks and waits only on those WAL partitions, always until `Healthy` regardless of `WalSaturationAcuteOnly`; saturation on an unrelated partition does not stall this batch. It queries the captured physical tree id, matching the WAL writer's signal key. When the partition-scoped interface is available and the snapshot contains partitions, it is used independently; no tree-wide signal is required. If the partition interface or a usable partition snapshot is unavailable but a tree-wide signal is registered, the saga fails closed to that verdict and wait. If neither usable signal is registered, the optional quiesce remains skipped. The wait is capped at `min(MaxSagaQuiesceWait, perTree.WalAppendDispatchTimeout)` (30 s by default, capped at the writer-side dispatch deadline so the saga's quiesce always wins). On recovery, the saga re-resolves routing; if the map epoch changed on the same physical copy, it persists the refreshed snapshot, expands participants to include prior and new shard owners, and re-dispatches before deciding. Physical-copy changes use the existing rebind path. Repeated changes fail before the commit decision. On budget expiry with a captured WAL partition still `Saturated`, the saga's fast-path refuses with `LatticeSaturatedException` and preserves its persisted state at `Execute` with the current `NextIndex` - the caller's next retry on the same `operationId` resumes from where the refusal stopped, idempotently. Running the saga's compensation pass here would write its abort decision and abort terminals into the same still-throttled storage account and amplify the 409-Conflict burst exactly as the historical pre-saga-saturation-fast-path retry loop did.

The saga also detects a writer-side `LatticeSaturatedException` bubbling through `SetManyAsync`'s leaf fan-out (typically wrapped in an `AggregateException`) and takes the same fast path: it throws a new `LatticeSaturatedException` with `SaturationSource = AtomicWriteSaga`, carrying the originating tree id for caller attribution and the writer's refusal as its inner exception.

### Snapshot-cursor open shedding

A snapshot-isolated cursor open (`ILattice.OpenSnapshotKeyCursorAsync`, `OpenSnapshotEntryCursorAsync` and their predicate variants) reads the tree's saturation verdict before it fans out the per-shard baseline capture. On `Saturated` - and only then, so a `Throttled` tree stays browsable - the open is refused at admission with `LatticeSaturatedException` carrying `SaturationSource = SnapshotCursorOpen`, so a heavy baseline capture is not piled onto shard roots already collapsing under write back-pressure. The verdict is read under the tree id the caller addressed, without resolving an alias to its bound physical copy. An unobserved logical alias therefore reads `Healthy`; the target physical copy's signal alone does not shed that open (see [Resolution and scope](#resolution-and-scope)). `LatticeOptions.ShedSnapshotOpensWhenSaturated` (default `true`) controls it; set it to `false` to let every open proceed regardless of the regime.

### Caller-side recovery shape

Both gates surface the same typed exception. Caller recovery is uniform regardless of which gate refused:

```csharp verify
var entries = new List<KeyValuePair<string, byte[]>>
{
    new("k1", new byte[] { 0x01 }),
};

try
{
    await lattice.SetManyAsync(entries);
}
catch (LatticeSaturatedException ex)
{
    // ex.TreeId attributes the back-pressure to the specific
    // tree. Back off (typical 1-10s), then retry against the
    // same lattice activation - saturation is recoverable.
    await Task.Delay(TimeSpan.FromSeconds(2));
    // retry...
}
```

`ex.SaturationSource` names the seam that refused: `WalAdmission` for the writer-side gate, `AtomicWriteSaga` for the saga gate and `SnapshotCursorOpen` for the snapshot-open shed. The same type also carries four refusals that do not come from this signal: `SetManyFanOut`, when a host opts into a finite `LatticeOptions.SetManyFanOutBudget` (unbounded by default) and a batch write's fan-out outlives it - that refusal rolls nothing back, so branches that already committed stay committed - `SetManyEnvelope`, when a host opts into a finite `LatticeOptions.SetManyEnvelopeBudget` (unbounded by default) and a batch write's stages *sum* past it even when no single stage breached it on its own, which is the additive shape in [#2685](https://github.com/NSTA1/Orleans.Lattice/issues/2685) and which likewise rolls nothing back, `TxRegistryCapacity`, when the transaction-registry row a new atomic-write saga routes to (the tree's single registry, or one of its `LatticeOptions.TxRegistryShardCount` shards) is still at or above `LatticeOptions.TxRegistryAdmissionBudgetBytes` after its expired tombstones are reclaimed - refused before the saga writes anything; back off, then retry, because with one registry shard (the default) the capacity returns only as completed sagas age out of `TxDecisionRetention`, while with more than one the refused saga discards its transaction id so a retry draws a fresh shard that may have room - and `ReplayPermitAdmission`, the only source that is safe to retry automatically after a bounded, jittered delay. `Unspecified` means the refusing seam was not recorded (an exception from a host that predates attribution, for example) and is to be treated as not automatically retryable. A retry policy should therefore branch on `SaturationSource` rather than on the exception type alone.

This is distinct from `LatticeShuttingDownException` (where retries against the same silo activation never succeed). See the [API Reference - Saturation back-pressure](api.md#saturation-back-pressure---latticesaturatedexception) for the full caller contract.

Every refusal is also counted on the `orleans.lattice.saturation.refusals` counter, tagged `tree`, `tenant` and `source` - the snake_case form of `SaturationSource`, for example `wal_admission` or `replay_permit_admission` - so a refusal rate can be attributed to its seam without reading logs. A `replay_permit_admission` refusal also carries an `arm` tag naming the predicate that refused: `wait_exceeded` (a non-empty permit queue had fresh recent waits averaging at or above `WalReplayPermitMaxQueueWait`), `no_progress` (the queue was past its depth bound and no queued activation had acquired a permit for that long) or `gc_share` (a background starvation drive found the drives' share of the permits full, or no permit free). The `tree` value is the id held at the refusing seam, so on an aliased tree a `wal_admission` or `replay_permit_admission` refusal is counted under the physical copy's id. Each refusing seam records its own refusal, so a writer-side refusal that surfaces through an atomic-write saga is counted under both `wal_admission` and `atomic_write_saga`, a saga refusal because its own quiesce budget elapsed is counted twice under `atomic_write_saga`, and a background drive of a starved leaf checkpoint that is refused a replay permit is counted under `replay_permit_admission` although it raises no exception. The counter is not primed: a missing series means no refusal from that source since the silo started.

## Disabling the sampler

Setting `LatticeOptions.WalSaturationSampleInterval = Timeout.InfiniteTimeSpan` leaves the sampler dormant. The interval is read once when the sampler starts, so the setting takes effect on the next silo start. Every tree's signal stays at `Healthy` forever, the observable gauge publishes no series (a tree appears in it only once the sampler has observed it), and `IWalSaturationObserver` callbacks never fire. Polling getters and `WaitForHealthyAsync` continue to return the cached state - which is `Healthy` for every tree, because the sampler never wrote anything else. The hosted-service startup hook logs a debug message at silo start when this option is set so operators see the disablement decision in the silo log.

This is the right shape when:

- The host already has an external back-pressure surface (a load balancer or service mesh observing the same workload) and does not want the per-silo signal duplicated.
- A test fixture needs deterministic state (the library's own tests then drive the sampler one tick at a time through an internal seam).

## Formal model

[`WalAdmission`](../../spec/scheduling/WalAdmissionRefinement.md) separates the partition's
parked saturation waits, paced release and admission capacity. A release from
`WaitForHealthyAsync` completes a gate wait; it does not reserve an admission
semaphore slot or acknowledge an append. Its bounded residue claim is therefore
about recovery-release rounds under the module's stated premises, not a
wall-clock latency or arbitrary offered-load guarantee. See
[Verified Scheduling and Progress](verified-scheduling.md) for the production
detector boundary and the limits of fairness assumptions.

[`WalBackPressure`](../../spec/scheduling/WalBackPressureRefinement.md) checks
classifier scope, recovery hysteresis and the writer, saga, snapshot, receiver
and view consumers as one bounded control loop. Flush, drain, GC, checkpoint,
pin and terminal relief do not wait on the saturation signal they relieve;
terminal writes still require admission capacity and storage completion.
Recovery requires sampling and relief opportunities, disappearing acute causes
and, for eventual Healthy, no sustained throttling input and a finite recovery
window. Fairness does not supply capacity or a finite latency guarantee.

## See also

- [API Reference - WAL saturation back-pressure](api.md#wal-saturation-back-pressure) - the call-site reference with code snippets.
- [WAL Tuning - When lifting the cap stops helping](wal-tuning.md#when-lifting-the-cap-stops-helping) - the operational context that motivated this signal.
- [Metrics](metrics.md) - the full instrument set including the saturation gauge and transitions counter.
- [Configuration](configuration.md) - the options reference, including the validator rules that reject out-of-range values.
