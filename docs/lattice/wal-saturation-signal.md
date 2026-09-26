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
| `Saturated` | Recent dispatch-timeout trip rate at or above `WalSaturationDispatchTimeoutThreshold` (default 1) in a single sample window, **or** recent provider-side commit failure rate at or above `WalSaturationProviderFailureRateThreshold` (default 1) in a single sample window, **or** (when enabled) the flush-latency classifier input has observed at least one provider-flush longer than `WalSaturationFlushLatencyThreshold` in each of the last `WalSaturationFlushLatencySampleWindows` sample windows in a row, **or** - only when `WalSaturationAcuteOnly` is `false` - an admission semaphore at cap with parked callers. With the default `WalSaturationAcuteOnly = true` an at-cap partition reads `Throttled` instead. | Pause new appends until the state returns to `Healthy`. Continuing to dispatch will fault parked callers with `TimeoutException` rather than improving throughput. |

States are totally ordered (`Healthy < Throttled < Saturated`), and the tree's state is the worst case across every partition / shard for that tree. The state space is open for additive extension - future minor releases may introduce intermediate or recovery states (for example a `Recovering` state when pressure has just dropped) without breaking subscribers that switch on the three documented values.

### Throttled regime stability (recovery window)

The per-tick `max(depth_ratio)` across the tree's WAL partitions is structurally bursty: one partition fills to cap, drains entirely in the next tick, the next partition fills. The cross-partition `max` consequently oscillates between `~1.0` (one partition at cap) and `~0.0` (the partition just emptied, the next one not yet filling) inside a single sampler period. Without smoothing, the classifier would see `Saturated` then `Healthy` in alternating ticks and the advisory `Throttled` band (the inclusive range `[WalSaturationThrottledRatio, 1.0)` of `max(depth_ratio)`) is never observed as a stable regime. (With the default `WalSaturationAcuteOnly = true` an at-cap partition reads `Throttled`, so this depth-driven oscillation arises only when that option is `false`; the window still smooths recovery after the acute causes that raise `Saturated`.)

The classifier applies an **upgrade rule** to make `Throttled` observable across the burst cycle: when the current-tick classification is `Healthy` but the tree was observed at `Saturated` within the past `LatticeOptions.WalSaturationRecoveryWindow` (default 1 second), the classifier upgrades the tree to `Throttled` instead. The upgrade rule preserves three invariants:

- **`Healthy -> Saturated` latency is unchanged.** `Saturated` still fires on the current tick's saturating condition, so the public saturation-signal surface's bound (transition latency under one `WalSaturationSampleInterval`) still holds.
- **Recovery is bounded.** Once the recovery window elapses AND the current tick observes no saturation pressure, the tree drops to `Healthy` and any pending `WaitForHealthyAsync` completes. The window only delays recovery by its own value.
- **Per-tree independence is preserved.** A tree that has never been observed `Saturated` is never upgraded, regardless of any other tree's regime.

Two sentinels disable or invert the upgrade:

- `WalSaturationRecoveryWindow = TimeSpan.Zero`: the upgrade is disabled entirely. The classifier behaves the way the sampler shipped originally - the per-tick depth observation drives the regime directly. Use this when the workload's WAL drain pattern is non-bursty (single-partition trees, or workloads where every partition tracks closely in lockstep) and the upgrade introduces no benefit.
- `WalSaturationRecoveryWindow = Timeout.InfiniteTimeSpan`: the upgrade is sticky. Once `Saturated` has been observed, every subsequent `Healthy`-classified tick is upgraded to `Throttled` forever. Useful for tests that want a deterministic sticky-Throttled floor without arming wall-clock dependencies, and for defensive production deployments that prefer the saturation regime to be sticky.

### Paced recovery release

Recovery is not only a question of *when* a partition is declared `Healthy` but of *how many* parked callers that declaration admits. `WaitForHealthyAsync` parks one waiter per blocked WAL dispatch, and releasing all of them on a single recovery is self-defeating once the parked population exceeds the partition's admission capacity (`WalMaxPendingBatches`): the released herd refills the pipeline to its cap before any meaningful drain, the classifier flips straight back to `Saturated` (in this depth-driven form, when `WalSaturationAcuteOnly` is `false`, under which an at-cap partition reads `Saturated`), and the cycle repeats. Callers observe recovery after recovery while no caller makes progress, each losing a full `WalAdmissionSaturationWaitBudget` per cycle while holding a concurrency slot. The gate stops being a back-pressure valve and becomes an absorbing state.

`WalSaturationRecoveryReleaseBatch` (default `16`) bounds that burst: a recovered partition admits at most this many parked waiters per sampler tick. Three properties make the pacing safe:

- **Oldest-first.** The longest-parked callers are released first, so metering cannot starve the callers closest to exhausting their wait budget.
- **Level-triggered.** Every tick that observes the partition `Healthy` releases a further batch, not just the tick that observes the `Saturated -> Healthy` transition. The residue therefore drains on subsequent ticks; an edge-triggered release would strand everything past the first batch until the next saturation episode, which is a worse failure than the herd it replaces.
- **Per partition.** Each partition meters its own backlog, so draining one does not consume another's quota.

The default equals `WalMaxPendingBatches`, admitting exactly one pipeline-fill per tick - about 80 admissions per second per partition at the default 200 ms sample interval. Set it to `0` to release every parked waiter at once (the original behaviour).

### Acute-only classification (on by default)

Historically an admission semaphore at its cap was a `Saturated` input. That made ordinary pipelining - a partition running at the cap it was sized for - close the writer's admission gate, and a parked caller then waited for `Healthy`, which the recovery window and the paced release both defer, while a newcomer arriving during the same `Throttled` window passed straight through.

`WalSaturationAcuteOnly` (default `true`) narrows `Saturated` to acute causes: dispatch-timeout trips, provider failures, and sustained flush latency. A partition at its cap falls through to the depth-ratio test and reads `Throttled` (the semaphore still enforces the cap itself), and a caller parked at the gate resumes as soon as its partition leaves `Saturated`. Set it to `false` to restore the historical classification, in which an at-cap partition reads `Saturated` to every consumer of the signal. See [`WalSaturationAcuteOnly`](configuration.md#walsaturationacuteonly).

### Flush-latency classifier input (opt-in)

The original three Saturated inputs (admission depth at cap, dispatch-timeout trips, provider-failure trips; the first of these now counts only when `WalSaturationAcuteOnly` is `false`) all require the WAL writer to have *already* shed work: callers parked on the admission semaphore, dispatch tasks tripped on their per-shard timeout, or the storage provider returned an error. The classifier therefore has a **small-batch blind spot** - a workload whose every flush calls into the provider just *slowly* (provider getting close to capacity, but not yet erroring or throttling enough to back up admission depth) can sail past all three inputs and never register as `Saturated`, even when steady-state flush latency has crept up by an order of magnitude.

`WalSaturationFlushLatencyThreshold` (default `null`, meaning disabled) and `WalSaturationFlushLatencySampleWindows` (default `3`) close that gap. When the threshold is set:

- Each WAL shard counts a per-(tree, shard) trip every time a provider flush's wall-clock latency meets or exceeds the threshold, from the same measurement that feeds `wal.append.provider.duration` on dashboards, so there is no second measurement path.
- On every sample tick the classifier reads the delta from the prior tick. A tree with a non-zero delta in the current window has the per-tree consecutive-window counter incremented; a tree with a zero delta has the counter reset.
- When the counter reaches `WalSaturationFlushLatencySampleWindows`, the classifier upgrades the tree to `Saturated`. The "sustained over N windows in a row" gate eliminates noisy single-flush blips (a one-off GC pause, a leader-election spike) while still firing well before the writer would otherwise back up to one of the existing inputs.

Sizing guidance: the threshold should be 5-10x the steady-state p99 of `wal.append.provider.duration` for the deployment's normal workload. The sample-window count should be at least 3 so a single noisy tick cannot flip the regime. For most Azure-Tables backed deployments a 500 ms - 1 s threshold over 3 windows (600 ms wall-clock at the default 200 ms sample interval) is a reasonable starting point.

The input is purely additive: leaving `WalSaturationFlushLatencyThreshold` at its default `null` means the WAL shard skips the trip-counter increment entirely and the classifier behaves exactly as it shipped before the input was introduced.

### Materialiser drain-lag classifier input (on by default)

The flush-latency input watches how fast the WAL *writes*; the drain-lag input watches whether the leaf materialisers keep up with what was written. Each sampler tick measures a tree's lag as the WAL head's wall clock minus that of the slowest fresh leaf-materialiser cursor (clamped at zero) - the age of the oldest WAL entry that cursor has not yet drained - and publishes it as the `orleans.lattice.materialiser.drain_lag` histogram (ms, tagged by tree). Once the lag stays above `WalSaturationMaterialiserLagThreshold` (default 30 seconds) for `WalSaturationMaterialiserLagSampleWindows` (default `3`) consecutive windows, the classifier holds the tree at `Throttled`. Like the durable-floor input below, it never escalates to `Saturated`: a sustained drain lag slows callers - through the writer's [throttle pacing](#writer-side-throttle-pacing) and a replication receiver's flow control - rather than faulting them. While a tree is over the threshold, `orleans.lattice.materialiser.lagging_consumers` counts how many individual consumers trail the head by more than it, which separates one dormant consumer holding the minimum down from many genuinely falling behind.

`WalDrainLagConsumerFreshness` (default 5 minutes) keeps a cold leaf from holding a live tree throttled: a consumer whose latest report is older than the window is left out of the lag minimum (while still pinning the WAL GC trim floor), and so is a leaf whose cursor has neither advanced within the window nor sits inside it. A never-checkpointed leaf's block pin is not treated as lag. Set the threshold to `null` to disable the input, and the freshness window to `TimeSpan.Zero` to restore the historical all-consumers minimum.

### Durable-floor classifier input (opt-in)

The flush-latency input above closes the blind spot on the WAL *write* path. A second, structurally different blind spot sits on the WAL *retention* path, and issue #2012 is what exposed it.

Every classifier input described so far is derived from in-memory state. That is also true of the materialiser drain-lag input, which compares the WAL head against the in-memory leaf cursor registry. But the floor the WAL garbage collector actually trims against is not the in-memory registry - it is the **durable** materialiser pin store. Those two can diverge completely: leaves keep reporting, the in-memory registry keeps advancing, drain lag reads zero, and every input reports `Healthy`, while the durable pin store is wedged and the trim floor has not moved in hours. The WAL grows without bound and nothing in the signal says so. That is precisely what happened in issue #2012, where a pin shard's non-reentrancy queue reached 165 deep and single writes ran for 43 seconds with the saturation signal reading `Healthy` throughout.

`WalSaturationMaterialiserPinLatencyThreshold` (default `null`, meaning disabled) and `WalSaturationMaterialiserPinLatencySampleWindows` (default `3`) close that gap:

- The reporting leaf measures each **durable** pin write at its own call site and counts a per-(tree, shard) trip when the write meets or exceeds the threshold, or faults. Measuring at the call site rather than inside the pin grain is deliberate: it captures the time a report spent queued ahead of the shard's non-reentrant activation, which is both what the reporting leaf actually experiences and the component of the delay that made #2012 pathological. It is also what makes the signal work at all - a pin activation lives on one silo while its reporting leaves are cluster-wide, so only the caller-side measurement is visible to the reporting silo's sampler.
- On every sample tick the classifier reads the delta from the prior tick, incrementing a per-tree consecutive-window counter on a non-zero delta and resetting it on a zero delta - the same shape as the flush-latency input.
- When the counter reaches `WalSaturationMaterialiserPinLatencySampleWindows`, the classifier holds the tree at `Throttled`.

**This input holds at `Throttled` and never escalates to `Saturated`.** That bound is load-bearing, not conservatism. `Saturated` engages the writer-side admission gate, which fast-fails callers with `LatticeSaturatedException` once `WalAdmissionSaturationWaitBudget` expires. A stalled pin store is a retention-floor maintenance problem, not an inability to accept writes, so escalating would convert slow WAL trimming into user-visible write failures and make the very incident the input exists to detect strictly worse. Slowing producers is the correct response: it gives the pin store room to drain. The materialiser drain-lag input is bounded at `Throttled` for the same reason.

Sizing guidance: set the threshold a few times the steady-state duration of `orleans.lattice.materialiser.pin.durable_write_latency` so healthy traffic stays quiet. If the input fires persistently, the corrective knob is [`WalMaterialiserPinBuckets`](configuration.md#walmaterialiserpinbuckets), which shrinks the durable blob each pin write rewrites; `orleans.lattice.materialiser.pin.reports_shed` reports how much steady-state pin traffic the reporters are already dropping to protect the store.

The input is purely additive: left at its default `null`, the reporting leaf skips the increment entirely and the classifier behaves exactly as it did before the input was introduced.

### Cause attribution

Two inputs now classify to `Throttled` for reasons unrelated to admission depth, so `WalSaturationStateChange` carries a `Cause` discriminator (`WalSaturationCause`) naming which input drove the classification: `None`, `DispatchTimeouts`, `ProviderFailures`, `FlushLatency`, `AdmissionDepth`, `MaterialiserDrainLag`, or `MaterialiserPinLatency`. The same cause is published, spelled `dispatch_timeouts`, `provider_failures`, `flush_latency`, `admission_depth`, `materialiser_drain_lag`, `materialiser_pin_latency` or `none`, as the `cause` tag on the `orleans.lattice.wal.saturation.transitions` counter (the state gauge carries only the `tree` tag), so a dashboard can distinguish "producers are outrunning the WAL" from "the WAL cannot retire what it already has" without correlating across panels.

Attribution is evaluated in exactly the same order as the classification itself, so the two can never disagree. It is computed *before* the recovery-window upgrade described above, so a tree that reaches `Throttled` only by that upgrade correctly reports `None` rather than attributing itself to whichever input last fired.

`Cause` is an additive field on an existing serialized type and the enum is open for extension: subscribers that do not read it are unaffected, and one that switches on it should treat an unrecognised value as `None`.
## Resolution and scope

- **Per-tree.** A multi-tree silo does not lump every tree's pressure together. A `Saturated` tree A does not affect tree B's signal. `IWalSaturationSignal.GetAggregateState()` exists for callers that want a single global signal across every observed tree (a TCP listener that fronts every tree at once, for example) and returns the worst case across the per-tree views.
- **Per-partition, for the writer.** Alongside each tree's roll-up the sampler publishes a verdict for every WAL partition it observed (issue #3348), re-running the classifier with that partition's own admission depth, so tree-wide causes still reach every partition while the admission-depth inputs stay partition-local. The writer's admission gate and throttle pacing act on that per-partition verdict; the public getters report the tree-wide roll-up.
- **Per-silo.** The signal is scoped to a single silo process. Each silo's `WalCommitLogWriter` singleton owns the admission gate for traffic it dispatches; the sampler reads only that singleton's tracker map. A multi-silo cluster's aggregate health is a dashboard concern (sum the `orleans.lattice.wal.saturation.state` observable gauge across silos), not a runtime one.
- **Per-tick.** The signal is recomputed by a silo-scoped `IHostedService` (`WalSaturationSampler`) that ticks at `LatticeOptions.WalSaturationSampleInterval` (default 200 ms). The worst-case subscriber transition latency is therefore one sample interval beyond the underlying signal crossing the threshold - well under the one-second bound documented on the public surface.

## Idle cost

The sampler is the only piece of the signal that runs unconditionally:

- **When no callers are subscribed** (no observers, no polling getters, no awaiters): the sampler still ticks, but per-tick work is a small `ConcurrentDictionary` enumeration plus per-tree state arithmetic. On an idle silo with no tree traffic the loop's work is a no-op - the dictionary is empty.
- **When polling getters are called**: one `ConcurrentDictionary.TryGetValue` returning an `enum`. No allocation, no grain call.
- **When `WaitForHealthyAsync` is called on an already-Healthy tree**: returns `Task.CompletedTask` synchronously. No allocation.
- **When `WaitForHealthyAsync` is called on a non-Healthy tree**: one `TaskCompletionSource` plus an optional `CancellationTokenRegistration`. The TCS settles on a sample tick that observes the tree at `Healthy`; each such tick releases at most `WalSaturationRecoveryReleaseBatch` waiters for the tree, oldest first, so a large parked population drains over several ticks.
- **When observers are registered**: one `ValueTask` per transition per observer. Transitions are rare (one per regime change), not per-call.
- **On the `SetAsync` / `SetManyAsync` hot path**: while the append's partition is `Healthy`, one concurrent-dictionary lookup each at the writer's admission gate and throttle pace, with no await and no allocation. A `Throttled` partition adds the `WalThrottledAdmissionPace` delay and a `Saturated` one parks at the admission gate (see [Library-side consumers](#library-side-consumers-admission-gate-and-saga-quiesce-gate)). Beyond that the writer records metrics and updates the per-(tree, shard) cumulative dispatch-timeout count via an `AddOrUpdate` on a static dictionary, which the sampler reads on its own thread.

## Choosing a shape

The three surfaces are designed to compose. Pick the one that matches the consumer's natural control flow.

| Consumer shape | Recommended surface |
|----------------|---------------------|
| A TCP read loop that wants to check before each `ReadAsync` | Polling: `signal.GetCurrentState(treeId)`. Cost is a single dictionary lookup per check. |
| A producer whose mainline needs to "pause until the silo recovers" before continuing | Await: `await signal.WaitForHealthyAsync(treeId, ct)`. Synchronous fast-path when already `Healthy`. |
| A control plane / circuit breaker / sidecar that reacts to *transitions* as events | Push: `IWalSaturationObserver` registered in DI. Single callback per regime change. |
| A Grafana dashboard or alert | Metrics: `orleans.lattice.wal.saturation.state` (gauge) + `orleans.lattice.wal.saturation.transitions` (counter). |

A host may use all three at once without drift - they all read from the same per-tree state cache populated by the sampler.

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
- **`LatticeOptions.WalSaturationProviderFailureRateThreshold`.** The third Saturated input, added so the signal also covers the regime where the downstream storage provider's commit calls return quickly (so neither the admission depth nor the dispatch deadline crosses the threshold) but terminally fail at a high rate - the canonical pattern on the Azure Tables single-account 409-Conflict burst. Counts non-timeout, non-cancellation exceptions surfaced from the writer's outbound `IWalShardGrain.AppendAsync` / `AppendBatchAsync` RPCs, per `(tree, shard)`, per sample window. Set to `0` to disable the trigger entirely.
- **`LatticeOptions.WalAdmissionSaturationWaitBudget` and [`LatticeSaturatedException`](api.md#saturation-back-pressure---latticesaturatedexception).** The library-side consumer of the signal. The WAL writer's pre-admission gate consults the verdict for the append's WAL partition before each admission acquire; on `Saturated` it parks up to the configured budget (default 5 s) - resuming, under the default `WalSaturationAcuteOnly = true`, as soon as the partition leaves `Saturated` - and, on expiry, refuses the dispatch with the typed exception. The atomic-write saga's quiesce gate runs the same pattern before each batched dispatch, refusing with the same exception on budget expiry rather than re-entering RowKeys into a still-throttled storage account. Both gates make the *runtime* leading-edge surface load-bearing: callers see typed back-pressure in budget time instead of parking on the admission semaphore until `WalAppendDispatchTimeout` (default 30 s). Set `WalAdmissionSaturationWaitBudget = TimeSpan.Zero` to opt out of the writer-side gate; the saga gate is always on when a signal is registered.
- **`LatticeShuttingDownException`.** Typed back-pressure exception the silo throws when an operation cannot complete because the WAL writer is draining for host shutdown. Distinct from `LatticeSaturatedException` (the runtime-leading-edge surface) - the shutdown exception is the terminal-refusal surface that fires when the silo has already decided to stop accepting traffic. Callers that observe this exception should abandon the operation rather than retry it; see the [API Reference - Shutdown back-pressure](api.md#shutdown-back-pressure---latticeshuttingdownexception) for the caller contract.
- **`WalCommitLogWriter` writer-side drain on host shutdown** ([wal-tuning.md - bounded shutdown](wal-tuning.md#bounded-shutdown-when-the-writer-is-wedged)). The drain closes the shutdown half of the saturation problem - parked callers are released within bounded time of SIGTERM. The saturation signal closes the runtime half - callers can stop offering load before parking becomes the dominant regime.
- **`LatticeOptions.WalThrottledAdmissionPace`.** The writer's pacing delay on a `Throttled` partition; see [Writer-side throttle pacing](#writer-side-throttle-pacing).
- **`LatticeMetrics.WalAppendDispatchTimeouts`, `WalAppendAdmissionTimeouts`, `WalAppendAdmissionSaturationRefusals`, `WalAppendAdmissionWait`.** The existing instruments that surface the underlying signals. The saturation gauge / counter sit one layer up: they classify the regime rather than counting the individual events. `WalAppendAdmissionSaturationRefusals` is the counter that distinguishes the writer-side saturation-budget refusal path from the dispatch-deadline and drain-release paths.

## Library-side consumers: admission gate and saga quiesce gate

Before the consumer-coverage gates landed, the signal was a *publish-only* surface: the sampler observed the writer's state and emitted the regime to observers, but no in-library hot path consumed the signal to refuse work. Under the canonical Azure Tables single-account 409-Conflict regime that meant the sampler raised `Saturated` many times before the first observable failure, while every new dispatch still admitted into the per-partition semaphore and parked at the cap for the full `WalAppendDispatchTimeout` (default 30 seconds) before surfacing as a generic `TimeoutException` - the saturation signal was correct but operationally inert.

The consumer-coverage gap is closed by wiring two library-side gates that consult the signal directly, plus a throttle pace on the writer's local admission path (a third, read-side consumer - [snapshot-cursor open shedding](#snapshot-cursor-open-shedding) - is described after them):

### Writer-side admission gate

Before each append acquires its partition's admission slot, the writer reads the saturation verdict for **that WAL partition** rather than the tree-wide roll-up (issue #3348), so one partition at its cap cannot refuse appends routed to its idle siblings; genuinely tree-wide causes still reach every partition's verdict. On `Healthy` / `Throttled` the gate check is a single concurrent-dictionary lookup and the caller proceeds towards the semaphore with no allocation - a `Throttled` partition first pays the [throttle pace](#writer-side-throttle-pacing). On `Saturated` the writer parks for at most `LatticeOptions.WalAdmissionSaturationWaitBudget` (default 5 s), or for whatever remains of the enclosing call's `WalAdmissionSaturationCallBudget` if that is smaller (the call budget is unbounded by default, so by default only the per-append budget applies). With `WalSaturationAcuteOnly` at its default `true` the caller resumes as soon as its partition leaves `Saturated`; with it `false` the caller waits for `Healthy`. If the budget expires with the partition still `Saturated` the writer throws `LatticeSaturatedException` so the caller sees the back-pressure as a typed refusal in budget time. The cumulative refusal rate lands on the `orleans.lattice.wal.writer.append.admission_saturation_refusals` counter (tagged `tree`, `partition`), distinct from the admission-deadline counter (`admission_timeouts`, an admission wait that outlived `WalAppendDispatchTimeout`) and the drain-release counter (`drain.releases`). A custom `IWalSaturationSignal` that cannot answer per partition falls back to the tree-wide verdict, and a caller parked at the gate then waits for `Healthy`.

A borderline-recovery race (the wait expires AND the partition recovered between the wait expiring and the re-check firing) is suppressed: the writer re-reads the partition's verdict once after budget expiry and proceeds without refusal when it is no longer `Saturated`.

### Writer-side throttle pacing

Before each append admits into its partition's semaphore, the writer also paces on the partition's verdict. On `Throttled` it waits a single `LatticeOptions.WalThrottledAdmissionPace` (default 25 ms) and then proceeds; on `Healthy` the check is one concurrent-dictionary lookup with no await, and on `Saturated` the pace is skipped because the admission gate already governs the dispatch. The pace never throws and never escalates to `LatticeSaturatedException`. It is what gives the `Throttled`-only inputs - materialiser drain lag and durable pin latency - an effect on a single silo's local write path, where no replication sender exists to drip-feed. A host-shutdown drain cuts a pending pace short. Set it to `TimeSpan.Zero` to opt out of local pacing.

### Saga-side quiesce gate (`AtomicWriteGrain`)

Before each batched `SetManyAsync` dispatch the saga calls a private `QuiesceOnSaturatedAsync` helper that, when the tree-wide verdict is `Saturated`, parks on `WaitForHealthyAsync` up to `min(MaxSagaQuiesceWait, perTree.WalAppendDispatchTimeout)` (30 s by default, capped at the writer-side dispatch deadline so the saga's quiesce always wins). Unlike the writer-side gate it reads the tree-wide verdict and always waits for `Healthy`, whatever `WalSaturationAcuteOnly` says. On clean recovery the saga proceeds into the dispatch as normal. On budget expiry with the tree still `Saturated`, the saga's fast-path refuses with `LatticeSaturatedException` and preserves its persisted state at `Execute` with the current `NextIndex` - the caller's next retry on the same `operationId` resumes from where the refusal stopped, idempotently. Running the saga's compensation pass here would re-enter the same RowKeys into a still-throttled storage account and amplify the 409-Conflict burst exactly as the historical pre-saga-saturation-fast-path retry loop did.

The saga also detects a writer-side `LatticeSaturatedException` bubbling through `SetManyAsync`'s leaf fan-out (typically wrapped in an `AggregateException`) and re-throws it typed, preserving the originating tree id for caller attribution.

### Snapshot-cursor open shedding

A snapshot-isolated cursor open (`ILattice.OpenSnapshotKeyCursorAsync`, `OpenSnapshotEntryCursorAsync` and their predicate variants) reads the tree's saturation verdict before it fans out the per-shard baseline capture. On `Saturated` - and only then, so a `Throttled` tree stays browsable - the open is refused at admission with `LatticeSaturatedException` carrying `SaturationSource = SnapshotCursorOpen`, so a heavy baseline capture is not piled onto shard roots already collapsing under write back-pressure. `LatticeOptions.ShedSnapshotOpensWhenSaturated` (default `true`) controls it; set it to `false` to let every open proceed regardless of the regime.

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

`ex.SaturationSource` names the seam that refused: `WalAdmission` for the writer-side gate, `AtomicWriteSaga` for the saga gate and `SnapshotCursorOpen` for the snapshot-open shed. The same type also carries two refusals that do not come from this signal: `SetManyFanOut`, when a host opts into a finite `LatticeOptions.SetManyFanOutBudget` (unbounded by default) and a batch write's fan-out outlives it - that refusal rolls nothing back, so branches that already committed stay committed - and `ReplayPermitAdmission`, the only source that is safe to retry automatically after a bounded, jittered delay. A retry policy should therefore branch on `SaturationSource` rather than on the exception type alone.

This is distinct from `LatticeShuttingDownException` (where retries against the same silo activation never succeed). See the [API Reference - Saturation back-pressure](api.md#saturation-back-pressure---latticesaturatedexception) for the full caller contract.

## Disabling the sampler

Setting `LatticeOptions.WalSaturationSampleInterval = Timeout.InfiniteTimeSpan` leaves the sampler dormant. The interval is read once when the sampler starts, so the setting takes effect on the next silo start. Every tree's signal stays at `Healthy` forever, the observable gauge publishes no series (a tree appears in it only once the sampler has observed it), and `IWalSaturationObserver` callbacks never fire. Polling getters and `WaitForHealthyAsync` continue to return the cached state - which is `Healthy` for every tree, because the sampler never wrote anything else. The hosted-service startup hook logs a debug message at silo start when this option is set so operators see the disablement decision in the silo log.

This is the right shape when:

- The host already has an external back-pressure surface (a load balancer or service mesh observing the same workload) and does not want the per-silo signal duplicated.
- A test fixture needs deterministic state (the library's own tests then drive the sampler one tick at a time through an internal seam).

## See also

- [API Reference - WAL saturation back-pressure](api.md#wal-saturation-back-pressure) - the call-site reference with code snippets.
- [WAL Tuning - When lifting the cap stops helping](wal-tuning.md#when-lifting-the-cap-stops-helping) - the operational context that motivated this signal.
- [Metrics](metrics.md) - the full instrument set including the saturation gauge and transitions counter.
- [Configuration](configuration.md) - the options reference, including the validator rules that reject out-of-range values.
