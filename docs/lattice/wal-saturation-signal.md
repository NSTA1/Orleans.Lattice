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
| `Throttled` | Admission depth at or above `WalSaturationThrottledRatio` (default 0.75) of the cap on at least one partition. | Slow down the offered rate. Continue dispatching - new appends will land, possibly after a brief admission wait. |
| `Saturated` | Admission semaphore at cap with parked callers, **or** recent dispatch-timeout trip rate at or above `WalSaturationDispatchTimeoutThreshold` (default 1) in a single sample window, **or** recent provider-side commit failure rate at or above `WalSaturationProviderFailureRateThreshold` (default 1) in a single sample window. | Pause new appends until the state returns to `Healthy`. Continuing to dispatch will fault parked callers with `TimeoutException` rather than improving throughput. |

States are totally ordered (`Healthy < Throttled < Saturated`), and the tree's state is the worst case across every partition / shard for that tree. The state space is open for additive extension - future minor releases may introduce intermediate or recovery states (for example a `Recovering` state when pressure has just dropped) without breaking subscribers that switch on the three documented values.

### Throttled regime stability (recovery window)

The per-tick `max(depth_ratio)` across the tree's WAL partitions is structurally bursty: one partition fills to cap, drains entirely in the next tick, the next partition fills. The cross-partition `max` consequently oscillates between `~1.0` (one partition at cap) and `~0.0` (the partition just emptied, the next one not yet filling) inside a single sampler period. Without smoothing, the classifier would see `Saturated` then `Healthy` in alternating ticks and the advisory `Throttled` band (the inclusive range `[WalSaturationThrottledRatio, 1.0)` of `max(depth_ratio)`) is never observed as a stable regime.

The classifier applies an **upgrade rule** to make `Throttled` observable across the burst cycle: when the current-tick classification is `Healthy` but the tree was observed at `Saturated` within the past `LatticeOptions.WalSaturationRecoveryWindow` (default 1 second), the classifier upgrades the tree to `Throttled` instead. The upgrade rule preserves three invariants:

- **`Healthy -> Saturated` latency is unchanged.** `Saturated` still fires on the current tick's at-cap condition, so the public saturation-signal surface's bound (transition latency under one `WalSaturationSampleInterval`) still holds.
- **Recovery is bounded.** Once the recovery window elapses AND the current tick observes no saturation pressure, the tree drops to `Healthy` and any pending `WaitForHealthyAsync` completes. The window only delays recovery by its own value.
- **Per-tree independence is preserved.** A tree that has never been observed `Saturated` is never upgraded, regardless of any other tree's regime.

Two sentinels disable or invert the upgrade:

- `WalSaturationRecoveryWindow = TimeSpan.Zero`: the upgrade is disabled entirely. The classifier behaves the way the sampler shipped originally - the per-tick depth observation drives the regime directly. Use this when the workload's WAL drain pattern is non-bursty (single-partition trees, or workloads where every partition tracks closely in lockstep) and the upgrade introduces no benefit.
- `WalSaturationRecoveryWindow = Timeout.InfiniteTimeSpan`: the upgrade is sticky. Once `Saturated` has been observed, every subsequent `Healthy`-classified tick is upgraded to `Throttled` forever. Useful for tests that want a deterministic sticky-Throttled floor without arming wall-clock dependencies, and for defensive production deployments that prefer the saturation regime to be sticky.

## Resolution and scope

- **Per-tree.** A multi-tree silo does not lump every tree's pressure together. A `Saturated` tree A does not affect tree B's signal. `IWalSaturationSignal.GetAggregateState()` exists for callers that want a single global signal across every observed tree (a TCP listener that fronts every tree at once, for example) and returns the worst case across the per-tree views.
- **Per-silo.** The signal is scoped to a single silo process. Each silo's `WalCommitLogWriter` singleton owns the admission gate for traffic it dispatches; the sampler reads only that singleton's tracker map. A multi-silo cluster's aggregate health is a dashboard concern (sum the `orleans.lattice.wal.saturation.state` observable gauge across silos), not a runtime one.
- **Per-tick.** The signal is recomputed by a silo-scoped `IHostedService` (`WalSaturationSampler`) that ticks at `LatticeOptions.WalSaturationSampleInterval` (default 200 ms). The worst-case subscriber transition latency is therefore one sample interval beyond the underlying signal crossing the threshold - well under the one-second bound documented on the public surface.

## Idle cost

The sampler is the only piece of the signal that runs unconditionally:

- **When no callers are subscribed** (no observers, no polling getters, no awaiters): the sampler still ticks, but per-tick work is a small `ConcurrentDictionary` enumeration plus per-tree state arithmetic. On an idle silo with no tree traffic the loop's work is a no-op - the dictionary is empty.
- **When polling getters are called**: one `ConcurrentDictionary.TryGetValue` returning an `enum`. No allocation, no grain call.
- **When `WaitForHealthyAsync` is called on an already-Healthy tree**: returns `Task.CompletedTask` synchronously. No allocation.
- **When `WaitForHealthyAsync` is called on a non-Healthy tree**: one `TaskCompletionSource` plus an optional `CancellationTokenRegistration`. The TCS settles on the next sample tick that observes the tree at `Healthy`.
- **When observers are registered**: one `ValueTask` per transition per observer. Transitions are rare (one per regime change), not per-call.
- **On the `SetAsync` / `SetManyAsync` hot path**: zero. The writer paths are unchanged - they record metrics and update the per-(tree, shard) cumulative dispatch-timeout count via an `AddOrUpdate` on a static dictionary, which the sampler reads on its own thread.

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
- **`LatticeOptions.WalSaturationProviderFailureRateThreshold`.** The third Saturated input, added so the signal also covers the regime where the downstream storage provider's commit calls return quickly (so neither the admission depth nor the dispatch deadline crosses the threshold) but terminally fail at a high rate - the canonical pattern on the Azure Tables single-account 409-Conflict burst. Counts non-cancellation exceptions surfaced from the writer's outbound `IWalShardGrain.AppendAsync` / `AppendBatchAsync` RPCs, per `(tree, shard)`, per sample window. Set to `0` to disable the trigger entirely.
- **`LatticeOptions.WalAdmissionSaturationWaitBudget` and [`LatticeSaturatedException`](api.md#saturation-back-pressure---latticesaturatedexception).** The library-side consumer of the signal. The WAL writer admission gate (`PartitionTracker.AcquireAsync`) consults the signal before each acquire; on `Saturated` it parks on `WaitForHealthyAsync` up to the configured budget (default 5 s) and, on expiry, refuses the dispatch with the typed exception. The atomic-write saga's quiesce gate runs the same pattern before each batched dispatch, refusing with the same exception on budget expiry rather than re-entering RowKeys into a still-throttled storage account. Both gates make the *runtime* leading-edge surface load-bearing: callers see typed back-pressure in budget time instead of parking on the admission semaphore until `WalAppendDispatchTimeout` (default 30 s). Set `WalAdmissionSaturationWaitBudget = TimeSpan.Zero` to opt out of the writer-side gate; the saga gate is always on when a signal is registered.
- **`LatticeShuttingDownException`.** Typed back-pressure exception the silo throws when an operation cannot complete because the WAL writer is draining for host shutdown. Distinct from `LatticeSaturatedException` (the runtime-leading-edge surface) - the shutdown exception is the terminal-refusal surface that fires when the silo has already decided to stop accepting traffic. Callers that observe this exception should abandon the operation rather than retry it; see the [API Reference - Shutdown back-pressure](api.md#shutdown-back-pressure---latticeshuttingdownexception) for the caller contract.
- **`WalCommitLogWriter` writer-side drain on host shutdown** ([wal-tuning.md - bounded shutdown](wal-tuning.md#bounded-shutdown-when-the-writer-is-wedged)). The drain closes the shutdown half of the saturation problem - parked callers are released within bounded time of SIGTERM. The saturation signal closes the runtime half - callers can stop offering load before parking becomes the dominant regime.
- **`LatticeMetrics.WalAppendDispatchTimeouts`, `WalAppendAdmissionTimeouts`, `WalAppendAdmissionSaturationRefusals`, `WalAppendAdmissionWait`.** The existing instruments that surface the underlying signals. The saturation gauge / counter sit one layer up: they classify the regime rather than counting the individual events. `WalAppendAdmissionSaturationRefusals` is the counter that distinguishes the writer-side saturation-budget refusal path from the dispatch-deadline and drain-release paths.

## Library-side consumers: admission gate and saga quiesce gate

Before the consumer-coverage gates landed, the signal was a *publish-only* surface: the sampler observed the writer's state and emitted the regime to observers, but no in-library hot path consumed the signal to refuse work. Under the canonical Azure Tables single-account 409-Conflict regime that meant the sampler raised `Saturated` many times before the first observable failure, while every new dispatch still admitted into the per-partition semaphore and parked at the cap for the full `WalAppendDispatchTimeout` (default 30 seconds) before surfacing as a generic `TimeoutException` - the saturation signal was correct but operationally inert.

The consumer-coverage gap is closed by wiring two library-side gates that consult the signal directly:

### Writer-side admission gate (`WalCommitLogWriter`)

Before each `PartitionTracker.AcquireAsync` the writer calls `signal.GetCurrentState(treeId)`. On `Healthy` / `Throttled` the check is a single concurrent-dictionary lookup and the caller proceeds directly into the semaphore (sub-microsecond, no allocation). On `Saturated` the writer awaits `WaitForHealthyAsync` bounded by `LatticeOptions.WalAdmissionSaturationWaitBudget` (default 5 s); if the signal recovers within the budget the caller proceeds as normal, if the budget expires with the tree still `Saturated` the writer throws `LatticeSaturatedException` so the caller sees the back-pressure as a typed refusal in budget time. The cumulative refusal rate lands on the `orleans.lattice.wal.writer.append.admission_saturation_refusals` counter (tagged `tree`, `partition`), distinct from the dispatch-timeout counter (`admission_timeouts`) and the drain-release counter (`drain.releases`).

A borderline-recovery race (the wait expires AND the signal recovered between the wait expiring and the re-check firing) is suppressed: the writer re-reads the signal once after budget expiry and proceeds without refusal when the tree is observed `Healthy`.

### Saga-side quiesce gate (`AtomicWriteGrain`)

Before each batched `SetManyAsync` dispatch the saga calls a private `QuiesceOnSaturatedAsync` helper that parks on `WaitForHealthyAsync` up to `min(MaxSagaQuiesceWait, perTree.WalAppendDispatchTimeout)` (30 s by default, capped at the writer-side dispatch deadline so the saga's quiesce always wins). On clean recovery the saga proceeds into the dispatch as normal. On budget expiry with the tree still `Saturated`, the saga's fast-path refuses with `LatticeSaturatedException` and preserves its persisted state at `Execute` with the current `NextIndex` - the caller's next retry on the same `operationId` resumes from where the refusal stopped, idempotently. Running the saga's compensation pass here would re-enter the same RowKeys into a still-throttled storage account and amplify the 409-Conflict burst exactly as the historical pre-saga-saturation-fast-path retry loop did.

The saga also detects a writer-side `LatticeSaturatedException` bubbling through `SetManyAsync`'s leaf fan-out (typically wrapped in an `AggregateException`) and re-throws it typed, preserving the originating tree id for caller attribution.

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

This is distinct from `LatticeShuttingDownException` (where retries against the same silo activation never succeed). See the [API Reference - Saturation back-pressure](api.md#saturation-back-pressure---latticesaturatedexception) for the full caller contract.

## Disabling the sampler

Setting `LatticeOptions.WalSaturationSampleInterval = Timeout.InfiniteTimeSpan` leaves the sampler dormant. Every tree's signal stays at `Healthy` forever, the observable gauge reports `0` for any tree it has not observed, and `IWalSaturationObserver` callbacks never fire. Polling getters and `WaitForHealthyAsync` continue to return the cached state - which is `Healthy` for every tree, because the sampler never wrote anything else. The hosted-service startup hook logs a debug message at silo start when this option is set so operators see the disablement decision in the silo log.

This is the right shape when:

- The host already has an external back-pressure surface (a load balancer or service mesh observing the same workload) and does not want the per-silo signal duplicated.
- A test fixture needs deterministic state and wants to drive the sampler manually via `WalSaturationSampler.SampleOnceAsync`.

## See also

- [API Reference - WAL saturation back-pressure](api.md#wal-saturation-back-pressure) - the call-site reference with code snippets.
- [WAL Tuning - When lifting the cap stops helping](wal-tuning.md#when-lifting-the-cap-stops-helping) - the operational context that motivated this signal.
- [Metrics](metrics.md) - the full instrument set including the saturation gauge and transitions counter.
- [Configuration](configuration.md) - the options reference, including the validator rules that reject out-of-range values.
