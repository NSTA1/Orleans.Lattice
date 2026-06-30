# Receiver-side flow control

The replication sender ships a configured `LatticeReplicationOptions.ShipBatchSize` worth of WAL entries per pump tick by default. That blind-push shape is fine when the receiver keeps up; under load it is the wrong shape because a struggling receiver has no in-band way to ask the sender to slow down short of letting RPCs time out. The receiver-side flow-control seam closes that gap by letting the receiver stamp two optional hints onto every `ReplicationAck`:

- `SuggestedBatchSize` - the largest per-tick batch the receiver would like the sender to ship next, in entries.
- `PauseForMs` - the number of milliseconds the sender should pause before the next pump tick to this peer.

Both slots are strictly additive on the wire (new `[Id(n)]` ints on `ReplicationAck`); pre-flow-control receivers and senders ignore the slots, so a heterogeneous peering rolls forward one side at a time without coordination.

## The seam

```csharp verify
public interface IReceiverFlowControlPolicy
{
    ValueTask<ReceiverFlowControlHint> EvaluateAsync(
        ReceiverFlowControlContext context,
        CancellationToken cancellationToken);
}
```

The receiver-side gRPC service calls `EvaluateAsync` after a successful apply, on every push. The policy gets the per-batch `ReceiverFlowControlContext`:

| Field | Semantics |
|---|---|
| `TreeName` | Logical tree id the batch was applied to. |
| `OriginClusterId` | Authoring cluster id of the just-applied entries. |
| `EntryCount` | Number of entries handed to the applier (includes deduped / parked entries). |
| `ApplyDurationMs` | Wall-clock duration of the apply call, in milliseconds. `0` indicates the receiver did not measure (e.g. empty heartbeat batch). |

The policy returns a `ReceiverFlowControlHint` whose two `int?` fields project directly onto `ReplicationAck.SuggestedBatchSize` / `ReplicationAck.PauseForMs`.

## Defaults

The default registration is `WalSaturationReceiverFlowControlPolicy` (see [Built-in WAL-saturation policy](#built-in-wal-saturation-policy) below): `AddLatticeReplication` registers it via `TryAddSingleton`, so receivers translate local WAL back-pressure into sender back-off out of the box. When no `IWalSaturationSignal` is present (e.g. a stand-alone gRPC host that never called `AddLattice`) the policy degrades to `ReceiverFlowControlHint.None`, preserving blind-push behaviour. Hosts opt **out** by pre-registering `NoOpReceiverFlowControlPolicy` (a stateless singleton that always returns `ReceiverFlowControlHint.None`) before `AddLatticeReplication`; because the default uses `TryAddSingleton`, that pre-registration wins. A fully custom `IReceiverFlowControlPolicy` is registered the same way, and `AddWalSaturationReceiverFlowControl` force-installs (remove-then-add) the saturation policy regardless of composition order.

## Built-in WAL-saturation policy

The receiver locally applies replicated mutations through the same per-tree apply path the public write surface uses, so a pushed batch routes through the writer-side WAL admission gate and can drive the receiver's local WAL into the core library's `Throttled` or `Saturated` regime. The `WalSaturationReceiverFlowControlPolicy` bridges that signal to the sender: on every successful push it reads `IWalSaturationSignal.GetCurrentState(treeName)` for the just-applied tree and maps the regime onto the hint.

| WAL state | `SuggestedBatchSize` | `PauseForMs` |
|---|---|---|
| `Healthy` | `null` (resume at `ShipBatchSize`) | `null` |
| `Throttled` | `ceil(ShipBatchSize * ThrottledBatchRatio)` | `ThrottledPauseMs` |
| `Saturated` | `SaturatedBatchSize` (a minimal drip-feed) | `SaturatedPauseMs` |

Without this bridge a saturated receiver keeps accepting full-size batches until the admission gate's wait budget expires and the apply throws `LatticeSaturatedException`, which surfaces to the sender as a hard push failure rather than a graceful slow-down. With it, the receiver asks the sender to back off *before* the gate runs out of headroom.

`WalSaturationReceiverFlowControlPolicy` is the default policy installed by `AddLatticeReplication`. To keep the old blind-push behaviour, pre-register the no-op before `AddLatticeReplication`:

```csharp verify
siloBuilder.Services.AddSingleton<IReceiverFlowControlPolicy>(NoOpReceiverFlowControlPolicy.Instance);
```

To tune the mapping, call `AddWalSaturationReceiverFlowControl`, which removes any prior `IReceiverFlowControlPolicy` registration (including the default) and installs the saturation policy with your options, so the result is deterministic regardless of composition order with `AddLatticeReplication`:

```csharp verify
siloBuilder.AddWalSaturationReceiverFlowControl(options =>
{
    options.ThrottledBatchRatio = 0.25;
    options.SaturatedPauseMs = 1000;
});
```

The mapping is tuned per tree through `WalSaturationReceiverFlowControlOptions`: `ThrottledBatchRatio` (the fraction of `ShipBatchSize` to suggest while throttled, clamped to `[0, 1]`), `ThrottledPauseMs`, `SaturatedBatchSize` (an absolute floor, defaulting to a single-entry drip-feed), and `SaturatedPauseMs`. Every suggested size is clamped to `[1, ShipBatchSize]`, and a non-positive pause is surfaced as "no pause requested" (`PauseForMs = null`). The hint rides the existing additive `ReplicationAck` slots, so there is no wire change. When no `IWalSaturationSignal` is registered (the signal is produced by `AddLattice`), the policy degrades to `ReceiverFlowControlHint.None` and the receiver keeps the existing blind-push behaviour.

## Sender-side semantics

The replication shipper consumes the ack hints on its next pump tick:

- **`SuggestedBatchSize`** clamps the per-tick batch cap to `min(options.ShipBatchSize, max(1, ack.SuggestedBatchSize))`. A `null` or non-positive value reverts to the configured `ShipBatchSize` - the canonical re-acceleration signal once the receiver has recovered.
- **`PauseForMs`** extends the per-peer retry deadline to `max(currentBackoffDeadline, now + PauseForMs)`. The success path already cleared the backoff deadline, so on the steady-state success path the composition collapses to `now + PauseForMs`; the `max(...)` shape only matters when a late pause races a still-in-flight exponential backoff. A receiver-requested pause never shortens an in-progress backoff.

Hint state is per-shipper-activation memory only. A grain re-activation resets the cap to the configured `ShipBatchSize`; the receiver re-stamps its preference on the next ack.

## Sender-side pipelining

By default the shipper is strictly serial per `(tree, peer)`: ship one batch, await its ack, advance the cursor, ship the next. On a high-latency link that leaves the transport round-trip time idle between batches. Raising `LatticeReplicationOptions.ShipMaxInFlight` above its default of `1` lets the shipper keep up to that many shipped-but-unacknowledged batches in flight, overlapping the round-trip latency with draining the next batch.

```csharp verify
var options = new LatticeReplicationOptions
{
    ClusterId = "site-a",
    // Keep up to four batches in flight per (tree, peer). Default is 1
    // (strictly serial); raising it trades a small per-batch allocation
    // for overlapped transport latency on high-RTT links.
    ShipMaxInFlight = 4,
};
```

The window preserves the same ordering and durability guarantees the serial path gives:

- **Per-origin FIFO + advance-strictly-on-ack.** Acks are consumed in strict FIFO order, and the durable per-peer cursor advances past a batch only once that batch *and* every lower-HLC batch before it have been acknowledged. The cursor never skips a hole.
- **Failure containment.** A transport failure or ack rejection anywhere in the window stops the cursor advancing; remaining in-flight sends are observed (so no task faults go unobserved) but their cursors are left un-advanced. The next tick re-ships from the durable cursor and the receiver dedupes the overlap.
- **Flow-control composition.** A non-null `SuggestedBatchSize` hint collapses the window back to `1` until the receiver clears it, so a struggling receiver throttles batch size and pipeline depth together. A `PauseForMs` hint gates the whole next tick via the retry deadline exactly as on the serial path.

A window greater than `1` issues concurrent `IReplicationTransport.SendAsync` calls for the same `(tree, peer)` pair, so a transport used with pipelining must tolerate concurrent invocation against one pair. The default window of `1` preserves strictly-serial-per-pair behaviour for transports that do not. The live window depth is surfaced on the outbound-only `orleans.lattice.replication.peer.ship_in_flight` gauge (`LatticeReplicationMetrics.ShipInFlightName`); see [Observability](observability.md).

## Sender-side adaptive batch sizing

The receiver hint described above only ever pushes the batch size *down*, and only once the receiver is already feeling pressure. With pipelining in place the sender has its own local signal - per-batch ack latency and error rate - so it can self-tune the batch size *before* the receiver has to raise a hint. That is the AIMD (additive-increase / multiplicative-decrease) controller behind `LatticeReplicationOptions.AdaptiveBatchSizingEnabled`.

The flag **defaults to `true`**. A healthy link whose window-mean ack latency stays at or below `AdaptiveBatchLatencyThreshold` sits pinned at the `ShipBatchSize` ceiling (modulated only downward by an active receiver hint), so steady-state behaviour matches the static path; the controller earns its keep on a degraded link, where its multiplicative decrease shrinks the batch on a repeated send/apply failure (such as a receiver phase-2 commit timeout under burst load) so the stream recovers instead of re-shipping the identical oversized batch forever. Set the flag to `false` to restore the static-sizing path.

```csharp verify
var options = new LatticeReplicationOptions
{
    ClusterId = "site-a",
    AdaptiveBatchSizingEnabled = true,
    // Optional tuning (defaults shown):
    AdaptiveBatchIncrement = 8,                                  // additive step per healthy ack
    AdaptiveBatchDecreaseFactor = 0.5,                           // multiplicative back-off
    AdaptiveBatchLatencyThreshold = TimeSpan.FromMilliseconds(50),
    AdaptiveBatchWindowLength = 16,                              // sliding-window length
};
```

When enabled, a per-`(tree, peer)` controller tracks ack latency over a sliding window of the last `AdaptiveBatchWindowLength` acks and adapts an effective batch size within `[1, ShipBatchSize]`:

- **Additive increase.** While the window-mean ack latency stays at or below `AdaptiveBatchLatencyThreshold`, the effective size grows by `AdaptiveBatchIncrement` entries per ack, capped at `ShipBatchSize`.
- **Multiplicative decrease.** When the window-mean ack latency rises above the threshold, or a send fails (transport throw or ack rejection), the effective size is multiplied by `AdaptiveBatchDecreaseFactor`, floored at `1`.

The controller starts at the `ShipBatchSize` ceiling (the optimistic posture: a healthy link stays at the configured ceiling and only backs off on observed degradation).

### The receiver hint is the hard ceiling and always wins

Sender adaptation only ever operates in the headroom *beneath* the receiver hint. The effective per-tick cap the shipper applies is:

```
effective = min(adaptive size, receiver-suggested size, ShipBatchSize), floored at 1
```

Because the composition is a minimum, the receiver's `SuggestedBatchSize` hint can never be exceeded by the adaptive controller: if the hint is below the adaptive size, the hint wins. The adaptive controller can only lower the cap further into the headroom the receiver hint (and the configured ceiling) already leaves. As with the static path, a non-null hint also collapses the pipelining window to `1`.

The adaptation never reorders work, never crosses an atomic-batch boundary, and never affects per-origin FIFO or advance-strictly-on-ack cursor semantics - it only chooses how many already-ordered entries to draw into the next batch.

Controller state is in-memory and activation-scoped (per `(tree, peer)` shipper activation). A grain re-activation resets the effective size to `ShipBatchSize` and the controller re-learns from the live link; nothing about the adaptive size is persisted.

Two observability histograms emit once per acknowledged batch regardless of the flag (they are useful even with static sizing): `orleans.lattice.replication.ship.effective_batch_size` and `orleans.lattice.replication.ship.ack_latency`. See [Observability](observability.md#sender-side-adaptive-batch-sizing-ship-effective_batch_size--ship-ack_latency).

## Failure mode

`EvaluateAsync` failure is swallowed and logged at `Warning`. The receiver already applied the batch and persisted the high-water-mark; surfacing a policy outage out of a successful apply would convert a diagnostic concern into a transport failure and unwind work the receiver already did. The next push re-evaluates the policy, so a transient outage self-heals.

## Wire compatibility

`ReplicationAck` already carried the additive `BlockedAtHlc` slot for cross-cluster TX-aware GC pinning; `SuggestedBatchSize` and `PauseForMs` follow the same shape and the same compatibility profile:

- A pre-flow-control receiver omits the slots; senders decode `null` and resume at the configured `ShipBatchSize`.
- A pre-flow-control sender ignores the slots stamped by a newer receiver and continues to ship at its configured `ShipBatchSize`.
- Both sides may upgrade independently; flow control becomes effective the moment both sides are running a build that carries the seam.

No version handshake is required.

## Registration

```csharp verify
sealed class MyFlowControlPolicy : IReceiverFlowControlPolicy
{
    public ValueTask<ReceiverFlowControlHint> EvaluateAsync(
        ReceiverFlowControlContext context,
        CancellationToken cancellationToken)
    {
        // Project an internal back-pressure signal onto the hint.
        var hint = new ReceiverFlowControlHint
        {
            SuggestedBatchSize = context.EntryCount > 64 ? 64 : null,
            PauseForMs = context.ApplyDurationMs > 250 ? 100 : null,
        };
        return ValueTask.FromResult(hint);
    }
}

static void Configure(IServiceCollection services)
{
    services.AddSingleton<IReceiverFlowControlPolicy, MyFlowControlPolicy>();
}
```

The seam is invoked on every successful push without serialisation across distinct `(TreeName, OriginClusterId)` pairs, so implementations must be thread-safe. Per-call work belongs behind a cached / observed back-pressure surface (queue depth on a downstream materialiser, CPU saturation, etc.) rather than inside the policy's hot path.
