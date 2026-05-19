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

The default registration is `NoOpReceiverFlowControlPolicy`, a stateless singleton that always returns `ReceiverFlowControlHint.None`. The ack carries `SuggestedBatchSize = null` and `PauseForMs = null`, the sender resumes at its configured `ShipBatchSize`, and existing blind-push behaviour is preserved verbatim. Hosts opt in by registering their own implementation before or after `AddLatticeReplication` / `AddLatticeReplicationGrpc`; both call sites register the no-op via `TryAddSingleton`, so a custom registration wins regardless of composition order.

## Sender-side semantics

`ReplicationShipperGrain.PumpOnceAsync` consumes the ack hints on the next pump tick:

- **`SuggestedBatchSize`** clamps the per-tick batch cap to `min(options.ShipBatchSize, max(1, ack.SuggestedBatchSize))`. A `null` or non-positive value reverts to the configured `ShipBatchSize` - the canonical re-acceleration signal once the receiver has recovered.
- **`PauseForMs`** extends the per-peer retry deadline to `max(currentBackoffDeadline, now + PauseForMs)`. The success path already cleared the backoff deadline, so on the steady-state success path the composition collapses to `now + PauseForMs`; the `max(...)` shape only matters when a late pause races a still-in-flight exponential backoff. A receiver-requested pause never shortens an in-progress backoff.

Hint state is per-shipper-activation memory only. A grain re-activation resets the cap to the configured `ShipBatchSize`; the receiver re-stamps its preference on the next ack.

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
