# Replication apply seam (`IReplicationApplier`)

`IReplicationApplier` is the public, in-process inbound seam over the per-tree apply pipeline. It installs a single `ReplogEntry` authored on a remote cluster onto the local tree while preserving the remote cluster's `HybridLogicalClock` and origin id end-to-end, and it filters re-delivery via a per-origin high-water-mark so at-least-once transports become at-most-once apply.

The contract is deliberately neutral: there is no transport binding, no per-peer state, no ack envelope. It is the seam custom transports, integration tests, and the future inbound replication pipeline plug into.

## API

The interface and result type live in `Orleans.Lattice.Replication`:

```text
public interface IReplicationApplier
{
    Task<ApplyResult> ApplyAsync(
        ReplogEntry entry,
        CancellationToken cancellationToken = default);
}

public readonly record struct ApplyResult
{
    public bool Applied { get; init; }
    public HybridLogicalClock HighWaterMark { get; init; }
}
```

| `ApplyResult` member | Semantics |
|---|---|
| `Applied` | `true` when the entry was merged onto the local tree; `false` when the entry was filtered out as a re-delivery (its `Timestamp` was at or below the per-origin high-water-mark) or rejected as inapplicable (its `OriginClusterId` matched the local cluster id and would have looped). |
| `HighWaterMark` | For point applies (`Set` / `Delete`) this is the per-origin HWM after the call — equal to `entry.Timestamp` when `Applied` is `true`, or the HWM that suppressed the apply when `Applied` is `false`. For range deletes and local-origin no-op rejections — neither of which consults the HWM — this is `HybridLogicalClock.Zero`. |

## Apply semantics

Three concerns the applier composes for every call:

### 1. Source-HLC and origin preservation

Point applies route through the core library's apply seam, which persists the entry's `LwwValue<byte[]>` with the supplied `Timestamp` and `OriginClusterId` **verbatim** — no fresh local HLC is stamped. This is what unlocks transitive replication (A → B → C with A's HLC intact) and deterministic LWW resolution against concurrent local writes.

Range deletes carry `HybridLogicalClock.Zero` by design (a range walk produces many per-leaf HLCs that cannot be faithfully collapsed into a single timestamp). The receiver walks the leaf chain locally and stamps each tombstone with a freshly-ticked local HLC; the remote `OriginClusterId` rides through an ambient `LatticeOriginContext` scope so the receiver-side change-feed observer publishes it on every emitted `LatticeMutation`.

### 2. Per-origin high-water-mark dedupe

The applier resolves a per-origin high-water-mark for every `(TreeId, OriginClusterId)` pair it sees. Before applying a point entry it reads the current HWM; if `entry.Timestamp <= hwm` the call is a no-op (`Applied = false`). After a successful point apply it advances the HWM monotonically — concurrent appliers that race ahead leave the HWM higher and the laggard's advance becomes a no-op, exactly the semantics at-most-once apply requires.

Range deletes bypass the HWM by design. Range applies are naturally idempotent at the leaf layer: re-running a range delete on already-tombstoned keys merges to the same state, so dedupe is unnecessary.

### 3. Local-origin defence-in-depth

A `ReplogEntry` whose `OriginClusterId` matches the local cluster id is rejected as a no-op (`Applied = false`). The outbound ship loop's origin filter already prevents this in steady state, but hand-built apply pipelines and tests can still hand the applier such an entry — surfacing it as an explicit rejection rather than silently merging it into the same cluster's state is the safer default.

## Validation

`ApplyAsync` throws `ArgumentException` when:

- `entry.TreeId` is null or empty.
- `entry.OriginClusterId` is null or empty.
- `entry.Op == Set` and `entry.Value` is null.
- `entry.Op == DeleteRange` and `entry.EndExclusiveKey` is null.

`OperationCanceledException` is thrown when the supplied `CancellationToken` is already cancelled or fires during a grain call.

## Registration

`AddLatticeReplication` registers the default `IReplicationApplier` implementation as a silo-side singleton:

```text
siloBuilder.AddLatticeReplication(o => o.ClusterId = "site-a");
```

Resolve it from inside a silo-side service (typically a transport adapter or a hosted-service inbound pipeline) via constructor injection on `IReplicationApplier`. The applier is not exposed on the cluster client — it is a silo-local seam by design, because the apply path must run inside the cluster that owns the receiving tree.

## Threading and concurrency

The applier is a stateless singleton that holds no per-call state; all coordination flows through the per-origin high-water-mark grain (single-threaded under Orleans turn semantics) and the per-tree apply grain (`StatelessWorker`). Concurrent `ApplyAsync` calls for the same `(tree, origin)` pair are serialised by the HWM grain; concurrent calls for different pairs are independent.

## Bootstrap handoff

The per-origin high-water-mark is the explicit handoff contract for the bootstrap protocol introduced in a later phase: a newly-bootstrapped peer pins the HWM to the snapshot's authoring HLC, then resumes incremental replication from that pinned frontier with exactly-once apply guarantees across the snapshot / incremental boundary. The pinned value may be lower than the receiver's prior HWM (a peer that rewinds to an older snapshot must accept the snapshot's frontier as the apply point); the underlying grain therefore exposes an unconditional pin alongside the monotonic advance.

## Caveats

- **Range deletes do not preserve a single source HLC.** The wire format does not carry per-leaf HLCs, so the receiver tombstones with fresh local HLCs. LWW resolution against a concurrent local write therefore depends on the local clock at apply time, not the remote walk's clock. Idempotence at the leaf layer is the primary correctness guarantee.
- **The HWM is per-origin, not per-shard.** A receiver applying entries from origin `X` against a tree split into N shards advances a single HWM row keyed `(tree, X)` regardless of which shard the entry targets. This is intentional: the HWM contract is the bootstrap-handoff seam, and bootstrap operates per-origin not per-shard.
