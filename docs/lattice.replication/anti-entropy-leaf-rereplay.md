# Anti-entropy targeted leaf re-replay

The [Merkle walk](anti-entropy-merkle-walk.md) is the *localisation* stage of the anti-entropy pipeline: once the [digest probe](anti-entropy-digest-probe.md) reports a shard-level `Mismatch`, the walk narrows the divergence to a single leaf or a small set of leaves and reports each leaf's cluster-stable `[StartKey, EndKey)` covering range. **Targeted leaf re-replay** is the *repair* stage: a strictly opt-in pass that re-ships the retained write-ahead-log entries covering those ranges to the diverged peer, so the missing writes converge.

The repair travels the **same** TX-aware, causal-stable apply path as ordinary replication. Re-shipped entries carry their source clock verbatim and are de-duplicated at the receiver on `(originClusterId, hlc)`, so re-sending is idempotent and never double-applies a counter or re-adds a set element.

## What it re-ships, and how it bounds it

Producer-side selection uses two inputs to bound what gets re-sent:

- the localised leaf `[StartKey, EndKey)` covering ranges (from the walk), and
- the diverged peer's high-water-mark cursor for this origin.

An entry is a candidate when it is the local cluster's own origin, its clock is strictly greater than the peer's cursor, and its key falls inside one of the localised ranges. Atomic-batch boundaries are respected: if any member of an atomic (`SetManyAtomicAsync`) batch is selected, every retained sibling of that batch ships with it, and the entry / byte caps are applied as whole units so a batch is never split across the cap boundary. At least one unit always ships.

## Scope and limitations

- **Peer cursor seam over gRPC.** Reading the peer's applied watermark needs a read-only RPC. The `Orleans.Lattice.Replication.Grpc` binding now implements `IReplicationDigestProbeTransport.GetPeerHighWaterMarkAsync` by resolving the peer's per-origin applied watermark, so the pass re-ships only entries whose clock is strictly greater than the peer's reported cursor instead of every in-range retained entry. An un-upgraded peer that has not bound the `GetPeerHighWaterMark` method answers `Unimplemented` and the seam falls back to `HybridLogicalClock.Zero` (re-ship everything, rely on the receiver's per-origin idempotent dedup) - rolling-upgrade safe. A custom transport can still override the method directly.
- **Cross-cluster push needs a real transport.** The re-ship goes through `IReplicationTransport`; the default no-op transport acks but does not deliver. Wire the gRPC binding (or a custom transport) for genuine cross-cluster repair.
- **Bounded read window.** The pass reads the oldest retained entries per partition up to a bounded budget; a divergence larger than the window makes partial progress per cadence as the peer's cursor advances.
- **`wal_trimmed` is operator-only.** When the local WAL has been garbage-collected past the divergence point, the missing entries are gone. The pass emits the `leaf_rereplay.skipped{reason=wal_trimmed}` alert and does **not** attempt repair; bootstrap-snapshot remediation (issue #517) is the follow-up.

## Enabling it

The repair ships **dark** and is gated five ways: the digest probe must be enabled, a mismatch must be found, `MerkleWalkEnabled` must be `true`, the walk must localise at least one leaf, and `LeafReReplayEnabled` must be `true`. An un-opted host sees no new behaviour.

```csharp verify
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "cluster-a";
    o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["orders"] = LatticeMergeMode.LwwRegister,
    };

    // Detection + localisation (both off by default).
    o.DigestProbeEnabled = true;
    o.MerkleWalkEnabled = true;

    // Repair (off by default). Runs only after a localised leaf.
    o.LeafReReplayEnabled = true;
    o.LeafReReplayMaxEntries = 4096;
    o.LeafReReplayMaxBytes = 1024 * 1024;
});
```

| Option | Default | Notes |
|---|---|---|
| `LeafReReplayEnabled` | `false` | Master switch for the repair pass. When `false`, a localised leaf is counted but never repaired (`leaf_rereplay.skipped{reason=disabled}`). |
| `LeafReReplayMaxEntries` | `4096` | Soft cap on entries re-shipped per pass; never splits an atomic batch. Validated `>= 1`. |
| `LeafReReplayMaxBytes` | `1048576` | Soft cap on the estimated re-shipped payload bytes per pass; never splits an atomic batch. Validated `>= 1`. |

The byte cap is applied to a cheap estimate of each entry's payload (value + delta + key plus a fixed framing allowance), not the exact encoded wire bytes, because selection runs on materialised records before any encode.

## Observability

Counters on the `orleans.lattice.replication` meter:

| Metric | Tags | Emitted |
|---|---|---|
| `orleans.lattice.replication.leaf_rereplay.entries` | `tree`, `peer` | By the number of WAL entries re-shipped to the peer in a pass. |
| `orleans.lattice.replication.leaf_rereplay.skipped` | `tree`, `peer`, `reason` | Once per pass that skipped without re-shipping. |

Skip reasons: `disabled` (the feature is off), `range_empty` (the localised range yielded no candidate entries), and `wal_trimmed` (the operator-only alert: the WAL was GC'd past the divergence point).

The metric-name constants and the skip-reason mapping are exposed for dashboards built from the public surface:

```csharp verify
_ = LatticeReplicationMetrics.LeafReReplayEntriesName;
_ = LatticeReplicationMetrics.LeafReReplaySkippedName;

string tag = LatticeReplicationMetrics.LeafReReplaySkipReasonTag(LeafReReplaySkipReason.WalTrimmed);
System.Diagnostics.Debug.Assert(tag == LatticeReplicationMetrics.LeafReReplaySkipWalTrimmed);
```

## Tightening the peer-cursor bound

A host that can answer the peer's applied watermark cheaply overrides the default seam to bound re-replay to the genuine gap rather than the whole retained in-range window:

```csharp verify
public sealed class MyWatermarkAwareProbeTransport : IReplicationDigestProbeTransport
{
    public Task<DigestProbeResponse> ProbeDigestAsync(
        string targetClusterId,
        DigestProbeRequest request,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(new DigestProbeResponse { DigestAvailable = false });
    }

    public Task<Orleans.Lattice.HybridLogicalClock> GetPeerHighWaterMarkAsync(
        string targetClusterId,
        string treeName,
        string originClusterId,
        CancellationToken cancellationToken)
    {
        // A real implementation would ask the peer for the highest clock it has
        // durably applied for (treeName, originClusterId).
        return Task.FromResult(Orleans.Lattice.HybridLogicalClock.Zero);
    }
}
```
