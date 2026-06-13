# Anti-entropy Merkle-walk drift localisation

The [digest probe](anti-entropy-digest-probe.md) is the *detection* half of the anti-entropy pipeline: it surfaces a metric when a shard's content digest disagrees with a peer's. But a shard-level mismatch tells you only *that* two clusters diverged for a shard, not *where*. The **Merkle walk** is the *localisation* stage: a strictly read-only, opt-in pass that, once the probe reports a `Mismatch`, descends the local cluster's B+ tree top-down and narrows the divergence to a single leaf or a small set of leaves. It performs no repair - that is a later stage.

The walk never mutates data and never advances any replication cursor.

## The cross-cluster coordinate problem

Two clusters have **independent physical B+ tree layouts**: different grain identities, and possibly different shard or node-split structure for the same logical data. You therefore cannot compare internal nodes by grain identity across clusters - there is no stable node-to-node correspondence.

The one coordinate both sides agree on is the B+ tree's **separator-key ranges** - the ordered key boundaries that define which keys live in which subtree. The walk runs **entirely on the local cluster** and descends its own internal-node tree from the shard root. At each node it asks the peer, over a dedicated read-only probe, for the peer's subtree digest covering the **same key-range** `[start, end)`, then compares:

- digest hashes equal -> that subtree has converged remotely, so the walk **prunes** it;
- hashes differ and the node is a leaf -> the divergence is **localised** to that leaf;
- hashes differ and the node is internal -> the walk **descends** into each child key-range, where each child's range is derived from the local node's separator-key array.

The shard root is depth `0`; each level descended increments the depth.

## The remote range-fold (wired over gRPC)

A key-range-keyed **remote** subtree digest requires two things: a public core API that folds an arbitrary key-range into a digest, and a transport that can invoke it on the peer. Both now exist.

The core library exposes `ILattice.GetLeafProjectionDigestForRangeAsync(int shardIndex, string? startKeyInclusive, string? endKeyExclusive, CancellationToken)`. The owning shard root descends its internal-node tree by separator-key range, touches only the leaves (and whole subtrees) that overlap the half-open `[start, end)` query range, and combines them with the same algebra the internal nodes use (XOR the raw projection hashes, sum the entry counts, max-reduce the checkpoint offsets) before wrapping the result in the identical `XxHash128(rawHash || entryCount || checkpointOffset)` shape an internal node spanning exactly that range would publish. A full-range `[null, null)` probe is byte-identical to the whole-shard `GetLeafProjectionDigestAsync`. The per-entry contribution is **content-only** - it never depends on the local WAL replay position - so two clusters holding the same logical entries in the range compute the same fold independent of how each physically split its leaves. This is the layout-independence property the walk needs.

The `Orleans.Lattice.Replication.Grpc` binding implements `ProbeMerkleWalkAsync` on top of that API: it resolves the peer's `GetLeafProjectionDigestForRangeAsync` over the same per-peer channel cache the push transport uses, so the walk no longer aborts `remote_unavailable` against a healthy gRPC peer. An un-upgraded peer that has not bound the `ProbeMerkleWalk` method answers `Unimplemented` (or momentary `Unavailable`) and the walk aborts cleanly with the remote-unavailable reason - rolling-upgrade safe and wire byte-identical to today.

### Cross-cluster comparison basis

The range digest deliberately folds in the max-reduced checkpoint offset so that a full-range probe is byte-identical to the same-cluster whole-shard digest, and so that the local node digest the walk compares against (computed the same way) lines up exactly with the existing `DigestProbeComparer` detect layer, which compares full published hashes including the checkpoint. Cross-cluster the checkpoint offset is each cluster's independent WAL replay position, so it differs even for byte-identical logical content. The Merkle walk is therefore a deliberately **conservative trigger**: hashes that differ only because the checkpoints differ still descend, so the walk may over-localise across clusters. That is safe - repair precision is delivered by the [peer high-water-mark bound](anti-entropy-leaf-rereplay.md) plus the receiver's per-origin idempotent dedup, not by the digest. The walk narrows divergence to a leaf or small key range; the re-replay stage re-ships only entries strictly above the peer's reported watermark and the receiver discards any duplicates.

## Enabling it

The walk ships **dark** and is gated three ways: the digest probe must be enabled, a mismatch must be found, and `MerkleWalkEnabled` must be `true`. An un-opted host sees no new behaviour.

```csharp verify
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "cluster-a";
    o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["orders"] = LatticeMergeMode.LwwRegister,
    };

    // Detection (off by default).
    o.DigestProbeEnabled = true;

    // Localisation (off by default). Runs only on a detected mismatch.
    o.MerkleWalkEnabled = true;
    o.MerkleWalkMaxDepth = 16;
    o.MerkleWalkMaxBytes = 1024 * 1024;
});
```

| Option | Default | Notes |
|---|---|---|
| `MerkleWalkEnabled` | `false` | Master switch for the localisation pass. When `false`, a detected mismatch is counted but never triggers a walk. |
| `MerkleWalkMaxDepth` | `16` | Recursion-depth cap. An internal node still diverging at this depth aborts the walk. Validated `>= 1`. |
| `MerkleWalkMaxBytes` | `1048576` | Per-walk budget of digest hash bytes inspected (local plus remote). Exhausting it aborts the walk. Validated `>= 1`. |

The caps bound the cost of a single pass so a pathological tree cannot turn a background safety net into an expensive scan.

## Observability

Two counters on the `orleans.lattice.replication` meter:

| Metric | Tags | Emitted |
|---|---|---|
| `orleans.lattice.replication.merkle_walk.localised` | `tree`, `depth` | Once per pass that narrows the mismatch to one or more leaves; the value is the number of diverging leaves, and `depth` is the level reached. |
| `orleans.lattice.replication.merkle_walk.aborted` | `reason` | Once per pass that stops before localising. |

Abort reasons: `depth_cap`, `byte_budget`, `remote_unavailable`, `version_skew`. With the default transport every triggered walk reports `remote_unavailable` (see the limitation above), so a sustained `reason=remote_unavailable` rate simply means no range-answering transport is wired up yet.

The metric-name constants are exposed for dashboards built from the public surface:

```csharp verify
_ = LatticeReplicationMetrics.MerkleWalkLocalisedName;
_ = LatticeReplicationMetrics.MerkleWalkAbortedName;

// Abort reasons map to their canonical tag strings.
string tag = LatticeReplicationMetrics.MerkleWalkAbortReasonTag(MerkleWalkAbortReason.RemoteUnavailable);
System.Diagnostics.Debug.Assert(tag == LatticeReplicationMetrics.MerkleWalkAbortRemoteUnavailable);
```

## Transport seam

The walk reuses the digest probe's read-only transport seam, `IReplicationDigestProbeTransport`. A new method, `ProbeMerkleWalkAsync`, asks a peer for its subtree digest over a key-range. It is a default interface method returning an unavailable response, so existing transports keep compiling unchanged; a host that can answer range folds overrides it.

```csharp verify
public sealed class MyRangeAwareProbeTransport : IReplicationDigestProbeTransport
{
    public Task<DigestProbeResponse> ProbeDigestAsync(
        string targetClusterId,
        DigestProbeRequest request,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(new DigestProbeResponse { DigestAvailable = false });
    }

    public Task<MerkleWalkProbeResponse> ProbeMerkleWalkAsync(
        string targetClusterId,
        MerkleWalkProbeRequest request,
        CancellationToken cancellationToken)
    {
        // A real implementation would fold the peer's subtree over
        // [request.RangeStartKey, request.RangeEndKey) into a digest.
        return Task.FromResult(MerkleWalkProbeResponse.Unavailable);
    }
}
```

Both `MerkleWalkProbeRequest` and `MerkleWalkProbeResponse` are immutable, Orleans-serializable value types, safe to ship across the grain-proxy and cross-cluster boundaries unchanged.
