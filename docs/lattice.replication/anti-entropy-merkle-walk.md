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

## Honest limitation: the remote range-fold

A clean key-range-keyed **remote** subtree digest is not computable with today's public surface. No public method folds an arbitrary key-range into a digest, and a peer's internal-node grain identities are not addressable from another cluster. The default probe transport therefore answers every range probe with `Available = false`.

The practical consequence: with the built-in (or no-op) transport, the walk **aborts immediately with reason `remote_unavailable`**. The local descent engine, the wire shape (`MerkleWalkProbeRequest` / `MerkleWalkProbeResponse`), the depth and byte caps, and both metrics are all real and exercised - but end-to-end cross-cluster localisation needs a host-supplied transport that can answer a range-keyed fold. A range-answering remote fold is the documented follow-up. Until then the feature is a complete, dark, read-only scaffold rather than a live cross-cluster localiser.

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
