# Anti-entropy digest probe

Cross-cluster replication in `Orleans.Lattice.Replication` is eventually consistent: every mutation rides the per-tree WAL to each peer, the receiver applies it HLC-monotonically, and concurrent edits converge through the per-tree `LatticeMergeMode`. In the steady state every cluster eventually holds the same data for a given shard. Silent divergence - two clusters that have applied different effective state for the same shard and stay that way - should never happen, but a transport bug, a partial GC, or an operator mistake can produce it. The **digest probe** is the *detection* half of the anti-entropy pipeline: a low-frequency, read-only background pass that compares each shard's local content digest against every peer's digest and surfaces a metric when they disagree.

The probe **detects** divergence; it does not repair it. Localisation and repair are layered on top by later anti-entropy stages. The probe never mutates data and never advances any replication cursor.

## What it compares

Every shard maintains a `LeafProjectionDigest` - a content hash plus an entry count, a checkpoint offset, and a contribution-function `Version` - read through the core library's `ILattice.GetLeafProjectionDigestAsync(shardIndex)`. The probe asks each peer for the same shard's digest over a dedicated read-only RPC and classifies the pair:

| Outcome | Meaning | Mismatch counted? |
|---|---|---|
| `Match` | Versions agree and the hashes are byte-identical - the two clusters have applied the same prefix of the same WAL for this shard. | No |
| `Mismatch` | Versions agree but the hashes differ - the two clusters have diverged for this shard. | **Yes** |
| `VersionSkew` | The digests carry different contribution-function `Version` values, so the hashes are not comparable (e.g. a rolling upgrade in flight). | No |
| `RemoteUnavailable` | The peer could not produce a digest (projection-digest maintenance disabled or latched off remotely). | No |

Only `Mismatch` represents real divergence, so only `Mismatch` increments the dedicated mismatch counter. Every comparison - including the three non-comparable outcomes - increments the per-comparison counter tagged with its `outcome`, so a dashboard can distinguish genuine divergence from a peer that simply has digesting turned off.

The classification itself is exposed as the pure, stateless `DigestProbeComparer`:

```csharp verify
var local = new LeafProjectionDigest
{
    Hash = new byte[] { 1, 2, 3 },
    EntryCount = 3,
    CheckpointOffset = 1,
    Version = LeafProjectionDigest.CurrentVersion,
};

var response = new DigestProbeResponse
{
    DigestAvailable = true,
    Digest = new LeafProjectionDigest
    {
        Hash = new byte[] { 9, 9, 9 },
        EntryCount = 3,
        CheckpointOffset = 1,
        Version = LeafProjectionDigest.CurrentVersion,
    },
};

DigestProbeOutcome outcome = DigestProbeComparer.Compare(local, response);
// Versions agree but the hashes differ -> Mismatch.
System.Diagnostics.Debug.Assert(outcome == DigestProbeOutcome.Mismatch);
```

## Enabling it

The probe ships **dark**: `DigestProbeEnabled` defaults to `false`, so an un-opted host sees no new RPC traffic, no scheduler, and no behaviour change. Enable it per tree and tune the cadence:

```csharp verify
siloBuilder.AddLatticeReplication(o =>
{
    o.ClusterId = "cluster-a";
    o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["orders"] = LatticeMergeMode.LwwRegister,
    };

    // Anti-entropy digest probe (off by default).
    o.DigestProbeEnabled = true;
    o.DigestProbeInterval = TimeSpan.FromMinutes(5);
    o.DigestProbeJitter = 0.2;
});
```

| Option | Default | Notes |
|---|---|---|
| `DigestProbeEnabled` | `false` | Per-tree master switch. When `false` the scheduler returns early without advancing its cadence. |
| `DigestProbeInterval` | 5 minutes | Base cadence between probe passes. Validated `> TimeSpan.Zero`. |
| `DigestProbeJitter` | `0.2` | Multiplicative spread applied to the interval so a fleet of silos does not probe in lockstep. Validated in `[0, 1]` (rejects `NaN`). |

The cadence is deliberately low: the probe is a slow background safety net, not a hot-path check. A five-minute interval with 20% jitter spreads probe passes across a window and keeps the added RPC and digest-read cost negligible against live replication traffic.

### Interaction with projection-digest maintenance

The probe respects the core library's `MaintainProjectionDigest` opt-out. A tree with `MaintainProjectionDigest = false` (including the system-tree default) has no digest to read, so the scheduler skips it - but still advances its cadence so the skip is cheap and quiet. If a tree's digest registry latches off permanently (a local digest read faults), the probe latches the skip for the activation lifetime rather than retrying on every pass. Remotely, a peer with digesting disabled returns a response whose `DigestAvailable` is `false`, which the comparer classifies as `RemoteUnavailable` rather than a mismatch.

## Observability

Two counters on the `orleans.lattice.replication` meter chart cross-cluster divergence:

| Metric | Tags | Emitted |
|---|---|---|
| `orleans.lattice.replication.digest_probe.compared` | `tree`, `shard`, `peer`, `outcome` | Once per shard/peer comparison, every pass. |
| `orleans.lattice.replication.digest_probe.mismatch` | `tree`, `shard`, `peer` | Only when the outcome is `Mismatch`. |

A non-zero, *sustained* mismatch rate for a `(tree, shard, peer)` triple is the signal that those two clusters have genuinely diverged for that shard and need remediation. A burst of `outcome=version_skew` during a rolling upgrade is expected and self-clears once both sides run the same contribution-function version. A steady `outcome=remote_unavailable` simply means the peer has digesting turned off for that tree.

The metric name constants are exposed for dashboards that build queries from the public surface: `LatticeReplicationMetrics.DigestProbeComparedName` and `LatticeReplicationMetrics.DigestProbeMismatchName`.

## Transport seam

The probe RPC travels over the existing replication push transport but is exposed through its own pluggable seam, `IReplicationDigestProbeTransport`, so it can be substituted independently of the live-push `IReplicationTransport`. The default DI registration is a no-op (`NoOpReplicationDigestProbeTransport`) that lets the detection pipeline be wired up in isolation; the gRPC binding replaces it with a real implementation that invokes the probe over the same per-peer channel cache the push transport uses. A host can register its own transport before `AddLatticeReplication`:

```csharp verify
public sealed class MyProbeTransport : IReplicationDigestProbeTransport
{
    public Task<DigestProbeResponse> ProbeDigestAsync(
        string targetClusterId,
        DigestProbeRequest request,
        CancellationToken cancellationToken)
    {
        // Forward the read-only probe to the peer over your own channel.
        return Task.FromResult(new DigestProbeResponse { DigestAvailable = false });
    }
}
```

Both the request (`DigestProbeRequest`) and the response (`DigestProbeResponse`) are immutable, Orleans-serializable value types, so they are safe to ship across the grain-proxy and cross-cluster boundaries unchanged.
