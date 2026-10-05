using Microsoft.Extensions.Options;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The replication <see cref="ITombstoneReapGate"/> (issue #4615). The grace
/// period alone is a wall-clock bound: a write older than a delete, delivered
/// after the delete's tombstone was reaped, finds no tombstone and resurrects
/// the key on that replica only. A replicated tree therefore reaps a tombstone
/// stamped <c>t</c> only when <c>t &lt; min(D, P)</c>:
/// <list type="bullet">
///   <item><b>D, nothing it beats is still on its way here.</b> For every
///   origin that has pushed the tree here or is a configured peer: the
///   receiver tree frontier's applied low watermark (every write of the origin
///   below it is applied here, or held or lost), clamped below every write of
///   the origin the tree's causal-apply buffer or dead-letter queue still
///   holds, or its last installed export lacked. A lost write is never
///   applied, so it holds nothing back. An origin with no valid watermark - a
///   degraded tree, a pending origin, one whose re-seed is outstanding - makes
///   D zero. Since an origin's clock floor bounds every write it authors later,
///   D also covers a write the origin has not authored yet.</item>
///   <item><b>P, every peer applied every delete this cluster stamped below
///   it.</b> For every configured peer: its shipper's reap watermark, frozen
///   while the peer is taken off the log. A peer whose re-seed rewind passed a
///   trimmed delete record counts once its watermark resumes past it: the
///   re-seed export, opened after the marker, carried the tombstone as a
///   committed delete. A detached peer has left the topology and does not
///   count; a peer with no watermark yet makes P zero.</item>
/// </list>
/// An unknown or stalled frontier never reaps. A tree replication is not
/// enabled for is ungated.
/// </summary>
internal sealed class ReplicationTombstoneReapGate(
    IGrainFactory grainFactory,
    IReplicatedTreeMembership membership,
    IReplicationTopology topology,
    IOptionsMonitor<LatticeReplicationOptions> options) : ITombstoneReapGate
{
    /// <inheritdoc />
    public async Task<HybridLogicalClock?> GetReapCeilingAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        if (!membership.IsReplicated(treeId))
        {
            return null;
        }

        var local = options.Get(treeId).ClusterId;
        var peers = topology.CurrentPeers
            .Where(p => !string.Equals(p, local, StringComparison.Ordinal))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var snapshot = await grainFactory.GetGrain<IReplicationTreeFrontierGrain>(treeId).GetAsync(cancellationToken);
        if (snapshot.Epoch == Guid.Empty)
        {
            return Bound(treeId, HybridLogicalClock.Zero, LatticeReplicationMetrics.ReapBoundDegradedOrigin);
        }

        var origins = new HashSet<string>(peers, StringComparer.Ordinal);
        origins.UnionWith(snapshot.KnownOrigins);
        origins.Remove(local);

        HybridLogicalClock? ceiling = null;
        var reason = LatticeReplicationMetrics.ReapBoundOriginFrontier;
        foreach (var origin in origins)
        {
            if (!snapshot.LowWatermarks.TryGetValue(origin, out var watermark) || watermark == HybridLogicalClock.Zero)
            {
                return Bound(treeId, HybridLogicalClock.Zero, LatticeReplicationMetrics.ReapBoundDegradedOrigin);
            }

            Lower(ref ceiling, ref reason, watermark, LatticeReplicationMetrics.ReapBoundOriginFrontier);
            var held = await grainFactory.GetGrain<IReplicationOriginFrontierGrain>(origin)
                .GetMinHeldForTreeAsync(treeId, cancellationToken);
            if (held is { } lowestHeld)
            {
                Lower(ref ceiling, ref reason, lowestHeld, LatticeReplicationMetrics.ReapBoundHeldEntry);
            }
        }

        foreach (var peer in peers)
        {
            var watermark = await grainFactory.GetGrain<IReplicationShipperGrain>($"{treeId}/{peer}").GetReapLowWatermarkAsync();
            if (watermark == HybridLogicalClock.Zero)
            {
                return Bound(treeId, HybridLogicalClock.Zero, LatticeReplicationMetrics.ReapBoundPeerFrontier);
            }

            Lower(ref ceiling, ref reason, watermark, LatticeReplicationMetrics.ReapBoundPeerFrontier);
        }

        // A replicated tree with no peer and no origin has nothing in flight.
        return ceiling is { } bound ? Bound(treeId, bound, reason) : null;
    }

    private static void Lower(ref HybridLogicalClock? ceiling, ref string reason, HybridLogicalClock candidate, string candidateReason)
    {
        if (ceiling is not { } current || candidate.CompareTo(current) < 0)
        {
            ceiling = candidate;
            reason = candidateReason;
        }
    }

    private static HybridLogicalClock Bound(string treeId, HybridLogicalClock ceiling, string reason)
    {
        LatticeReplicationMetrics.TombstoneReapBound.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason),
            LatticeTenantLabel.ForTree(treeId));
        return ceiling;
    }
}
