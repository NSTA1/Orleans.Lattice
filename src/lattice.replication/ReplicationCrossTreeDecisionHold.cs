using System.Collections.Immutable;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The replication <see cref="ICrossTreeDecisionHold"/> (issue #4684). A
/// receiver that imports one participant tree of a cross-tree atomic write
/// settles the tree's arrival at its cross-tree barrier from the sub-saga's
/// decision row; an export taken after the origin purged that row carries only
/// committed rows, and the sibling that already delegated to the barrier waits
/// for ever. The origin therefore keeps every participant's decision until
/// every peer of every participant tree has durably acknowledged past that
/// participant's terminal.
/// <list type="bullet">
///   <item><b>Boundary.</b> Each participant records, the first time its own
///   registry asks, each partition's next sequence of its write-ahead log; its
///   decision was forgotten by then, so its terminal lies below. A participant
///   whose log was rebound since is re-recorded against the new log, which is
///   stricter.</item>
///   <item><b>Peers.</b> The configured peers of a replicated participant,
///   together with every peer that ever attached a shipper to it
///   (<see cref="ICrossTreePeerEnrolmentGrain"/>): a detached peer still holds,
///   because it can be added back against barriers that need the
///   decision.</item>
///   <item><b>Acknowledged.</b> The peer's shipper publishes the lowest offset
///   per partition it has not durably acknowledged; the receiver acknowledges a
///   cross-tree terminal only after the barrier recorded it. A shipper that is
///   detached, bound to another log, or not yet published holds.</item>
/// </list>
/// While any silo predates the hold, nothing is released (fail closed).
/// </summary>
internal sealed class ReplicationCrossTreeDecisionHold(
    IGrainFactory grainFactory,
    IReplicatedTreeMembership membership,
    IReplicationTopology topology,
    IOptionsMonitor<LatticeReplicationOptions> options,
    LatticeOptionsResolver optionsResolver,
    IServiceProvider services) : ICrossTreeDecisionHold
{
    /// <inheritdoc />
    public async Task<bool> MayPurgeAsync(
        string treeId, Guid txid, CrossTreeMembership crossTreeMembership, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(crossTreeMembership);
        if (!PurgeHoldSupport.AllSilosHonourCrossTreeHold(services))
        {
            return false;
        }

        var tracker = grainFactory.GetGrain<ICrossTreeHoldTrackerGrain>(crossTreeMembership.OperationId);
        var snapshot = await tracker.GetAsync();
        if (snapshot.Completed)
        {
            return true;
        }

        var participants = crossTreeMembership.Participants.Add(treeId).Distinct(StringComparer.Ordinal).ToList();
        var released = true;
        foreach (var participant in participants)
        {
            var physical = await ReadPhysicalTreeIdAsync(participant);
            snapshot.Boundaries.TryGetValue(participant, out var boundary);
            var rebound = boundary is not null
                && !string.Equals(boundary.PhysicalTreeId, physical, StringComparison.Ordinal);
            if (rebound || (boundary is null && string.Equals(participant, treeId, StringComparison.Ordinal)))
            {
                // Only the participant's own registry records its first
                // boundary: its decision is forgotten by then, so its terminal
                // is in its log. A rebound log is re-recorded by anyone.
                await tracker.RecordBoundaryAsync(participant, await ReadBoundaryAsync(physical, cancellationToken));
                released = false;
                continue;
            }

            if (boundary is null || (released && !await AllPeersPastAsync(participant, boundary)))
            {
                released = false;
            }
        }

        if (!released)
        {
            return false;
        }

        await tracker.MarkReleasedAsync(treeId, participants);
        return true;
    }

    private async Task<string> ReadPhysicalTreeIdAsync(string treeId)
    {
        var entry = await grainFactory.GetLatticeRegistry().GetEntryAsync(treeId);
        return entry?.PhysicalTreeId ?? treeId;
    }

    private async Task<CrossTreeHoldBoundary> ReadBoundaryAsync(string physical, CancellationToken cancellationToken)
    {
        var partitions = await optionsResolver.GetWalPartitionsAsync(physical);
        var tails = new Task<long>[Math.Max(0, partitions)];
        for (var p = 0; p < tails.Length; p++)
        {
            tails[p] = grainFactory.GetGrain<IWalShardGrain>($"{physical}/{p}").GetNextSequenceAsync(cancellationToken).AsTask();
        }

        return new CrossTreeHoldBoundary(physical, [.. await Task.WhenAll(tails)]);
    }

    private async Task<bool> AllPeersPastAsync(string treeId, CrossTreeHoldBoundary boundary)
    {
        var local = options.Get(treeId).ClusterId;
        var peers = new HashSet<string>(StringComparer.Ordinal);
        if (membership.IsReplicated(treeId))
        {
            peers.UnionWith(topology.CurrentPeers);
        }

        peers.UnionWith(await grainFactory.GetGrain<ICrossTreePeerEnrolmentGrain>(treeId).GetAsync());
        peers.Remove(local);
        foreach (var peer in peers)
        {
            var positions = await grainFactory.GetGrain<IReplicationShipperGrain>($"{treeId}/{peer}")
                .AsReference<IWalOffsetConsumer>()
                .GetDurableReadPositionsAsync(boundary.PhysicalTreeId);
            if (!IsPast(positions, boundary.Tails))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Whether <paramref name="positions"/> (the lowest unacknowledged offset of
    /// each partition the shipper reads) are at or past <paramref name="tails"/>
    /// on every partition both describe. Null (detached, another log) or empty
    /// (not yet published) holds.
    /// </summary>
    internal static bool IsPast(long[]? positions, ImmutableArray<long> tails)
    {
        if (positions is null || positions.Length == 0)
        {
            return false;
        }

        var shared = Math.Min(positions.Length, tails.Length);
        for (var p = 0; p < shared; p++)
        {
            if (positions[p] < tails[p])
            {
                return false;
            }
        }

        return true;
    }
}
