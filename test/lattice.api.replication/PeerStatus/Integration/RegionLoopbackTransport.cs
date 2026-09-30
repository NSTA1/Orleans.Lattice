using System.Collections.Concurrent;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus.Integration;

/// <summary>
/// In-process <see cref="IReplicationTransport"/> for
/// <see cref="TwoRegionStatusClusterFixture"/>: delivers a shipped batch straight
/// to the peer region's own silo-registered <see cref="IReplicationApplier"/>, so
/// the receiver records its inbound contact into that silo's
/// <see cref="ReplicationPeerStats"/> exactly as a real receiver does. A batch
/// for a region not yet registered is ack-rejected, which leaves the shipper's
/// cursor where it is.
/// </summary>
internal sealed class RegionLoopbackTransport : IReplicationTransport
{
    private static readonly ConcurrentDictionary<string, IServiceProvider> Regions = new(StringComparer.Ordinal);

    /// <summary>Makes <paramref name="regionId"/> reachable through its silo's services.</summary>
    /// <param name="regionId">The region (cluster) id.</param>
    /// <param name="siloServices">The region's silo service provider.</param>
    public static void Register(string regionId, IServiceProvider siloServices) => Regions[regionId] = siloServices;

    /// <summary>Makes <paramref name="regionId"/> unreachable.</summary>
    /// <param name="regionId">The region (cluster) id.</param>
    public static void Unregister(string regionId) => Regions.TryRemove(regionId, out _);

    /// <inheritdoc />
    public async Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
    {
        if (!Regions.TryGetValue(batch.TargetClusterId, out var peer))
        {
            return new ReplicationAck { Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero };
        }

        var applier = (IReplicationApplier)peer.GetService(typeof(IReplicationApplier))!;
        var encoder = (IWalRecordEncoder)peer.GetService(typeof(IWalRecordEncoder))!;
        var encoded = batch.EncodedEnvelope?.EncodedEntries ?? ReadOnlyMemory<ArraySegment<byte>>.Empty;
        var mode = batch.EncodedEnvelope?.Header.Mode ?? LatticeMergeMode.LwwRegister;
        var entries = new List<WalRecord>(encoded.Length);
        for (var i = 0; i < encoded.Length; i++)
        {
            entries.Add(encoder.Decode(encoded.Span[i].AsSpan(), batch.TreeName, mode));
        }

        var result = await applier.ApplyBatchAsync(entries, cancellationToken).ConfigureAwait(false);
        return new ReplicationAck { Accepted = true, HighestAppliedHlc = result.HighWaterMark };
    }
}
