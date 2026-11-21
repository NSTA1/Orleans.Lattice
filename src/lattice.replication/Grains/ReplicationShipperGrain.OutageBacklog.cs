using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Outage backlog (issue #5587). The per-peer <c>entries_behind</c> figure was
/// recorded only on a successful acknowledgement, so while the peer was
/// unreachable it stayed frozen at whatever the last ack measured - usually
/// zero - while the source log grew. After every failure attributable to the
/// peer round-trip (a transport throw or a rejected ack) the shipper now
/// measures the backlog directly as the source log's tail minus the peer's
/// acknowledged watermark, summed over the partitions. The shipper retries on
/// the capped backoff schedule, so the figure is refreshed at least once per
/// <see cref="LatticeReplicationOptions.ShipBackoffMax"/> for as long as the
/// outage lasts.
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    // Set by ApplyBackoff on a peer-attributable failure; consumed at the end
    // of the pump tick by RefreshOutageBacklogAsync.
    private bool _outageBacklogDue;

    /// <summary>
    /// Records the outbound entry backlog as the sum over partitions of the
    /// source log's next sequence minus the peer's durable acknowledged cursor.
    /// Runs only after a peer-attributable failure, never on the shipping hot
    /// path. A tail read that fails leaves the previous figure in place.
    /// </summary>
    private async Task RefreshOutageBacklogAsync()
    {
        if (!_outageBacklogDue)
        {
            return;
        }

        _outageBacklogDue = false;
        var partitions = Math.Min(_partitionCount, _partitionGrainCache.Length);
        if (partitions == 0)
        {
            return;
        }

        long behind = 0;
        for (var p = 0; p < partitions; p++)
        {
            var grain = _partitionGrainCache[p] ??=
                _grainFactory.GetGrain<IWalShardGrain>($"{_walTreeId}/{p}");
            long tail;
            try
            {
                tail = await grain.GetNextSequenceAsync(CancellationToken.None);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogDebug(ex,
                    "Shipper {Context} could not read WAL partition {Partition}'s tail to measure the outage backlog",
                    LogContext, p);
                return;
            }

            var cursor = state.State.PartitionCursors.TryGetValue(p, out var c) ? Math.Max(0, c) : 0;
            if (tail > cursor)
            {
                behind = tail - cursor > long.MaxValue - behind ? long.MaxValue : behind + (tail - cursor);
            }
        }

        _peerStats.RecordEntriesBehind(_treeName, _peerClusterId, behind);
    }
}
