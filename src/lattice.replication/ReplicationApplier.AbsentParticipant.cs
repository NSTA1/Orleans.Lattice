using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

internal sealed partial class ReplicationApplier
{
    /// <summary>
    /// Tells a cross-tree receiver barrier that a participating tree is no longer
    /// replicated here (issue #4692). Called only from the enrollment gate's
    /// not-replicated drop, so only for a tree the receiver's own configuration
    /// no longer replicates - never because a terminal was dropped for another
    /// reason. The barrier stops waiting for the tree and, if every remaining
    /// tree has arrived, decides; the remaining trees are then finalized here,
    /// as the terminal that completes a barrier finalizes them. The tree's own
    /// pending bucket of the dropped sub-saga is discarded: the tree is no
    /// longer a replica of the origin, so nothing will ever settle it.
    /// <para>
    /// The tree and operation ids are wire-supplied. A barrier that has not
    /// opened is left untouched and persists nothing, a pending bucket is only
    /// discarded on a tree that is registered here, and, like the lost-write
    /// marks this drop records, nothing is done for an origin outside a
    /// configured <see cref="LatticeReplicationOptions.ReplicationPeers"/> list.
    /// </para>
    /// </summary>
    private async Task NotifyCrossTreeParticipantAbsentAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        if (entry.Op is not (MutationKind.TxCommit or MutationKind.TxAbort)
            || string.IsNullOrEmpty(entry.CrossTreeOperationId)
            || string.IsNullOrEmpty(entry.OriginClusterId)
            || string.IsNullOrEmpty(entry.TreeId)
            || entry.TransactionId == Guid.Empty)
        {
            return;
        }

        var peers = options.CurrentValue.ReplicationPeers;
        if (peers is not null && !peers.Contains(entry.OriginClusterId))
        {
            return;
        }

        var receiverKey = LatticeCrossTreeReceiverGrain.ComputeKey(entry.OriginClusterId, entry.CrossTreeOperationId);
        var decision = await grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(receiverKey)
            .NotifyParticipantAbsentAsync(entry.TreeId)
            .ConfigureAwait(false);
        if (decision.Decided)
        {
            foreach (var finalize in decision.TreesToFinalize)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await grainFactory.GetGrain<IReplicationApplyGrain>(finalize.TreeId)
                    .FinalizeCrossTreeTerminalAsync(
                        finalize.TransactionId,
                        decision.Committed,
                        finalize.ObservedSourceShards,
                        finalize.TerminalHlc,
                        finalize.OriginClusterId,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        await DiscardAbsentParticipantBucketAsync(entry.TreeId, entry.TransactionId, cancellationToken).ConfigureAwait(false);
    }

    private async Task DiscardAbsentParticipantBucketAsync(string treeId, Guid transactionId, CancellationToken cancellationToken)
    {
        var registry = grainFactory.GetLatticeRegistry();
        if (!await registry.ExistsAsync(treeId).ConfigureAwait(false))
        {
            return;
        }

        var physicalTreeId = await registry.ResolveAsync(treeId).ConfigureAwait(false);
        var shardMap = await registry.GetShardMapAsync(treeId).ConfigureAwait(false)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);
        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(false);
            while (leafId is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                await leaf.DiscardPendingTransactionAsync(transactionId).ConfigureAwait(false);
                leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(false);
            }
        }

        _logger.LogWarning(
            "Tree '{Tree}' is no longer replicated here: discarded its pending bucket of transaction {TransactionId}, "
            + "whose terminal was dropped at the enrollment gate.",
            treeId, transactionId);
    }
}
