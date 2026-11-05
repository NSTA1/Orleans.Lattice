using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The split's half of #5841's rule A: fences every activation of a split donor that may
/// still hold the span it had before the split. Runs only after the donor's narrowed state
/// is durable. It reads the donor's epoch row fresh and, when any activation ever claimed
/// (the row exists), raises the row by a conditional write on the ETag it just read, then
/// claims the raised epoch for the leaf at every WAL partition. Every epoch issued before
/// the raise is then below the leaf's entry everywhere: a duplicate's stamped append is
/// rejected as stale, and its claim, at a partition old or new, is refused.
/// </summary>
/// <remarks>
/// <para>
/// A duplicate that claims after this read is caught by the claim's half instead: it writes
/// the row, then reads the leaf's state, which already carries the narrowing
/// (<see cref="LeafStateCurrencyProbe"/>). The pair is a Dekker exchange across two
/// stores - the epoch row and the leaf's grain state - so it needs each side's completed
/// write to be visible to the other side's later read (A2): strong read-after-write across
/// clients, which the Azure Table and memory providers give. A provider with session or
/// eventual consistency does not meet it.
/// </para>
/// <para>
/// The raise is held per leaf, in the leaf's own entry at each partition, never in the
/// partition's shared retired floor: a split is frequent, and raising the shared floor on
/// each one would refuse the next claim of every other leaf below it.
/// </para>
/// </remarks>
internal static class LeafSplitDuplicateFence
{
    /// <summary>How many times the row write re-reads after losing to a concurrent claim.</summary>
    internal const int MaxRowWriteAttempts = 8;

    /// <summary>
    /// Fences the donor <paramref name="leafId"/>'s activations; see the type remarks.
    /// </summary>
    /// <param name="storage">The tree's grain storage provider, holding the epoch row.</param>
    /// <param name="shardFor">The WAL shard grain of a partition.</param>
    /// <param name="partitionCount">The tree's WAL partition count.</param>
    /// <param name="leafId">The donor leaf's grain key.</param>
    /// <param name="onRaised">Called with the row as written, before any partition is claimed, so a live donor adopts its ETag and epoch (A3).</param>
    /// <param name="onClaimed">Called with a partition and its claim head when the raised epoch was accepted there.</param>
    /// <param name="cancellationToken">The fence's cancellation.</param>
    /// <returns>
    /// Every partition's head, read after the raised epoch's claim there - every append
    /// stamped with an older epoch was allocated below it or is rejected - or
    /// <see langword="null"/> when no activation ever claimed and there is nothing to fence.
    /// </returns>
    /// <exception cref="InvalidOperationException">The row write lost to concurrent claims every time.</exception>
    internal static async Task<long[]?> FenceAsync(
        IGrainStorage storage,
        Func<int, IWalShardGrain> shardFor,
        int partitionCount,
        Guid leafId,
        Action<GrainState<LeafAppendEpochState>>? onRaised,
        Action<int, long>? onClaimed,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(shardFor);
        GrainState<LeafAppendEpochState>? raised = null;
        for (var attempt = 1; attempt <= MaxRowWriteAttempts && raised is null; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var row = await WalAppendFenceRows.ReadLeafEpochAsync(storage, leafId).ConfigureAwait(false);
            if (!row.RecordExists)
            {
                return null;
            }

            row.State.Epoch++;
            try
            {
                await WalAppendFenceRows.WriteAsync(
                    storage,
                    WalAppendFenceRows.LeafEpochGrainType,
                    WalAppendFenceRows.LeafEpochKey(leafId),
                    row).ConfigureAwait(false);
                raised = row;
            }
            catch (InconsistentStateException)
            {
                // A claim wrote the row since the read: read it again and raise above it.
            }
        }

        if (raised is null)
        {
            throw new InvalidOperationException(
                $"Leaf {leafId:N}: the split could not raise its epoch row over {MaxRowWriteAttempts} concurrent claims; its duplicates are not yet fenced.");
        }

        onRaised?.Invoke(raised);
        var epoch = raised.State.Epoch;
        var claims = new Task<long>[Math.Max(1, partitionCount)];
        for (var p = 0; p < claims.Length; p++)
        {
            claims[p] = ClaimAtAsync(shardFor(p), p, leafId, epoch, onClaimed, cancellationToken);
        }

        return await Task.WhenAll(claims).ConfigureAwait(false);
    }

    private static async Task<long> ClaimAtAsync(
        IWalShardGrain shard,
        int partition,
        Guid leafId,
        long epoch,
        Action<int, long>? onClaimed,
        CancellationToken cancellationToken)
    {
        var result = await shard.ClaimAppendEpochAsync(leafId, epoch, cancellationToken).ConfigureAwait(false);
        if (result.Accepted)
        {
            onClaimed?.Invoke(partition, result.Head);
            return result.Head;
        }

        // Refused: the entry or the partition's floor is already at or above the raised
        // epoch, which outranks every epoch issued before it just as well. The head is read
        // after the refusal, so every older-epoch append here lies below it.
        return await shard.GetNextSequenceAsync(cancellationToken).ConfigureAwait(false);
    }
}
