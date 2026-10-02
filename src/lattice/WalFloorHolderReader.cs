using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice;

/// <summary>
/// The two durable reads that name and classify the leaf behind a WAL
/// materialiser pin, shared by the WAL GC scheduler's floor-holder census and
/// the on-demand floor-holder probe (issue #4195), so the two can never
/// disagree about which leaf a pin belongs to or what state it is in.
/// </summary>
/// <remarks>
/// Both reads are pure: neither activates the leaf. The leaf a wedge strands is
/// precisely one that cannot be activated into fixing itself, so a read that
/// needed the leaf live would measure the wrong leaves.
/// </remarks>
internal static class WalFloorHolderReader
{
    /// <summary>
    /// Durable state name of <c>BPlusLeafGrain</c>'s persisted
    /// <see cref="LeafNodeState"/>, as declared by its
    /// <c>[PersistentState("leaf", ...)]</c> injection. Reading the same slot
    /// the grain would is what makes a direct read equivalent to asking the leaf.
    /// </summary>
    internal const string LeafStateName = "leaf";

    /// <summary>
    /// Parses a materialiser consumer id back into the grain id of the leaf that
    /// published it and the WAL partition the pin belongs to.
    /// </summary>
    /// <remarks>
    /// Fail-closed: anything that does not match the exact expected shape
    /// resolves nothing. A consumer id carrying no partition suffix is partition
    /// <c>0</c>, matching the legacy single-partition shape. The suffix is
    /// stripped only when the tree is actually partitioned, so a grain id that
    /// legitimately ends in <c>_&lt;digits&gt;</c> on a single-partition tree is
    /// not silently truncated.
    /// </remarks>
    /// <param name="treeId">The physical tree id the pin belongs to.</param>
    /// <param name="consumerId">The materialiser consumer id.</param>
    /// <param name="walPartitions">The tree's WAL partition count.</param>
    /// <param name="leafGrainId">The leaf that published the pin, when parsed.</param>
    /// <param name="partition">The WAL partition the pin belongs to, when parsed.</param>
    /// <returns><see langword="true"/> when the id parsed.</returns>
    internal static bool TryParseConsumerId(
        string treeId,
        string consumerId,
        int walPartitions,
        out GrainId leafGrainId,
        out int partition)
    {
        leafGrainId = default;
        partition = 0;

        var expectedStart = $"{BPlusTree.Grains.ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_";
        if (!consumerId.StartsWith(expectedStart, StringComparison.Ordinal))
        {
            return false;
        }

        var remainder = consumerId[expectedStart.Length..];
        if (remainder.Length == 0)
        {
            return false;
        }

        if (walPartitions > 1)
        {
            var lastSeparator = remainder.LastIndexOf('_');
            if (lastSeparator > 0
                && remainder.AsSpan(lastSeparator + 1).Length > 0
                && ulong.TryParse(remainder.AsSpan(lastSeparator + 1), out var parsedPartition))
            {
                remainder = remainder[..lastSeparator];
                partition = parsedPartition > int.MaxValue ? int.MaxValue : (int)parsedPartition;
            }
        }

        return GrainId.TryParse(remainder, out leafGrainId);
    }

    /// <summary>
    /// Reads one leaf's persisted projection checkpoint for a partition directly
    /// from the storage provider and maps it onto a
    /// <see cref="WalGcBlockingPinState"/>, returning the numeric checkpoint the
    /// state was derived from (<see langword="null"/> when none was read).
    /// Never activates the leaf.
    /// </summary>
    /// <remarks>
    /// A storage fault propagates: the caller decides how to log it and maps it
    /// to <see cref="WalGcBlockingPinState.Unreadable"/>. A missing storage
    /// provider is a property of the measurement rather than of the leaf, so it
    /// reads as <see cref="WalGcBlockingPinState.Unreadable"/>, never as an
    /// absence of durable state.
    /// </remarks>
    /// <param name="storage">The leaf state storage provider, or <see langword="null"/> when this silo has none.</param>
    /// <param name="leafGrainId">The leaf to read.</param>
    /// <param name="partition">The WAL partition whose checkpoint to read.</param>
    /// <returns>The classification and the persisted checkpoint behind it.</returns>
    internal static async Task<(WalGcBlockingPinState State, long? Checkpoint)> ReadLeafCheckpointAsync(
        IGrainStorage? storage,
        GrainId leafGrainId,
        int partition)
    {
        if (storage is null)
        {
            return (WalGcBlockingPinState.Unreadable, null);
        }

        var grainState = new GrainState<LeafNodeState>(new LeafNodeState());
        await storage.ReadStateAsync(LeafStateName, leafGrainId, grainState);

        if (!grainState.RecordExists || grainState.State is null)
        {
            return (WalGcBlockingPinState.NoDurableState, null);
        }

        // The tree id is read before the checkpoint (issue #3105): a pin can only
        // exist if the leaf carried a tree id when it was written, so finding none
        // proves the state was cleared afterwards and the pin outlived its
        // publisher.
        if (string.IsNullOrEmpty(grainState.State.TreeId))
        {
            return (WalGcBlockingPinState.Orphaned, null);
        }

        return (
            LatticeWalGcScheduler.ClassifyCheckpoint(grainState.State, partition),
            LatticeWalGcScheduler.ReadPersistedCheckpoint(grainState.State, partition));
    }
}
