namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The receiver bootstrap read fence (issue #4526).
/// <para>
/// A snapshot bootstrap drains a source cluster's export into the tree one row
/// at a time. The export carries a committed atomic batch as independent
/// committed rows - nothing identifies them as one saga once its terminal has
/// drained at the source - so a read part-way through the drain could observe
/// some of the batch's keys and not the rest. The bootstrap coordinator therefore
/// arms this fence on every shard of the tree before the drain applies its first
/// entry and lifts it only after the last, so a reader is either refused or sees
/// the whole import. A failed drain leaves it armed.
/// </para>
/// <para>
/// The fence is enforced in the shard's incoming call filter, the one seam every
/// read of the tree funnels through: the routing tier reaches leaves only through
/// a shard root. It refuses exactly the methods
/// <see cref="IsBootstrapFencedMethod"/> names - every value-, key- or
/// count-bearing read, the read-modify-write verbs whose outcome depends on the
/// current value, and the snapshot baseline capture that backups and snapshot
/// cursors are built from. Writes, replication applies, saga terminals, routing
/// and maintenance pass, so the drain itself and live replication keep applying.
/// An armed fence also keeps the shard from opening a split or consolidation, so
/// no new shard appears unfenced mid-drain.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <inheritdoc />
    public async Task SetBootstrapReadFenceAsync(bool fenced)
    {
        EnsureInternalOrigin(LatticeOperation.Replication);
        if (state.State.BootstrapReadFenced == fenced)
        {
            return;
        }

        state.State.BootstrapReadFenced = fenced;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            state.State.BootstrapReadFenced = !fenced;
            throw;
        }
    }

    /// <inheritdoc />
    public Task<bool> IsBootstrapReadFencedAsync() => Task.FromResult(state.State.BootstrapReadFenced);

    /// <summary>
    /// The refusal for a read the fence turns away, or <see langword="null"/> to
    /// admit the call. A single field read on the admitted path.
    /// </summary>
    private Task? RefuseIfBootstrapFenced(Orleans.Serialization.Invocation.IInvokable request)
    {
        if (!state.State.BootstrapReadFenced
            || request.GetInterfaceType() != typeof(IShardRootGrain)
            || !IsBootstrapFencedMethod(request.GetMethodName()))
        {
            return null;
        }

        return Task.FromException(new LatticeTreeBootstrappingException(
            $"Tree '{TreeId}' is being bootstrapped from a snapshot; reads are refused until the import completes. Retry after a short backoff.",
            TreeId));
    }

    /// <summary>
    /// The <see cref="IShardRootGrain"/> methods the bootstrap read fence refuses.
    /// Every method of the interface is classified here or in the test that pins
    /// the classification, so a new read cannot slip past the fence unnoticed.
    /// </summary>
    internal static bool IsBootstrapFencedMethod(string? methodName) => methodName switch
    {
        nameof(IShardRootGrain.TryGetOptimisticAsync) => true,
        nameof(IShardRootGrain.GetAsync) => true,
        nameof(IShardRootGrain.GetWithVersionAsync) => true,
        nameof(IShardRootGrain.ExistsAsync) => true,
        nameof(IShardRootGrain.GetManyAsync) => true,
        nameof(IShardRootGrain.GetRawEntryAsync) => true,
        nameof(IShardRootGrain.GetRawEntriesAsync) => true,
        nameof(IShardRootGrain.GetOrSetAsync) => true,
        nameof(IShardRootGrain.SetIfVersionAsync) => true,
        nameof(IShardRootGrain.SetManyWherePredicateAsync) => true,
        nameof(IShardRootGrain.AnyAsync) => true,
        nameof(IShardRootGrain.AnyBoundedAsync) => true,
        nameof(IShardRootGrain.CountAsync) => true,
        nameof(IShardRootGrain.CountBoundedAsync) => true,
        nameof(IShardRootGrain.CountWithMovedAwayAsync) => true,
        nameof(IShardRootGrain.CountWithMovedAwayBoundedAsync) => true,
        nameof(IShardRootGrain.CountForSlotsAsync) => true,
        nameof(IShardRootGrain.CountForSlotsBoundedAsync) => true,
        nameof(IShardRootGrain.GetSortedKeysBatchAsync) => true,
        nameof(IShardRootGrain.GetSortedKeysBatchReverseAsync) => true,
        nameof(IShardRootGrain.GetSortedEntriesBatchAsync) => true,
        nameof(IShardRootGrain.GetSortedEntriesBatchReverseAsync) => true,
        nameof(IShardRootGrain.GetSortedKeysBatchForSlotsAsync) => true,
        nameof(IShardRootGrain.GetSortedEntriesBatchForSlotsAsync) => true,
        nameof(IShardRootGrain.CaptureSnapshotBaselineAsync) => true,
        nameof(IShardRootGrain.CaptureGatedSnapshotBaselineAsync) => true,
        _ => false,
    };
}
