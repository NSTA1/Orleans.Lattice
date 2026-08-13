namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Online reshard orchestration entry points on the public
/// <see cref="ILattice"/> surface. Delegates to the per-tree
/// <see cref="ITreeReshardGrain"/> coordinator, which drives the migration
/// asynchronously via reminders.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    /// <remarks>
    /// Transparently absorbs <see cref="ShardActivationTimeoutException"/>
    /// from the underlying coordinator's shard-root activation-readiness
    /// seed for a small bounded number of retries
    /// (<see cref="ShardActivationRetry.MaxAttempts"/>, default 3) before
    /// surfacing the exception to the caller. Callers therefore do not
    /// need to special-case the cold-start race where a reshard call lands
    /// before the registry / root-leaf grain is visible; see
    /// <see cref="ShardActivationRetry"/> for the retry shape.
    /// </remarks>
    public async Task ReshardAsync(int newShardCount, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.TreeLifecycle, cancellationToken);
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => reshard.ReshardAsync(newShardCount),
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> IsReshardCompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(TreeId);
        return await reshard.IsIdleAsync();
    }
}
