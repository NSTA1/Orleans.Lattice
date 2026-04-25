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
    public async Task ReshardAsync(int newShardCount, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(TreeId);
        await reshard.ReshardAsync(newShardCount);
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
