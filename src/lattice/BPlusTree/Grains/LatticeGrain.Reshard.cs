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
    /// <remarks>
    /// Gated on <see cref="LatticeOperation.Read"/>. <see cref="ReshardAsync"/>
    /// enforces <see cref="LatticeOperation.TreeLifecycle"/> to start a reshard,
    /// but this status verb previously enforced nothing, leaking tree existence
    /// and lifecycle state to an unauthorized in-cluster caller. Read (not
    /// TreeLifecycle) because the verb only observes.
    /// <para>
    /// The external caller of this verb,
    /// <c>LatticeTreeAdmin.GetReshardStatusAsync</c>, already authorizes a tree
    /// read before dialing it, so the added enforcement is redundant there by
    /// design and changes no externally-visible behaviour; it closes the direct
    /// in-cluster grain-call path.
    /// </para>
    /// <para>
    /// <b>Internal pollers must carry system origin</b> - see
    /// <see cref="IsMergeCompleteAsync"/> for the
    /// <see cref="HotShardMonitorGrain"/> interaction.
    /// </para>
    /// </remarks>
    public async Task<bool> IsReshardCompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Read, cancellationToken);
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(TreeId);
        return await reshard.IsIdleAsync();
    }
}
