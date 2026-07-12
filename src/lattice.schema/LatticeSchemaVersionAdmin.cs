namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaVersionAdmin"/>. It delegates config
/// mutations to the durable <see cref="ILatticeSchemaVersionStore"/> and eagerly
/// evicts the local <see cref="ILatticeSchemaVersionProvider"/> cache on a change,
/// so the new config takes effect on this silo's next write / read without waiting
/// for the mutation observer to propagate the eviction. Mirrors
/// <c>LatticeSchemaAdmin</c>.
/// </summary>
internal sealed class LatticeSchemaVersionAdmin(
    ILatticeSchemaVersionStore store,
    ILatticeSchemaVersionProvider provider,
    IGrainFactory grainFactory) : ILatticeSchemaVersionAdmin
{
    /// <inheritdoc />
    public async Task SetVersionConfigAsync(
        string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        await store.SetConfigAsync(treeId, config, cancellationToken).ConfigureAwait(false);
        provider.Invalidate(treeId);
    }

    /// <inheritdoc />
    public Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return store.GetConfigAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var current = await store.GetConfigAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (current is not { } config)
        {
            throw new InvalidOperationException(
                $"Tree '{treeId}' is not versioned; call {nameof(SetVersionConfigAsync)} to opt it in before advancing.");
        }

        if (newTargetVersion <= config.TargetVersion)
        {
            throw new InvalidOperationException(
                $"Target version for tree '{treeId}' is monotonic: the new target ({newTargetVersion}) must be " +
                $"greater than the current target ({config.TargetVersion}).");
        }

        var advanced = config with { TargetVersion = newTargetVersion };
        await store.SetConfigAsync(treeId, advanced, cancellationToken).ConfigureAwait(false);
        provider.Invalidate(treeId);
        return advanced;
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));

        // Advance the target first (monotonic; reuses the existing validation), so
        // new writes stamp at the new version immediately and the lazy read path
        // upcasts existing values while the eager migration re-stamps them.
        var advanced = await AdvanceTargetVersionAsync(treeId, newTargetVersion, cancellationToken)
            .ConfigureAwait(false);

        return await grainFactory.GetGrain<ILatticeSchemaRemediationGrain>(treeId)
            .StartVersionMigrationAsync(advanced.SchemaId, advanced.TargetVersion, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));

        var current = await store.GetConfigAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (current is not { } config)
        {
            throw new InvalidOperationException(
                $"Tree '{treeId}' is not versioned; call {nameof(SetVersionConfigAsync)} to opt it in before migrating.");
        }

        return await grainFactory.GetGrain<ILatticeSchemaRemediationGrain>(treeId)
            .StartVersionMigrationAsync(config.SchemaId, config.TargetVersion, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var removed = await store.ClearConfigAsync(treeId, cancellationToken).ConfigureAwait(false);
        provider.Invalidate(treeId);
        return removed;
    }
}
