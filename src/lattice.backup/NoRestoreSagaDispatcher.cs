namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="IRestoreSagaDispatcher"/> registered by
/// <see cref="LatticeBackupServiceCollectionExtensions.AddLatticeBackup(Orleans.Hosting.ISiloBuilder, System.Action{LatticeBackupOptions})"/>.
/// Never dispatches, which is the correct behaviour for a single-cluster
/// deployment where the replication package is not wired: every restore takes the
/// plain local shadow-cutover / in-place path. A multi-cluster host replaces this
/// registration with the replication package's implementation that promotes a
/// restore into a replicated tree to a coordinated saga.
/// </summary>
internal sealed class NoRestoreSagaDispatcher : IRestoreSagaDispatcher
{
    /// <inheritdoc />
    public Task<LatticeRestoreResult?> TryDispatchAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return Task.FromResult<LatticeRestoreResult?>(null);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<LatticeRestoreResult>?> TryDispatchSetAsync(
        string setId,
        LatticeRestoreMode mode,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);
        return Task.FromResult<IReadOnlyList<LatticeRestoreResult>?>(null);
    }
}
