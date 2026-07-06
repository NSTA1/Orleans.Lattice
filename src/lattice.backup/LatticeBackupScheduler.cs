namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupScheduler"/>. A thin facade that resolves
/// the per-scope <see cref="ILatticeBackupSchedulerGrain"/> - keyed by
/// <see cref="BackupScopeKey.For(BackupScopeSelector)"/> - and forwards each
/// operation to it, so all scheduling, triggering, and retention for a scope is
/// serialized through that single grain.
/// </summary>
internal sealed class LatticeBackupScheduler(IGrainFactory grainFactory) : ILatticeBackupScheduler
{
    /// <inheritdoc />
    public Task<string?> TriggerFullBackupAsync(BackupScopeSelector scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return Grain(scope).TriggerFullAsync(scope);
    }

    /// <inheritdoc />
    public Task<string?> TriggerIncrementalBackupAsync(BackupScopeSelector scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return Grain(scope).TriggerIncrementalAsync(scope);
    }

    /// <inheritdoc />
    public Task EnsureScheduleAsync(BackupScopeSelector scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return Grain(scope).EnsureScheduleAsync(scope);
    }

    /// <inheritdoc />
    public Task<BackupRetentionReport> PruneAsync(BackupScopeSelector scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return Grain(scope).PruneAsync(scope);
    }

    private ILatticeBackupSchedulerGrain Grain(BackupScopeSelector scope) =>
        grainFactory.GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
}
