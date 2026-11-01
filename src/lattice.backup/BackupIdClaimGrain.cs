namespace Orleans.Lattice.Backup;

/// <summary>The default <see cref="IBackupIdClaimGrain"/>.</summary>
internal sealed class BackupIdClaimGrain(
    IGrainContext context,
    ILatticeBackupCatalogStore catalog,
    ILatticeBackupSink sink)
    : IGrainBase, IBackupIdClaimGrain
{
    // A publish's claim lapses if it never reports back, so a capture that fails
    // between its claim and its release cannot hold the id for good.
    internal static readonly TimeSpan PendingClaimLifetime = TimeSpan.FromMinutes(15);

    // One entry per claim granted and not yet released: a scope can hold several
    // at once (two captures of the same content), and each release retires one.
    private readonly List<(string ScopeKey, DateTimeOffset Until)> _pending = [];

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    /// <inheritdoc />
    public async Task<bool> TryClaimAsync(string scopeKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);
        var backupId = context.GrainId.Key.ToString()!;
        var now = DateTimeOffset.UtcNow;
        _pending.RemoveAll(claim => claim.Until <= now);
        if (_pending.Exists(claim => !string.Equals(claim.ScopeKey, scopeKey, StringComparison.Ordinal)))
        {
            return false;
        }

        var owner = await BackupIdOwnership.OwnerScopeKeyAsync(catalog, sink, backupId);
        if (owner is not null && !string.Equals(owner, scopeKey, StringComparison.Ordinal))
        {
            return false;
        }

        // Recorded for the owning scope too (issue #5627): its publish may wait on
        // the scope's hold while a retention pass or a delete retires the id, and
        // the id must stay this scope's until that publish completes, or another
        // scope could take the ownerless id and have it overwritten.
        _pending.Add((scopeKey, now + PendingClaimLifetime));
        return true;
    }

    /// <inheritdoc />
    public Task ReleaseAsync(string scopeKey)
    {
        var index = _pending.FindIndex(claim => string.Equals(claim.ScopeKey, scopeKey, StringComparison.Ordinal));
        if (index >= 0)
        {
            _pending.RemoveAt(index);
        }

        return Task.CompletedTask;
    }
}
