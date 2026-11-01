namespace Orleans.Lattice.Backup;

/// <summary>The default <see cref="IBackupIdClaimGrain"/>.</summary>
internal sealed class BackupIdClaimGrain(
    IGrainContext context,
    ILatticeBackupCatalogStore catalog,
    ILatticeBackupSink sink)
    : IGrainBase, IBackupIdClaimGrain
{
    // A publish's claim lapses if it never reports back, so a capture that fails
    // between its claim and its sink-manifest write cannot hold the id for good.
    internal static readonly TimeSpan PendingClaimLifetime = TimeSpan.FromMinutes(15);

    private string? _pendingScopeKey;
    private DateTimeOffset _pendingUntil;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    /// <inheritdoc />
    public async Task<bool> TryClaimAsync(string scopeKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);
        var backupId = context.GrainId.Key.ToString()!;
        var now = DateTimeOffset.UtcNow;
        if (_pendingScopeKey is { } pending && _pendingUntil > now)
        {
            return string.Equals(pending, scopeKey, StringComparison.Ordinal);
        }

        _pendingScopeKey = null;
        var owner = await BackupIdOwnership.OwnerScopeKeyAsync(catalog, sink, backupId);
        if (owner is not null)
        {
            return string.Equals(owner, scopeKey, StringComparison.Ordinal);
        }

        _pendingScopeKey = scopeKey;
        _pendingUntil = now + PendingClaimLifetime;
        return true;
    }

    /// <inheritdoc />
    public Task ReleaseAsync(string scopeKey)
    {
        if (string.Equals(_pendingScopeKey, scopeKey, StringComparison.Ordinal))
        {
            _pendingScopeKey = null;
        }

        return Task.CompletedTask;
    }
}
