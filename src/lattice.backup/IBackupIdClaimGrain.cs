namespace Orleans.Lattice.Backup;

/// <summary>
/// Arbitrates which scope owns a full backup's bare content-addressed id (issue
/// #5620). One activation exists per bare id. Its calls are not interleaved, so
/// two scopes that capture identical content at once cannot both take the bare
/// id: the second sees the first's claim and takes its scope-qualified id
/// (<see cref="BackupIdentity.ScopeQualifiedFull"/>) instead.
/// </summary>
/// <remarks>
/// Ownership is read from durable state - the scope of the catalog row, or else
/// of the sink manifest, stored under the id - so nothing is persisted here. The
/// only activation-local state is the claim of a publish that has not yet written
/// its sink manifest, which expires if the publish never completes.
/// </remarks>
[Alias(BackupTypeAliases.IBackupIdClaimGrain)]
internal interface IBackupIdClaimGrain : IGrainWithStringKey
{
    /// <summary>
    /// Claims the bare id for <paramref name="scopeKey"/>, or confirms the scope
    /// already owns it. Returns <see langword="false"/> when a backup of another
    /// scope holds the id or another scope's publish has claimed it.
    /// </summary>
    /// <param name="scopeKey">The claiming scope's key (<see cref="BackupScopeKey.For"/>).</param>
    Task<bool> TryClaimAsync(string scopeKey);

    /// <summary>
    /// Releases <paramref name="scopeKey"/>'s pending claim once its publish has
    /// written the sink manifest (or failed), after which the durable state speaks
    /// for it. A no-op when the claim is not that scope's.
    /// </summary>
    /// <param name="scopeKey">The scope whose claim to release.</param>
    Task ReleaseAsync(string scopeKey);
}
