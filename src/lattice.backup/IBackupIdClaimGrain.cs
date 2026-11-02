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
/// only activation-local state is the claims of publishes that have not yet
/// completed, one per publish and released one at a time, each expiring if its
/// publish never reports back. The owning scope's publishes are claimed too
/// (issue #5627): a publish may wait while its own scope retires the id, and the
/// claim keeps another scope from taking the id in that window.
/// </remarks>
[Alias(BackupTypeAliases.IBackupIdClaimGrain)]
internal interface IBackupIdClaimGrain : IGrainWithStringKey
{
    /// <summary>
    /// Claims the bare id for one publish of <paramref name="scopeKey"/>, whether
    /// the id is unowned or the scope already owns it. Returns
    /// <see langword="false"/>, recording nothing, when a backup of another scope
    /// holds the id or another scope's publish has a claim on it.
    /// </summary>
    /// <param name="scopeKey">The claiming scope's key (<see cref="BackupScopeKey.For"/>).</param>
    Task<bool> TryClaimAsync(string scopeKey);

    /// <summary>
    /// Releases one of <paramref name="scopeKey"/>'s claims once its publish has
    /// completed (or failed), after which the durable state speaks for it. Other
    /// claims of the same scope stay. A no-op when the scope holds none.
    /// </summary>
    /// <param name="scopeKey">The scope whose claim to release.</param>
    Task ReleaseAsync(string scopeKey);
}
