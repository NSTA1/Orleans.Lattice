namespace Orleans.Lattice.Backup;

/// <summary>
/// Persistent state of a <c>BackupIdClaimGrain</c>: the live claims on its bare
/// id. Persisted so that losing the grain's activation while a claimant's publish
/// waits on its scope's hold cannot hand the id to another scope (issue #5632).
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupIdClaimState)]
internal sealed class BackupIdClaimState
{
    /// <summary>One entry per claim granted and not yet released.</summary>
    [Id(0)]
    public List<BackupIdPendingClaim> Claims { get; set; } = [];
}
