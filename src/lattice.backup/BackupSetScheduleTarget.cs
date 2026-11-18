namespace Orleans.Lattice.Backup;

/// <summary>
/// What the API facade resolves for a backup-set schedule when a caller
/// registers it, and hands to the schedule's grain with the request: the tenant
/// namespace the schedule lives in (and an all-trees schedule enumerates), and the
/// scopes a schedule with a fixed membership captures. Resolved where a real
/// caller exists (registration), never at a reminder, which runs with no caller.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetScheduleTarget)]
[Immutable]
internal sealed record BackupSetScheduleTarget
{
    /// <summary>Initializes a new <see cref="BackupSetScheduleTarget"/>.</summary>
    /// <param name="tenantTreePrefix">
    /// The tree-id prefix of the registering tenant's namespace (<c>t/{tenant}/</c>),
    /// or <c>null</c> for the default tenant, whose trees carry no prefix.
    /// </param>
    /// <param name="fixedScopes">
    /// The effective scopes captured by a schedule whose membership does not move:
    /// a named-trees schedule's scopes, or the trees an all-trees schedule pinned at
    /// registration. Empty for an all-trees schedule that re-resolves every run.
    /// </param>
    /// <param name="excludedTreePrefixes">The effective (tenant-composed) excluded tree-id prefixes.</param>
    public BackupSetScheduleTarget(
        string? tenantTreePrefix,
        IReadOnlyList<BackupScopeSelector> fixedScopes,
        IReadOnlyList<string> excludedTreePrefixes)
    {
        ArgumentNullException.ThrowIfNull(fixedScopes);
        ArgumentNullException.ThrowIfNull(excludedTreePrefixes);
        TenantTreePrefix = tenantTreePrefix;
        FixedScopes = fixedScopes;
        ExcludedTreePrefixes = excludedTreePrefixes;
    }

    /// <summary>The registering tenant's tree-id prefix, or <c>null</c> for the default tenant.</summary>
    [Id(0)]
    public string? TenantTreePrefix { get; init; }

    /// <summary>The effective scopes of a schedule whose membership does not move.</summary>
    [Id(1)]
    public IReadOnlyList<BackupScopeSelector> FixedScopes { get; init; }

    /// <summary>The effective excluded tree-id prefixes of an all-trees schedule.</summary>
    [Id(2)]
    public IReadOnlyList<string> ExcludedTreePrefixes { get; init; }
}
