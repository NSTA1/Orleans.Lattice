namespace Orleans.Lattice.Backup;

/// <summary>
/// The persisted form of a backup-set schedule's configuration: a plain,
/// settable mirror of <see cref="LatticeBackupSetScheduleRequest"/>, whose
/// validating constructor a grain-storage serializer must never have to satisfy.
/// The request is rebuilt (and so re-validated) from it on every read.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetScheduleConfig)]
internal sealed class BackupSetScheduleConfig
{
    /// <summary>The schedule's name.</summary>
    [Id(0)]
    public string Name { get; set; } = string.Empty;

    /// <summary>How the set chooses its trees.</summary>
    [Id(1)]
    public BackupSetMembershipMode Membership { get; set; }

    /// <summary>The effective scopes of an explicit schedule.</summary>
    [Id(2)]
    public List<BackupScopeSelector> Scopes { get; set; } = [];

    /// <summary>The cadence between runs.</summary>
    [Id(3)]
    public TimeSpan Interval { get; set; }

    /// <summary>Keep the newest this many sets, or <c>null</c>.</summary>
    [Id(4)]
    public int? RetentionKeepLast { get; set; }

    /// <summary>Keep every set captured within this window, or <c>null</c>.</summary>
    [Id(5)]
    public TimeSpan? RetentionMaxAge { get; set; }

    /// <summary>Whether each set is captured at a single cross-tree fence.</summary>
    [Id(6)]
    public bool CrossTreeConsistent { get; set; }

    /// <summary>Whether an all-trees schedule re-resolves its trees at every run.</summary>
    [Id(7)]
    public bool IncludeNewTrees { get; set; }

    /// <summary>The tree-name prefixes an all-trees schedule leaves out.</summary>
    [Id(8)]
    public List<string> ExcludedTreePrefixes { get; set; } = [];

    /// <summary>Builds the persisted form of <paramref name="request"/>.</summary>
    /// <param name="request">The request.</param>
    /// <returns>The persisted form.</returns>
    public static BackupSetScheduleConfig From(LatticeBackupSetScheduleRequest request) => new()
    {
        Name = request.Name,
        Membership = request.Membership,
        Scopes = request.Scopes.ToList(),
        Interval = request.Interval,
        RetentionKeepLast = request.RetentionKeepLast,
        RetentionMaxAge = request.RetentionMaxAge,
        CrossTreeConsistent = request.CrossTreeConsistent,
        IncludeNewTrees = request.IncludeNewTrees,
        ExcludedTreePrefixes = request.ExcludedTreePrefixes.ToList(),
    };

    /// <summary>Rebuilds the request this configuration was persisted from.</summary>
    /// <returns>The request.</returns>
    public LatticeBackupSetScheduleRequest ToRequest() => new(
        Name,
        Membership,
        Membership == BackupSetMembershipMode.Explicit ? Scopes : null,
        Interval,
        RetentionKeepLast,
        RetentionMaxAge,
        CrossTreeConsistent,
        IncludeNewTrees,
        Membership == BackupSetMembershipMode.AllTrees ? ExcludedTreePrefixes : null);
}
