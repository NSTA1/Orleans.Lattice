namespace Orleans.Lattice.Backup;

/// <summary>
/// A request to register (or update) a named, recurring backup-set schedule:
/// every <see cref="Interval"/> one backup set is captured over the schedule's
/// member trees - cross-tree consistent by default - and the schedule's own
/// retention prunes whole sets. Build one with <see cref="ForTrees"/> (named
/// trees) or <see cref="ForAllTrees"/> (every tree in the tenant).
/// </summary>
/// <remarks>
/// A scheduled set is the right unit when trees are written together - by
/// cross-tree atomic writes, or by an application that keeps rows in several
/// trees referring to each other - and must be restored to one coherent point in
/// time. Per-tree schedules capture each tree at its own instant, so restoring the
/// latest backup of every tree mixes points in time.
/// </remarks>
[GenerateSerializer]
[Alias(BackupTypeAliases.LatticeBackupSetScheduleRequest)]
[Immutable]
public sealed record LatticeBackupSetScheduleRequest
{
    /// <summary>The longest schedule name accepted.</summary>
    public const int MaxNameLength = 128;

    /// <summary>Initializes a new <see cref="LatticeBackupSetScheduleRequest"/>.</summary>
    /// <param name="name">
    /// The schedule's name, unique within the caller's tenant. 1 to
    /// <see cref="MaxNameLength"/> characters drawn from ASCII letters, digits,
    /// <c>-</c>, <c>_</c> and <c>.</c>.
    /// </param>
    /// <param name="membership">How the set chooses its trees.</param>
    /// <param name="scopes">
    /// The scopes to capture, one per distinct tree, for <see cref="BackupSetMembershipMode.Explicit"/>;
    /// must be <c>null</c> or empty for <see cref="BackupSetMembershipMode.AllTrees"/>.
    /// </param>
    /// <param name="interval">The cadence between scheduled captures. Must be strictly positive; clamped up to the scheduler minimum.</param>
    /// <param name="retentionKeepLast">Keep the newest this many sets; <c>null</c> for no count bound. At least 1 when set.</param>
    /// <param name="retentionMaxAge">Keep every set captured within this window; <c>null</c> for no age bound. Strictly positive when set.</param>
    /// <param name="crossTreeConsistent">Whether each set is captured at a single cross-tree fence. Defaults to <c>true</c>.</param>
    /// <param name="includeNewTrees">
    /// For <see cref="BackupSetMembershipMode.AllTrees"/>: <c>true</c> to re-resolve
    /// the trees at every run, so trees created later are captured; <c>false</c> to
    /// resolve them once, at registration, and pin them. Ignored for
    /// <see cref="BackupSetMembershipMode.Explicit"/>.
    /// </param>
    /// <param name="excludedTreePrefixes">
    /// For <see cref="BackupSetMembershipMode.AllTrees"/>: tree-name prefixes
    /// (tenant-local) whose trees the set leaves out, for example scratch trees.
    /// Must be <c>null</c> or empty for <see cref="BackupSetMembershipMode.Explicit"/>.
    /// </param>
    /// <exception cref="ArgumentException">A name, scope list, retention or exclusion argument is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is not strictly positive, or a retention bound is out of range.</exception>
    public LatticeBackupSetScheduleRequest(
        string name,
        BackupSetMembershipMode membership,
        IReadOnlyList<BackupScopeSelector>? scopes,
        TimeSpan interval,
        int? retentionKeepLast,
        TimeSpan? retentionMaxAge,
        bool crossTreeConsistent = true,
        bool includeNewTrees = true,
        IReadOnlyList<string>? excludedTreePrefixes = null)
    {
        ValidateName(name);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(interval.Ticks, nameof(interval));
        if (!Enum.IsDefined(membership))
        {
            throw new ArgumentOutOfRangeException(nameof(membership), membership, "Unknown backup-set membership mode.");
        }

        if (retentionKeepLast is { } keepLast && keepLast < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(retentionKeepLast), keepLast, "Keep-last retention must keep at least one set.");
        }

        if (retentionMaxAge is { } maxAge && maxAge <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retentionMaxAge), maxAge, "Max-age retention must be strictly positive.");
        }

        if (retentionKeepLast is null && retentionMaxAge is null)
        {
            throw new ArgumentException(
                "A backup-set schedule needs a retention bound: set retentionKeepLast, retentionMaxAge, or both. "
                + "The newest set is always kept.",
                nameof(retentionKeepLast));
        }

        var scopeList = scopes ?? Array.Empty<BackupScopeSelector>();
        var prefixList = excludedTreePrefixes ?? Array.Empty<string>();
        if (membership == BackupSetMembershipMode.Explicit)
        {
            if (scopeList.Count == 0)
            {
                throw new ArgumentException("A backup-set schedule of named trees must name at least one scope.", nameof(scopes));
            }

            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var scope in scopeList)
            {
                ArgumentNullException.ThrowIfNull(scope, nameof(scopes));
                if (!seen.Add(scope.TreeId))
                {
                    throw new ArgumentException(
                        $"A backup-set schedule cannot contain two scopes for the same tree ('{scope.TreeId}').",
                        nameof(scopes));
                }
            }

            if (prefixList.Count > 0)
            {
                throw new ArgumentException(
                    "Excluded tree prefixes apply only to a backup-set schedule of all trees.", nameof(excludedTreePrefixes));
            }
        }
        else
        {
            if (scopeList.Count > 0)
            {
                throw new ArgumentException(
                    "A backup-set schedule of all trees names no scopes; it resolves its trees itself.", nameof(scopes));
            }

            foreach (var prefix in prefixList)
            {
                ArgumentException.ThrowIfNullOrEmpty(prefix, nameof(excludedTreePrefixes));
            }
        }

        Name = name;
        Membership = membership;
        Scopes = scopeList;
        Interval = interval;
        RetentionKeepLast = retentionKeepLast;
        RetentionMaxAge = retentionMaxAge;
        CrossTreeConsistent = crossTreeConsistent;
        IncludeNewTrees = membership == BackupSetMembershipMode.AllTrees && includeNewTrees;
        ExcludedTreePrefixes = prefixList;
    }

    /// <summary>The schedule's name, unique within the caller's tenant.</summary>
    [Id(0)]
    public string Name { get; init; }

    /// <summary>How the set chooses its trees.</summary>
    [Id(1)]
    public BackupSetMembershipMode Membership { get; init; }

    /// <summary>The scopes captured by an <see cref="BackupSetMembershipMode.Explicit"/> schedule; empty otherwise.</summary>
    [Id(2)]
    public IReadOnlyList<BackupScopeSelector> Scopes { get; init; }

    /// <summary>The cadence between scheduled captures.</summary>
    [Id(3)]
    public TimeSpan Interval { get; init; }

    /// <summary>Keep the newest this many sets, or <c>null</c> for no count bound.</summary>
    [Id(4)]
    public int? RetentionKeepLast { get; init; }

    /// <summary>Keep every set captured within this window, or <c>null</c> for no age bound.</summary>
    [Id(5)]
    public TimeSpan? RetentionMaxAge { get; init; }

    /// <summary>Whether each set is captured at a single cross-tree fence.</summary>
    [Id(6)]
    public bool CrossTreeConsistent { get; init; }

    /// <summary>
    /// Whether an <see cref="BackupSetMembershipMode.AllTrees"/> schedule
    /// re-resolves its trees at every run (<c>true</c>) or pinned them at
    /// registration (<c>false</c>). Always <c>false</c> for an
    /// <see cref="BackupSetMembershipMode.Explicit"/> schedule.
    /// </summary>
    [Id(7)]
    public bool IncludeNewTrees { get; init; }

    /// <summary>The tree-name prefixes an <see cref="BackupSetMembershipMode.AllTrees"/> schedule leaves out.</summary>
    [Id(8)]
    public IReadOnlyList<string> ExcludedTreePrefixes { get; init; }

    /// <summary>
    /// Builds a schedule over the named scopes, one per distinct tree.
    /// </summary>
    /// <param name="name">The schedule's name.</param>
    /// <param name="scopes">The scopes to capture.</param>
    /// <param name="interval">The cadence between captures.</param>
    /// <param name="retentionKeepLast">Keep the newest this many sets, or <c>null</c>.</param>
    /// <param name="retentionMaxAge">Keep every set captured within this window, or <c>null</c>.</param>
    /// <param name="crossTreeConsistent">Whether each set is captured at a single cross-tree fence.</param>
    /// <returns>The request.</returns>
    public static LatticeBackupSetScheduleRequest ForTrees(
        string name,
        IReadOnlyList<BackupScopeSelector> scopes,
        TimeSpan interval,
        int? retentionKeepLast,
        TimeSpan? retentionMaxAge,
        bool crossTreeConsistent = true)
    {
        ArgumentNullException.ThrowIfNull(scopes);
        return new LatticeBackupSetScheduleRequest(
            name, BackupSetMembershipMode.Explicit, scopes, interval, retentionKeepLast, retentionMaxAge, crossTreeConsistent);
    }

    /// <summary>
    /// Builds a schedule over every user tree in the caller's tenant.
    /// </summary>
    /// <param name="name">The schedule's name.</param>
    /// <param name="interval">The cadence between captures.</param>
    /// <param name="includeNewTrees"><c>true</c> to capture trees created after registration too; <c>false</c> to pin the trees that exist now.</param>
    /// <param name="retentionKeepLast">Keep the newest this many sets, or <c>null</c>.</param>
    /// <param name="retentionMaxAge">Keep every set captured within this window, or <c>null</c>.</param>
    /// <param name="excludedTreePrefixes">Tree-name prefixes to leave out, or <c>null</c>.</param>
    /// <param name="crossTreeConsistent">Whether each set is captured at a single cross-tree fence.</param>
    /// <returns>The request.</returns>
    public static LatticeBackupSetScheduleRequest ForAllTrees(
        string name,
        TimeSpan interval,
        bool includeNewTrees,
        int? retentionKeepLast,
        TimeSpan? retentionMaxAge,
        IReadOnlyList<string>? excludedTreePrefixes = null,
        bool crossTreeConsistent = true) =>
        new(
            name,
            BackupSetMembershipMode.AllTrees,
            scopes: null,
            interval,
            retentionKeepLast,
            retentionMaxAge,
            crossTreeConsistent,
            includeNewTrees,
            excludedTreePrefixes);

    /// <summary>
    /// Validates a backup-set schedule name.
    /// </summary>
    /// <param name="name">The name to validate.</param>
    /// <exception cref="ArgumentException">The name is null, empty, too long, or carries a character outside the allowed set.</exception>
    public static void ValidateName(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        if (name.Length > MaxNameLength)
        {
            throw new ArgumentException(
                $"A backup-set schedule name must be at most {MaxNameLength} characters.", nameof(name));
        }

        foreach (var c in name)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.'))
            {
                throw new ArgumentException(
                    "A backup-set schedule name may contain only ASCII letters, digits, '-', '_' and '.'.", nameof(name));
            }
        }
    }
}
