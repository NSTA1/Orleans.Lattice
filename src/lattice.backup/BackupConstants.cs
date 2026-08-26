namespace Orleans.Lattice.Backup;

/// <summary>
/// Well-known names for the reserved, dogfooded <c>ILattice</c> trees that will
/// back the backup catalog and manifest store, plus the guard that keeps that
/// namespace from being shadowed by an application tree. Like the sibling
/// membership (<c>sys-membership-*</c>) and authorization (<c>sys-auth-*</c>)
/// packages, the backup catalog trees are ordinary user-addressable trees that
/// carry the core <c>sys-</c> system-data prefix, so they self-register, stay
/// durable and individually auditable, yet are hidden from the default
/// cluster-state tree catalog surfaced through the state API.
/// <para>
/// This scaffolding release reserves the <see cref="ReservedTreePrefix"/> so the
/// catalog / manifest release can create its trees inside a collision-free
/// namespace.
/// </para>
/// </summary>
internal static class BackupConstants
{
    /// <summary>
    /// The shared prefix identifying every backup-owned reserved tree. A
    /// governed tree id colliding with this prefix is rejected by
    /// <see cref="ThrowIfReservedTree"/> so an application tree can never shadow
    /// the backup catalog. Nested under the core <c>sys-</c> system-data prefix,
    /// so it inherits the state-catalog hiding behaviour without a core change.
    /// </summary>
    internal const string ReservedTreePrefix = "sys-backup-";

    /// <summary>
    /// The reserved, dogfooded <c>ILattice</c> tree the default in-cluster sink
    /// stores backup artifacts and manifests into. Keyed
    /// <c>m\u001f{backupId}</c> for manifests and
    /// <c>a\u001f{artifactId}\u001f{chunkIndex}</c> for streamed artifact chunks.
    /// Nested under the reserved prefix so it inherits state-catalog hiding.
    /// </summary>
    internal const string StoreTree = "sys-backup-store";

    /// <summary>
    /// The reserved, dogfooded <c>ILattice</c> tree that indexes backup manifests
    /// for enumeration and audit, keyed by backup id. This is the in-cluster
    /// catalog the backup API enumerates; a durable per-key history view is
    /// created over it so the catalog stays auditable.
    /// </summary>
    internal const string CatalogTree = "sys-backup-catalog";

    /// <summary>Durable per-key history view name for <see cref="CatalogTree"/>.</summary>
    internal const string CatalogHistoryView = "sys-backup-catalog-history";

    /// <summary>
    /// Backup-catalog index materialised view name for <see cref="CatalogTree"/>.
    /// Re-keys each catalogued backup so the view scans newest-first with set
    /// members contiguous, backing the filtered, created-descending, paged catalog
    /// listing surfaced by the backup control API.
    /// </summary>
    internal const string CatalogIndexView = "sys-backup-catalog-index";

    /// <summary>
    /// The reserved, dogfooded <c>ILattice</c> tree that persists the latest
    /// per-backup health-verification report and the per-backup health-monitor
    /// configuration, so the periodic monitor and the management UI share one
    /// verification result. Keyed <c>r\u001f{backupId}</c> for reports and
    /// <c>c\u001f{backupId}</c> for configuration. Nested under the reserved prefix
    /// so it inherits state-catalog hiding.
    /// </summary>
    internal const string HealthTree = "sys-backup-health";

    /// <summary>Field separator used inside composite sink / catalog keys.</summary>
    internal const char KeySeparator = '\u001f';

    /// <summary>Key discriminator for a manifest row in the in-cluster sink store.</summary>
    internal const char ManifestKeyPrefix = 'm';

    /// <summary>Key discriminator for an artifact chunk row in the in-cluster sink store.</summary>
    internal const char ArtifactKeyPrefix = 'a';

    /// <summary>Key discriminator for a health-report row in the in-cluster health tree.</summary>
    internal const char HealthReportKeyPrefix = 'r';

    /// <summary>Key discriminator for a health-config row in the in-cluster health tree.</summary>
    internal const char HealthConfigKeyPrefix = 'c';

    /// <summary>Enumerates the reserved backing tree names owned by the backup package.</summary>
    internal static IReadOnlyList<string> AllTrees { get; } = new[] { StoreTree, CatalogTree, HealthTree };

    /// <summary>
    /// The exclusive upper bound of every key sharing <paramref name="prefix"/>:
    /// the prefix with its last code unit below <see cref="char.MaxValue"/>
    /// incremented and any trailing <see cref="char.MaxValue"/> units dropped.
    /// Used to bound a prefix scan of the in-cluster sink / catalog trees.
    /// Returns <see langword="null"/> when no finite upper bound exists - an
    /// empty prefix, or one consisting solely of <see cref="char.MaxValue"/>
    /// units - meaning the scan is unbounded above.
    /// </summary>
    /// <param name="prefix">The inclusive key prefix.</param>
    /// <returns>
    /// The exclusive upper bound of the prefix range, or <see langword="null"/>
    /// when the range has no finite upper bound.
    /// </returns>
    internal static string? PrefixUpperBound(string prefix) =>
        LatticeKeyRange.PrefixUpperBound(prefix);

    /// <summary>
    /// Rejects a tree id that collides with the reserved <c>sys-backup-*</c>
    /// namespace, mirroring the guard the authorization and membership packages
    /// enforce on their own reserved namespaces.
    /// </summary>
    /// <param name="treeId">The candidate tree id.</param>
    /// <param name="paramName">The caller's parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> starts with <see cref="ReservedTreePrefix"/>.</exception>
    internal static void ThrowIfReservedTree(string treeId, string paramName)
    {
        if (treeId.StartsWith(ReservedTreePrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Tree ID '{treeId}' is reserved: names starting with '{ReservedTreePrefix}' " +
                "are reserved for the Orleans.Lattice.Backup catalog. Choose a tree ID that " +
                $"does not start with '{ReservedTreePrefix}'.",
                paramName);
        }
    }
}
