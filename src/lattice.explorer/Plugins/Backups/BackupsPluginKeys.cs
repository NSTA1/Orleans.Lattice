namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The Backups plugin's own key vocabulary: the stable id its access decisions
/// are filed under, and the scope names for the per-tree decisions the backup
/// capability probe produces.
/// <para>
/// The plugin owns these strings; nothing outside the Backups feature needs to
/// know them, which is what lets a decision be keyed without a shared record.
/// </para>
/// </summary>
public static class BackupsPluginKeys
{
    /// <summary>The stable plugin id the Backups area is registered and keyed under.</summary>
    public const string PluginId = "orleans.lattice.backups";

    /// <summary>
    /// The scope suffix marking a per-tree capture decision. Appended to the tree
    /// id, so the key reads <c>{treeId}/capture</c>.
    /// </summary>
    public const string CaptureSuffix = "/capture";

    /// <summary>The scope suffix marking a per-tree incremental-capture decision.</summary>
    public const string CaptureIncrementalSuffix = "/capture-incremental";

    /// <summary>The scope suffix marking a per-tree restore decision.</summary>
    public const string RestoreSuffix = "/restore";

    /// <summary>The scope suffix marking a per-tree delete decision.</summary>
    public const string DeleteSuffix = "/delete";

    /// <summary>
    /// The scope a tree's list / read decision is filed under. This is the bare
    /// tree id, so a caller that only wants "may I see this scope at all" needs
    /// no suffix vocabulary.
    /// </summary>
    /// <param name="treeId">The tree id. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <see langword="null"/>.</exception>
    public static string ListScope(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return treeId;
    }

    /// <summary>The scope a tree's capture decision is filed under.</summary>
    /// <param name="treeId">The tree id. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <see langword="null"/>.</exception>
    public static string CaptureScope(string treeId) => Compose(treeId, CaptureSuffix);

    /// <summary>The scope a tree's incremental-capture decision is filed under.</summary>
    /// <param name="treeId">The tree id. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <see langword="null"/>.</exception>
    public static string CaptureIncrementalScope(string treeId) => Compose(treeId, CaptureIncrementalSuffix);

    /// <summary>The scope a tree's restore decision is filed under.</summary>
    /// <param name="treeId">The tree id. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <see langword="null"/>.</exception>
    public static string RestoreScope(string treeId) => Compose(treeId, RestoreSuffix);

    /// <summary>The scope a tree's delete decision is filed under.</summary>
    /// <param name="treeId">The tree id. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <see langword="null"/>.</exception>
    public static string DeleteScope(string treeId) => Compose(treeId, DeleteSuffix);

    /// <summary>
    /// Whether <paramref name="scope"/> names a tree's list / read decision
    /// rather than one of its operation decisions. A list scope is the bare tree
    /// id, so this is "carries none of the operation suffixes".
    /// <para>
    /// This is what lets the plugin gate re-derive "at least one tree still
    /// grants me list access" from the keyed store at probe time, instead of
    /// remembering that a scope once did.
    /// </para>
    /// </summary>
    /// <param name="scope">The scope name to classify. Must not be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the scope is a tree's list scope.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <see langword="null"/>.</exception>
    public static bool IsListScope(string scope)
    {
        ArgumentNullException.ThrowIfNull(scope);

        return !scope.EndsWith(CaptureSuffix, StringComparison.Ordinal)
            && !scope.EndsWith(CaptureIncrementalSuffix, StringComparison.Ordinal)
            && !scope.EndsWith(RestoreSuffix, StringComparison.Ordinal)
            && !scope.EndsWith(DeleteSuffix, StringComparison.Ordinal);
    }

    private static string Compose(string treeId, string suffix)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return string.Concat(treeId, suffix);
    }
}
