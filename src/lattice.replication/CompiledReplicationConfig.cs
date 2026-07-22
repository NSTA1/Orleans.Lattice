namespace Orleans.Lattice.Replication;

/// <summary>
/// An immutable, atomically-swappable projection of the whole runtime
/// replication configuration: a frozen map of target tree id to
/// <see cref="ReplicationConfigProjection"/>, compiled from the live
/// <see cref="LatticeReplicationConfigEntry"/> set read out of the
/// <see cref="LatticeSystemTreeNames.ReplicationConfig"/> tree. The dynamic
/// <see cref="SnapshotReplicatedTreeMembership"/> and
/// <see cref="SnapshotLatticeMergeModeResolver"/> read a single reference to an
/// instance of this type on the commit hot path, so a config change is applied
/// by swapping the reference rather than by touching the reader.
/// </summary>
internal sealed class CompiledReplicationConfig
{
    /// <summary>The empty snapshot: no tree configured.</summary>
    public static readonly CompiledReplicationConfig Empty =
        new(new Dictionary<string, ReplicationConfigProjection>(0, StringComparer.Ordinal), Array.Empty<string>());

    private readonly IReadOnlyDictionary<string, ReplicationConfigProjection> _byTree;
    private readonly string[] _enabledTrees;

    private CompiledReplicationConfig(
        IReadOnlyDictionary<string, ReplicationConfigProjection> byTree,
        string[] enabledTrees)
    {
        _byTree = byTree;
        _enabledTrees = enabledTrees;
    }

    /// <summary>The number of target trees the snapshot carries a projection for.</summary>
    public int TreeCount => _byTree.Count;

    /// <summary>
    /// The ids of every tree whose enablement flag is currently set. Backs the
    /// membership union; the returned array is owned by the snapshot and must
    /// not be mutated.
    /// </summary>
    public IReadOnlyList<string> EnabledTrees => _enabledTrees;

    /// <summary>
    /// Looks up the projection for <paramref name="treeId"/>. Returns
    /// <see langword="true"/> and sets <paramref name="projection"/> when the
    /// tree is configured; otherwise returns <see langword="false"/>.
    /// </summary>
    /// <param name="treeId">The target tree id to look up.</param>
    /// <param name="projection">The projection when configured; otherwise <see langword="default"/>.</param>
    /// <returns><see langword="true"/> when the tree has a projection.</returns>
    public bool TryGetTree(string treeId, out ReplicationConfigProjection projection)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _byTree.TryGetValue(treeId, out projection);
    }

    /// <summary>
    /// Compiles a snapshot from the live per-tree entries read out of the config
    /// tree. Each entry is distilled into a <see cref="ReplicationConfigProjection"/>
    /// via <see cref="LatticeReplicationConfigEntry.IsEnabled"/>,
    /// <see cref="LatticeReplicationConfigEntry.TryGetMode"/>, and
    /// <see cref="LatticeReplicationConfigEntry.HasAmbiguousMode"/>.
    /// </summary>
    /// <param name="entries">The live per-tree config entries, keyed by target tree id.</param>
    /// <returns>The compiled, immutable snapshot.</returns>
    public static CompiledReplicationConfig Compile(
        IReadOnlyDictionary<string, LatticeReplicationConfigEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return Empty;
        }

        var byTree = new Dictionary<string, ReplicationConfigProjection>(entries.Count, StringComparer.Ordinal);
        var enabled = new List<string>(entries.Count);
        foreach (var (treeId, entry) in entries)
        {
            if (entry is null)
            {
                continue;
            }

            var ambiguous = entry.HasAmbiguousMode;
            LatticeMergeMode? mode = !ambiguous && entry.TryGetMode(out var m) ? m : null;
            var isEnabled = entry.IsEnabled;
            byTree[treeId] = new ReplicationConfigProjection(isEnabled, mode, ambiguous);
            if (isEnabled)
            {
                enabled.Add(treeId);
            }
        }

        return new CompiledReplicationConfig(byTree, enabled.ToArray());
    }
}
