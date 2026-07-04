namespace Orleans.Lattice.Auth;

/// <summary>
/// An immutable, in-memory compilation of the full authorization rule set, keyed
/// by governed tree id. Built by <see cref="Compile"/> from every rule in the
/// policy store and swapped atomically by the snapshot maintainer whenever the
/// policy tree changes. The decision engine reads a snapshot without touching
/// storage, so a warm authorization decision is a pure in-memory lookup.
/// </summary>
/// <remarks>
/// This type is in-process singleton state. It is never serialized and never
/// crosses a grain boundary, so it carries no Orleans serialization attributes.
/// </remarks>
internal sealed class CompiledPolicy
{
    private readonly IReadOnlyDictionary<string, CompiledTree> _trees;

    private CompiledPolicy(IReadOnlyDictionary<string, CompiledTree> trees) => _trees = trees;

    /// <summary>The empty snapshot: no rules for any tree. Used before the first compile.</summary>
    public static CompiledPolicy Empty { get; } =
        new(new Dictionary<string, CompiledTree>(0, StringComparer.Ordinal));

    /// <summary>Attempts to get the compiled rules governing <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <param name="tree">The compiled tree when present; otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when the tree has any rules.</returns>
    public bool TryGetTree(string treeId, out CompiledTree? tree) => _trees.TryGetValue(treeId, out tree);

    /// <summary>The number of governed trees in the snapshot. Exposed for tests.</summary>
    internal int TreeCount => _trees.Count;

    /// <summary>
    /// Compiles a full rule set into an immutable snapshot. Rules are grouped by
    /// their governed tree id and each group is compiled into a
    /// <see cref="CompiledTree"/>.
    /// </summary>
    /// <param name="rules">The full authorization rule set. Must not be <c>null</c>.</param>
    /// <returns>The compiled snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="rules"/> is <c>null</c>.</exception>
    public static CompiledPolicy Compile(IEnumerable<LatticeAuthorizationRule> rules)
    {
        ArgumentNullException.ThrowIfNull(rules);

        var byTree = new Dictionary<string, List<LatticeAuthorizationRule>>(StringComparer.Ordinal);
        foreach (var rule in rules)
        {
            var treeId = rule.Scope.TreeId;
            if (!byTree.TryGetValue(treeId, out var list))
            {
                list = new List<LatticeAuthorizationRule>();
                byTree[treeId] = list;
            }

            list.Add(rule);
        }

        if (byTree.Count == 0)
        {
            return Empty;
        }

        var trees = new Dictionary<string, CompiledTree>(byTree.Count, StringComparer.Ordinal);
        foreach (var (treeId, treeRules) in byTree)
        {
            trees[treeId] = CompiledTree.Build(treeRules);
        }

        return new CompiledPolicy(trees);
    }
}
