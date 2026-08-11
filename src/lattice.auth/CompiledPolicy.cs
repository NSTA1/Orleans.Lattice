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

    private CompiledPolicy(IReadOnlyDictionary<string, CompiledTree> trees, int distinctSubjectCount)
    {
        _trees = trees;
        DistinctSubjectCount = distinctSubjectCount;
        AllTrees = trees.TryGetValue(LatticeScope.ClusterWideTreeId, out var allTrees) ? allTrees : null;
    }

    /// <summary>The empty snapshot: no rules for any tree. Used before the first compile.</summary>
    public static CompiledPolicy Empty { get; } =
        new(new Dictionary<string, CompiledTree>(0, StringComparer.Ordinal), 0);

    /// <summary>
    /// The compiled all-trees (<c>Tree:*</c>) bucket - the rules scoped over
    /// <see cref="LatticeScope.ClusterWideTreeId"/> - resolved once at compile
    /// time so the decision engine's all-trees tier never does a per-evaluate
    /// dictionary lookup by the <c>"*"</c> string. <c>null</c> when no rule is
    /// scoped cluster-wide. Consulted by <see cref="PolicyEvaluator"/> only when
    /// <see cref="LatticeAuthOptions.AllTreesGrantsEnabled"/> is set and the target
    /// tree is neither the reserved authorization namespace nor the sentinel id
    /// itself.
    /// </summary>
    public CompiledTree? AllTrees { get; }

    /// <summary>Attempts to get the compiled rules governing <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <param name="tree">The compiled tree when present; otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when the tree has any rules.</returns>
    public bool TryGetTree(string treeId, out CompiledTree? tree) => _trees.TryGetValue(treeId, out tree);

    /// <summary>The number of governed trees in the snapshot. Exposed for tests.</summary>
    internal int TreeCount => _trees.Count;

    /// <summary>
    /// The number of <b>distinct</b> subjects (users and groups) any rule in the
    /// snapshot references - the count of members for which an authorization
    /// policy is configured. A user and a group that happen to share an id count
    /// separately; the same subject referenced by many rules or across many trees
    /// counts once. Backs the compiled-snapshot <c>subjects</c> observable gauge.
    /// </summary>
    public int DistinctSubjectCount { get; }

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
        var distinctSubjects = new HashSet<(LatticeSubjectSelectorKind Kind, string Id)>();
        foreach (var rule in rules)
        {
            distinctSubjects.Add((rule.Subject.Kind, rule.Subject.Id));

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

        return new CompiledPolicy(trees, distinctSubjects.Count);
    }
}
