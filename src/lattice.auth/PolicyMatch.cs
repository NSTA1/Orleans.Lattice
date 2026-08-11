namespace Orleans.Lattice.Auth;

/// <summary>
/// The outcome of resolving a single point request against a
/// <see cref="CompiledTree"/>: whether any rule matched, and if so the winning
/// rule's effect, id, and scope (used to build a human-readable decision reason).
/// A default value (<see cref="Matched"/> is <c>false</c>) means no rule matched
/// and the caller applies its configured default effect.
/// </summary>
/// <remarks>In-process value only; carries no Orleans serialization attributes.</remarks>
internal readonly record struct PolicyMatch
{
    /// <summary>Initializes a matched result.</summary>
    /// <param name="effect">The winning rule's effect.</param>
    /// <param name="ruleId">The winning rule's id.</param>
    /// <param name="scopeKind">The scope tier the winning rule was found at.</param>
    /// <param name="scopeValue">The exact key or prefix of the winning rule, or <c>null</c> for a tree-wide rule.</param>
    /// <param name="allTrees">
    /// <c>true</c> when the winning rule was resolved from the all-trees
    /// (<c>Tree:*</c>) tier rather than the requested tree's own bucket, so a
    /// decision reason can truthfully render its scope as "all trees" instead of
    /// "tree". See <see cref="LatticeScope.ClusterWideTreeId"/>.
    /// </param>
    public PolicyMatch(LatticeEffect effect, string ruleId, LatticeScopeKind scopeKind, string? scopeValue, bool allTrees = false)
    {
        Matched = true;
        Effect = effect;
        RuleId = ruleId;
        ScopeKind = scopeKind;
        ScopeValue = scopeValue;
        AllTrees = allTrees;
    }

    /// <summary><c>true</c> when a rule matched the request.</summary>
    public bool Matched { get; }

    /// <summary>The winning rule's effect. Meaningful only when <see cref="Matched"/> is <c>true</c>.</summary>
    public LatticeEffect Effect { get; }

    /// <summary>The winning rule's id. Meaningful only when <see cref="Matched"/> is <c>true</c>.</summary>
    public string? RuleId { get; }

    /// <summary>The scope tier the winning rule was found at.</summary>
    public LatticeScopeKind ScopeKind { get; }

    /// <summary>The exact key or prefix of the winning rule, or <c>null</c> for a tree-wide rule.</summary>
    public string? ScopeValue { get; }

    /// <summary>
    /// <c>true</c> when the winning rule was resolved from the all-trees
    /// (<c>Tree:*</c>) tier - a cluster-wide grant that governs every non-system
    /// tree - rather than the requested tree's own bucket. Lets a decision reason
    /// render "all trees" instead of "tree". Meaningful only when
    /// <see cref="Matched"/> is <c>true</c>.
    /// </summary>
    public bool AllTrees { get; }
}
