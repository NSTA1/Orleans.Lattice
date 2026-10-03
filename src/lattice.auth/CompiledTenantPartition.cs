using System.Collections.Frozen;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The tenant partition of a compiled policy snapshot: every tenant-tier
/// (<see cref="LatticeTenantRuleIds.Prefix"/>) rule, indexed by the tree it governs
/// and, for tenant-wide rules, by owning tenant. Built by
/// <see cref="CompiledPolicy.Compile(IEnumerable{LatticeAuthorizationRule}, bool)"/>
/// only while the tenant layer is active (<see cref="ITenantRuleLayer.IsActive"/>),
/// so a cluster without the layer never pays for it.
/// </summary>
/// <remarks>
/// <para>
/// Each rule is re-validated with <see cref="TenantRuleConfinement.CheckTenantRule"/>
/// at build time and dropped when it does not conform. The policy store already
/// refuses such a rule on its write path; this is defence in depth against a rule
/// that reached the policy tree without passing through the store (a restore or a
/// replicated write), so a malformed tenant rule can never govern a tree outside its
/// tenant.
/// </para>
/// <para>
/// Lookups are allocation-free: the tree bucket is keyed by the full tree id the
/// request already carries, and the tenant-wide bucket is keyed by tenant id and
/// probed with a <see cref="ReadOnlySpan{T}"/> alternate lookup over the tenant
/// slice of the tree id, so no tenant id string is materialised per decision.
/// </para>
/// <para>In-process snapshot state: never serialized, never crosses a grain boundary.</para>
/// </remarks>
internal sealed class CompiledTenantPartition
{
    private readonly FrozenDictionary<string, CompiledTree> _trees;
    private readonly FrozenDictionary<string, CompiledTree>.AlternateLookup<ReadOnlySpan<char>> _tenantWide;

    private CompiledTenantPartition(
        FrozenDictionary<string, CompiledTree> trees,
        FrozenDictionary<string, CompiledTree> tenantWide)
    {
        _trees = trees;
        TenantWideCount = tenantWide.Count;
        _tenantWide = tenantWide.GetAlternateLookup<ReadOnlySpan<char>>();
    }

    /// <summary>The number of trees carrying tree-scoped tenant rules. Exposed for tests.</summary>
    internal int TreeCount => _trees.Count;

    /// <summary>The number of tenants carrying tenant-wide rules. Exposed for tests.</summary>
    internal int TenantWideCount { get; }

    /// <summary>
    /// Resolves the tenant-layer buckets that govern <paramref name="treeId"/>: the
    /// tree's own tenant rules and its owning tenant's tenant-wide rules. Returns
    /// <c>false</c> - so the evaluator takes the operator-only path unchanged - when
    /// the tree is not a tenant-layer tree (see
    /// <see cref="TenantRuleConfinement.TryGetTenantLayerTree"/>) or neither bucket
    /// exists. Allocation-free.
    /// </summary>
    /// <param name="treeId">The requested tree id.</param>
    /// <param name="tree">The tree's tenant rules, or <c>null</c>.</param>
    /// <param name="tenantWide">The owning tenant's tenant-wide rules, or <c>null</c>.</param>
    /// <returns><c>true</c> when at least one tenant bucket governs the tree.</returns>
    public bool TryGetBuckets(string treeId, out CompiledTree? tree, out CompiledTree? tenantWide)
    {
        tree = null;
        tenantWide = null;
        if (!TenantRuleConfinement.TryGetTenantLayerTree(treeId, out var tenant))
        {
            return false;
        }

        _trees.TryGetValue(treeId, out tree);
        _tenantWide.TryGetValue(tenant, out tenantWide);
        return tree is not null || tenantWide is not null;
    }

    /// <summary>
    /// Builds the partition from the tenant-tier rules, dropping any that do not
    /// conform to <see cref="TenantRuleConfinement.CheckTenantRule"/>. Returns
    /// <c>null</c> when no rule survives.
    /// </summary>
    /// <param name="rules">The tenant-tier rules. Must not be <c>null</c>.</param>
    /// <param name="distinctSubjects">Receives the subject of every rule kept.</param>
    /// <returns>The partition, or <c>null</c> when it would be empty.</returns>
    public static CompiledTenantPartition? Build(
        IReadOnlyList<LatticeAuthorizationRule> rules,
        HashSet<(LatticeSubjectSelectorKind Kind, string Id)> distinctSubjects)
    {
        ArgumentNullException.ThrowIfNull(rules);
        ArgumentNullException.ThrowIfNull(distinctSubjects);

        Dictionary<string, List<LatticeAuthorizationRule>>? byTree = null;
        Dictionary<string, List<LatticeAuthorizationRule>>? byTenant = null;
        foreach (var rule in rules)
        {
            if (TenantRuleConfinement.CheckTenantRule(rule, out var tenant) is not null)
            {
                continue;
            }

            distinctSubjects.Add((rule.Subject.Kind, rule.Subject.Id));
            if (rule.Scope.IsTenantWide())
            {
                Add(byTenant ??= new(StringComparer.Ordinal), tenant.Value, rule);
            }
            else
            {
                Add(byTree ??= new(StringComparer.Ordinal), rule.Scope.TreeId, rule);
            }
        }

        if (byTree is null && byTenant is null)
        {
            return null;
        }

        return new CompiledTenantPartition(Freeze(byTree), Freeze(byTenant));
    }

    private static void Add(Dictionary<string, List<LatticeAuthorizationRule>> map, string key, LatticeAuthorizationRule rule)
    {
        if (!map.TryGetValue(key, out var list))
        {
            list = new List<LatticeAuthorizationRule>();
            map[key] = list;
        }

        list.Add(rule);
    }

    private static FrozenDictionary<string, CompiledTree> Freeze(Dictionary<string, List<LatticeAuthorizationRule>>? map)
    {
        if (map is null)
        {
            return FrozenDictionary.ToFrozenDictionary(Array.Empty<KeyValuePair<string, CompiledTree>>(), StringComparer.Ordinal);
        }

        var compiled = new Dictionary<string, CompiledTree>(map.Count, StringComparer.Ordinal);
        foreach (var (key, list) in map)
        {
            compiled[key] = CompiledTree.Build(list);
        }

        return compiled.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
