using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>
/// How the Access area names operations, scopes and subjects, which operations
/// are scopeless cluster-wide capabilities, and the presentation-only precedence
/// order rules are listed in. None of it is a second opinion on a verdict: the
/// server's answer is always rendered as it arrives.
/// </summary>
internal static class AccessRuleFormat
{
    /// <summary>
    /// The capabilities that are checked against the cluster-wide scope itself and
    /// so can only be granted by a rule at <see cref="LatticeScope.ClusterWide"/>.
    /// </summary>
    public const LatticeOperation ScopelessOperations = LatticeOperation.AppInstall | LatticeOperation.Telemetry;

    /// <summary>Every operation the checklist offers, in the order and groups it offers them.</summary>
    public static IReadOnlyList<AccessOperationOption> Operations { get; } =
    [
        new(LatticeOperation.Read, "Read", AccessOperationGroup.Data),
        new(LatticeOperation.Write, "Write", AccessOperationGroup.Data),
        new(LatticeOperation.Delete, "Delete", AccessOperationGroup.Data),
        new(LatticeOperation.RangeRead, "Range read", AccessOperationGroup.Data),
        new(LatticeOperation.RangeDelete, "Range delete", AccessOperationGroup.Data),
        new(LatticeOperation.CrdtApply, "CRDT apply", AccessOperationGroup.Data),
        new(LatticeOperation.AtomicWrite, "Atomic write", AccessOperationGroup.Data),
        new(LatticeOperation.BulkLoad, "Bulk load", AccessOperationGroup.Data),
        new(LatticeOperation.Admin, "Admin", AccessOperationGroup.Administration),
        new(LatticeOperation.Backup, "Backup", AccessOperationGroup.Administration),
        new(LatticeOperation.Restore, "Restore", AccessOperationGroup.Administration),
        new(LatticeOperation.SchemaAdmin, "Schema admin", AccessOperationGroup.Administration),
        new(LatticeOperation.Replication, "Replication", AccessOperationGroup.Administration),
        new(LatticeOperation.TreeLifecycle, "Tree lifecycle", AccessOperationGroup.Administration),
        new(LatticeOperation.Telemetry, "Telemetry", AccessOperationGroup.ClusterWide),
        new(LatticeOperation.AppInstall, "App install", AccessOperationGroup.ClusterWide),
    ];

    /// <summary>Whether <paramref name="scope"/> is the cluster-wide scope.</summary>
    /// <param name="scope">The scope.</param>
    public static bool IsClusterWide(LatticeScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return scope.Kind == LatticeScopeKind.Tree
            && string.Equals(scope.TreeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal);
    }

    /// <summary>Whether <paramref name="scope"/> is the access-administration (policy tree) scope.</summary>
    /// <param name="scope">The scope.</param>
    public static bool IsAccessAdministration(LatticeScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return scope.Kind == LatticeScopeKind.Tree
            && string.Equals(scope.TreeId, LatticeAuthReservedTrees.PolicyTreeId, StringComparison.Ordinal);
    }

    /// <summary>The label of one operation flag, or its enum name for a flag the catalogue does not know.</summary>
    /// <param name="operation">A single operation.</param>
    public static string OperationLabel(LatticeOperation operation)
    {
        foreach (var option in Operations)
        {
            if (option.Flag == operation)
            {
                return option.Label;
            }
        }

        return operation.ToString();
    }

    /// <summary>The labels of every flag in <paramref name="operations"/>, comma separated, or <c>none</c>.</summary>
    /// <param name="operations">The operation flags.</param>
    public static string OperationsLabel(LatticeOperation operations)
    {
        if (operations == LatticeOperation.None)
        {
            return "none";
        }

        var parts = new List<string>(Operations.Count);
        var known = LatticeOperation.None;
        foreach (var option in Operations)
        {
            known |= option.Flag;
            if ((operations & option.Flag) == option.Flag)
            {
                parts.Add(option.Label);
            }
        }

        var unknown = operations & ~known;
        if (unknown != LatticeOperation.None)
        {
            parts.Add(unknown.ToString());
        }

        return string.Join(", ", parts);
    }

    /// <summary>The label of a subject selector, such as <c>group:ops</c>.</summary>
    /// <param name="subject">The subject.</param>
    public static string SubjectLabel(LatticeSubjectSelector subject)
    {
        ArgumentNullException.ThrowIfNull(subject);
        return string.Concat(SubjectKindLabel(subject.Kind), ":", subject.Id);
    }

    /// <summary>The lower-case word for a subject kind.</summary>
    /// <param name="kind">The kind.</param>
    public static string SubjectKindLabel(LatticeSubjectSelectorKind kind) =>
        kind == LatticeSubjectSelectorKind.Group ? "group" : "user";

    /// <summary>The label of a scope: <c>all trees</c>, <c>access administration</c>, or the tree with its key or prefix.</summary>
    /// <param name="scope">The scope.</param>
    public static string ScopeLabel(LatticeScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (IsClusterWide(scope))
        {
            return "all trees (cluster-wide)";
        }

        if (IsAccessAdministration(scope))
        {
            return "access administration";
        }

        return scope.Kind switch
        {
            LatticeScopeKind.Key => $"{scope.TreeId} key {scope.KeyOrPrefix}",
            LatticeScopeKind.Prefix => $"{scope.TreeId} prefix {scope.KeyOrPrefix}",
            _ => scope.TreeId,
        };
    }

    /// <summary>The word for an effect.</summary>
    /// <param name="effect">The effect.</param>
    public static string EffectLabel(LatticeEffect effect) => effect == LatticeEffect.Deny ? "Deny" : "Allow";

    /// <summary>
    /// Orders <paramref name="rules"/> as the policy evaluator ranks them, as a
    /// reading aid only: an all-trees deny first, then narrower scopes before wider
    /// ones, deny before allow at equal standing, an all-trees allow last, and the
    /// rule id as a deterministic tie-break.
    /// </summary>
    /// <param name="rules">The rules.</param>
    public static IReadOnlyList<LatticeAuthorizationRule> InPrecedenceOrder(IEnumerable<LatticeAuthorizationRule> rules)
    {
        ArgumentNullException.ThrowIfNull(rules);
        var ordered = rules.ToList();
        ordered.Sort(static (a, b) =>
        {
            var byTier = Tier(b).CompareTo(Tier(a));
            if (byTier != 0)
            {
                return byTier;
            }

            var bySpecificity = Specificity(b.Scope).CompareTo(Specificity(a.Scope));
            if (bySpecificity != 0)
            {
                return bySpecificity;
            }

            var byEffect = (b.Effect == LatticeEffect.Deny).CompareTo(a.Effect == LatticeEffect.Deny);
            if (byEffect != 0)
            {
                return byEffect;
            }

            var byTree = string.CompareOrdinal(a.Scope.TreeId, b.Scope.TreeId);
            return byTree != 0 ? byTree : string.CompareOrdinal(a.RuleId, b.RuleId);
        });
        return ordered;
    }

    private static int Tier(LatticeAuthorizationRule rule) =>
        !IsClusterWide(rule.Scope) ? 1 : rule.Effect == LatticeEffect.Deny ? 2 : 0;

    private static int Specificity(LatticeScope scope) => scope.Kind switch
    {
        LatticeScopeKind.Key => 2_000_000,
        LatticeScopeKind.Prefix => 1_000_000 + (scope.KeyOrPrefix?.Length ?? 0),
        _ => 0,
    };
}
