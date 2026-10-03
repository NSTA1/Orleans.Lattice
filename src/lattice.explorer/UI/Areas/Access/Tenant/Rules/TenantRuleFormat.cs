using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// How a tenant's rules are named on its Rules and Explain pages: the scope, the
/// subject, the layer and origin, the confinement reasons the policy gives, and
/// the data-plane operations a tenant rule may govern. None of it is a second
/// opinion on a verdict: the server's answer is always rendered as it arrives.
/// </summary>
internal static class TenantRuleFormat
{
    /// <summary>The scope value of a whole tree.</summary>
    public const string TreeScope = "tree";

    /// <summary>The scope value of a key prefix in a tree.</summary>
    public const string PrefixScope = "prefix";

    /// <summary>The scope value of a single key in a tree.</summary>
    public const string KeyScope = "key";

    /// <summary>The scope value of every tree the tenant owns.</summary>
    public const string TenantWideScope = "tenant";

    /// <summary>What a withheld subject or scope reads as.</summary>
    public const string WithheldText = "withheld";

    /// <summary>
    /// The operations a tenant rule may govern, in the order and groups the
    /// checklist offers them: the data-plane mask (<see cref="LatticeAuthOperations.All"/>),
    /// never a cluster-wide capability, replication or tree lifecycle.
    /// </summary>
    public static IReadOnlyList<AccessOperationOption> DataPlaneOperations { get; } = BuildDataPlaneOperations();

    /// <summary>The scope value of <paramref name="kind"/>.</summary>
    /// <param name="kind">The scope kind.</param>
    /// <returns>The scope value.</returns>
    public static string ScopeValue(TenantRuleScopeKind kind) => kind switch
    {
        TenantRuleScopeKind.Key => KeyScope,
        TenantRuleScopeKind.Prefix => PrefixScope,
        TenantRuleScopeKind.TenantWide => TenantWideScope,
        _ => TreeScope,
    };

    /// <summary>The scope kind a scope value names.</summary>
    /// <param name="value">The scope value.</param>
    /// <returns>The scope kind; a whole tree for an unknown value.</returns>
    public static TenantRuleScopeKind ScopeKind(string? value) => value switch
    {
        KeyScope => TenantRuleScopeKind.Key,
        PrefixScope => TenantRuleScopeKind.Prefix,
        TenantWideScope => TenantRuleScopeKind.TenantWide,
        _ => TenantRuleScopeKind.Tree,
    };

    /// <summary>The label of a rule's scope: every tree in the tenant, or the tree with its key or prefix.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The label; <see cref="WithheldText"/> when the scope is withheld.</returns>
    public static string ScopeLabel(TenantRuleView rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        if (rule.ScopeKind == TenantRuleScopeKind.TenantWide)
        {
            return "every tree in this tenant";
        }

        if (rule.TreeName is not { } tree)
        {
            return WithheldText;
        }

        return rule.ScopeKind switch
        {
            TenantRuleScopeKind.Key => $"{tree} key {rule.KeyOrPrefix}",
            TenantRuleScopeKind.Prefix => $"{tree} prefix {rule.KeyOrPrefix}",
            _ => tree,
        };
    }

    /// <summary>The label of a subject, such as <c>tenant-group:eng</c>.</summary>
    /// <param name="kind">The subject kind.</param>
    /// <param name="id">The subject id, or <see langword="null"/> when withheld.</param>
    /// <returns>The label; <see cref="WithheldText"/> when the subject is withheld.</returns>
    public static string SubjectLabel(TenantSubjectKind kind, string? id) =>
        id is null ? WithheldText : string.Concat(SubjectKindLabel(kind), ":", id);

    /// <summary>The lower-case word for a subject kind.</summary>
    /// <param name="kind">The kind.</param>
    /// <returns><c>tenant-group</c>, <c>group</c> or <c>user</c>.</returns>
    public static string SubjectKindLabel(TenantSubjectKind kind) => kind switch
    {
        TenantSubjectKind.TenantGroup => "tenant-group",
        TenantSubjectKind.ClusterGroup => "group",
        _ => "user",
    };

    /// <summary>The subject picker's kind value of a subject kind.</summary>
    /// <param name="kind">The kind.</param>
    /// <returns>The picker's kind value.</returns>
    public static string SubjectKindValue(TenantSubjectKind kind) => kind switch
    {
        TenantSubjectKind.TenantGroup => AccessSubjectPicker.TenantGroupValue,
        TenantSubjectKind.ClusterGroup => AccessSubjectPicker.ClusterGroupValue,
        _ => AccessSubjectPicker.UserValue,
    };

    /// <summary>The subject kind a picker kind value names.</summary>
    /// <param name="value">The kind value, such as an address's <c>kind</c> query.</param>
    /// <returns>The subject kind; a user for an unknown value.</returns>
    public static TenantSubjectKind SubjectKindOf(string? value) => value switch
    {
        AccessSubjectPicker.TenantGroupValue => TenantSubjectKind.TenantGroup,
        AccessSubjectPicker.ClusterGroupValue or AccessSubjectPicker.GroupValue => TenantSubjectKind.ClusterGroup,
        _ => TenantSubjectKind.User,
    };

    /// <summary>The name of a layer.</summary>
    /// <param name="layer">The layer.</param>
    /// <returns><c>Platform</c> or <c>Tenant</c>.</returns>
    public static string LayerLabel(TenantRuleLayer layer) => layer == TenantRuleLayer.Platform ? "Platform" : "Tenant";

    /// <summary>What a rule of <paramref name="origin"/> is called in a sentence.</summary>
    /// <param name="origin">The origin.</param>
    /// <returns>The phrase.</returns>
    public static string OriginLabel(TenantRuleOrigin origin) => origin switch
    {
        TenantRuleOrigin.PlatformWide => "A platform-wide rule",
        TenantRuleOrigin.AppRole => "An app role",
        TenantRuleOrigin.Tenant => "A tenant rule",
        _ => "A platform rule",
    };

    /// <summary>The short reason a confinement refusal is shown with.</summary>
    /// <param name="rule">The confinement rule that refused the write.</param>
    /// <returns>The reason.</returns>
    public static string ConfinementReason(TenantAccessConfinementRule rule) => rule switch
    {
        TenantAccessConfinementRule.GroupNesting => "Group nesting is not allowed",
        TenantAccessConfinementRule.ForeignTenantGroup => "Another tenant's group",
        TenantAccessConfinementRule.RuleTree => "Not one of this tenant's trees",
        TenantAccessConfinementRule.RuleOperations => "Not a data-plane operation",
        TenantAccessConfinementRule.ReservedRuleId => "A reserved rule id",
        _ => "Refused by the tenant's confinement",
    };

    /// <summary>
    /// Why a tree id cannot carry a tenant rule, before anything is sent, or
    /// <see langword="null"/> when it may: the tenant's own trees only, never an
    /// app-owned, reserved or system tree, and never another tenant's tree.
    /// </summary>
    /// <param name="tree">The tenant-local tree name as typed.</param>
    /// <returns>The problem, or <see langword="null"/>.</returns>
    public static string? TreeProblem(string? tree)
    {
        if (string.IsNullOrWhiteSpace(tree))
        {
            return null;
        }

        var name = tree.Trim();
        if (name.StartsWith("a/", StringComparison.Ordinal))
        {
            return "App-owned trees are governed only by their app's roles, never by tenant rules.";
        }

        if (name.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            return "Name one of this tenant's own trees; another tenant's tree can never be governed here.";
        }

        if (name.StartsWith("sys-", StringComparison.Ordinal) || name.StartsWith("_lattice_", StringComparison.Ordinal))
        {
            return "Reserved and system trees can never carry tenant rules.";
        }

        if (string.Equals(name, "*", StringComparison.Ordinal))
        {
            return "Choose the scope 'Every tree in this tenant' to govern every tree.";
        }

        return null;
    }

    private static AccessOperationOption[] BuildDataPlaneOperations()
    {
        var options = new List<AccessOperationOption>(AccessRuleFormat.Operations.Count);
        foreach (var option in AccessRuleFormat.Operations)
        {
            if ((option.Flag & LatticeAuthOperations.All) == option.Flag)
            {
                options.Add(option);
            }
        }

        return [.. options];
    }
}
