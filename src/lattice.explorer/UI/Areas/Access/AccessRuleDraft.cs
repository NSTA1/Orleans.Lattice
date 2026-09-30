using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The rule editor's working copy of a rule: plain fields a form binds to, a
/// validation that refuses what the cluster would refuse (an app-owned id, a
/// scopeless capability paired with a narrower scope), and the conversion back
/// to a <see cref="LatticeAuthorizationRule"/>.
/// </summary>
internal sealed class AccessRuleDraft
{
    /// <summary>The scope-kind value for the cluster-wide scope.</summary>
    public const string ClusterScope = "cluster";

    /// <summary>The scope-kind value for a whole tree.</summary>
    public const string TreeScope = "tree";

    /// <summary>The scope-kind value for a key prefix.</summary>
    public const string PrefixScope = "prefix";

    /// <summary>The scope-kind value for a single key.</summary>
    public const string KeyScope = "key";

    /// <summary>The scope-kind value for access administration (the policy tree).</summary>
    public const string AccessAdministrationScope = "access-administration";

    /// <summary>The sentence shown when a scopeless capability is paired with a narrower scope.</summary>
    public const string ScopelessMessage = "App install and Telemetry are cluster-wide capabilities: they can only be granted with the cluster-wide scope. Choose that scope, or clear them.";

    /// <summary>Whether the draft edits an existing rule, whose id and governed tree are then fixed.</summary>
    public bool IsExisting { get; private init; }

    /// <summary>The rule id.</summary>
    public string RuleId { get; set; } = string.Empty;

    /// <summary>The effect.</summary>
    public LatticeEffect Effect { get; set; } = LatticeEffect.Allow;

    /// <summary>The subject kind.</summary>
    public LatticeSubjectSelectorKind SubjectKind { get; set; } = LatticeSubjectSelectorKind.Group;

    /// <summary>The subject id.</summary>
    public string SubjectId { get; set; } = string.Empty;

    /// <summary>The scope kind: one of the <c>*Scope</c> constants.</summary>
    public string ScopeKind { get; set; } = TreeScope;

    /// <summary>The governed tree id, for a tree, prefix or key scope.</summary>
    public string TreeId { get; set; } = string.Empty;

    /// <summary>The key or prefix, for a key or prefix scope.</summary>
    public string KeyOrPrefix { get; set; } = string.Empty;

    /// <summary>The granted or denied operations.</summary>
    public LatticeOperation Operations { get; set; }

    /// <summary>The optional condition, carried through unchanged when blank.</summary>
    public string Condition { get; set; } = string.Empty;

    /// <summary>Whether the chosen scope is the cluster-wide scope.</summary>
    public bool IsClusterWide => ScopeKind == ClusterScope;

    /// <summary>Whether the chosen scope names a tree.</summary>
    public bool NeedsTree => ScopeKind is TreeScope or PrefixScope or KeyScope;

    /// <summary>Whether the chosen scope names a key or prefix.</summary>
    public bool NeedsKeyOrPrefix => ScopeKind is PrefixScope or KeyScope;

    /// <summary>A draft for a new rule.</summary>
    public static AccessRuleDraft New() => new();

    /// <summary>A draft editing <paramref name="rule"/>.</summary>
    /// <param name="rule">The rule.</param>
    public static AccessRuleDraft From(LatticeAuthorizationRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        var scope = rule.Scope;
        var kind = AccessRuleFormat.IsClusterWide(scope) ? ClusterScope
            : AccessRuleFormat.IsAccessAdministration(scope) ? AccessAdministrationScope
            : scope.Kind switch
            {
                LatticeScopeKind.Key => KeyScope,
                LatticeScopeKind.Prefix => PrefixScope,
                _ => TreeScope,
            };

        return new AccessRuleDraft
        {
            IsExisting = true,
            RuleId = rule.RuleId,
            Effect = rule.Effect,
            SubjectKind = rule.Subject.Kind,
            SubjectId = rule.Subject.Id,
            ScopeKind = kind,
            TreeId = kind is TreeScope or PrefixScope or KeyScope ? scope.TreeId : string.Empty,
            KeyOrPrefix = scope.KeyOrPrefix ?? string.Empty,
            Operations = rule.Operations,
            Condition = rule.Condition ?? string.Empty,
        };
    }

    /// <summary>Sets or clears one operation flag.</summary>
    /// <param name="operation">The flag.</param>
    /// <param name="granted">Whether it is included.</param>
    public void SetOperation(LatticeOperation operation, bool granted) =>
        Operations = granted ? Operations | operation : Operations & ~operation;

    /// <summary>Whether <paramref name="operation"/> is included.</summary>
    /// <param name="operation">The flag.</param>
    public bool HasOperation(LatticeOperation operation) => (Operations & operation) == operation;

    /// <summary>
    /// Validates the draft, returning every problem keyed by field. An empty result
    /// means the draft converts with <see cref="ToRule"/>.
    /// </summary>
    public AccessRuleDraftErrors Validate()
    {
        var errors = new AccessRuleDraftErrors();
        var ruleId = RuleId.Trim();
        if (ruleId.Length == 0)
        {
            errors.RuleId = "Enter a rule id.";
        }
        else if (!IsExisting && LatticeAppRuleIds.IsAppOwned(ruleId))
        {
            errors.RuleId = $"Ids starting with {LatticeAppRuleIds.Prefix} belong to installed apps and are compiled from their manifests. Choose another id.";
        }

        if (SubjectId.Trim().Length == 0)
        {
            errors.Subject = "Choose who the rule is about.";
        }

        if (NeedsTree)
        {
            var tree = TreeId.Trim();
            if (tree.Length == 0)
            {
                errors.Tree = "Enter the tree the rule governs.";
            }
            else if (string.Equals(tree, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal))
            {
                errors.Tree = "Choose the cluster-wide scope to govern every tree.";
            }
            else if (LatticeAuthReservedTrees.IsReserved(tree))
            {
                errors.Tree = "Reserved system trees cannot be named here.";
            }
        }

        if (NeedsKeyOrPrefix && KeyOrPrefix.Length == 0)
        {
            errors.KeyOrPrefix = ScopeKind == KeyScope ? "Enter the key." : "Enter the prefix.";
        }

        if (Operations == LatticeOperation.None)
        {
            errors.Operations = "Choose at least one operation.";
        }
        else if (!IsClusterWide && (Operations & AccessRuleFormat.ScopelessOperations) != LatticeOperation.None)
        {
            errors.Operations = ScopelessMessage;
        }

        return errors;
    }

    /// <summary>The rule the draft describes. Call only after <see cref="Validate"/> reported no error.</summary>
    /// <exception cref="InvalidOperationException">The draft does not validate.</exception>
    public LatticeAuthorizationRule ToRule()
    {
        if (Validate().HasAny)
        {
            throw new InvalidOperationException("The rule draft does not validate.");
        }

        var scope = ScopeKind switch
        {
            ClusterScope => LatticeScope.ClusterWide(),
            AccessAdministrationScope => LatticeScope.Tree(LatticeAuthReservedTrees.PolicyTreeId),
            KeyScope => LatticeScope.Key(TreeId.Trim(), KeyOrPrefix),
            PrefixScope => LatticeScope.Prefix(TreeId.Trim(), KeyOrPrefix),
            _ => LatticeScope.Tree(TreeId.Trim()),
        };

        return new LatticeAuthorizationRule(
            RuleId.Trim(),
            new LatticeSubjectSelector(SubjectKind, SubjectId.Trim()),
            scope,
            Operations,
            Effect,
            string.IsNullOrWhiteSpace(Condition) ? null : Condition.Trim());
    }
}
