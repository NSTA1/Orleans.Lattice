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

    /// <summary>The scope-kind value for one tenant's capability scope (tenant telemetry).</summary>
    public const string TenantCapabilityScope = "tenant-capability";

    /// <summary>The sentence shown when a scopeless capability is paired with a narrower scope.</summary>
    public const string ScopelessMessage = "App install and Telemetry are cluster-wide capabilities: they can only be granted with the cluster-wide scope (or, for Telemetry, one tenant's capability scope). Choose that scope, or clear them.";

    /// <summary>The sentence shown when a tenant capability rule carries anything but Telemetry.</summary>
    public const string TenantCapabilityMessage = "A tenant capability rule grants Telemetry for that one tenant and nothing else. Clear the other operations.";

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

    /// <summary>The tenant id, for a tenant capability scope.</summary>
    public string Tenant { get; set; } = string.Empty;

    /// <summary>The key or prefix, for a key or prefix scope.</summary>
    public string KeyOrPrefix { get; set; } = string.Empty;

    /// <summary>The granted or denied operations.</summary>
    public LatticeOperation Operations { get; set; }

    /// <summary>
    /// The condition the edited rule already carries, kept verbatim, or
    /// <see langword="null"/>. The editor never authors one: the authorization
    /// engine does not evaluate conditions in this version, so offering one would
    /// imply a narrowing that does not exist. A condition written through the API
    /// is carried through a save unchanged so nothing is lost.
    /// </summary>
    public string? Condition { get; private init; }

    /// <summary>Whether the edited rule carries a condition.</summary>
    public bool HasCondition => !string.IsNullOrEmpty(Condition);

    /// <summary>Whether the chosen scope is the cluster-wide scope.</summary>
    public bool IsClusterWide => ScopeKind == ClusterScope;

    /// <summary>Whether the chosen scope is one tenant's capability scope.</summary>
    public bool IsTenantCapability => ScopeKind == TenantCapabilityScope;

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
            : scope.TryGetTenantCapabilityTenant(out _) ? TenantCapabilityScope
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
            Tenant = scope.TryGetTenantCapabilityTenant(out var tenant) ? tenant.Value! : string.Empty,
            KeyOrPrefix = scope.KeyOrPrefix ?? string.Empty,
            Operations = rule.Operations,
            Condition = rule.Condition,
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
        else if (SubjectKind == LatticeSubjectSelectorKind.Group && SubjectId.Trim().StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            // The cluster confines a rule naming a tenant group to that tenant's own
            // trees (TenantRuleConfinement); say so before it is sent.
            var confinement = TenantGroupConfinement(SubjectId.Trim());
            if (confinement is not null)
            {
                if (NeedsTree && TreeId.Trim().Length > 0)
                {
                    errors.Tree = confinement;
                }
                else if (!NeedsTree)
                {
                    errors.Subject = confinement;
                }
            }
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

        if (IsTenantCapability)
        {
            var tenant = Tenant.Trim();
            if (tenant.Length == 0)
            {
                errors.Tenant = "Enter the tenant whose telemetry the rule grants.";
            }
            else if (!Orleans.Lattice.TenantId.TryParse(tenant, out var parsed))
            {
                errors.Tenant = $"{tenant} is not a valid tenant id.";
            }
            else if (parsed.IsDefault)
            {
                errors.Tenant = "The default tenant has no tenant capability scope. Grant its telemetry with the cluster-wide scope.";
            }
        }

        if (Operations == LatticeOperation.None)
        {
            errors.Operations = "Choose at least one operation.";
        }
        else if (IsTenantCapability)
        {
            if (Operations != LatticeOperation.Telemetry)
            {
                errors.Operations = TenantCapabilityMessage;
            }
        }
        else if (!IsClusterWide && (Operations & AccessRuleFormat.ScopelessOperations) != LatticeOperation.None)
        {
            errors.Operations = ScopelessMessage;
        }

        return errors;
    }

    /// <summary>
    /// Why a rule naming the tenant group <paramref name="subject"/> cannot have this
    /// draft's scope, or <see langword="null"/> when it can: the cluster admits such a
    /// rule only on one of that tenant's own trees (<c>t/{tenant}/{name}</c>, not an
    /// app-owned <c>a/...</c> or system tree). The cluster checks again on write.
    /// </summary>
    /// <param name="subject">The tenant group id the rule names.</param>
    /// <returns>The refusal, or <see langword="null"/>.</returns>
    internal string? TenantGroupConfinement(string subject)
    {
        if (!LatticeTenantGroupId.TryParse(subject, out var group))
        {
            return $"{subject} is not a well-formed tenant group id (t/{{tenant}}/{{name}}).";
        }

        var tenant = group.Tenant.Value;
        var refusal = $"A rule naming tenant group {subject} may govern only tenant {tenant}'s own trees (t/{tenant}/...). Choose one of them.";
        if (!NeedsTree)
        {
            return refusal;
        }

        var tree = TreeId.Trim();
        var prefix = string.Concat(LatticeTenantTrees.SegmentPrefix, tenant, "/");
        if (!tree.StartsWith(prefix, StringComparison.Ordinal))
        {
            return refusal;
        }

        var local = tree[prefix.Length..];
        return local.Length == 0
            || local == "*"
            || local.StartsWith("a/", StringComparison.Ordinal)
            || local.StartsWith("sys-", StringComparison.Ordinal)
            || local.StartsWith("_lattice_", StringComparison.Ordinal)
                ? refusal
                : null;
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
            TenantCapabilityScope => LatticeScope.TenantCapability(Orleans.Lattice.TenantId.Parse(Tenant.Trim())),
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
            Condition);
    }
}
