using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// The shadow check the tenant rule editor runs before it saves: the platform
/// rules, listed on the tenant's trees, that already decide the rule's scope for
/// its subject. Platform rules are evaluated first and are final when they match,
/// so a tenant rule they shadow never takes effect there.
/// </summary>
/// <remarks>
/// The check is a reading aid over the rules the tenant administrator can see: it
/// matches the subject exactly (no group expansion), and platform-wide and app
/// role rules, which are never listed to a tenant administrator, cannot be
/// checked. Explain is the authority for any one request.
/// </remarks>
internal static class TenantRuleShadow
{
    /// <summary>
    /// The platform rules that decide the whole of <paramref name="form"/>'s scope
    /// for its subject on at least one of its operations.
    /// </summary>
    /// <param name="form">The rule being edited.</param>
    /// <param name="rules">The rules governing the tenant, both layers.</param>
    /// <returns>The shadowing rules, each with the operations it decides first; empty when none.</returns>
    public static IReadOnlyList<TenantRuleShadowHit> Find(TenantRuleForm form, IReadOnlyList<TenantRuleView> rules)
    {
        ArgumentNullException.ThrowIfNull(form);
        ArgumentNullException.ThrowIfNull(rules);
        var subject = form.SubjectId.Trim();
        if (subject.Length == 0 || form.Operations == LatticeOperation.None)
        {
            return [];
        }

        List<TenantRuleShadowHit>? hits = null;
        foreach (var rule in rules)
        {
            if (rule.Layer != TenantRuleLayer.Platform
                || rule.SubjectWithheld
                || rule.SubjectKind != form.SubjectKind
                || !string.Equals(rule.SubjectId, subject, StringComparison.Ordinal))
            {
                continue;
            }

            var operations = rule.Operations & form.Operations;
            if (operations == LatticeOperation.None || !Covers(rule, form))
            {
                continue;
            }

            (hits ??= []).Add(new TenantRuleShadowHit(rule, operations, operations != form.Operations));
        }

        return hits ?? (IReadOnlyList<TenantRuleShadowHit>)[];
    }

    /// <summary>The sentence a shadowing rule is reported with.</summary>
    /// <param name="hit">The shadowing rule.</param>
    /// <param name="form">The rule being edited.</param>
    /// <returns>The sentence, such as "This rule will not take effect for tenant-group:eng: platform rule R decides first."</returns>
    public static string Message(TenantRuleShadowHit hit, TenantRuleForm form)
    {
        ArgumentNullException.ThrowIfNull(hit);
        ArgumentNullException.ThrowIfNull(form);
        var subject = TenantRuleFormat.SubjectLabel(form.SubjectKind, form.SubjectId.Trim());
        var where = form.ScopeKind == TenantRuleScopeKind.TenantWide ? $" on tree {hit.Rule.TreeName}" : string.Empty;
        var what = hit.Partial ? $" ({AccessRuleFormat.OperationsLabel(hit.Operations)})" : string.Empty;
        return $"This rule will not take effect for {subject}{where}{what}: platform rule {hit.Rule.RuleId} decides first.";
    }

    /// <summary>Whether <paramref name="rule"/>'s scope contains the whole of <paramref name="form"/>'s scope on one tree.</summary>
    private static bool Covers(TenantRuleView rule, TenantRuleForm form)
    {
        if (rule.TreeName is not { } tree || rule.ScopeKind == TenantRuleScopeKind.TenantWide)
        {
            return false;
        }

        if (form.ScopeKind == TenantRuleScopeKind.TenantWide)
        {
            // A platform rule over a whole tree decides the tenant-wide rule there.
            return rule.ScopeKind == TenantRuleScopeKind.Tree;
        }

        if (!string.Equals(tree, form.TreeName.Trim(), StringComparison.Ordinal))
        {
            return false;
        }

        var narrow = form.KeyOrPrefix;
        return rule.ScopeKind switch
        {
            TenantRuleScopeKind.Tree => true,
            TenantRuleScopeKind.Prefix => form.ScopeKind is TenantRuleScopeKind.Prefix or TenantRuleScopeKind.Key
                && rule.KeyOrPrefix is { } prefix
                && narrow.StartsWith(prefix, StringComparison.Ordinal),
            TenantRuleScopeKind.Key => form.ScopeKind == TenantRuleScopeKind.Key
                && string.Equals(rule.KeyOrPrefix, narrow, StringComparison.Ordinal),
            _ => false,
        };
    }
}
