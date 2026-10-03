using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// Completes the address line against the access catalogue: <c>group:{name}</c>
/// and <c>rule:{id}</c>. Typing a <c>group:</c> or <c>rule:</c> prefix narrows to
/// that kind; plain text matches both, prefix matches first. A raw
/// <c>/access/groups/</c> or <c>/access/rules/</c> address completes the same way.
/// From a tenant-rooted address only that tenant's rules complete, and no group -
/// unless the tenant's access administration is delegated to the caller: then the
/// tenant's own groups and tenant-tier rules complete, rooted at the tenant, and a
/// raw <c>/t/{tenant}/access/</c> address completes its sections too.
/// </summary>
/// <param name="catalog">The circuit's memoised access catalogue.</param>
/// <param name="tenantAccess">The circuit's tenant access catalogue, or <see langword="null"/> for none.</param>
internal sealed class AccessCompletionSource(AccessCatalog catalog, TenantAccessCatalog? tenantAccess = null) : IAddressCompletionSource
{
    /// <summary>The label prefix of a group completion.</summary>
    public const string GroupPrefix = "group:";

    /// <summary>The label prefix of a rule completion.</summary>
    public const string RulePrefix = "rule:";

    private static readonly string GroupAddressPrefix = AccessRoutes.Groups.Format() + "/";
    private static readonly string RuleAddressPrefix = AccessRoutes.Rules.Format() + "/";

    private readonly AccessCatalog _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (query.Current.Tenant is { } tenant
            && tenantAccess is { IsServed: true }
            && TenantAccessCatalog.Administers(tenant)
            && (await tenantAccess.GetStateAsync(tenant, cancellationToken).ConfigureAwait(false)).IsDelegated)
        {
            return await CompleteTenantAsync(query, tenant, tenantAccess, cancellationToken).ConfigureAwait(false);
        }

        if (!TryReadTerm(query, out var term, out var wantGroups, out var wantRules))
        {
            return [];
        }

        // At a tenant-rooted address only that tenant's rules complete, rooted at
        // it; groups belong to no tenant, so they complete only cluster-wide.
        var scope = query.Current.Tenant;
        wantGroups &= scope is null;
        var results = new List<AddressCompletion>(query.Limit);
        var later = new List<AddressCompletion>();

        if (wantGroups)
        {
            foreach (var group in await _catalog.GetGroupsAsync(cancellationToken).ConfigureAwait(false))
            {
                var rank = Rank(term, group.GroupId, group.DisplayName);
                if (rank < 0)
                {
                    continue;
                }

                var completion = new AddressCompletion(GroupPrefix + group.GroupId, AccessRoutes.Group(group.GroupId), group.DisplayName);
                (rank == 0 ? results : later).Add(completion);
            }
        }

        if (wantRules)
        {
            foreach (var rule in await _catalog.GetRulesAsync(scope, cancellationToken).ConfigureAwait(false))
            {
                var rank = Rank(term, rule.RuleId, null);
                if (rank < 0)
                {
                    continue;
                }

                var detail = string.Concat(
                    AccessRuleFormat.EffectLabel(rule.Effect), " ",
                    AccessRuleFormat.SubjectLabel(rule.Subject), " on ",
                    AccessRuleFormat.ScopeLabel(rule.Scope));
                var completion = new AddressCompletion(RulePrefix + rule.RuleId, AccessRoutes.Rule(rule.RuleId, rule.Scope.TreeId).WithTenant(scope), detail);
                (rank == 0 ? results : later).Add(completion);
            }
        }

        results.AddRange(later);
        return results.Count > query.Limit ? results.GetRange(0, query.Limit) : results;
    }

    private static async Task<IReadOnlyList<AddressCompletion>> CompleteTenantAsync(
        AddressQuery query, string tenant, TenantAccessCatalog access, CancellationToken cancellationToken)
    {
        var text = query.Text.Trim();
        var term = text;
        var wantGroups = true;
        var wantRules = true;
        var wantSections = false;
        switch (query.Mode)
        {
            case AddressQueryMode.Search:
                if (text.StartsWith(GroupPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    term = text[GroupPrefix.Length..];
                    wantRules = false;
                }
                else if (text.StartsWith(RulePrefix, StringComparison.OrdinalIgnoreCase))
                {
                    term = text[RulePrefix.Length..];
                    wantGroups = false;
                }
                else if (text.Length == 0)
                {
                    return [];
                }

                break;

            case AddressQueryMode.Address:
                var root = AccessRoutes.TenantRoot(tenant).Format() + "/";
                if (!text.StartsWith(root, StringComparison.OrdinalIgnoreCase))
                {
                    return [];
                }

                var rest = text[root.Length..];
                if (rest.StartsWith(AccessRoutes.GroupsSegment + "/", StringComparison.OrdinalIgnoreCase))
                {
                    term = Uri.UnescapeDataString(rest[(AccessRoutes.GroupsSegment.Length + 1)..]);
                    wantRules = false;
                }
                else if (rest.StartsWith(AccessRoutes.RulesSegment + "/", StringComparison.OrdinalIgnoreCase))
                {
                    term = Uri.UnescapeDataString(rest[(AccessRoutes.RulesSegment.Length + 1)..]);
                    wantGroups = false;
                }
                else
                {
                    term = rest;
                    wantGroups = false;
                    wantRules = false;
                    wantSections = true;
                }

                break;

            default:
                return [];
        }

        var results = new List<AddressCompletion>(query.Limit);
        var later = new List<AddressCompletion>();
        if (wantSections)
        {
            foreach (var (segment, label, address) in AccessRoutes.TenantSections(tenant))
            {
                if (segment.StartsWith(term, StringComparison.OrdinalIgnoreCase))
                {
                    results.Add(new AddressCompletion(address.Format(), address, label));
                }
            }
        }

        if (wantGroups)
        {
            foreach (var group in await access.GetGroupsAsync(tenant, cancellationToken).ConfigureAwait(false))
            {
                var rank = Rank(term, group.Name, group.DisplayName);
                if (rank < 0)
                {
                    continue;
                }

                var completion = new AddressCompletion(
                    GroupPrefix + group.Name,
                    AccessRoutes.TenantGroup(tenant, group.Name),
                    TenantGroupSuggestionSource.Describe(TenantGroupSuggestionSource.SourceLabel, group.DisplayName));
                (rank == 0 ? results : later).Add(completion);
            }
        }

        if (wantRules)
        {
            foreach (var rule in await access.GetRulesAsync(tenant, cancellationToken).ConfigureAwait(false))
            {
                // Only the tenant's own tier has a page here; platform rules are read on the Rules page.
                if (rule.Layer != TenantRuleLayer.Tenant)
                {
                    continue;
                }

                var rank = Rank(term, rule.RuleId, null);
                if (rank < 0)
                {
                    continue;
                }

                var detail = string.Concat(AccessRuleFormat.EffectLabel(rule.Effect), " on ", rule.TreeName ?? "every tree of the tenant");
                var completion = new AddressCompletion(RulePrefix + rule.RuleId, AccessRoutes.TenantRule(tenant, rule.RuleId), detail);
                (rank == 0 ? results : later).Add(completion);
            }
        }

        results.AddRange(later);
        return results.Count > query.Limit ? results.GetRange(0, query.Limit) : results;
    }

    private static bool TryReadTerm(AddressQuery query, out string term, out bool wantGroups, out bool wantRules)
    {
        var text = query.Text.Trim();
        term = text;
        wantGroups = true;
        wantRules = true;

        switch (query.Mode)
        {
            case AddressQueryMode.Search:
                if (text.StartsWith(GroupPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    term = text[GroupPrefix.Length..];
                    wantRules = false;
                }
                else if (text.StartsWith(RulePrefix, StringComparison.OrdinalIgnoreCase))
                {
                    term = text[RulePrefix.Length..];
                    wantGroups = false;
                }
                else if (text.Length == 0)
                {
                    return false;
                }

                return true;

            case AddressQueryMode.Address:
                if (text.StartsWith(GroupAddressPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    term = Uri.UnescapeDataString(text[GroupAddressPrefix.Length..]);
                    wantRules = false;
                    return true;
                }

                if (text.StartsWith(RuleAddressPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    term = Uri.UnescapeDataString(text[RuleAddressPrefix.Length..]);
                    wantGroups = false;
                    return true;
                }

                return false;

            default:
                return false;
        }
    }

    // 0: the id starts with the term; 1: the id or display name contains it; -1: no match.
    private static int Rank(string term, string id, string? displayName)
    {
        if (term.Length == 0 || id.StartsWith(term, StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }

        return id.Contains(term, StringComparison.OrdinalIgnoreCase)
            || (displayName is not null && displayName.Contains(term, StringComparison.OrdinalIgnoreCase))
            ? 1
            : -1;
    }
}
