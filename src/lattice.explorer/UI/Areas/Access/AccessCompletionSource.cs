using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// Completes the address line against the access catalogue: <c>group:{name}</c>
/// and <c>rule:{id}</c>. Typing a <c>group:</c> or <c>rule:</c> prefix narrows to
/// that kind; plain text matches both, prefix matches first. A raw
/// <c>/access/groups/</c> or <c>/access/rules/</c> address completes the same way.
/// </summary>
/// <param name="catalog">The circuit's memoised access catalogue.</param>
internal sealed class AccessCompletionSource(AccessCatalog catalog) : IAddressCompletionSource
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
        if (!TryReadTerm(query, out var term, out var wantGroups, out var wantRules))
        {
            return [];
        }

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
            foreach (var rule in await _catalog.GetRulesAsync(cancellationToken).ConfigureAwait(false))
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
                var completion = new AddressCompletion(RulePrefix + rule.RuleId, AccessRoutes.Rule(rule.RuleId, rule.Scope.TreeId), detail);
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
