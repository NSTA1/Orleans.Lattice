using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// The local ids the tenant's own rules already use, so a new rule's id can be
/// checked for a clash as it is typed. Read from the circuit's tenant access
/// catalogue, which is forgotten after every write, so a freshly removed id is
/// never flagged. Platform rules are left out: their ids live in another layer.
/// </summary>
/// <remarks>
/// It remembers nothing itself: the list is the caller-keyed catalogue's. The
/// catalogue holds one bounded page of rules, so the check is a guide; saving
/// an existing id through the editor replaces that rule, which is why the editor
/// refuses a clash.
/// </remarks>
/// <param name="catalog">The circuit's tenant access catalogue.</param>
/// <param name="tenant">The tenant whose rules are checked.</param>
internal sealed class TenantRuleIdSuggestionSource(TenantAccessCatalog catalog, string tenant) : ILtSuggestionSource
{
    /// <summary>The note shown when the rules cannot be read.</summary>
    public const string UnavailableReason = "The tenant's rules could not be listed, so a clashing id is not flagged.";

    private readonly TenantAccessCatalog _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));

    /// <summary>The tenant whose rules are checked.</summary>
    public string Tenant { get; } = tenant ?? throw new ArgumentNullException(nameof(tenant));

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        IReadOnlyList<TenantRuleView> rules;
        try
        {
            rules = await _catalog.GetRulesAsync(Tenant, cancellationToken).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return LtSuggestionSet.Unavailable(UnavailableReason);
        }

        var ids = new List<LtSuggestion>(rules.Count);
        foreach (var rule in rules)
        {
            if (rule.Layer == TenantRuleLayer.Tenant)
            {
                ids.Add(new LtSuggestion(rule.RuleId, TenantRuleFormat.ScopeLabel(rule)));
            }
        }

        return SuggestionMatcher.Match(ids, text, limit);
    }
}
