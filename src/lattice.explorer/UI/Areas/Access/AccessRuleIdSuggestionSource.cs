using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The rule ids already in use under the tree a rule governs, so a new rule's id
/// can be checked for a collision as it is typed. Read from the circuit's access
/// catalogue, which is keyed on the asserted tenant and forgotten after every
/// write, so a freshly removed id is never flagged.
/// </summary>
/// <remarks>
/// The catalogue holds one bounded page of rules, so the check is a guide, not a
/// guarantee; the cluster still refuses a real collision on save.
/// </remarks>
/// <param name="catalog">The circuit's access catalogue.</param>
/// <param name="tree">The tree the rule governs, read at query time, or <see langword="null"/> for every tree.</param>
internal sealed class AccessRuleIdSuggestionSource(AccessCatalog catalog, Func<string?> tree) : ILtSuggestionSource
{
    /// <summary>The note shown when the rules cannot be read.</summary>
    public const string UnavailableReason = "The existing rules could not be listed, so a clashing id is not flagged.";

    private Projection? _projection;

    /// <inheritdoc />
    public async ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        IReadOnlyList<Orleans.Lattice.Auth.LatticeAuthorizationRule> rules;
        try
        {
            rules = await catalog.GetRulesAsync(cancellationToken).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return LtSuggestionSet.Unavailable(UnavailableReason);
        }

        var governed = tree()?.Trim() ?? string.Empty;
        if (_projection is not { } projection
            || !ReferenceEquals(projection.Rules, rules)
            || !string.Equals(projection.Tree, governed, StringComparison.Ordinal))
        {
            var ids = new List<LtSuggestion>(rules.Count);
            foreach (var rule in rules)
            {
                if (governed.Length == 0 || string.Equals(rule.Scope.TreeId, governed, StringComparison.Ordinal))
                {
                    ids.Add(new LtSuggestion(rule.RuleId, rule.Scope.TreeId));
                }
            }

            projection = new Projection(rules, governed, ids);
            _projection = projection;
        }

        return SuggestionMatcher.Match(projection.Ids, text, limit);
    }

    /// <summary>One catalogue page's rule ids under one tree, projected once.</summary>
    private sealed record Projection(IReadOnlyList<Orleans.Lattice.Auth.LatticeAuthorizationRule> Rules, string Tree, IReadOnlyList<LtSuggestion> Ids);
}
