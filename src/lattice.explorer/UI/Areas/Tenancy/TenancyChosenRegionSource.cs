using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// Suggests only the regions a form has already chosen, such as a new tenant's
/// initial residency, which is chosen from the regions it is allowed. The list
/// is read at query time, so it follows the form as it changes.
/// </summary>
/// <param name="regions">The chosen regions, read at query time.</param>
internal sealed class TenancyChosenRegionSource(Func<IReadOnlyList<string>> regions) : ILtSuggestionSource
{
    /// <summary>The detail beside each suggested region.</summary>
    public const string Detail = "Allowed";

    /// <inheritdoc />
    public ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        var values = regions().Distinct(StringComparer.Ordinal).Select(region => new LtSuggestion(region, Detail)).ToArray();
        return ValueTask.FromResult(SuggestionMatcher.Match(values, text, limit));
    }
}
