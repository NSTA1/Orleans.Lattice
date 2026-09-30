using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Suggestions;

/// <summary>
/// Matches typed text against a list of existing values, bounded and best first:
/// the exact value, then values that start with the text, then values that
/// contain it, each in the list's own order. Case is ignored for ranking but not
/// for the exact match, because ids are case-sensitive.
/// </summary>
/// <remarks>
/// Runs on every keystroke, so it allocates only the bounded result: no LINQ, no
/// intermediate copies, and the values' own <see cref="LtSuggestion"/> instances
/// are reused rather than rebuilt.
/// </remarks>
internal static class SuggestionMatcher
{
    /// <summary>Matches <paramref name="text"/> against <paramref name="values"/>.</summary>
    /// <param name="values">Every existing value, in the order ties should keep.</param>
    /// <param name="text">What has been typed.</param>
    /// <param name="limit">The most values to return.</param>
    /// <returns>At most <paramref name="limit"/> values, best first.</returns>
    public static LtSuggestionSet Match(IReadOnlyList<LtSuggestion> values, string text, int limit)
    {
        ArgumentNullException.ThrowIfNull(values);
        ArgumentNullException.ThrowIfNull(text);
        if (limit <= 0 || values.Count == 0)
        {
            return LtSuggestionSet.Empty;
        }

        var results = new List<LtSuggestion>(Math.Min(limit, values.Count));
        var exact = -1;
        if (text.Length > 0)
        {
            for (var i = 0; i < values.Count; i++)
            {
                if (string.Equals(values[i].Value, text, StringComparison.Ordinal))
                {
                    exact = i;
                    results.Add(values[i]);
                    break;
                }
            }
        }

        var truncated = false;
        for (var pass = 0; pass < 2 && !truncated; pass++)
        {
            for (var i = 0; i < values.Count; i++)
            {
                if (i == exact)
                {
                    continue;
                }

                var value = values[i].Value;
                var starts = value.StartsWith(text, StringComparison.OrdinalIgnoreCase);
                var matches = pass == 0 ? starts : !starts && value.Contains(text, StringComparison.OrdinalIgnoreCase);
                if (!matches)
                {
                    continue;
                }

                if (results.Count == limit)
                {
                    truncated = true;
                    break;
                }

                results.Add(values[i]);
            }
        }

        return LtSuggestionSet.Of(results, truncated);
    }
}
