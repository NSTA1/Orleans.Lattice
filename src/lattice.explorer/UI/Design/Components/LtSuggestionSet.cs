namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// An <see cref="ILtSuggestionSource"/>'s answer to one query: the matching
/// existing values, best first, or the reason the source cannot list them.
/// </summary>
/// <remarks>
/// A source that cannot list its values - no identity directory, a facade the
/// head does not serve, a refused read - answers <see cref="Unavailable"/> rather
/// than throwing. A combobox then accepts free text and shows the reason, so a
/// missing source never blocks the field.
/// </remarks>
public sealed class LtSuggestionSet
{
    private LtSuggestionSet(IReadOnlyList<LtSuggestion> items, bool truncated, string? unavailableReason)
    {
        Items = items;
        Truncated = truncated;
        UnavailableReason = unavailableReason;
    }

    /// <summary>An available answer with no matches.</summary>
    public static LtSuggestionSet Empty { get; } = new([], false, null);

    /// <summary>The matching values, best first; an exact match, when there is one, comes first.</summary>
    public IReadOnlyList<LtSuggestion> Items { get; }

    /// <summary>Whether more values match than the query's limit allowed.</summary>
    public bool Truncated { get; }

    /// <summary>
    /// One sentence saying why the source cannot list its values, or
    /// <see langword="null"/> when it could.
    /// </summary>
    public string? UnavailableReason { get; }

    /// <summary>Whether the source could list its values.</summary>
    public bool IsAvailable => UnavailableReason is null;

    /// <summary>An available answer.</summary>
    /// <param name="items">The matching values, best first, at most the query's limit.</param>
    /// <param name="truncated">Whether more values match than were returned.</param>
    /// <returns>The answer.</returns>
    public static LtSuggestionSet Of(IReadOnlyList<LtSuggestion> items, bool truncated = false)
    {
        ArgumentNullException.ThrowIfNull(items);
        return items.Count == 0 && !truncated ? Empty : new LtSuggestionSet(items, truncated, null);
    }

    /// <summary>An answer saying the source cannot list its values.</summary>
    /// <param name="reason">One sentence naming why, such as "No identity directory is configured."</param>
    /// <returns>The answer.</returns>
    public static LtSuggestionSet Unavailable(string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return new LtSuggestionSet([], false, reason);
    }

    /// <summary>The item whose value equals <paramref name="value"/> exactly, or <see langword="null"/>.</summary>
    /// <param name="value">The value to find.</param>
    /// <returns>The matching item.</returns>
    public LtSuggestion? Find(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        for (var i = 0; i < Items.Count; i++)
        {
            if (string.Equals(Items[i].Value, value, StringComparison.Ordinal))
            {
                return Items[i];
            }
        }

        return null;
    }
}
