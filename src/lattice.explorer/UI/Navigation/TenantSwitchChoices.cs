using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// What the top bar's tenant switcher offers one caller: the tenants the caller
/// may switch between, the active one marked, and whether there is anything to
/// switch between at all. It is also the switcher's suggestion source, so the
/// field can never list a tenant the offer did not.
/// </summary>
/// <remarks>
/// It is an immutable snapshot. A new snapshot is a new source, which tells the
/// combobox that every answer it holds is stale.
/// </remarks>
internal sealed class TenantSwitchChoices : ILtSuggestionSource
{
    /// <summary>The detail beside the active tenant.</summary>
    public const string ActiveDetail = "Active tenant";

    private readonly LtSuggestion[] _suggestions;

    private TenantSwitchChoices(string? active, string[] tenants)
    {
        Active = active;
        Tenants = tenants;
        _suggestions = new LtSuggestion[tenants.Length];
        for (var i = 0; i < tenants.Length; i++)
        {
            var current = string.Equals(tenants[i], active, StringComparison.Ordinal);
            _suggestions[i] = new LtSuggestion(tenants[i], current ? ActiveDetail : null) { Current = current };
        }
    }

    /// <summary>The offer for a caller who may not switch, or who has nowhere to switch to.</summary>
    public static TenantSwitchChoices None { get; } = new(null, []);

    /// <summary>
    /// Whether the switcher is shown: only when there are at least two tenants
    /// to choose between. With one or none it is absent, not disabled.
    /// </summary>
    public bool Offered => Tenants.Count > 1;

    /// <summary>The active tenant, or <see langword="null"/> when none is offered.</summary>
    public string? Active { get; }

    /// <summary>The tenants the caller may switch to, in the accessible-tenant source's order.</summary>
    public IReadOnlyList<string> Tenants { get; }

    /// <summary>
    /// The offer for <paramref name="tenants"/>, with <paramref name="active"/>
    /// marked: duplicates and empty ids are dropped, and the order is kept.
    /// </summary>
    /// <param name="active">The active tenant, or <see langword="null"/>.</param>
    /// <param name="tenants">The reachable tenants, best first.</param>
    /// <returns>The offer; <see cref="None"/> when nothing is left.</returns>
    public static TenantSwitchChoices Of(string? active, IReadOnlyList<string> tenants)
    {
        ArgumentNullException.ThrowIfNull(tenants);

        var distinct = new List<string>(tenants.Count);
        foreach (var tenant in tenants)
        {
            if (!string.IsNullOrEmpty(tenant) && !distinct.Contains(tenant, StringComparer.Ordinal))
            {
                distinct.Add(tenant);
            }
        }

        return distinct.Count == 0 ? None : new TenantSwitchChoices(active, [.. distinct]);
    }

    /// <summary>Whether this offer lists the same tenants, in the same order, with the same one active.</summary>
    /// <param name="other">The other offer.</param>
    public bool SameAs(TenantSwitchChoices other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return string.Equals(Active, other.Active, StringComparison.Ordinal)
            && Tenants.SequenceEqual(other.Tenants, StringComparer.Ordinal);
    }

    /// <inheritdoc />
    public ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(text);
        return new ValueTask<LtSuggestionSet>(SuggestionMatcher.Match(_suggestions, text, limit));
    }
}
