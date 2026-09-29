using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Navigation.Completion;

/// <summary>
/// The chrome's own completion source for a free search: the visible areas
/// whose key or name matches, so typing <c>back</c> offers Backups.
/// </summary>
internal sealed class AreaCompletionSource : IAddressCompletionSource
{
    private readonly IReadOnlyList<ExplorerAreaEntry> _entries;

    /// <summary>Creates the source over the directory's current stops.</summary>
    /// <param name="entries">The shown stops; only visible ones are offered.</param>
    public AreaCompletionSource(IReadOnlyList<ExplorerAreaEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        _entries = entries;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        IReadOnlyList<AddressCompletion> matches =
        [
            .. _entries
                .Where(entry => entry.IsVisible
                    && (entry.Area.Key.Contains(query.Text, StringComparison.OrdinalIgnoreCase)
                        || entry.Area.DisplayName.Contains(query.Text, StringComparison.OrdinalIgnoreCase)))
                .Take(query.Limit)
                .Select(entry => new AddressCompletion(
                    entry.Area.Key,
                    ExplorerAddress.ForArea(entry.Area.Key).WithTenant(query.Current.Tenant),
                    entry.Area.DisplayName)),
        ];

        return ValueTask.FromResult(matches);
    }
}
