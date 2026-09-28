using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>
/// Where the user is, as the layout cascades it to the chrome and to every page:
/// the canonical address and the directory's shown stops.
/// </summary>
/// <param name="Address">The canonical address of the current page.</param>
/// <param name="Entries">The shown stops, in spine order.</param>
/// <param name="EntriesLoaded">Whether <paramref name="Entries"/> has been asked for yet; until then it is empty.</param>
/// <param name="TenancyActive">Whether tenancy is on, so the address has a tenant node.</param>
internal sealed record ExplorerLocation(
    ExplorerAddress Address,
    IReadOnlyList<ExplorerAreaEntry> Entries,
    bool EntriesLoaded,
    bool TenancyActive)
{
    /// <summary>The location before anything is known: Home, no stops.</summary>
    public static ExplorerLocation Initial { get; } = new(ExplorerAddress.Home, [], EntriesLoaded: false, TenancyActive: false);

    /// <summary>The entry of the current area, or <see langword="null"/> at Home or when the area is not shown.</summary>
    public ExplorerAreaEntry? CurrentEntry =>
        Address.Area is { } key ? Entries.FirstOrDefault(entry => string.Equals(entry.Area.Key, key, StringComparison.Ordinal)) : null;
}
