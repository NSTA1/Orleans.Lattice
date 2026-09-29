namespace Orleans.Lattice.Explorer.UI.Navigation.Completion;

/// <summary>Chooses which completion sources answer a given kind of input.</summary>
internal static class AddressCompletionPlanner
{
    /// <summary>The group key of the chrome's own area-name source.</summary>
    public const string AreasSourceKey = "areas";

    /// <summary>The group key of the chrome's own tenant source.</summary>
    public const string TenantsSourceKey = "tenants";

    /// <summary>
    /// The sources to ask for <paramref name="mode"/>: for a free search, the
    /// visible areas by name and every visible area's own source; for <c>a/</c>
    /// or a literal address, every visible area's own source; for <c>t/</c>, the
    /// tenants; and for the command palette, none.
    /// </summary>
    /// <param name="mode">The input's mode.</param>
    /// <param name="entries">The directory's shown stops. Unavailable areas are never asked.</param>
    /// <param name="tenants">The chrome's tenant source.</param>
    public static IReadOnlyList<AddressCompletionSourceEntry> SourcesFor(
        AddressQueryMode mode,
        IReadOnlyList<ExplorerAreaEntry> entries,
        IAddressCompletionSource tenants)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(tenants);

        switch (mode)
        {
            case AddressQueryMode.Command:
                return [];

            case AddressQueryMode.Tenant:
                return [new AddressCompletionSourceEntry(TenantsSourceKey, "Tenants", tenants)];
        }

        var sources = new List<AddressCompletionSourceEntry>(entries.Count + 1);
        if (mode == AddressQueryMode.Search)
        {
            sources.Add(new AddressCompletionSourceEntry(AreasSourceKey, "Areas", new AreaCompletionSource(entries)));
        }

        foreach (var entry in entries)
        {
            if (entry.IsVisible && entry.Area.Completions is { } source)
            {
                sources.Add(new AddressCompletionSourceEntry(entry.Area.Key, entry.Area.DisplayName, source));
            }
        }

        return sources;
    }
}
