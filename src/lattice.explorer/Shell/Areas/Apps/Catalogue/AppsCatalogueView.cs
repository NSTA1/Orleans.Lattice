using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;

/// <summary>
/// What the catalogue shows, read from and written to its address: the query string
/// is the state, so every view is linkable.
/// </summary>
/// <param name="SourceKey">The selected source's key, or <see langword="null"/> for every source.</param>
/// <param name="Filter">Which apps to show relative to the tenant's installations.</param>
/// <param name="Text">The text filter, or <see langword="null"/>.</param>
internal sealed record AppsCatalogueView(string? SourceKey, AvailableAppFilter Filter, string? Text)
{
    /// <summary>Every source, every app, no text.</summary>
    public static AppsCatalogueView Default { get; } = new(null, AvailableAppFilter.All, null);

    /// <summary>Reads the view from a catalogue address.</summary>
    /// <param name="address">The address.</param>
    public static AppsCatalogueView FromAddress(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);

        var source = address.GetQuery(AppsRoutes.SourceQuery);
        var text = address.GetQuery(AppsRoutes.TextQuery);
        return new AppsCatalogueView(
            string.IsNullOrWhiteSpace(source) || string.Equals(source, AppsRoutes.AllSources, StringComparison.Ordinal) ? null : source,
            AppsRoutes.ReadFilter(address.GetQuery(AppsRoutes.FilterQuery)),
            string.IsNullOrWhiteSpace(text) ? null : text.Trim());
    }

    /// <summary>The listing request for this view.</summary>
    /// <param name="textHonoured">Whether the selected source(s) honour a text filter; otherwise it is not sent.</param>
    /// <param name="continuation">The continuation of the page to read, or <see langword="null"/> for the first.</param>
    public AvailableAppQuery ToQuery(bool textHonoured, string? continuation = null) => new()
    {
        SourceKey = SourceKey,
        Filter = Filter,
        Text = textHonoured ? Text : null,
        PageSize = AvailableAppQuery.DefaultPageSize,
        Continuation = continuation,
    };
}
