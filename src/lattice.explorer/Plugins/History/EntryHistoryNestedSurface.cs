using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.History;

/// <summary>
/// The per-key revision timeline, contributed as a <em>nested</em> per-selection
/// view rather than as a tier tab.
/// <para>
/// This is deliberate and matches the shipped product: the timeline has never
/// been a tab. An operator reaches it from a row on the value drill-down
/// surface, through that row's History button, for the key they drilled into.
/// Registering it in the strip would add a tab that does not exist today, which
/// would be a behaviour change dressed up as a conversion.
/// </para>
/// <para>
/// Contributing it through <see cref="ISelectionNestedSurface"/> is what lets it
/// ship as its own package anyway: the hosting surface renders whatever the
/// registry returns for
/// <see cref="SelectionNestedSurfaceKeys.EntryHistory"/> and neither package
/// references the other. A head that does not register this one simply gets no
/// History button.
/// </para>
/// </summary>
public sealed class EntryHistoryNestedSurface : ISelectionNestedSurface
{
    /// <inheritdoc />
    public string SurfaceId => SelectionNestedSurfaceKeys.EntryHistory;

    /// <inheritdoc />
    public Type ViewType => typeof(HistoryTab);
}
