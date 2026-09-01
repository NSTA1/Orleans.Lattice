using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// A detail surface with a deliberately long label, registered so the per-selection
/// tab strip genuinely overflows at narrow widths.
/// <para>
/// <b>Why a plugin rather than a stub strip.</b> The permanent regression proof this
/// exists for is that the overflow menu lies wholly inside the viewport at every width
/// from 320px up - the audit measured it clipped by a constant 25.2px right across the
/// compact band. Proving that in a browser needs a real overflow menu, and the four
/// surfaces the product ships for a tree ("Data", "Topology", "Metrics", "Dead-letter")
/// measure narrow enough to stay inline even at 320px, so no menu ever appears. This
/// plugin supplies the <i>condition</i>; everything being measured - the capacity
/// measurement, the promotion of the active item, the menu's geometry and its clamp -
/// is shipped code.
/// </para>
/// <para>
/// Ordered after every shipped surface so it can never displace "Data" as the surface
/// a tree opens on, which a sibling journey asserts.
/// </para>
/// </summary>
internal sealed class JourneyWideSurfacePlugin : IExplorerPlugin
{
    /// <summary>The plugin id.</summary>
    internal const string PluginId = "reconciliation";

    /// <summary>
    /// The surface label. Long on purpose: inline capacity is measured from label
    /// length, so this is what pushes the strip past the width a phone viewport has.
    /// </summary>
    internal const string SurfaceLabel = "Reconciliation and settlement history";

    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = PluginId,
        Label = SurfaceLabel,
        Surface = ExplorerPluginSurface.Selection,
        Order = 9000,
        SelectionKinds = ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View,
    };

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(JourneyLedgerView);

    /// <inheritdoc />
    public Type? DomainContract => null;

    /// <inheritdoc />
    public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
}
