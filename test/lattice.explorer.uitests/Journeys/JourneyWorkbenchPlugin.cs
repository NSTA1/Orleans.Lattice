using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// A second, always-reachable area for the journey head.
/// <para>
/// Every area the product ships gates on a cluster the journey head does not have, so
/// without this the rail offers exactly one area and "switch area and come back" has
/// nowhere to go. Registering one plugin whose gate is the framework's own
/// <see cref="ExplorerPluginAccessGates.Allowed"/> gives the rail a genuine second
/// destination, driven entirely through the published plugin seam - the shell's
/// routing, area restoration and rail rendering are untouched and real.
/// </para>
/// </summary>
internal sealed class JourneyWorkbenchPlugin : IExplorerPlugin
{
    /// <summary>The plugin id, which is also the route slug the shell derives from it.</summary>
    internal const string PluginId = "workbench";

    /// <summary>The area label the rail renders.</summary>
    internal const string AreaLabel = "Workbench";

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor { get; } = new()
    {
        PluginId = PluginId,
        Label = AreaLabel,
        Surface = ExplorerPluginSurface.Area,
        Order = 900,
    };

    /// <inheritdoc />
    public Type ViewType => typeof(JourneyWorkbenchView);

    /// <inheritdoc />
    public Type? DomainContract => null;

    /// <inheritdoc />
    public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
}
