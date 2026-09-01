using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The area behind <see cref="JourneyLedgerGate"/>: one destination whose reachability
/// genuinely depends on who is signed in, so the journeys can watch the rail move it
/// between prominent, demoted and open.
/// </summary>
/// <param name="gate">The four-state gate deciding this area's reachability.</param>
internal sealed class JourneyLedgerPlugin(JourneyLedgerGate gate) : IExplorerPlugin
{
    /// <summary>The plugin id, which is also the route slug the shell derives from it.</summary>
    internal const string PluginId = "ledger";

    /// <summary>The area label the rail renders.</summary>
    internal const string AreaLabel = "Ledger";

    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = PluginId,
        Label = AreaLabel,
        Surface = ExplorerPluginSurface.Area,
        Order = 950,
    };

    private readonly JourneyLedgerGate _gate = gate ?? throw new ArgumentNullException(nameof(gate));

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(JourneyLedgerView);

    /// <inheritdoc />
    public Type? DomainContract => null;

    /// <inheritdoc />
    public IExplorerPluginAccessGate AccessGate => _gate;
}
