using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Telemetry.Views;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The Telemetry area as a plugin: its descriptor, the panel the shell renders
/// for it, the controlled domain contract it operates against, and the access
/// gate this package owns. The shell learns of it only through
/// <see cref="IExplorerPlugin"/>, so registering or withholding this type is the
/// whole of the head's opt-in.
/// <para>
/// It declares <see cref="ITelemetryDomain"/> through
/// <see cref="IExplorerPlugin{TDomain}"/>, so the reach of the whole surface is
/// a compile-time fact stated once in this signature (epic decision D3): the
/// panel resolves that one contract from its bound host context and receives
/// nothing else from the host - no cluster connection, no gRPC channel, no
/// telemetry wire type.
/// </para>
/// </summary>
/// <param name="gate">
/// The telemetry seam's availability probe, which doubles as this plugin's
/// gate. It reads the catalogue - the cheapest call on the surface, and the one
/// call every caller is entitled to make - and resolves all four access states
/// from the answer.
/// </param>
public sealed class TelemetryAreaPlugin(TelemetryAvailability gate) : IExplorerPlugin<ITelemetryDomain>
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = TelemetryPluginKeys.PluginId,
        Label = "Telemetry",
        Surface = ExplorerPluginSurface.Area,

        // After the areas that administer something: Backups (100), Access
        // (200), Schema (300), Tenants (400), My Tenant (500). This one reads
        // and changes nothing, and - like My Tenant - it is a surface that
        // disappears entirely on a deployment that does not serve it, so it
        // sits at the end rather than leaving a gap mid-strip when it does.
        // 600 rather than a shared number because two areas claiming one Order
        // hands their relative position to an arbitrary tie-break instead of to
        // intent, which the assembled-host guard fails on.
        Order = 600,
    };

    private readonly TelemetryAvailability _gate = gate ?? throw new ArgumentNullException(nameof(gate));

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(TelemetryPanel);

    /// <inheritdoc />
    /// <remarks>
    /// The gate resolves all four states: allowed when the cluster offers this
    /// caller at least one query, an
    /// <see cref="ExplorerPluginAccessState.AuthenticationRequired"/> for a
    /// connection carrying no accepted credential - so the shell offers a
    /// sign-in rather than an inert grey-out - and
    /// <see cref="ExplorerPluginAccessState.Unavailable"/> both for a cluster
    /// serving no telemetry facade and for one offering this caller nothing,
    /// which the facade deliberately makes indistinguishable.
    /// </remarks>
    public IExplorerPluginAccessGate AccessGate => _gate;
}
