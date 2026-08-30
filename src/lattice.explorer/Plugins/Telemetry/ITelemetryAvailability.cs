using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// Answers whether this deployment has a telemetry surface worth rendering at
/// all, in the four-state vocabulary a plugin gate speaks.
/// </summary>
public interface ITelemetryAvailability
{
    /// <summary>
    /// Probes the cluster's telemetry facade and reports what a telemetry plugin
    /// should do: render, ask for a sign-in, show a denial, or disappear.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The access decision. Never throws for a probe failure.</returns>
    ValueTask<ExplorerPluginAccess> ProbeAsync(CancellationToken cancellationToken = default);
}
