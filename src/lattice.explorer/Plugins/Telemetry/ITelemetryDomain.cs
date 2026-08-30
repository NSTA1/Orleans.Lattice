using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// The controlled domain model a telemetry plugin operates against: the
/// operations it may perform, the tenant visibility the head is currently
/// requesting, and the availability probe its gate uses. It is the whole of what
/// a plugin may reach - never the connection, never a channel, never a wire type.
/// </summary>
/// <remarks>
/// <para>
/// <b>The requested visibility is offered; the effective one is reported.</b>
/// <see cref="RequestedVisibility"/> is what the head would <em>like</em>, taken
/// from the shell's own tenant view, and a panel puts it on a request. What was
/// actually applied comes back on
/// <see cref="ExplorerTelemetryResult.Scope"/>, decided by the facade. There is
/// deliberately no member here that reports an effective tenant, because a panel
/// that read one from the client rather than from the response would be reading a
/// value nothing had validated.
/// </para>
/// <para>
/// <b>Nothing here filters.</b> A panel receives every series the facade
/// returned. Narrowing by tenant on a desktop head is trivially bypassable, which
/// is precisely why the facade is routable and enforces scope itself.
/// </para>
/// </remarks>
public interface ITelemetryDomain
{
    /// <summary>The operations a telemetry panel performs.</summary>
    ITelemetryQueryService Queries { get; }

    /// <summary>
    /// <see langword="true"/> when the head has tenant scoping enabled, so a panel
    /// may offer a visibility control at all. On a deployment without the tenancy
    /// add-on there is one tenant and nothing to choose between.
    /// </summary>
    bool IsTenancyEnabled { get; }

    /// <summary>
    /// The visibility the head is currently requesting, for a panel to put on its
    /// requests. It is a request the facade re-validates, never a granted scope.
    /// </summary>
    ExplorerTelemetryVisibility RequestedVisibility { get; }

    /// <summary>
    /// Probes whether this deployment has a telemetry surface worth rendering.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The access decision.</returns>
    ValueTask<ExplorerPluginAccess> ProbeAvailabilityAsync(CancellationToken cancellationToken = default);
}
