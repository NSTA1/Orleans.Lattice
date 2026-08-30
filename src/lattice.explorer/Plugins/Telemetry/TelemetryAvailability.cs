using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The telemetry seam's availability probe, and the access gate a telemetry
/// plugin declares. It reads the catalogue - the cheapest call on the surface,
/// and the one call every caller is entitled to make - and turns the answer into
/// one of the four access states.
/// </summary>
/// <remarks>
/// <para>
/// <b>An empty catalogue means unavailable, not denied.</b> The facade reports an
/// empty catalogue both for a cluster with no metrics backend configured and for
/// a caller entitled to run nothing, and it makes the two deliberately
/// indistinguishable so a caller cannot probe its own entitlement. The probe
/// therefore must not guess which it was: it reports that there is nothing here
/// to render, which is true either way, rather than accusing the caller of
/// lacking permission it may well have.
/// </para>
/// <para>
/// <b>Unreachable also means unavailable.</b> Reading the catalogue never fails
/// for a backend fault - the facade degrades to an empty catalogue instead - so a
/// transport failure on this call means the endpoint or the facade could not be
/// reached. A telemetry surface then disappears rather than rendering an error a
/// user cannot act on.
/// </para>
/// <para>
/// No fault escapes a probe: every classified failure becomes an access state, so
/// a gate never has to catch anything.
/// </para>
/// </remarks>
/// <param name="queries">The telemetry operations surface. Must not be <see langword="null"/>.</param>
public sealed class TelemetryAvailability(ITelemetryQueryService queries)
    : ITelemetryAvailability, IExplorerPluginAccessGate
{
    private const string NothingOffered =
        "This cluster offers no telemetry queries.";

    private const string Unreachable =
        "The telemetry surface could not be reached.";

    private const string Disconnected =
        "The explorer is not connected to a cluster.";

    private readonly ITelemetryQueryService _queries = queries ?? throw new ArgumentNullException(nameof(queries));

    /// <inheritdoc />
    public async ValueTask<ExplorerPluginAccess> ProbeAsync(CancellationToken cancellationToken = default)
    {
        var catalog = await _queries.GetCatalogAsync(cancellationToken).ConfigureAwait(false);
        if (!catalog.IsSuccess)
        {
            return Classify(catalog);
        }

        return catalog.Value is { IsEmpty: false }
            ? ExplorerPluginAccess.Allowed
            : ExplorerPluginAccess.ReportUnavailable(NothingOffered);
    }

    /// <inheritdoc />
    public ValueTask<ExplorerPluginAccess> ProbeAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        // The shell already knows the connection is down, so there is nothing to
        // learn from a call that can only fail. A later connection-status change
        // re-probes.
        return context.Connection.IsUsable
            ? ProbeAsync(cancellationToken)
            : new ValueTask<ExplorerPluginAccess>(ExplorerPluginAccess.ReportUnavailable(Disconnected));
    }

    private static ExplorerPluginAccess Classify(TelemetryOperationResult catalog)
    {
        var reason = string.IsNullOrWhiteSpace(catalog.Message) ? null : catalog.Message;

        return catalog.Status switch
        {
            // The facade is not registered here, or could not be reached at all.
            // Either way there is no telemetry surface to render.
            TelemetryQueryStatus.Unavailable => ExplorerPluginAccess.ReportUnavailable(reason),
            TelemetryQueryStatus.BackendUnavailable => ExplorerPluginAccess.ReportUnavailable(reason ?? Unreachable),
            TelemetryQueryStatus.AuthenticationRequired => ExplorerPluginAccess.RequireAuthentication(reason),
            _ => ExplorerPluginAccess.Deny(reason ?? Unreachable),
        };
    }
}
