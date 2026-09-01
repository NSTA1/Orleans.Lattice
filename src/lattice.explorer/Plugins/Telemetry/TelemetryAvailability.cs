using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The telemetry seam's availability probe, and the access gate a telemetry
/// plugin declares. It reads the catalogue - the cheapest call on the surface,
/// and the one call every caller is entitled to make - and reports what it
/// found; <see cref="ExplorerPluginAccessContract"/> turns that into one of the
/// four access states.
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
/// No fault escapes a probe: every classified failure becomes a fact, so a gate
/// never has to catch anything.
/// </para>
/// </remarks>
/// <param name="queries">The telemetry operations surface. Must not be <see langword="null"/>.</param>
/// <param name="session">
/// The Explorer's sign-in seam, read only to tell an anonymous refusal from an
/// authenticated one.
/// </param>
public sealed class TelemetryAvailability(
    ITelemetryQueryService queries,
    IExplorerAuthSession? session = null)
    : ExplorerPluginAccessGate, ITelemetryAvailability
{
    private const string NothingOffered =
        "This cluster offers no telemetry queries.";

    private const string Unreachable =
        "The telemetry surface could not be reached.";

    private const string Disconnected =
        "The explorer is not connected to a cluster.";

    /// <summary>
    /// The grant a refused caller is missing, and who issues it. Cached, so
    /// attaching it to a denial costs nothing per probe.
    /// </summary>
    private static readonly ExplorerAccessRemedy MissingGrant =
        ExplorerAccessRemedy.Requiring("Telemetry", ExplorerVocabulary.GrantAudience);

    private readonly ITelemetryQueryService _queries = queries ?? throw new ArgumentNullException(nameof(queries));

    private readonly IExplorerAuthSession? _session = session;

    /// <inheritdoc />
    public override ExplorerAccessRemedy Remedy => MissingGrant;

    /// <inheritdoc />
    protected override bool IsCallerAuthenticated => _session?.IsAuthenticated ?? false;

    /// <inheritdoc />
    public ValueTask<ExplorerPluginAccess> ProbeAsync(CancellationToken cancellationToken = default)
    {
        var pending = EvaluateCatalogAsync(cancellationToken);
        return pending.IsCompletedSuccessfully
            ? new ValueTask<ExplorerPluginAccess>(Resolve(pending.Result))
            : ResolveCatalogAsync(pending);
    }

    /// <inheritdoc />
    protected override ValueTask<ExplorerPluginAccessFacts> EvaluateAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken) =>
        // The shell already knows the connection is down, so there is nothing to
        // learn from a call that can only fail. A later connection-status change
        // re-probes.
        context.Connection.IsUsable
            ? EvaluateCatalogAsync(cancellationToken)
            : new ValueTask<ExplorerPluginAccessFacts>(ExplorerPluginAccessFacts.CapabilityAbsent(Disconnected));

    private async ValueTask<ExplorerPluginAccessFacts> EvaluateCatalogAsync(CancellationToken cancellationToken)
    {
        var catalog = await _queries.GetCatalogAsync(cancellationToken).ConfigureAwait(false);

        // A seam that answered nothing proved nothing, and there is no catalogue
        // to render either way.
        if (catalog is null)
        {
            return ExplorerPluginAccessFacts.CapabilityAbsent(Unreachable);
        }

        if (!catalog.IsSuccess)
        {
            return Classify(catalog);
        }

        return catalog.Value is { IsEmpty: false }
            ? ExplorerPluginAccessFacts.Granted
            : ExplorerPluginAccessFacts.CapabilityAbsent(NothingOffered);
    }

    private async ValueTask<ExplorerPluginAccess> ResolveCatalogAsync(ValueTask<ExplorerPluginAccessFacts> pending) =>
        Resolve(await pending.ConfigureAwait(false));

    private ExplorerPluginAccess Resolve(in ExplorerPluginAccessFacts facts) =>
        ExplorerPluginAccessContract.Resolve(facts, MissingGrant, IsCallerAuthenticated);

    private static ExplorerPluginAccessFacts Classify(TelemetryOperationResult catalog)
    {
        var reason = string.IsNullOrWhiteSpace(catalog.Message) ? null : catalog.Message;

        return catalog.Status switch
        {
            // The facade is not registered here, or could not be reached at all.
            // Either way there is no telemetry surface to render.
            TelemetryQueryStatus.Unavailable => ExplorerPluginAccessFacts.CapabilityAbsent(reason),
            TelemetryQueryStatus.BackendUnavailable =>
                ExplorerPluginAccessFacts.CapabilityAbsent(reason ?? Unreachable),
            TelemetryQueryStatus.AuthenticationRequired => ExplorerPluginAccessFacts.CredentialMissing(reason),
            _ => ExplorerPluginAccessFacts.Withhold(reason ?? Unreachable),
        };
    }
}
