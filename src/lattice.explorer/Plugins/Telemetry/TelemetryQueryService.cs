namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The production <see cref="ITelemetryQueryService"/>. Reads the server-authored
/// catalogue, remembers it for the life of the circuit, projects every wire
/// answer onto the Explorer's own model, and classifies every fault into a status
/// a panel can act on.
/// </summary>
/// <remarks>
/// <para>
/// <b>Discovery is remembered; evaluation is not.</b> The catalogue changes only
/// when the cluster's offering does, so re-reading it on every poll would be pure
/// waste; an evaluation is the whole point of a poll and is always sent. A
/// remembered catalogue is returned through a completed
/// <see cref="ValueTask{TResult}"/>, so a panel that discovers on every render
/// pays no allocation at all for it.
/// </para>
/// <para>
/// <b>The catalogue is only ever remembered on success.</b> A failure is
/// returned and forgotten, so a transient outage does not pin a panel to an error
/// for the rest of the session.
/// </para>
/// <para>
/// <b>The seam never invents a discovery answer from an execution one, or the
/// reverse.</b> A query id the remembered catalogue does not list is still sent:
/// the facade decides whether it exists, and answering "unknown" locally would
/// mean a stale cache could hide a query the cluster does offer. The only thing
/// checked locally is the window, against bounds the server itself published, and
/// only when the caller chose one.
/// </para>
/// </remarks>
/// <param name="client">The transport seam. Must not be <see langword="null"/>.</param>
public sealed class TelemetryQueryService(ITelemetryQueryClient client) : ITelemetryQueryService
{
    private const string CatalogRead = "Read the telemetry catalogue.";

    // A fixed literal rather than an interpolated one: this is the message of the
    // single result a panel allocates per poll, and the query id it would name is
    // already on the result.
    private const string QueryEvaluated = "Evaluated the telemetry query.";

    private readonly ITelemetryQueryClient _client = client ?? throw new ArgumentNullException(nameof(client));

    private TelemetryOperationResult<ExplorerTelemetryCatalog>? _catalog;

    /// <inheritdoc />
    public ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>> GetCatalogAsync(
        CancellationToken cancellationToken = default)
    {
        var remembered = Volatile.Read(ref _catalog);
        return remembered is not null
            ? new ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>>(remembered)
            : new ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>>(ReadCatalogAsync(cancellationToken));
    }

    /// <inheritdoc />
    public ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>> RefreshCatalogAsync(
        CancellationToken cancellationToken = default)
    {
        Volatile.Write(ref _catalog, null);
        return new ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>>(ReadCatalogAsync(cancellationToken));
    }

    /// <inheritdoc />
    public ValueTask<TelemetryOperationResult<ExplorerTelemetryResult>> QueryAsync(
        ExplorerTelemetryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // A request with no query id selects nothing, so there is nothing to send.
        // It is reported as a classified failure rather than thrown, because a
        // panel binding a control to an unset selection should render a message
        // rather than unwind. The required-property contract makes this reachable
        // only through an initializer that explicitly set it to nothing.
        if (string.IsNullOrEmpty(request.QueryId))
        {
            return new ValueTask<TelemetryOperationResult<ExplorerTelemetryResult>>(
                TelemetryOperationResult<ExplorerTelemetryResult>.Failure(
                    TelemetryQueryStatus.InvalidRequest,
                    "A telemetry request must name the catalogue entry to evaluate."));
        }

        var refused = RefuseOutOfBoundsWindow(request);
        return refused is not null
            ? new ValueTask<TelemetryOperationResult<ExplorerTelemetryResult>>(refused)
            : new ValueTask<TelemetryOperationResult<ExplorerTelemetryResult>>(
                EvaluateAsync(request, cancellationToken));
    }

    private async Task<TelemetryOperationResult<ExplorerTelemetryCatalog>> ReadCatalogAsync(
        CancellationToken cancellationToken)
    {
        try
        {
            var catalog = await _client.GetCatalogAsync(cancellationToken).ConfigureAwait(false);
            var result = TelemetryOperationResult<ExplorerTelemetryCatalog>.Success(
                TelemetryProjection.ToCatalog(catalog),
                CatalogRead);
            Volatile.Write(ref _catalog, result);
            return result;
        }
        catch (Exception ex) when (TelemetryFaultMapper.IsFault(ex, cancellationToken))
        {
            return TelemetryFaultMapper.Fail<ExplorerTelemetryCatalog>(ex);
        }
    }

    private async Task<TelemetryOperationResult<ExplorerTelemetryResult>> EvaluateAsync(
        ExplorerTelemetryRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            var response = await _client
                .QueryAsync(TelemetryProjection.ToWireRequest(request), cancellationToken)
                .ConfigureAwait(false);
            return TelemetryOperationResult<ExplorerTelemetryResult>.Success(
                TelemetryProjection.ToResult(response),
                QueryEvaluated);
        }
        catch (Exception ex) when (TelemetryFaultMapper.IsFault(ex, cancellationToken))
        {
            return TelemetryFaultMapper.Fail<ExplorerTelemetryResult>(ex);
        }
    }

    /// <summary>
    /// Refuses a window the selected entry's published bounds already rule out,
    /// naming the specific limit, or returns <see langword="null"/> to send the
    /// request.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three conditions must all hold before anything is refused, and each one
    /// exists to stop a specific way this check could do harm:
    /// </para>
    /// <list type="number">
    /// <item>
    /// <b>The caller chose a window.</b> An unset window is a request for the
    /// facade's own default, and expanding it locally into a concrete one - to an
    /// entry's maximum range, say, which at the default step overruns its point
    /// budget - would turn the first request every panel makes into a refusal.
    /// </item>
    /// <item>
    /// <b>The bounds are already known.</b> Only a catalogue already read is
    /// consulted; no discovery call is made to check a window, so evaluating one
    /// query never silently costs two round trips.
    /// </item>
    /// <item>
    /// <b>The entry is one the remembered catalogue lists.</b> An id it does not
    /// list is sent rather than refused, because whether a query exists is the
    /// facade's answer to give and a stale cache must not be able to hide one.
    /// </item>
    /// </list>
    /// <para>
    /// The check is clock-independent, so it neither depends on the head's clock
    /// agreeing with the cluster's nor rejects the default window for being older
    /// than any retention limit. It grants nothing: the facade re-checks every
    /// request it is sent.
    /// </para>
    /// </remarks>
    private TelemetryOperationResult<ExplorerTelemetryResult>? RefuseOutOfBoundsWindow(ExplorerTelemetryRequest request)
    {
        if (request.Window.IsUnset)
        {
            return null;
        }

        if (Volatile.Read(ref _catalog)?.Value is not { } catalog
            || !catalog.TryGetQuery(request.QueryId, out var query))
        {
            return null;
        }

        var violation = query.Bounds.ValidateWithoutClock(request.Window);
        return violation == ExplorerTelemetryBoundsViolation.None
            ? null
            : TelemetryOperationResult<ExplorerTelemetryResult>.Failure(
                TelemetryQueryStatus.OutOfBounds,
                $"The requested window is outside the bounds '{query.QueryId}' declares: {violation}.",
                violation);
    }
}
