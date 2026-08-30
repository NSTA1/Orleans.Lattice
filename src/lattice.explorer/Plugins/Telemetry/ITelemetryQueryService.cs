namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The operations a telemetry panel performs, in Explorer terms: discover what
/// the cluster offers, and evaluate one of the entries it offered.
/// </summary>
/// <remarks>
/// <para>
/// <b>Discovery comes first, and drives everything.</b> A panel reads the
/// catalogue and renders the titles, units, semantics, and bounds the
/// <em>server</em> published; it does not carry a list of query ids of its own.
/// A panel label therefore cannot drift from the instrument behind it.
/// </para>
/// <para>
/// Every operation reports outcomes as a
/// <see cref="TelemetryOperationResult{TValue}"/> rather than by throwing, so a
/// panel renders a classified failure instead of unwinding. Only a caller's own
/// cancellation, and an argument the seam rejects outright, escape as exceptions.
/// </para>
/// </remarks>
public interface ITelemetryQueryService
{
    /// <summary>
    /// Reads the curated catalogue the cluster offers this caller, answering from
    /// the last successful read when there is one, so a polling panel discovers
    /// once rather than on every tick.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The catalogue, which is empty - and successfully so - when the cluster has
    /// no telemetry backend configured or offers this caller nothing.
    /// </returns>
    ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>> GetCatalogAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Re-reads the catalogue from the cluster, discarding the remembered one.
    /// Use it after a reconnect or a sign-in, when what the caller is offered may
    /// have changed.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The freshly read catalogue.</returns>
    ValueTask<TelemetryOperationResult<ExplorerTelemetryCatalog>> RefreshCatalogAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Evaluates one catalogue entry with the bounded parameters the request
    /// supplies, and returns the series together with the scope the facade
    /// actually pinned.
    /// </summary>
    /// <remarks>
    /// The requested visibility and tenant travel unchanged; the facade
    /// re-validates both and reports what it applied on
    /// <see cref="ExplorerTelemetryResult.Scope"/>. Nothing is filtered here.
    /// </remarks>
    /// <param name="request">The entry to evaluate and its bounded parameters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The evaluated result, or a classified failure.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
    ValueTask<TelemetryOperationResult<ExplorerTelemetryResult>> QueryAsync(
        ExplorerTelemetryRequest request,
        CancellationToken cancellationToken = default);
}
