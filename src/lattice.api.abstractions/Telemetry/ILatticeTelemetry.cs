namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Transport-agnostic <b>telemetry</b> facade: the read-only time-series surface
/// over the cluster's metrics backend. A caller discovers the curated queries it
/// may run and then evaluates one by id with bounded parameters. Every transport
/// binding (the gRPC service, the MCP tool group, and the Explorer's client seam)
/// is a thin adapter over this one surface, so the curated, tenant-scoped,
/// fail-closed semantics are written and tested once.
/// </summary>
/// <remarks>
/// <para>
/// <b>Curated queries only.</b> The facade exposes a server-authored catalogue and
/// evaluates entries selected by id. It accepts no query text from any caller, on
/// any transport, in any deployment mode. There is no request field that carries
/// an expression, so the rule is enforced by the contract's shape rather than by a
/// sanitiser that has to be trusted to be complete.
/// </para>
/// <para>
/// <b>Tenant scoping is derived, never asserted.</b> The effective tenant comes
/// from the authenticated caller. A caller may request a wider or elsewhere scope
/// on <see cref="TelemetryQueryRequest.RequestedVisibility"/> - and, for
/// <see cref="TelemetryTenantVisibility.SingleTenant"/>, name the tenant it wants
/// on <see cref="TelemetryQueryRequest.RequestedTenantId"/> - which the facade
/// honours only after validating it as a platform operator server-side; an
/// unvalidated request degrades, fail-closed, to the caller's active tenant and
/// the requested tenant id is ignored entirely. What was actually applied is
/// always reported on <see cref="TelemetryQueryResponse.Scope"/>. No operation
/// takes a tenant on trust from a request.
/// </para>
/// <para>
/// <b>Leak-free discovery.</b> <see cref="GetCatalogAsync"/> returns only the
/// entries the caller is entitled to run, and
/// <see cref="QueryAsync"/> rejects any other id with the same
/// <see cref="TelemetryQueryNotFoundException"/> it raises for an id that does not
/// exist, so the two cases are indistinguishable.
/// </para>
/// <para>
/// <b>Read-only.</b> The facade evaluates queries. It never writes metrics,
/// evaluates alerts, or manages recording rules.
/// </para>
/// </remarks>
public interface ILatticeTelemetry
{
    /// <summary>
    /// Reads the curated named-query catalogue the current caller may select from,
    /// with entries in ascending <see cref="TelemetryQueryDescriptor.QueryId"/>
    /// order. The catalogue is scoped to the caller's entitlement, so an entry the
    /// caller may not run is absent rather than present-and-denied. A cluster with
    /// no telemetry backend configured reports
    /// <see cref="TelemetryQueryCatalog.Empty"/> rather than failing, so a client
    /// degrades to rendering no panels.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's catalogue; <see cref="TelemetryQueryCatalog.Empty"/> when it may run none.</returns>
    Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Evaluates the curated query named by
    /// <see cref="TelemetryQueryRequest.QueryId"/> with the bounded parameters the
    /// request supplies, under the tenant scope the facade derives from the
    /// authenticated caller.
    /// </summary>
    /// <param name="request">The query selection and its bounded parameters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The evaluated series together with the tenant scope actually applied and the
    /// window actually evaluated.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
    /// <exception cref="TelemetryQueryNotFoundException">
    /// No such query is registered, or it is not offered to this caller - the two
    /// are deliberately indistinguishable.
    /// </exception>
    /// <exception cref="TelemetryQueryBoundsException">
    /// The requested window violates the bounds the catalogue entry declares.
    /// </exception>
    Task<TelemetryQueryResponse> QueryAsync(
        TelemetryQueryRequest request,
        CancellationToken cancellationToken = default);
}
