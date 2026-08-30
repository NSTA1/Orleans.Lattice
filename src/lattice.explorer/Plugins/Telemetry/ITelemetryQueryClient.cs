using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// The Explorer's transport seam onto the cluster's telemetry facade: curated
/// catalogue discovery and curated query evaluation, and nothing else.
/// </summary>
/// <remarks>
/// <para>
/// This is the <em>only</em> type in the seam that speaks the control API's own
/// vocabulary. Everything a panel touches is projected onto the Explorer's own
/// model behind <see cref="ITelemetryDomain"/>, and a reflection guard fails the
/// build if a wire type ever becomes reachable from there.
/// </para>
/// <para>
/// <b>Transport only.</b> The client forwards the visibility - and, for an
/// operator, the tenant - the caller <em>requests</em>, and returns whatever
/// scope the facade <em>pinned</em>. It derives no tenant, substitutes none, and
/// filters no series by one.
/// </para>
/// </remarks>
public interface ITelemetryQueryClient
{
    /// <summary>
    /// Reads the curated named-query catalogue the caller may select from, in
    /// ascending query-id order.
    /// </summary>
    /// <remarks>
    /// A cluster with no telemetry backend configured, and a caller entitled to
    /// run none of the entries, both report
    /// <see cref="TelemetryQueryCatalog.Empty"/> rather than failing - the two
    /// are deliberately indistinguishable.
    /// </remarks>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's catalogue; empty when it may run none.</returns>
    Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Evaluates the curated query the request names, with the bounded parameters
    /// it supplies.
    /// </summary>
    /// <param name="request">The query selection and its bounded parameters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The evaluated series, the scope the facade applied, and the window it evaluated.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
    Task<TelemetryQueryResponse> QueryAsync(
        TelemetryQueryRequest request,
        CancellationToken cancellationToken = default);
}
