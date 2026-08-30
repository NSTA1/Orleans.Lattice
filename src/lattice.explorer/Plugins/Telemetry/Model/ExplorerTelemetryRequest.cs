namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// A panel's request to evaluate one curated query: the catalogue id it selected
/// and the bounded parameters the entry declares. There is no field for query
/// text, so a panel can only select a server-authored query, never compose one.
/// </summary>
/// <remarks>
/// <para>
/// <b>A defaulted request is the normal case.</b> Only
/// <see cref="QueryId"/> is required; everything else defaults, and the facade
/// then supplies the window, the step, and the fail-closed active-tenant scope.
/// A panel sends exactly this before a user has touched a control, so the seam
/// must forward the defaults as defaults rather than expanding them into
/// concrete values of its own.
/// </para>
/// <para>
/// <b>The two tenancy fields are requests, not assertions.</b> The facade
/// re-validates both server-side and reports what it actually applied on
/// <see cref="ExplorerTelemetryResult.Scope"/>. The seam forwards them
/// unchanged: it neither widens them nor narrows them, and it never filters a
/// returned series by tenant, because enforcing scope on a desktop head is the
/// bypassable path a routable facade exists to prevent.
/// </para>
/// </remarks>
public sealed record ExplorerTelemetryRequest
{
    /// <summary>The catalogue id of the entry to evaluate.</summary>
    public required string QueryId { get; init; }

    /// <summary>
    /// The window to evaluate. Left unset by default, which asks the facade for
    /// the entry's own default window and step.
    /// </summary>
    public ExplorerTelemetryWindow Window { get; init; }

    /// <summary>
    /// An optional single logical tree id to narrow to, honoured only when the
    /// entry declares <see cref="ExplorerTelemetryParameters.TreeFilter"/>. It
    /// narrows within the effective tenant scope and can never widen it.
    /// </summary>
    public string? TreeId { get; init; }

    /// <summary>
    /// The tenant visibility the panel would like. Defaults, fail-closed, to
    /// <see cref="ExplorerTelemetryVisibility.ActiveTenant"/>.
    /// </summary>
    public ExplorerTelemetryVisibility RequestedVisibility { get; init; }

    /// <summary>
    /// The tenant a platform operator would like the query evaluated against,
    /// used only when <see cref="RequestedVisibility"/> is
    /// <see cref="ExplorerTelemetryVisibility.SingleTenant"/> and honoured only
    /// after the facade validates the caller server-side. Ignored entirely for
    /// every other visibility.
    /// </summary>
    public string? RequestedTenantId { get; init; }

    /// <summary>
    /// Creates the simplest possible request: a query id and nothing else, with
    /// every parameter left for the facade to default.
    /// </summary>
    /// <param name="queryId">The catalogue id to evaluate.</param>
    /// <returns>A fully defaulted request.</returns>
    /// <exception cref="ArgumentException"><paramref name="queryId"/> is <see langword="null"/> or empty.</exception>
    public static ExplorerTelemetryRequest For(string queryId)
    {
        ArgumentException.ThrowIfNullOrEmpty(queryId);
        return new ExplorerTelemetryRequest { QueryId = queryId };
    }
}
