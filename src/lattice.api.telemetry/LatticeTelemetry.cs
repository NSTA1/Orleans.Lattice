using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The transport-neutral implementation of <see cref="ILatticeTelemetry"/>: it
/// serves the curated named-query catalogue and evaluates an entry selected by id,
/// under a tenant scope it derives server-side from the authenticated caller.
/// </summary>
/// <remarks>
/// <para>
/// <b>No caller-supplied query text, structurally.</b> The only query expressions
/// that exist are the server-authored templates in
/// <see cref="LatticeTelemetryQueries"/>. This type reaches
/// <see cref="IPrometheusQueryClient"/> only with a string rendered from a compiled
/// template, and the sole caller-supplied value that reaches a rendered query is
/// the optional tree filter, which is validated and escaped as a label value. There
/// is no code path from a request field to a query expression.
/// </para>
/// <para>
/// <b>The tenant is derived and pinned here.</b> Every evaluation carries the
/// tenant matcher <see cref="TelemetryTenantScopeResolver"/> decided, scoping on
/// the repository-wide derived <c>tenant</c> dimension rather than on a
/// <c>tree</c> regex, so platform-owned series can never leak into a tenant's
/// view. The scope actually applied - including any fail-closed degradation of a
/// widening request - is reported on every response.
/// </para>
/// <para>
/// <b>Bounds are clamped and then enforced.</b> The resolution step is clamped into
/// the entry's declared step budget; the window is then validated against the
/// entry's bounds and, separately, against the deployment-wide guardrails in
/// <see cref="TelemetryRangeGuardrails"/>, and a window outside either is rejected
/// rather than silently narrowed, because a panel rendered over a window the caller
/// did not ask for is a lie an operator cannot see.
/// </para>
/// <para>
/// <b>Not configured degrades, not fails.</b> A cluster with no telemetry backend
/// address configured serves <see cref="TelemetryQueryCatalog.Empty"/> and offers
/// no query, so a client renders no panels instead of erroring.
/// </para>
/// </remarks>
public sealed class LatticeTelemetry : ILatticeTelemetry
{
    /// <summary>
    /// The window a request falls back to when it supplies no start and its entry
    /// declares no usable point budget, so an unset window is still a bounded one.
    /// </summary>
    private static readonly TimeSpan DefaultUnboundedSpan = TimeSpan.FromHours(1);

    private readonly LatticeTelemetryQueryCatalog _catalog;
    private readonly TelemetryTenantScopeResolver _scopes;
    private readonly TelemetryAccessAuthorizer _authorizer;
    private readonly IPrometheusQueryClient _backend;
    private readonly LatticeTelemetryOptions _options;
    private readonly TimeProvider _time;

    /// <summary>
    /// Initializes the facade.
    /// </summary>
    /// <param name="catalog">The compiled named-query catalogue.</param>
    /// <param name="scopes">The server-side tenant-scope resolver.</param>
    /// <param name="authorizer">The fail-closed telemetry authorization seam.</param>
    /// <param name="backend">The read-only metrics-backend client.</param>
    /// <param name="options">The telemetry options carrying the backend address and guardrails.</param>
    /// <param name="timeProvider">
    /// The clock used to evaluate lookback bounds and to default an unset evaluation
    /// instant. Injected rather than read ambiently so bounds checking is
    /// deterministic under test.
    /// </param>
    /// <exception cref="ArgumentNullException">Any required argument is <see langword="null"/>.</exception>
    public LatticeTelemetry(
        LatticeTelemetryQueryCatalog catalog,
        TelemetryTenantScopeResolver scopes,
        TelemetryAccessAuthorizer authorizer,
        IPrometheusQueryClient backend,
        IOptions<LatticeTelemetryOptions> options,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(backend);
        ArgumentNullException.ThrowIfNull(options);

        _catalog = catalog;
        _scopes = scopes;
        _authorizer = authorizer;
        _backend = backend;
        _options = options.Value;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
    {
        if (!IsBackendConfigured)
        {
            return TelemetryQueryCatalog.Empty;
        }

        try
        {
            await _authorizer.AuthorizeClusterTelemetryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            // Discovery degrades rather than failing: a caller entitled to no query
            // receives the empty catalogue, exactly as one on a cluster with no
            // backend does, so a client renders no panels instead of erroring.
            return TelemetryQueryCatalog.Empty;
        }

        return _catalog.Catalog;
    }

    /// <inheritdoc />
    public async Task<TelemetryQueryResponse> QueryAsync(
        TelemetryQueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.QueryId);

        await _authorizer.AuthorizeClusterTelemetryAsync(cancellationToken).ConfigureAwait(false);

        // A cluster with no backend configured offers no query at all, so an id that
        // exists in the catalogue is still reported as unavailable - matching the
        // empty catalogue discovery returns.
        if (!IsBackendConfigured || !_catalog.TryGetPlan(request.QueryId, out var plan))
        {
            throw new TelemetryQueryNotFoundException(request.QueryId);
        }

        var descriptor = plan.Descriptor;
        var range = ResolveRange(descriptor, request.Range);
        var scope = await _scopes
            .ResolveAsync(request.RequestedVisibility, request.RequestedTenantId, cancellationToken)
            .ConfigureAwait(false);

        var selector = BuildSelector(descriptor, scope, request.TreeId);
        var window = TelemetryRateWindow.ForStep(
            descriptor.Kind == TelemetryQueryKind.Range ? range.Step : descriptor.Bounds.DefaultStep);
        var query = plan.Template.Render(selector, window);

        var response = await ExecuteAsync(descriptor, query, range, cancellationToken).ConfigureAwait(false);
        var (resultKind, series) = TelemetryResponseMapper.Map(response.Data);

        return new TelemetryQueryResponse
        {
            QueryId = descriptor.QueryId,
            Scope = scope,
            ResultKind = resultKind,
            Series = series,
            Range = range,
        };
    }

    private bool IsBackendConfigured => _options.BackendAddress is not null;

    /// <summary>
    /// Clamps the requested step into the entry's declared budget and validates the
    /// resulting window, returning the window that will actually be evaluated.
    /// </summary>
    private TelemetryTimeRange ResolveRange(
        TelemetryQueryDescriptor descriptor,
        TelemetryTimeRange requested)
    {
        var now = _time.GetUtcNow();

        // An instant entry evaluates at a single instant and ignores the start and
        // step entirely, so the window is normalised before the bounds see it and a
        // caller cannot smuggle a range past an instant entry's point budget.
        var range = descriptor.Kind == TelemetryQueryKind.Instant
            ? TelemetryTimeRange.At(requested.EndUtc == default ? now : requested.EndUtc)
            : NormalizeRange(descriptor, requested, now, _options.MaxRange);

        var violation = descriptor.Bounds.Validate(range, now);
        if (violation != TelemetryBoundsViolation.None)
        {
            throw new TelemetryQueryBoundsException(descriptor.QueryId, violation);
        }

        return range;
    }

    private static TelemetryTimeRange NormalizeRange(
        TelemetryQueryDescriptor descriptor,
        TelemetryTimeRange requested,
        DateTimeOffset now,
        TimeSpan deploymentMaxRange)
    {
        var end = requested.EndUtc == default ? now : requested.EndUtc;

        // The step is a bounded parameter, so it is clamped into the declared budget
        // rather than rejected: a caller asking for a finer resolution than the entry
        // permits gets the finest it permits. The window itself is never clamped.
        var step = descriptor.Accepts(TelemetryQueryParameters.Step)
            ? descriptor.Bounds.EffectiveStep(requested.Step)
            : descriptor.Bounds.EffectiveStep(TimeSpan.Zero);

        var start = requested.StartUtc == default
            ? end - DefaultSpan(descriptor.Bounds, step, deploymentMaxRange)
            : requested.StartUtc;

        return TelemetryTimeRange.Between(start, end, step);
    }

    /// <summary>
    /// The window a request that supplies no start is served over: the widest one
    /// that satisfies <em>every</em> bound the request will then be validated
    /// against - the entry's point budget at the resolved step, its maximum range,
    /// and the deployment-wide range guardrail.
    /// </summary>
    /// <remarks>
    /// Defaulting to the entry's maximum range alone would be self-defeating: at the
    /// entry's default step that window yields far more points than its own point
    /// budget permits, so the most natural request a binding can make - a query id
    /// and nothing else - would be rejected by the very bounds that produced it. The
    /// default must be consistent with the bounds that validate it.
    /// </remarks>
    private static TimeSpan DefaultSpan(
        TelemetryQueryBounds bounds,
        TimeSpan step,
        TimeSpan deploymentMaxRange)
    {
        var span = bounds.MaxPoints > 1 && step > TimeSpan.Zero
            ? step * (bounds.MaxPoints - 1)
            : DefaultUnboundedSpan;

        if (bounds.MaxRange > TimeSpan.Zero && span > bounds.MaxRange)
        {
            span = bounds.MaxRange;
        }

        return deploymentMaxRange > TimeSpan.Zero && span > deploymentMaxRange
            ? deploymentMaxRange
            : span;
    }

    /// <summary>
    /// Builds the label-matcher fragment: the tenant the facade pinned, plus the
    /// caller's tree filter when the entry declares one.
    /// </summary>
    private static TelemetryScopeSelector BuildSelector(
        TelemetryQueryDescriptor descriptor,
        TelemetryTenantScope scope,
        string? requestedTreeId)
    {
        string? escapedTree = null;

        // A tree filter is honoured only where the entry declares it; supplied
        // anywhere else it is ignored, and it can never widen the query because the
        // tenant matcher is rendered alongside it.
        if (descriptor.Accepts(TelemetryQueryParameters.TreeFilter)
            && !string.IsNullOrEmpty(requestedTreeId))
        {
            if (!PromQlLabelValue.IsRenderable(requestedTreeId))
            {
                throw new ArgumentException(
                    "The requested tree filter contains a control character, which is not a legal "
                    + "label value and cannot name a tree.",
                    nameof(requestedTreeId));
            }

            escapedTree = PromQlLabelValue.Escape(requestedTreeId);
        }

        return scope.TenantId is { } tenantId
            ? TelemetryScopeSelector.ForTenant(tenantId, escapedTree)
            : TelemetryScopeSelector.ForTree(escapedTree);
    }

    private async Task<PrometheusQueryResponse> ExecuteAsync(
        TelemetryQueryDescriptor descriptor,
        string query,
        TelemetryTimeRange range,
        CancellationToken cancellationToken)
    {
        PrometheusQueryResponse response;
        try
        {
            if (descriptor.Kind == TelemetryQueryKind.Range)
            {
                // The deployment-wide guardrails bound what any binding may ask of the
                // backend, over and above the entry's own bounds, so a host can cap a
                // catalogue it did not author.
                if (!TelemetryRangeGuardrails.TryValidateRange(
                        _options, range.StartUtc, range.EndUtc, range.Step, out var violationMessage))
                {
                    throw new TelemetryQueryBoundsException(
                        descriptor.QueryId,
                        HostGuardrailViolation(range),
                        violationMessage!);
                }

                response = await _backend
                    .RangeQueryAsync(query, range.StartUtc, range.EndUtc, range.Step, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                response = await _backend
                    .InstantQueryAsync(query, range.EndUtc, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is not TelemetryQueryBoundsException)
        {
            throw new TelemetryBackendException(
                descriptor.QueryId,
                $"The telemetry backend request for query '{descriptor.QueryId}' failed: {ex.Message}",
                ex);
        }

        if (!string.Equals(response.Status, "success", StringComparison.Ordinal))
        {
            throw new TelemetryBackendException(
                descriptor.QueryId,
                string.IsNullOrEmpty(response.Status)
                    ? $"The telemetry backend returned no status for query '{descriptor.QueryId}'."
                    : $"The telemetry backend reported status '{response.Status}' for query "
                        + $"'{descriptor.QueryId}'.");
        }

        return response;
    }

    /// <summary>
    /// Maps a deployment-guardrail rejection onto the typed violation the contract
    /// reports, applying the same fixed check order
    /// <see cref="TelemetryRangeGuardrails"/> uses so the typed reason always agrees
    /// with the message it travels with.
    /// </summary>
    private TelemetryBoundsViolation HostGuardrailViolation(TelemetryTimeRange range)
    {
        if (range.EndUtc < range.StartUtc)
        {
            return TelemetryBoundsViolation.RangeNotAscending;
        }

        if (range.Step <= TimeSpan.Zero)
        {
            return TelemetryBoundsViolation.StepBelowMinimum;
        }

        return range.Duration > _options.MaxRange
            ? TelemetryBoundsViolation.RangeTooLong
            : TelemetryBoundsViolation.StepAboveMaximum;
    }
}
