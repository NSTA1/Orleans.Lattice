namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Derives the tenant scope a telemetry query is evaluated under from the
/// <b>authenticated caller</b>, and never from a request field. A caller may
/// <em>request</em> a wider or elsewhere visibility; this seam re-validates that
/// request server-side and pins the scope it actually applied.
/// </summary>
/// <remarks>
/// <para>
/// <b>The effective tenant is derived, not asserted.</b> The caller's active tenant
/// comes from <see cref="ITenantContextResolver"/> - the same seam the data facades
/// resolve an effective tree id through - so a request that names a tenant can
/// never widen what the query reads. A caller that cannot be attributed to any
/// tenant is refused rather than silently defaulted.
/// </para>
/// <para>
/// <b>Widening is honoured only after platform-operator validation.</b>
/// <see cref="TelemetryTenantVisibility.AllTenants"/> and
/// <see cref="TelemetryTenantVisibility.SingleTenant"/> are both honoured only for
/// a caller <see cref="TelemetryAccessAuthorizer.IsPlatformOperatorAsync"/>
/// validates against the cluster's platform-operator root of trust.
/// </para>
/// <para>
/// <b>An unvalidated widening degrades; it does not throw.</b> Matching the
/// Explorer's existing <c>ExplorerTenantVisibility</c> contract, a refused request
/// is served at the caller's own active tenant and the degradation is reported
/// through <see cref="TelemetryTenantScope.WasDowngraded"/>, so a client labels the
/// panel for the scope it got rather than the one it asked for. An honoured
/// single-tenant view is <em>not</em> a degradation and is reported through
/// <see cref="TelemetryTenantScope.AtRequestedTenant"/>, whose requested and
/// effective visibilities agree.
/// </para>
/// <para>
/// <b>Tenancy-absent deployments.</b> With no tenancy add-on registered the core
/// no-op resolver returns the reserved default tenant synchronously, so every query
/// scopes to <c>tenant="default"</c> with no await and no allocation, and the same
/// catalogue is served either way.
/// </para>
/// </remarks>
public sealed class TelemetryTenantScopeResolver
{
    private readonly ITenantContextResolver _tenants;
    private readonly TelemetryAccessAuthorizer _authorizer;

    /// <summary>
    /// Initializes the resolver.
    /// </summary>
    /// <param name="tenants">
    /// The active-tenant context seam. In a cluster without the tenancy add-on this
    /// is the core no-op resolver, which resolves the reserved default tenant.
    /// </param>
    /// <param name="authorizer">The seam that validates a caller as a platform operator.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="tenants"/> or <paramref name="authorizer"/> is <see langword="null"/>.
    /// </exception>
    public TelemetryTenantScopeResolver(
        ITenantContextResolver tenants,
        TelemetryAccessAuthorizer authorizer)
    {
        ArgumentNullException.ThrowIfNull(tenants);
        ArgumentNullException.ThrowIfNull(authorizer);

        _tenants = tenants;
        _authorizer = authorizer;
    }

    /// <summary>
    /// Resolves the scope to evaluate under, given what the caller requested.
    /// </summary>
    /// <param name="requestedVisibility">The visibility the caller asked for.</param>
    /// <param name="requestedTenantId">
    /// The tenant the caller asked for, meaningful only alongside
    /// <see cref="TelemetryTenantVisibility.SingleTenant"/> and ignored entirely for
    /// every other visibility and for every caller that does not validate as a
    /// platform operator.
    /// </param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The scope the query must be evaluated under.</returns>
    /// <exception cref="LatticeTenantAccessDeniedException">
    /// The caller could not be attributed to any tenant, so the query is refused
    /// rather than served at an arbitrary scope.
    /// </exception>
    public ValueTask<TelemetryTenantScope> ResolveAsync(
        TelemetryTenantVisibility requestedVisibility,
        string? requestedTenantId = null,
        CancellationToken cancellationToken = default)
    {
        // Warm path: the core no-op resolver (and any resolver holding the tenant in
        // ambient context) answers synchronously, so a tenancy-off cluster resolves
        // with no await and no allocation.
        if (_tenants.TryResolveCurrent(out var tenant))
        {
            var active = RequireTenantId(tenant);

            // The default visibility needs no validation, so the overwhelmingly
            // common request never consults the access gate at all.
            return requestedVisibility == TelemetryTenantVisibility.ActiveTenant
                ? new ValueTask<TelemetryTenantScope>(
                    TelemetryTenantScope.PinnedTo(active, TelemetryTenantVisibility.ActiveTenant))
                : ValidateWideningAsync(active, requestedVisibility, requestedTenantId, cancellationToken);
        }

        return ResolveSlowAsync(requestedVisibility, requestedTenantId, cancellationToken);
    }

    private async ValueTask<TelemetryTenantScope> ResolveSlowAsync(
        TelemetryTenantVisibility requestedVisibility,
        string? requestedTenantId,
        CancellationToken cancellationToken)
    {
        var tenant = await _tenants.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        var active = RequireTenantId(tenant);

        if (requestedVisibility == TelemetryTenantVisibility.ActiveTenant)
        {
            return TelemetryTenantScope.PinnedTo(active, TelemetryTenantVisibility.ActiveTenant);
        }

        return await ValidateWideningAsync(active, requestedVisibility, requestedTenantId, cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask<TelemetryTenantScope> ValidateWideningAsync(
        string activeTenantId,
        TelemetryTenantVisibility requestedVisibility,
        string? requestedTenantId,
        CancellationToken cancellationToken)
    {
        // Fail closed on an unrecognised visibility: anything that is not the
        // default is a widening request, and a value outside the contract's closed
        // set can never be honoured.
        if (requestedVisibility is not (TelemetryTenantVisibility.AllTenants
            or TelemetryTenantVisibility.SingleTenant))
        {
            return TelemetryTenantScope.PinnedTo(activeTenantId, requestedVisibility);
        }

        if (!await _authorizer.IsPlatformOperatorAsync(cancellationToken).ConfigureAwait(false))
        {
            // The fail-closed degradation: serve the caller's own tenant and report
            // the refusal rather than throwing, matching ExplorerTenantVisibility.
            // The requested tenant id is ignored in full.
            return TelemetryTenantScope.PinnedTo(activeTenantId, requestedVisibility);
        }

        if (requestedVisibility == TelemetryTenantVisibility.AllTenants)
        {
            return TelemetryTenantScope.AcrossAllTenants();
        }

        // An honoured single-tenant view is a genuine operator scope, not a
        // degradation, so it must never be reported as downgraded. An operator that
        // named no usable tenant still degrades, because there is nothing to pin to.
        return TryNormalizeTenantId(requestedTenantId, out var pinned)
            ? TelemetryTenantScope.AtRequestedTenant(pinned)
            : TelemetryTenantScope.PinnedTo(activeTenantId, requestedVisibility);
    }

    /// <summary>
    /// Validates a tenant id the facade is willing to embed in a label matcher: a
    /// syntactically valid tenant id, or the reserved platform sentinel so an
    /// operator can inspect platform-internal series directly instead of fetching
    /// every tenant and discarding the rest. Any other value is refused, so no
    /// unvalidated caller-supplied text ever reaches a query.
    /// </summary>
    private static bool TryNormalizeTenantId(string? candidate, out string normalized)
    {
        if (string.Equals(candidate, LatticeTenantLabel.PlatformTenant, StringComparison.Ordinal))
        {
            normalized = LatticeTenantLabel.PlatformTenant;
            return true;
        }

        if (TenantId.TryParse(candidate, out var tenant) && tenant.Value is { } value)
        {
            normalized = value;
            return true;
        }

        normalized = string.Empty;
        return false;
    }

    private static string RequireTenantId(TenantId tenant) =>
        // A resolver denies by resolving the uninitialised "no tenant" value; a
        // request that cannot be attributed to a tenant is refused, never defaulted.
        tenant.Value ?? throw new LatticeTenantAccessDeniedException();
}
