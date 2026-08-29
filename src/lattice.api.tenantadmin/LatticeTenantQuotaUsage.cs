using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The in-process implementation of the transport-agnostic read-only
/// <see cref="ILatticeTenantQuotaUsage"/> usage-against-quota facade. It is the
/// single narrowest seam at which a caller is authorized to read a tenant's
/// consumption, and the only place the tenancy engine's per-tenant usage index is
/// projected onto the control-API contract. It is an append-only sibling of
/// <see cref="LatticeTenantAdmin"/>, <see cref="LatticeTenantRegionAdmin"/>, and
/// <see cref="LatticeTenantSelfService"/> and shares their conventions, so those
/// surfaces are unchanged.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two-tier, fail-closed authorization.</b> The read is a <i>tenant-admin</i>
/// tier action, so it reuses
/// <see cref="TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync(TenantId, CancellationToken)"/>
/// - a platform operator or a live admin subject on the tenant record - which is
/// independent of the data-plane default effect. It deliberately does <b>not</b>
/// use the identically-named
/// <see cref="TenantAdminAccessAuthorizer.AuthorizeTenantAdminAsync(CancellationToken)"/>,
/// which despite its name is platform-operator-only and would lock a tenant admin
/// out of its own usage.
/// </para>
/// <para>
/// <b>No existence probe.</b> The authorizer distinguishes its refusals (an
/// operator learns a tenant is absent; a non-operator is denied), but this facade
/// deliberately collapses both onto the same
/// <see cref="TenantNotFoundException"/> the sibling
/// <see cref="ILatticeTenantSelfService.GetTenantAsync"/> raises, so a caller can
/// never distinguish "no such tenant" from "not yours" and use the read as an
/// existence oracle.
/// </para>
/// <para>
/// <b>Allocation.</b> The quota surface is polled by a panel, so the read is kept
/// lean: one authorizer pass (which also returns the tenant record, so the
/// registry is read once), one warm-index probe plus one metered-overage read,
/// and a single report record - the five per-dimension figures are value types
/// written inline, so nothing is allocated per dimension.
/// </para>
/// </remarks>
internal sealed class LatticeTenantQuotaUsage : ILatticeTenantQuotaUsage
{
    private readonly TenantRegionResidencyAuthorizer _authorizer;
    private readonly ITenantUsageReader _usageReader;

    /// <summary>
    /// Initializes a new <see cref="LatticeTenantQuotaUsage"/>.
    /// </summary>
    /// <param name="authorizer">The two-tier fail-closed operator-or-tenant-admin authorization seam. Must not be <c>null</c>.</param>
    /// <param name="usageReader">The tenancy engine's by-id usage-reading seam. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public LatticeTenantQuotaUsage(
        TenantRegionResidencyAuthorizer authorizer,
        ITenantUsageReader usageReader)
    {
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(usageReader);

        _authorizer = authorizer;
        _usageReader = usageReader;
    }

    /// <inheritdoc />
    public async Task<TenantQuotaUsageReport> GetQuotaUsageAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);

        TenantRecord record;
        try
        {
            record = await _authorizer
                .AuthorizeTenantAdminAsync(tenant, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            // Unify "not authorized to read it" with "no such tenant" so a caller
            // can never probe for the existence of a tenant outside its authority
            // by telling the two refusals apart. The operator tier already reports
            // an absent tenant as not-found, so both paths now agree.
            throw new TenantNotFoundException(tenant.Value);
        }

        var reading = await _usageReader.ReadAsync(tenant, cancellationToken).ConfigureAwait(false);
        if (reading is { } present)
        {
            return TenantQuotaUsageMapping.ToReport(tenant, present);
        }

        // A registered tenant whose warm usage view has not compiled yet still
        // gets its authoritative ceilings; only the consumption is reported
        // unmeasured, and the scope is resolved directly so the report is never
        // silently qualified as global.
        return TenantQuotaUsageMapping.ToUnmeasuredReport(
            tenant, record.Quotas, _usageReader.ResolveScope(tenant));
    }

    private static TenantId ParseTenant(string tenantId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        if (!TenantId.TryParse(tenantId, out var tenant))
        {
            throw new ArgumentException(
                $"'{tenantId}' is not a valid tenant id.", nameof(tenantId));
        }

        return tenant;
    }
}
