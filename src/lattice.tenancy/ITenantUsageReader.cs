namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Reads one named tenant's usage-against-quota reading off the warm per-tenant
/// usage index, under the <see cref="TenantEnforcementScope"/> that governs that
/// tenant's admission. The plumbing seam a control facade projects a tenant's
/// quota panel from, so the facade does not need the tenancy engine's internal
/// admission index.
/// </summary>
/// <remarks>
/// <para>
/// <b>This seam performs no visibility check.</b> It is a projection over engine
/// state keyed by tenant id, exactly as <see cref="ITenantRegistry"/> is, and it
/// is therefore <b>not</b> a substitute for <see cref="ITenantObservabilityView"/>,
/// which is the fail-closed read surface for a caller reading its own series.
/// Every consumer must authorize the caller against the requested tenant at its
/// own single narrowest seam <em>before</em> calling this - the control facade
/// does so with its two-tier operator-or-tenant-admin authorizer - and must never
/// expose it unauthorized.
/// </para>
/// <para>
/// The read is off the write-admission hot path but is sampled frequently by a
/// quota panel, so it is a bounded, allocation-light lookup: a warm dictionary
/// probe plus one durable metered-overage read, returning a value type.
/// </para>
/// </remarks>
public interface ITenantUsageReader
{
    /// <summary>
    /// Resolves the <see cref="TenantEnforcementScope"/> that governs
    /// <paramref name="tenant"/>'s quota admission, so a consumer can qualify a
    /// reading even when the tenant has no usage view yet. A pure, allocation-free
    /// lookup.
    /// </summary>
    /// <param name="tenant">The tenant whose scope is being resolved.</param>
    /// <returns>The enforcement scope for the tenant.</returns>
    TenantEnforcementScope ResolveScope(TenantId tenant);

    /// <summary>
    /// Reads <paramref name="tenant"/>'s usage-against-quota reading, or
    /// <see langword="null"/> when the tenant has no view in the warm usage index
    /// (it is unregistered, or the index has not yet compiled it). The reading's
    /// usage aggregate is the one the tenant's enforcement scope admits against,
    /// and the scope is reported alongside it.
    /// </summary>
    /// <param name="tenant">The tenant to read. An uninitialised tenant id yields <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tenant's reading, or <see langword="null"/> when it has no usage view.</returns>
    Task<TenantUsageReading?> ReadAsync(TenantId tenant, CancellationToken cancellationToken = default);
}
