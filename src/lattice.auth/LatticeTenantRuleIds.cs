namespace Orleans.Lattice.Auth;

/// <summary>
/// The reserved rule-id namespace for <b>tenant-tier</b> authorization rules: the
/// rules a tenant's administrators author over their own tenant's trees through
/// the tenant policy administration surface. Every tenant-tier rule id has the
/// form <c>{Prefix}{tenant}:{localId}</c>, which encodes both the tier and the
/// owning tenant in the id itself with no change to the
/// <see cref="LatticeAuthorizationRule"/> shape. Operators author their own rules
/// with ids outside this prefix; a direct write or delete of a tenant-tier rule
/// id outside a system-origin scope is rejected by the policy store with a
/// <see cref="LatticeTenantOwnedRuleException"/>.
/// </summary>
/// <remarks>
/// The reserved <see cref="TenantId.Default"/> tenant (the legacy-adoption
/// tenant) has no tenant-tier rules, so <see cref="For"/> refuses it and
/// <see cref="TryGetTenant"/> does not report it as an owner.
/// </remarks>
public static class LatticeTenantRuleIds
{
    /// <summary>
    /// The prefix every tenant-tier authorization rule id starts with. A rule id
    /// starting with it is reserved to the tenant tier however the remainder is
    /// formed, so ownership is decided by this prefix alone.
    /// </summary>
    public const string Prefix = "tenant:";

    /// <summary>
    /// Composes the tenant-tier rule id <c>tenant:{tenant}:{localId}</c> for a rule
    /// owned by <paramref name="tenant"/>.
    /// </summary>
    /// <param name="tenant">The owning tenant. Must be an initialised tenant other than <see cref="TenantId.Default"/>.</param>
    /// <param name="localId">The tenant-local rule id. Must not be <c>null</c> or empty.</param>
    /// <returns>The composed tenant-tier rule id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="localId"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="localId"/> is empty, or <paramref name="tenant"/> is the
    /// uninitialised "no tenant" value or the reserved <see cref="TenantId.Default"/> tenant.
    /// </exception>
    public static string For(TenantId tenant, string localId)
    {
        ArgumentException.ThrowIfNullOrEmpty(localId);

        if (tenant.Value is null || !TenantId.IsValid(tenant.Value.AsSpan()))
        {
            throw new ArgumentException(
                "Cannot compose a tenant-tier rule id from the uninitialised 'no tenant' value.",
                nameof(tenant));
        }

        if (tenant.IsDefault)
        {
            throw new ArgumentException(
                $"The reserved '{TenantId.DefaultId}' tenant has no tenant-tier rules; its access is operator-administered.",
                nameof(tenant));
        }

        return string.Concat(Prefix, tenant.Value, ":", localId);
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="ruleId"/> is in the tenant-tier
    /// rule-id namespace, that is when it starts with <see cref="Prefix"/> under an
    /// ordinal comparison. Allocation-free.
    /// </summary>
    /// <param name="ruleId">The candidate rule id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> if the id is tenant-owned; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="ruleId"/> is <c>null</c>.</exception>
    public static bool IsTenantOwned(string ruleId)
    {
        ArgumentNullException.ThrowIfNull(ruleId);
        return ruleId.StartsWith(Prefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Extracts the owning tenant from a well-formed tenant-tier rule id
    /// (<c>tenant:{tenant}:{localId}</c> with a valid, non-default tenant and a
    /// non-empty local id). Allocation-free unless an owner is found (then one
    /// string for the tenant id is materialised).
    /// </summary>
    /// <param name="ruleId">The candidate rule id. Must not be <c>null</c>.</param>
    /// <param name="tenant">The owning tenant when this returns <c>true</c>; otherwise <c>default</c>.</param>
    /// <returns><c>true</c> when <paramref name="ruleId"/> is a well-formed tenant-tier rule id; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="ruleId"/> is <c>null</c>.</exception>
    public static bool TryGetTenant(string ruleId, out TenantId tenant)
    {
        ArgumentNullException.ThrowIfNull(ruleId);

        var span = ruleId.AsSpan();
        if (span.StartsWith(Prefix, StringComparison.Ordinal))
        {
            var rest = span[Prefix.Length..];
            var colon = rest.IndexOf(':');
            if (colon > 0 && colon < rest.Length - 1)
            {
                var id = rest[..colon];
                if (TenantId.IsValid(id) && !id.Equals(TenantId.DefaultId, StringComparison.Ordinal))
                {
                    tenant = TenantId.ForValidated(new string(id));
                    return true;
                }
            }
        }

        tenant = default;
        return false;
    }
}
