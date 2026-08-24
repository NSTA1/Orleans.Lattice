using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient active-tenant scope used to carry the caller's active
/// <see cref="TenantId"/> from the client edge down to the silo on the Orleans
/// <see cref="RequestContext"/>, following the same marker idiom as
/// <see cref="LatticeCredentialContext"/> and <see cref="LatticeOriginContext"/>.
/// The active tenant is the channel a tenant-aware choke point scopes an
/// operation by; nothing in the core library reads it (the core ships only the
/// no-op <see cref="NullTenantContextResolver"/>, which resolves the reserved
/// <see cref="TenantId.Default"/>), so an unset active tenant adds no cost and
/// changes no read/write semantics.
/// </summary>
/// <remarks>
/// <para>
/// The active tenant flows on every outgoing grain call via a
/// <see cref="RequestContext"/> entry keyed
/// <see cref="LatticeEventConstants.ActiveTenantRequestContextKey"/>. A caller
/// (in the real system, the auth / membership seam) stamps it at the boundary
/// of a logical operation with <c>using var _ = LatticeActiveTenantContext.With(tenant);</c>;
/// the marker clears when the scope disposes. When no scope is entered,
/// <see cref="Current"/> is <c>null</c> and <see cref="IsActive"/> is
/// <c>false</c> - a single dictionary lookup with no allocation.
/// </para>
/// <para>
/// The stored value is the Orleans-serializable <see cref="TenantId"/> value
/// type, so it round-trips across silo hops. The tenancy add-on's real
/// <see cref="ITenantContextResolver"/> reads this context and validates the
/// asserted tenant against the subject's membership set; the core no-op
/// resolver ignores it entirely.
/// </para>
/// </remarks>
public static class LatticeActiveTenantContext
{
    /// <summary>
    /// <c>true</c> when an active-tenant scope is currently set on the ambient
    /// <see cref="RequestContext"/>. Cheaper than reading <see cref="Current"/>
    /// because the result is a <c>bool</c> rather than a nullable
    /// <see cref="TenantId"/>; a tenant-aware choke point uses this to
    /// short-circuit on the (default) cold path so callers who never stamp an
    /// active tenant pay no extra cost.
    /// </summary>
    public static bool IsActive =>
        RequestContext.Get(LatticeEventConstants.ActiveTenantRequestContextKey) is TenantId;

    /// <summary>
    /// Gets or sets the active tenant on the ambient <see cref="RequestContext"/>.
    /// Setting <c>null</c> (or the uninitialised <c>default(TenantId)</c>
    /// "no tenant" value) removes the key rather than storing it, matching the
    /// "no active tenant" default.
    /// </summary>
    public static TenantId? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.ActiveTenantRequestContextKey);
            return raw is TenantId tenant ? tenant : null;
        }
        set
        {
            if (value is not { Value: not null } tenant)
            {
                RequestContext.Remove(LatticeEventConstants.ActiveTenantRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.ActiveTenantRequestContextKey, tenant);
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="tenant"/> for the lifetime
    /// of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    /// <param name="tenant">
    /// The active tenant to stamp onto calls authored inside the scope, or
    /// <c>null</c> to explicitly clear the ambient active tenant.
    /// </param>
    public static IDisposable With(TenantId? tenant)
    {
        var previous = Current;
        Current = tenant;
        return new Scope(previous);
    }

    private sealed class Scope(TenantId? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Current = previous;
        }
    }
}
