namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus;

/// <summary>
/// An <see cref="ITenantContextResolver"/> that resolves a fixed tenant
/// synchronously, or the uninitialised "no tenant" value to model a resolver
/// that denies the caller.
/// </summary>
internal sealed class FixedTenantContextResolver(TenantId tenant) : ITenantContextResolver
{
    /// <summary>A resolver that denies every caller (resolves no tenant).</summary>
    public static FixedTenantContextResolver Denying { get; } = new(default);

    /// <summary>A resolver for the named tenant.</summary>
    /// <param name="tenant">The tenant id.</param>
    /// <returns>The resolver.</returns>
    public static FixedTenantContextResolver For(string tenant) => new(TenantId.Parse(tenant));

    /// <inheritdoc />
    public bool TryResolveCurrent(out TenantId resolved)
    {
        resolved = tenant;
        return true;
    }

    /// <inheritdoc />
    public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) => new(tenant);
}
