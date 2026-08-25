namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Configurable stub <see cref="ITenantContextResolver"/> for unit-testing the
/// active-tenant resolution seam without a real membership implementation. It
/// resolves a fixed <see cref="TenantId"/> and can model any of the three seam
/// behaviours the resolution boundary must handle: a synchronous fast-path
/// resolution, an async-only resolution (the synchronous
/// <see cref="ITenantContextResolver.TryResolveCurrent"/> fast path declines),
/// and a denial (resolving the uninitialised "no tenant" value).
/// </summary>
internal sealed class FakeTenantContextResolver : ITenantContextResolver
{
    private readonly TenantId _tenant;
    private readonly bool _resolvesSynchronously;

    /// <summary>
    /// Initializes a new instance of the <see cref="FakeTenantContextResolver"/>
    /// class.
    /// </summary>
    /// <param name="tenant">
    /// The tenant to resolve. Pass <c>default</c> to model a denial (a "no
    /// tenant" resolution).
    /// </param>
    /// <param name="resolvesSynchronously">
    /// <c>true</c> (the default) to resolve via the synchronous
    /// <see cref="ITenantContextResolver.TryResolveCurrent"/> fast path;
    /// <c>false</c> to decline the fast path so callers exercise
    /// <see cref="ITenantContextResolver.ResolveCurrentAsync"/>.
    /// </param>
    public FakeTenantContextResolver(TenantId tenant, bool resolvesSynchronously = true)
    {
        _tenant = tenant;
        _resolvesSynchronously = resolvesSynchronously;
    }

    /// <summary>Gets the number of times the async path was invoked.</summary>
    public int AsyncResolutionCount { get; private set; }

    /// <inheritdoc />
    public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        AsyncResolutionCount++;
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<TenantId>(_tenant);
    }

    /// <inheritdoc />
    public bool TryResolveCurrent(out TenantId tenant)
    {
        if (_resolvesSynchronously)
        {
            tenant = _tenant;
            return true;
        }

        tenant = default;
        return false;
    }
}
