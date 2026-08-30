namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// A scripted <see cref="ITenantContextResolver"/>: resolves a fixed tenant, either
/// synchronously (the warm path the core no-op resolver takes) or only through the
/// asynchronous path, so both branches of the scope resolver are exercised.
/// </summary>
internal sealed class StubTenantContextResolver(TenantId tenant, bool resolvesSynchronously = true)
    : ITenantContextResolver
{
    /// <summary>How many times the asynchronous path was taken.</summary>
    public int AsyncResolutions { get; private set; }

    /// <inheritdoc />
    public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        AsyncResolutions++;
        return new ValueTask<TenantId>(tenant);
    }

    /// <inheritdoc />
    public bool TryResolveCurrent(out TenantId resolved)
    {
        if (!resolvesSynchronously)
        {
            resolved = default;
            return false;
        }

        resolved = tenant;
        return true;
    }
}
