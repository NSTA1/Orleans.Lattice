namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Test double for <see cref="ITenantContextResolver"/> that resolves the tenant
/// stamped on the ambient <see cref="LatticeActiveTenantContext"/>, mirroring the
/// shape of the real tenancy add-on's resolver without pulling the add-on (and
/// its membership / policy dependencies) into this package's unit tests.
/// </summary>
/// <remarks>
/// <para>
/// Reading the SAME ambient context that <c>LatticeTreeAdmin.ThrowIfReserved</c>
/// reads is the point: it is what makes a composed <c>t/{tenant}/{name}</c> id
/// pass that guard, and a test double that invented a tenant from thin air would
/// not exercise that coupling.
/// </para>
/// <para>
/// The three constructor switches cover the three behaviours a facade must
/// survive: the synchronous warm path (the default, and what both the core no-op
/// resolver and the real resolver take when a tenant assertion is already
/// validated), the asynchronous fallback (a membership cache miss), and a
/// fail-closed denial (an assertion the caller may not make), which resolves the
/// uninitialised "no tenant" value the core turns into a
/// <see cref="LatticeTenantAccessDeniedException"/>.
/// </para>
/// </remarks>
internal sealed class AmbientTenantContextResolver(
    bool resolveSynchronously = true,
    bool deny = false) : ITenantContextResolver
{
    /// <summary>The number of times the synchronous fast path resolved a tenant.</summary>
    public int SynchronousResolutions { get; private set; }

    /// <summary>The number of times the asynchronous fallback resolved a tenant.</summary>
    public int AsynchronousResolutions { get; private set; }

    /// <summary>The total number of resolutions over either path.</summary>
    public int Resolutions => SynchronousResolutions + AsynchronousResolutions;

    /// <inheritdoc />
    public bool TryResolveCurrent(out TenantId tenant)
    {
        if (!resolveSynchronously)
        {
            tenant = default;
            return false;
        }

        SynchronousResolutions++;
        tenant = Resolve();
        return true;
    }

    /// <inheritdoc />
    public async ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        // Yield so the caller genuinely suspends, proving the facade's slow path
        // is correct rather than only ever exercising a completed ValueTask.
        await Task.Yield();
        AsynchronousResolutions++;
        return Resolve();
    }

    private TenantId Resolve()
    {
        if (deny)
        {
            // The fail-closed contract: an unattributable request resolves the
            // uninitialised "no tenant" value, never a silent default.
            return default;
        }

        return LatticeActiveTenantContext.Current is { Value: not null } tenant
            ? tenant
            : TenantId.Default;
    }
}
